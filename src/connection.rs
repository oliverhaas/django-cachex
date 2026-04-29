// Connection layer for the Rust I/O driver.
//
// Verbatim port from django-vcache (MIT, by David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache/-/blob/main/src/connection.rs
// Keep in lockstep with upstream — do not diverge without discussion.

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::caching::{CacheConfig, CacheStatistics};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{AsyncCommands, Client, RedisResult, TlsCertificates};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Optional client-side caching parameters (passed from Python).
#[derive(Clone, Debug)]
pub struct ClientCacheOpts {
    pub max_size: usize,
    pub ttl_secs: u64,
}

/// TLS certificate options (PEM bytes, read from files by caller).
#[derive(Clone)]
pub struct TlsOpts {
    pub root_cert: Option<Vec<u8>>,
    pub client_cert: Option<Vec<u8>>,
    pub client_key: Option<Vec<u8>>,
}

impl TlsOpts {
    fn to_tls_certs(&self) -> TlsCertificates {
        let client_tls = match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) => Some(redis::ClientTlsConfig {
                client_cert: cert.clone(),
                client_key: key.clone(),
            }),
            _ => None,
        };
        TlsCertificates {
            client_tls,
            root_cert: self.root_cert.clone(),
        }
    }
}

/// Create a redis::Client, using `build_with_tls` when TLS opts are provided.
fn create_client(url: &str, tls_opts: Option<&TlsOpts>) -> redis::RedisResult<Client> {
    match tls_opts {
        Some(opts) => Client::build_with_tls(url, opts.to_tls_certs()),
        None => Client::open(url),
    }
}

fn conn_manager_config(cache: Option<&ClientCacheOpts>) -> ConnectionManagerConfig {
    let mut cfg = ConnectionManagerConfig::new()
        .set_pipeline_buffer_size(1000)
        .set_response_timeout(Some(Duration::from_secs(30)));
    if let Some(opts) = cache {
        let cc = CacheConfig::new()
            .set_size(NonZeroUsize::new(opts.max_size).unwrap_or(NonZeroUsize::MIN))
            .set_default_client_ttl(Duration::from_secs(opts.ttl_secs));
        cfg = cfg.set_cache_config(cc);
    }
    cfg
}

/// Ensure the URL uses RESP3 protocol.
fn url_with_resp3(url: &str) -> String {
    if url.contains("protocol=") {
        return url.to_string();
    }
    // Handle fragment (#...) — query params must come before it.
    let (base, fragment) = match url.split_once('#') {
        Some((b, f)) => (b, Some(f)),
        None => (url, None),
    };
    let sep = if base.contains('?') { '&' } else { '?' };
    match fragment {
        Some(f) => format!("{base}{sep}protocol=resp3#{f}"),
        None => format!("{base}{sep}protocol=resp3"),
    }
}

/// Config for blocking operations — no response timeout since BLMOVE/BLMPOP
/// intentionally wait for data (possibly minutes). Never has caching.
fn blocking_conn_manager_config() -> ConnectionManagerConfig {
    ConnectionManagerConfig::new()
        .set_pipeline_buffer_size(1000)
        .set_response_timeout(None)
}

/// Sentinel-aware connection that re-discovers master on failover.
#[derive(Clone)]
pub struct SentinelConn {
    inner: Arc<RwLock<ConnectionManager>>,
    sentinel_urls: Arc<[String]>,
    service_name: Arc<str>,
    db: i64,
    is_blocking: bool,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
}

impl SentinelConn {
    fn conn_config(&self) -> ConnectionManagerConfig {
        if self.is_blocking {
            blocking_conn_manager_config()
        } else {
            conn_manager_config(self.cache_opts.as_ref())
        }
    }

    pub async fn get_conn(&self) -> ConnectionManager {
        self.inner.read().await.clone()
    }

    fn is_failover_error(e: &redis::RedisError) -> bool {
        matches!(
            e.kind(),
            redis::ErrorKind::Io
                | redis::ErrorKind::Server(redis::ServerErrorKind::BusyLoading)
                | redis::ErrorKind::Server(redis::ServerErrorKind::TryAgain)
                | redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly)
        ) || e.is_connection_dropped()
    }

    pub async fn rediscover(&self) -> RedisResult<()> {
        for sentinel_url in self.sentinel_urls.iter() {
            let client = match create_client(sentinel_url.as_str(), self.tls_opts.as_ref()) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let mut conn =
                match ConnectionManager::new_with_config(client, conn_manager_config(None)).await {
                    Ok(c) => c,
                    Err(_) => continue,
                };
            let result: RedisResult<Vec<String>> = redis::cmd("SENTINEL")
                .arg("get-master-addr-by-name")
                .arg(&*self.service_name)
                .query_async(&mut conn)
                .await;

            if let Ok(addr) = result {
                if addr.len() == 2 {
                    let scheme = if self.tls_opts.is_some() {
                        "rediss"
                    } else {
                        "redis"
                    };
                    let base_url =
                        format!("{scheme}://{}:{}/{}", addr[0], addr[1], self.db);
                    let master_url = url_with_resp3(&base_url);
                    let client =
                        create_client(master_url.as_str(), self.tls_opts.as_ref())?;
                    let new_mgr =
                        ConnectionManager::new_with_config(client, self.conn_config()).await?;
                    let mut guard = self.inner.write().await;
                    *guard = new_mgr;
                    return Ok(());
                }
            }
        }
        Err(redis::RedisError::from((
            redis::ErrorKind::Io,
            "Failed to rediscover master from any sentinel",
        )))
    }
}

/// Inner connection enum — one per connection type.
/// All methods for individual Redis commands live here.
#[derive(Clone)]
enum ValkeyConnInner {
    Standard(ConnectionManager),
    Cluster(ClusterConnection),
    Sentinel(SentinelConn),
}

// Macro to dispatch a redis command to the correct connection variant.
// For sentinel, retries once on failover error after re-discovering master.
macro_rules! dispatch_cmd {
    ($self:expr, $cmd:expr) => {
        match $self {
            ValkeyConnInner::Standard(c) => $cmd.query_async(c).await,
            ValkeyConnInner::Cluster(c) => $cmd.query_async(c).await,
            ValkeyConnInner::Sentinel(s) => {
                let cmd_retry = $cmd.clone();
                let mut c = s.get_conn().await;
                match $cmd.query_async(&mut c).await {
                    Ok(v) => Ok(v),
                    Err(e) if SentinelConn::is_failover_error(&e) => {
                        s.rediscover().await?;
                        let mut c = s.get_conn().await;
                        cmd_retry.query_async(&mut c).await
                    }
                    Err(e) => Err(e),
                }
            }
        }
    };
}

// Macro for sentinel retry on AsyncCommands methods.
macro_rules! sentinel_retry {
    ($s:expr, $c:ident, $op:expr) => {{
        let mut $c = $s.get_conn().await;
        match $op {
            Ok(v) => Ok(v),
            Err(e) if SentinelConn::is_failover_error(&e) => {
                $s.rediscover().await?;
                let mut $c = $s.get_conn().await;
                $op
            }
            Err(e) => Err(e),
        }
    }};
}

// Macro for methods where all three variants use the same AsyncCommands call.
macro_rules! conn_method {
    ($self:expr, $c:ident, $op:expr) => {
        match $self {
            ValkeyConnInner::Standard($c) => $op,
            ValkeyConnInner::Cluster($c) => $op,
            ValkeyConnInner::Sentinel(s) => sentinel_retry!(s, $c, $op),
        }
    };
}

impl ValkeyConnInner {
    pub async fn get_bytes(&mut self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        conn_method!(self, c, c.get(key).await)
    }

    pub async fn set_bytes(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> RedisResult<()> {
        match ttl {
            Some(t) => conn_method!(self, c, c.set_ex(key, value.as_slice(), t).await),
            None => conn_method!(self, c, c.set(key, value.as_slice()).await),
        }
    }

    pub async fn set_nx(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> RedisResult<bool> {
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(value).arg("NX");
        if let Some(t) = ttl {
            cmd.arg("EX").arg(t);
        }
        let result: Option<String> = dispatch_cmd!(self, cmd)?;
        Ok(result.is_some())
    }

    pub async fn del(&mut self, key: &str) -> RedisResult<i64> {
        conn_method!(self, c, c.del(key).await)
    }

    pub async fn del_many(&mut self, keys: &[String]) -> RedisResult<i64> {
        match self {
            Self::Cluster(c) => {
                let mut total: i64 = 0;
                for key in keys {
                    let n: i64 = c.del(key.as_str()).await?;
                    total += n;
                }
                Ok(total)
            }
            _ => {
                let mut cmd = redis::cmd("DEL");
                for key in keys {
                    cmd.arg(key.as_str());
                }
                dispatch_cmd!(self, cmd)
            }
        }
    }

    pub async fn exists(&mut self, key: &str) -> RedisResult<bool> {
        conn_method!(self, c, c.exists(key).await)
    }

    pub async fn expire(&mut self, key: &str, seconds: u64) -> RedisResult<bool> {
        conn_method!(self, c, c.expire(key, seconds as i64).await)
    }

    pub async fn persist(&mut self, key: &str) -> RedisResult<bool> {
        conn_method!(self, c, c.persist(key).await)
    }

    pub async fn mget_bytes(&mut self, keys: &[String]) -> RedisResult<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Cluster(c) => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let r: Option<Vec<u8>> = c.get(key.as_str()).await?;
                    results.push(r);
                }
                Ok(results)
            }
            _ => {
                let mut cmd = redis::cmd("MGET");
                cmd.arg(keys);
                dispatch_cmd!(self, cmd)
            }
        }
    }

    pub async fn pipeline_set(
        &mut self,
        entries: &[(String, Vec<u8>)],
        ttl: Option<u64>,
    ) -> RedisResult<()> {
        match self {
            Self::Standard(c) => {
                let mut pipe = redis::pipe();
                for (key, value) in entries {
                    match ttl {
                        Some(t) => {
                            pipe.set_ex(key.as_str(), value.as_slice(), t);
                        }
                        None => {
                            pipe.set(key.as_str(), value.as_slice());
                        }
                    }
                }
                pipe.query_async::<()>(c).await
            }
            Self::Cluster(c) => {
                for (key, value) in entries {
                    match ttl {
                        Some(t) => {
                            c.set_ex::<_, _, ()>(key.as_str(), value.as_slice(), t)
                                .await?;
                        }
                        None => {
                            c.set::<_, _, ()>(key.as_str(), value.as_slice()).await?;
                        }
                    }
                }
                Ok(())
            }
            Self::Sentinel(s) => {
                let build_pipe = || {
                    let mut pipe = redis::pipe();
                    for (key, value) in entries {
                        match ttl {
                            Some(t) => {
                                pipe.set_ex(key.as_str(), value.as_slice(), t);
                            }
                            None => {
                                pipe.set(key.as_str(), value.as_slice());
                            }
                        }
                    }
                    pipe
                };
                let mut c = s.get_conn().await;
                match build_pipe().query_async::<()>(&mut c).await {
                    Ok(()) => Ok(()),
                    Err(e) if SentinelConn::is_failover_error(&e) => {
                        s.rediscover().await?;
                        let mut c = s.get_conn().await;
                        build_pipe().query_async::<()>(&mut c).await
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    pub async fn flushdb(&mut self) -> RedisResult<()> {
        dispatch_cmd!(self, redis::cmd("FLUSHDB"))
    }

    pub async fn incr_by(&mut self, key: &str, delta: i64) -> RedisResult<i64> {
        conn_method!(self, c, c.incr(key, delta).await)
    }

    // Lock operations

    pub async fn lock_acquire(
        &mut self,
        key: &str,
        token: &str,
        timeout_ms: Option<u64>,
    ) -> RedisResult<bool> {
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(token).arg("NX");
        if let Some(ms) = timeout_ms {
            cmd.arg("PX").arg(ms);
        }
        let result: Option<String> = dispatch_cmd!(self, cmd)?;
        Ok(result.is_some())
    }

    pub async fn lock_release(&mut self, key: &str, token: &str) -> RedisResult<i64> {
        let script = r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        "#;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(script).arg(1).arg(key).arg(token);
        dispatch_cmd!(self, cmd)
    }

    pub async fn lock_extend(
        &mut self,
        key: &str,
        token: &str,
        additional_ms: u64,
    ) -> RedisResult<i64> {
        let script = r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("pexpire", KEYS[1], ARGV[2])
            else
                return 0
            end
        "#;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(script)
            .arg(1)
            .arg(key)
            .arg(token)
            .arg(additional_ms);
        dispatch_cmd!(self, cmd)
    }

    // Eval

    pub async fn eval(
        &mut self,
        script: &str,
        keys: &[String],
        args: &[Vec<u8>],
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(script).arg(keys.len());
        for k in keys {
            cmd.arg(k.as_str());
        }
        for a in args {
            cmd.arg(a.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn evalsha(
        &mut self,
        sha: &str,
        keys: &[String],
        args: &[Vec<u8>],
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("EVALSHA");
        cmd.arg(sha).arg(keys.len());
        for k in keys {
            cmd.arg(k.as_str());
        }
        for a in args {
            cmd.arg(a.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn script_load(&mut self, script: &str) -> RedisResult<String> {
        let mut cmd = redis::cmd("SCRIPT");
        cmd.arg("LOAD").arg(script);
        dispatch_cmd!(self, cmd)
    }

    /// Execute a pipeline of arbitrary commands.
    pub async fn pipeline_exec(
        &mut self,
        commands: Vec<(String, Vec<Vec<u8>>)>,
    ) -> RedisResult<Vec<redis::Value>> {
        match self {
            Self::Standard(c) => {
                let mut pipe = redis::pipe();
                for (cmd_name, args) in &commands {
                    let mut cmd = redis::cmd(cmd_name);
                    for a in args {
                        cmd.arg(a.as_slice());
                    }
                    pipe.add_command(cmd);
                }
                pipe.query_async(c).await
            }
            Self::Cluster(_) => {
                let mut results = Vec::with_capacity(commands.len());
                for (cmd_name, args) in &commands {
                    let mut cmd = redis::cmd(cmd_name);
                    for a in args {
                        cmd.arg(a.as_slice());
                    }
                    let val: redis::Value = dispatch_cmd!(self, cmd)?;
                    results.push(val);
                }
                Ok(results)
            }
            Self::Sentinel(s) => {
                let build_pipe = || {
                    let mut pipe = redis::pipe();
                    for (cmd_name, args) in &commands {
                        let mut cmd = redis::cmd(cmd_name);
                        for a in args {
                            cmd.arg(a.as_slice());
                        }
                        pipe.add_command(cmd);
                    }
                    pipe
                };
                let mut c = s.get_conn().await;
                match build_pipe().query_async(&mut c).await {
                    Ok(v) => Ok(v),
                    Err(e) if SentinelConn::is_failover_error(&e) => {
                        s.rediscover().await?;
                        let mut c = s.get_conn().await;
                        build_pipe().query_async(&mut c).await
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    // List operations

    pub async fn lpush(&mut self, key: &str, values: Vec<Vec<u8>>) -> RedisResult<i64> {
        let mut cmd = redis::cmd("LPUSH");
        cmd.arg(key);
        for v in &values {
            cmd.arg(v.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn rpush(&mut self, key: &str, values: Vec<Vec<u8>>) -> RedisResult<i64> {
        let mut cmd = redis::cmd("RPUSH");
        cmd.arg(key);
        for v in &values {
            cmd.arg(v.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn rpop(&mut self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        conn_method!(self, c, c.rpop(key, None).await)
    }

    pub async fn lrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("LRANGE");
        cmd.arg(key).arg(start).arg(stop);
        dispatch_cmd!(self, cmd)
    }

    pub async fn lrem(&mut self, key: &str, count: i64, value: &[u8]) -> RedisResult<i64> {
        let mut cmd = redis::cmd("LREM");
        cmd.arg(key).arg(count).arg(value);
        dispatch_cmd!(self, cmd)
    }

    pub async fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> RedisResult<()> {
        let mut cmd = redis::cmd("LTRIM");
        cmd.arg(key).arg(start).arg(stop);
        dispatch_cmd!(self, cmd)
    }

    pub async fn llen(&mut self, key: &str) -> RedisResult<i64> {
        conn_method!(self, c, c.llen(key).await)
    }

    pub async fn blmove(
        &mut self,
        source: &str,
        destination: &str,
        wherefrom: &str,
        whereto: &str,
        timeout: f64,
    ) -> RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("BLMOVE");
        cmd.arg(source)
            .arg(destination)
            .arg(wherefrom)
            .arg(whereto)
            .arg(timeout);
        dispatch_cmd!(self, cmd)
    }

    pub async fn blmpop(
        &mut self,
        timeout: f64,
        keys: &[String],
        direction: &str,
        count: i64,
    ) -> RedisResult<Option<(String, Vec<Vec<u8>>)>> {
        let mut cmd = redis::cmd("BLMPOP");
        cmd.arg(timeout).arg(keys.len());
        for k in keys {
            cmd.arg(k.as_str());
        }
        cmd.arg(direction);
        cmd.arg("COUNT").arg(count);
        let val: redis::Value = dispatch_cmd!(self, cmd)?;
        match val {
            redis::Value::Nil => Ok(None),
            redis::Value::Array(mut items) if items.len() == 2 => {
                let elems_val = items.pop().unwrap();
                let key_val = items.pop().unwrap();
                let key: String = redis::from_redis_value(key_val)?;
                let elements: Vec<Vec<u8>> = redis::from_redis_value(elems_val)?;
                Ok(Some((key, elements)))
            }
            _ => Ok(None),
        }
    }

    // Scan

    pub async fn scan_all(&mut self, pattern: &str, count: i64) -> RedisResult<Vec<String>> {
        match self {
            Self::Cluster(_) => {
                let mut cmd = redis::cmd("KEYS");
                cmd.arg(pattern);
                dispatch_cmd!(self, cmd)
            }
            Self::Standard(c) => {
                let mut all_keys = Vec::new();
                let mut cursor: u64 = 0;
                loop {
                    let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(pattern)
                        .arg("COUNT")
                        .arg(count)
                        .query_async(c)
                        .await?;
                    all_keys.extend(keys);
                    if new_cursor == 0 {
                        break;
                    }
                    cursor = new_cursor;
                }
                Ok(all_keys)
            }
            Self::Sentinel(s) => {
                let mut all_keys = Vec::new();
                let mut cursor: u64 = 0;
                let mut c = s.get_conn().await;
                loop {
                    let result: RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(pattern)
                        .arg("COUNT")
                        .arg(count)
                        .query_async(&mut c)
                        .await;
                    match result {
                        Ok((new_cursor, keys)) => {
                            all_keys.extend(keys);
                            if new_cursor == 0 {
                                break;
                            }
                            cursor = new_cursor;
                        }
                        Err(e) if cursor == 0 && SentinelConn::is_failover_error(&e) => {
                            s.rediscover().await?;
                            c = s.get_conn().await;
                            all_keys.clear();
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(all_keys)
            }
        }
    }
}

// =========================================================================
// Connection config — stored for lazy blocking connection creation
// =========================================================================

#[derive(Clone)]
enum ConnConfig {
    Standard {
        url: Arc<str>,
        tls_opts: Option<TlsOpts>,
    },
    Cluster {
        urls: Arc<[String]>,
        tls_opts: Option<TlsOpts>,
    },
    Sentinel {
        sentinel_urls: Arc<[String]>,
        service_name: Arc<str>,
        db: i64,
        tls_opts: Option<TlsOpts>,
    },
}

impl ValkeyConnInner {
    fn cache_statistics(&self) -> Option<CacheStatistics> {
        match self {
            Self::Standard(c) => c.get_cache_statistics(),
            Self::Cluster(_) => None, // cluster connection doesn't expose this yet
            Self::Sentinel(s) => {
                // Can't block here — return None if lock is contested.
                s.inner.try_read().ok().and_then(|c| c.get_cache_statistics())
            }
        }
    }
}

// =========================================================================
// ValkeyConn — public wrapper with separate regular + blocking connections
// =========================================================================

/// Public connection handle. Uses one connection for regular (fast) ops and
/// a lazily-created second connection for blocking ops (BLMOVE, BLMPOP).
///
/// Redis processes commands sequentially per connection — a BLMOVE with a
/// 1-second timeout blocks ALL other commands multiplexed on that connection.
/// The separate blocking connection prevents this head-of-line blocking.
#[derive(Clone)]
pub struct ValkeyConn {
    regular: ValkeyConnInner,
    blocking: Arc<tokio::sync::OnceCell<ValkeyConnInner>>,
    config: ConnConfig,
}

impl ValkeyConn {
    /// Get or lazily create the blocking connection.
    async fn get_blocking(&self) -> RedisResult<ValkeyConnInner> {
        self.blocking
            .get_or_try_init(|| async {
                match &self.config {
                    ConnConfig::Standard { url, tls_opts } => {
                        let blocking_url = url_with_resp3(url);
                        let client =
                            create_client(blocking_url.as_str(), tls_opts.as_ref())?;
                        let mgr = ConnectionManager::new_with_config(
                            client,
                            blocking_conn_manager_config(),
                        )
                        .await?;
                        Ok(ValkeyConnInner::Standard(mgr))
                    }
                    ConnConfig::Cluster { urls, tls_opts } => {
                        let url_refs: Vec<&str> =
                            urls.iter().map(|s| s.as_str()).collect();
                        let client = match tls_opts {
                            Some(opts) => ClusterClient::builder(url_refs)
                                .certs(opts.to_tls_certs())
                                .build()?,
                            None => ClusterClient::new(url_refs)?,
                        };
                        let conn = client.get_async_connection().await?;
                        Ok(ValkeyConnInner::Cluster(conn))
                    }
                    ConnConfig::Sentinel {
                        sentinel_urls,
                        service_name,
                        db,
                        tls_opts,
                    } => {
                        create_sentinel_inner(
                            sentinel_urls,
                            service_name,
                            *db,
                            true,
                            None,
                            tls_opts.clone(),
                        )
                        .await
                    }
                }
            })
            .await
            .cloned()
    }

    // === Regular ops — delegate to self.regular ===

    pub async fn get_bytes(&mut self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        self.regular.get_bytes(key).await
    }

    pub async fn set_bytes(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> RedisResult<()> {
        self.regular.set_bytes(key, value, ttl).await
    }

    pub async fn set_nx(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> RedisResult<bool> {
        self.regular.set_nx(key, value, ttl).await
    }

    pub async fn del(&mut self, key: &str) -> RedisResult<i64> {
        self.regular.del(key).await
    }

    pub async fn del_many(&mut self, keys: &[String]) -> RedisResult<i64> {
        self.regular.del_many(keys).await
    }

    pub async fn exists(&mut self, key: &str) -> RedisResult<bool> {
        self.regular.exists(key).await
    }

    pub async fn expire(&mut self, key: &str, seconds: u64) -> RedisResult<bool> {
        self.regular.expire(key, seconds).await
    }

    pub async fn persist(&mut self, key: &str) -> RedisResult<bool> {
        self.regular.persist(key).await
    }

    pub async fn mget_bytes(&mut self, keys: &[String]) -> RedisResult<Vec<Option<Vec<u8>>>> {
        self.regular.mget_bytes(keys).await
    }

    pub async fn pipeline_set(
        &mut self,
        entries: &[(String, Vec<u8>)],
        ttl: Option<u64>,
    ) -> RedisResult<()> {
        self.regular.pipeline_set(entries, ttl).await
    }

    pub async fn flushdb(&mut self) -> RedisResult<()> {
        self.regular.flushdb().await
    }

    pub async fn incr_by(&mut self, key: &str, delta: i64) -> RedisResult<i64> {
        self.regular.incr_by(key, delta).await
    }

    pub async fn lock_acquire(
        &mut self,
        key: &str,
        token: &str,
        timeout_ms: Option<u64>,
    ) -> RedisResult<bool> {
        self.regular.lock_acquire(key, token, timeout_ms).await
    }

    pub async fn lock_release(&mut self, key: &str, token: &str) -> RedisResult<i64> {
        self.regular.lock_release(key, token).await
    }

    pub async fn lock_extend(
        &mut self,
        key: &str,
        token: &str,
        additional_ms: u64,
    ) -> RedisResult<i64> {
        self.regular.lock_extend(key, token, additional_ms).await
    }

    pub async fn eval(
        &mut self,
        script: &str,
        keys: &[String],
        args: &[Vec<u8>],
    ) -> RedisResult<redis::Value> {
        self.regular.eval(script, keys, args).await
    }

    pub async fn evalsha(
        &mut self,
        sha: &str,
        keys: &[String],
        args: &[Vec<u8>],
    ) -> RedisResult<redis::Value> {
        self.regular.evalsha(sha, keys, args).await
    }

    pub async fn script_load(&mut self, script: &str) -> RedisResult<String> {
        self.regular.script_load(script).await
    }

    pub async fn pipeline_exec(
        &mut self,
        commands: Vec<(String, Vec<Vec<u8>>)>,
    ) -> RedisResult<Vec<redis::Value>> {
        self.regular.pipeline_exec(commands).await
    }

    pub async fn lpush(&mut self, key: &str, values: Vec<Vec<u8>>) -> RedisResult<i64> {
        self.regular.lpush(key, values).await
    }

    pub async fn rpush(&mut self, key: &str, values: Vec<Vec<u8>>) -> RedisResult<i64> {
        self.regular.rpush(key, values).await
    }

    pub async fn rpop(&mut self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        self.regular.rpop(key).await
    }

    pub async fn lrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> RedisResult<Vec<Vec<u8>>> {
        self.regular.lrange(key, start, stop).await
    }

    pub async fn lrem(&mut self, key: &str, count: i64, value: &[u8]) -> RedisResult<i64> {
        self.regular.lrem(key, count, value).await
    }

    pub async fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> RedisResult<()> {
        self.regular.ltrim(key, start, stop).await
    }

    pub async fn llen(&mut self, key: &str) -> RedisResult<i64> {
        self.regular.llen(key).await
    }

    pub async fn scan_all(&mut self, pattern: &str, count: i64) -> RedisResult<Vec<String>> {
        self.regular.scan_all(pattern, count).await
    }

    pub fn cache_statistics(&self) -> Option<CacheStatistics> {
        self.regular.cache_statistics()
    }

    // === Blocking ops — use separate connection ===

    pub async fn blmove(
        &mut self,
        source: &str,
        destination: &str,
        wherefrom: &str,
        whereto: &str,
        timeout: f64,
    ) -> RedisResult<Option<Vec<u8>>> {
        let mut conn = self.get_blocking().await?;
        conn.blmove(source, destination, wherefrom, whereto, timeout)
            .await
    }

    pub async fn blmpop(
        &mut self,
        timeout: f64,
        keys: &[String],
        direction: &str,
        count: i64,
    ) -> RedisResult<Option<(String, Vec<Vec<u8>>)>> {
        let mut conn = self.get_blocking().await?;
        conn.blmpop(timeout, keys, direction, count).await
    }
}

// =========================================================================
// Connection constructors
// =========================================================================

/// Helper: create a SentinelConn inner (used by both regular and lazy blocking).
async fn create_sentinel_inner(
    sentinel_urls: &[String],
    service_name: &str,
    db: i64,
    is_blocking: bool,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
) -> RedisResult<ValkeyConnInner> {
    let config = if is_blocking {
        blocking_conn_manager_config()
    } else {
        conn_manager_config(cache_opts.as_ref())
    };
    let mut last_err = String::from("No sentinels provided");

    for sentinel_url in sentinel_urls {
        let client = match create_client(sentinel_url.as_str(), tls_opts.as_ref()) {
            Ok(c) => c,
            Err(e) => {
                last_err = format!("Sentinel {sentinel_url}: {e}");
                continue;
            }
        };

        let mut conn =
            match ConnectionManager::new_with_config(client, conn_manager_config(None)).await {
                Ok(c) => c,
                Err(e) => {
                    last_err = format!("Sentinel {sentinel_url}: {e}");
                    continue;
                }
            };

        let result: RedisResult<Vec<String>> = redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(service_name)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(addr) if addr.len() == 2 => {
                let scheme = if tls_opts.is_some() { "rediss" } else { "redis" };
                let base_url =
                    format!("{scheme}://{}:{}/{}", addr[0], addr[1], db);
                let master_url = url_with_resp3(&base_url);
                let master_client =
                    create_client(master_url.as_str(), tls_opts.as_ref())?;
                let mgr = ConnectionManager::new_with_config(master_client, config).await?;
                return Ok(ValkeyConnInner::Sentinel(SentinelConn {
                    inner: Arc::new(RwLock::new(mgr)),
                    sentinel_urls: Arc::from(sentinel_urls),
                    service_name: Arc::from(service_name),
                    db,
                    is_blocking,
                    cache_opts,
                    tls_opts,
                }));
            }
            Ok(_) => {
                last_err = format!("Sentinel {sentinel_url}: unexpected response format");
            }
            Err(e) => {
                last_err = format!("Sentinel {sentinel_url}: {e}");
            }
        }
    }

    Err(redis::RedisError::from((
        redis::ErrorKind::Io,
        "Failed to discover master from any sentinel",
        last_err,
    )))
}

/// Connect to a single Valkey/Redis node.
pub async fn connect_standard(
    url: &str,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
) -> Result<ValkeyConn, String> {
    let conn_url = url_with_resp3(url);
    let client = create_client(conn_url.as_str(), tls_opts.as_ref())
        .map_err(|e| format!("Invalid URL: {e}"))?;
    let mgr =
        ConnectionManager::new_with_config(client, conn_manager_config(cache_opts.as_ref()))
            .await
            .map_err(|e| format!("Connection failed: {e}"))?;
    Ok(ValkeyConn {
        regular: ValkeyConnInner::Standard(mgr),
        blocking: Arc::new(tokio::sync::OnceCell::new()),
        config: ConnConfig::Standard {
            url: Arc::from(url),
            tls_opts,
        },
    })
}

/// Connect to a Valkey/Redis cluster.
pub async fn connect_cluster(
    urls: Vec<String>,
    tls_opts: Option<TlsOpts>,
) -> Result<ValkeyConn, String> {
    // Client-side caching not yet supported for cluster mode in redis-rs.
    let url_refs: Vec<&str> = urls.iter().map(|s| s.as_str()).collect();
    let client = match &tls_opts {
        Some(opts) => ClusterClient::builder(url_refs)
            .certs(opts.to_tls_certs())
            .build(),
        None => ClusterClient::new(url_refs),
    }
    .map_err(|e| format!("Invalid cluster URLs: {e}"))?;
    let conn = client
        .get_async_connection()
        .await
        .map_err(|e| format!("Cluster connection failed: {e}"))?;
    Ok(ValkeyConn {
        regular: ValkeyConnInner::Cluster(conn),
        blocking: Arc::new(tokio::sync::OnceCell::new()),
        config: ConnConfig::Cluster {
            urls: Arc::from(urls),
            tls_opts,
        },
    })
}

/// Connect via Sentinel with automatic failover support.
pub async fn connect_sentinel(
    sentinel_urls: Vec<String>,
    service_name: &str,
    db: i64,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
) -> Result<ValkeyConn, String> {
    let inner = create_sentinel_inner(
        &sentinel_urls,
        service_name,
        db,
        false,
        cache_opts,
        tls_opts.clone(),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(ValkeyConn {
        regular: inner,
        blocking: Arc::new(tokio::sync::OnceCell::new()),
        config: ConnConfig::Sentinel {
            sentinel_urls: Arc::from(sentinel_urls),
            service_name: Arc::from(service_name),
            db,
            tls_opts,
        },
    })
}
