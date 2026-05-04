// Connection layer for the Rust adapter.
//
// Original (lines through `connect_sentinel`): verbatim port from django-vcache
// (MIT, by David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache/-/blob/main/src/connection.rs
// Keep that section in lockstep with upstream.
//
// Below the "django-cachex extensions" marker at the bottom of the file are
// extra command methods (hashes, sets, sorted sets, streams, admin/introspection)
// that django-cachex needs but vcache does not expose. They live in the same
// file so they can use the file-local dispatch macros.

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
///
/// Public so [`Conn`] can expose it as the target of its [`Deref`]
/// impls — non-blocking commands resolve straight to the inner without
/// per-method passthroughs. Crate is a `cdylib`, so the "public" boundary
/// is just the PyO3 surface; this type is still effectively internal.
#[derive(Clone)]
pub enum ConnInner {
    Standard(ConnectionManager),
    Cluster(ClusterConnection),
    Sentinel(SentinelConn),
}

// Macro to dispatch a redis command to the correct connection variant.
// For sentinel, retries once on failover error after re-discovering master.
macro_rules! dispatch_cmd {
    ($self:expr, $cmd:expr) => {
        match $self {
            ConnInner::Standard(c) => $cmd.query_async(c).await,
            ConnInner::Cluster(c) => $cmd.query_async(c).await,
            ConnInner::Sentinel(s) => {
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
            ConnInner::Standard($c) => $op,
            ConnInner::Cluster($c) => $op,
            ConnInner::Sentinel(s) => sentinel_retry!(s, $c, $op),
        }
    };
}

impl ConnInner {
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

    pub async fn set_with_flags(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<u64>,
        nx: bool,
        xx: bool,
        get: bool,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(value);
        if let Some(t) = ttl {
            cmd.arg("EX").arg(t);
        }
        if nx {
            cmd.arg("NX");
        }
        if xx {
            cmd.arg("XX");
        }
        if get {
            cmd.arg("GET");
        }
        dispatch_cmd!(self, cmd)
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

    pub async fn pexpire(&mut self, key: &str, milliseconds: i64) -> RedisResult<bool> {
        let mut cmd = redis::cmd("PEXPIRE");
        cmd.arg(key).arg(milliseconds);
        dispatch_cmd!(self, cmd)
    }

    pub async fn expireat(&mut self, key: &str, ts_seconds: i64) -> RedisResult<bool> {
        let mut cmd = redis::cmd("EXPIREAT");
        cmd.arg(key).arg(ts_seconds);
        dispatch_cmd!(self, cmd)
    }

    pub async fn pexpireat(&mut self, key: &str, ts_milliseconds: i64) -> RedisResult<bool> {
        let mut cmd = redis::cmd("PEXPIREAT");
        cmd.arg(key).arg(ts_milliseconds);
        dispatch_cmd!(self, cmd)
    }

    pub async fn expiretime(&mut self, key: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("EXPIRETIME");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn persist(&mut self, key: &str) -> RedisResult<bool> {
        conn_method!(self, c, c.persist(key).await)
    }

    pub async fn rename(&mut self, src: &str, dst: &str) -> RedisResult<()> {
        let mut cmd = redis::cmd("RENAME");
        cmd.arg(src).arg(dst);
        let _: redis::Value = dispatch_cmd!(self, cmd)?;
        Ok(())
    }

    pub async fn renamenx(&mut self, src: &str, dst: &str) -> RedisResult<bool> {
        let mut cmd = redis::cmd("RENAMENX");
        cmd.arg(src).arg(dst);
        dispatch_cmd!(self, cmd)
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

    /// Execute a pipeline of arbitrary commands.
    ///
    /// When `transaction` is true, wraps the batch in MULTI/EXEC for atomicity.
    /// Cluster mode ignores `transaction` because cross-slot transactions aren't
    /// supported; the per-command fan-out matches what redis-py does.
    pub async fn pipeline_exec(
        &mut self,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> RedisResult<Vec<redis::Value>> {
        match self {
            Self::Standard(c) => {
                let mut pipe = redis::pipe();
                if transaction {
                    pipe.atomic();
                }
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
                    if transaction {
                        pipe.atomic();
                    }
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


    pub async fn blpop(
        &mut self,
        keys: &[String],
        timeout: f64,
    ) -> RedisResult<Option<(String, Vec<u8>)>> {
        self.bpop_inner("BLPOP", keys, timeout).await
    }

    pub async fn brpop(
        &mut self,
        keys: &[String],
        timeout: f64,
    ) -> RedisResult<Option<(String, Vec<u8>)>> {
        self.bpop_inner("BRPOP", keys, timeout).await
    }

    async fn bpop_inner(
        &mut self,
        command: &str,
        keys: &[String],
        timeout: f64,
    ) -> RedisResult<Option<(String, Vec<u8>)>> {
        let mut cmd = redis::cmd(command);
        for k in keys {
            cmd.arg(k.as_str());
        }
        cmd.arg(timeout);
        let val: redis::Value = dispatch_cmd!(self, cmd)?;
        match val {
            redis::Value::Nil => Ok(None),
            redis::Value::Array(mut items) if items.len() == 2 => {
                let value_val = items.pop().unwrap();
                let key_val = items.pop().unwrap();
                let key: String = redis::from_redis_value(key_val)?;
                let value: Vec<u8> = redis::from_redis_value(value_val)?;
                Ok(Some((key, value)))
            }
            _ => Ok(None),
        }
    }

    // Scan


    /// One SCAN iteration. Returns `(next_cursor, keys)`; caller drives
    /// the loop. ``next_cursor == 0`` signals end of iteration.
    ///
    /// Cluster mode has no global cursor (each node has its own keyspace
    /// slice); here it falls back to a single ``KEYS`` call and returns
    /// cursor=0, mirroring the existing cluster behaviour of ``scan_all``.
    pub async fn scan_one(
        &mut self,
        cursor: u64,
        pattern: &str,
        count: i64,
        type_filter: Option<&str>,
    ) -> RedisResult<(u64, Vec<String>)> {
        match self {
            Self::Cluster(_) => {
                // KEYS has no TYPE option; emulate by calling TYPE per key when
                // a type filter is supplied. Bounded by KEYS's existing scan,
                // so the cost is the same order of magnitude.
                let mut cmd = redis::cmd("KEYS");
                cmd.arg(pattern);
                let mut keys: Vec<String> = dispatch_cmd!(self, cmd)?;
                if let Some(t) = type_filter {
                    let want = t.to_ascii_lowercase();
                    let mut filtered = Vec::with_capacity(keys.len());
                    for key in keys.drain(..) {
                        let ty: String = {
                            let mut tcmd = redis::cmd("TYPE");
                            tcmd.arg(key.as_str());
                            dispatch_cmd!(self, tcmd)?
                        };
                        if ty.eq_ignore_ascii_case(&want) {
                            filtered.push(key);
                        }
                    }
                    keys = filtered;
                }
                Ok((0, keys))
            }
            Self::Standard(c) => {
                let mut cmd = redis::cmd("SCAN");
                cmd.arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(count);
                if let Some(t) = type_filter {
                    cmd.arg("TYPE").arg(t);
                }
                let (next_cursor, keys): (u64, Vec<String>) = cmd.query_async(c).await?;
                Ok((next_cursor, keys))
            }
            Self::Sentinel(s) => {
                let build_cmd = || {
                    let mut cmd = redis::cmd("SCAN");
                    cmd.arg(cursor)
                        .arg("MATCH")
                        .arg(pattern)
                        .arg("COUNT")
                        .arg(count);
                    if let Some(t) = type_filter {
                        cmd.arg("TYPE").arg(t);
                    }
                    cmd
                };
                let mut c = s.get_conn().await;
                let result: RedisResult<(u64, Vec<String>)> =
                    build_cmd().query_async(&mut c).await;
                match result {
                    Ok(pair) => Ok(pair),
                    Err(e) if cursor == 0 && SentinelConn::is_failover_error(&e) => {
                        s.rediscover().await?;
                        let mut c = s.get_conn().await;
                        let pair: (u64, Vec<String>) = build_cmd().query_async(&mut c).await?;
                        Ok(pair)
                    }
                    Err(e) => Err(e),
                }
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

impl ConnInner {
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
// Conn — public wrapper with separate regular + blocking connections
// =========================================================================

/// Public connection handle. Uses one connection for regular (fast) ops and
/// a lazily-created second connection for blocking ops (BLMOVE, BLMPOP).
///
/// Redis processes commands sequentially per connection — a BLMOVE with a
/// 1-second timeout blocks ALL other commands multiplexed on that connection.
/// The separate blocking connection prevents this head-of-line blocking.
#[derive(Clone)]
pub struct Conn {
    regular: ConnInner,
    blocking: Arc<tokio::sync::OnceCell<ConnInner>>,
    config: ConnConfig,
}

// Non-blocking commands route through the regular connection. Deref lets
// `conn.X(args).await` auto-resolve to `conn.regular.X(args).await` without
// hand-written passthroughs for every command. Blocking commands (BLMOVE,
// BL{POP,RPOP,MPOP}) bypass this by being defined as inherent methods on
// `Conn` — inherent methods take precedence over `Deref`-resolved
// ones.
impl std::ops::Deref for Conn {
    type Target = ConnInner;
    fn deref(&self) -> &Self::Target {
        &self.regular
    }
}

impl std::ops::DerefMut for Conn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.regular
    }
}

impl Conn {
    /// Get or lazily create the blocking connection.
    async fn get_blocking(&self) -> RedisResult<ConnInner> {
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
                        Ok(ConnInner::Standard(mgr))
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
                        Ok(ConnInner::Cluster(conn))
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

    pub async fn pexpire(&mut self, key: &str, milliseconds: i64) -> RedisResult<bool> {
        self.regular.pexpire(key, milliseconds).await
    }

    pub async fn expireat(&mut self, key: &str, ts_seconds: i64) -> RedisResult<bool> {
        self.regular.expireat(key, ts_seconds).await
    }

    pub async fn pexpireat(&mut self, key: &str, ts_milliseconds: i64) -> RedisResult<bool> {
        self.regular.pexpireat(key, ts_milliseconds).await
    }

    pub async fn expiretime(&mut self, key: &str) -> RedisResult<i64> {
        self.regular.expiretime(key).await
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

    pub async fn pipeline_exec(
        &mut self,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> RedisResult<Vec<redis::Value>> {
        self.regular.pipeline_exec(commands, transaction).await
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

    pub async fn blpop(
        &mut self,
        keys: &[String],
        timeout: f64,
    ) -> RedisResult<Option<(String, Vec<u8>)>> {
        let mut conn = self.get_blocking().await?;
        conn.blpop(keys, timeout).await
    }

    pub async fn brpop(
        &mut self,
        keys: &[String],
        timeout: f64,
    ) -> RedisResult<Option<(String, Vec<u8>)>> {
        let mut conn = self.get_blocking().await?;
        conn.brpop(keys, timeout).await
    }

    /// XREAD: when `block_ms` is set, route to the blocking connection so a
    /// long BLOCK doesn't stall the multiplexed regular connection. Without
    /// `block_ms` the Deref-resolved `regular.xread` is used (faster path).
    pub async fn xread(
        &mut self,
        keys: &[String],
        ids: &[String],
        count: Option<i64>,
        block_ms: Option<i64>,
    ) -> RedisResult<redis::Value> {
        if block_ms.is_some() {
            let mut conn = self.get_blocking().await?;
            conn.xread(keys, ids, count, block_ms).await
        } else {
            self.regular.xread(keys, ids, count, None).await
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn xreadgroup(
        &mut self,
        group: &str,
        consumer: &str,
        keys: &[String],
        ids: &[String],
        count: Option<i64>,
        block_ms: Option<i64>,
        noack: bool,
    ) -> RedisResult<redis::Value> {
        if block_ms.is_some() {
            let mut conn = self.get_blocking().await?;
            conn.xreadgroup(group, consumer, keys, ids, count, block_ms, noack)
                .await
        } else {
            self.regular
                .xreadgroup(group, consumer, keys, ids, count, None, noack)
                .await
        }
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
) -> RedisResult<ConnInner> {
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
                return Ok(ConnInner::Sentinel(SentinelConn {
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
) -> Result<Conn, String> {
    let conn_url = url_with_resp3(url);
    let client = create_client(conn_url.as_str(), tls_opts.as_ref())
        .map_err(|e| format!("Invalid URL: {e}"))?;
    let mgr =
        ConnectionManager::new_with_config(client, conn_manager_config(cache_opts.as_ref()))
            .await
            .map_err(|e| format!("Connection failed: {e}"))?;
    Ok(Conn {
        regular: ConnInner::Standard(mgr),
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
) -> Result<Conn, String> {
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
    Ok(Conn {
        regular: ConnInner::Cluster(conn),
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
) -> Result<Conn, String> {
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
    Ok(Conn {
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

// =========================================================================
// django-cachex extensions
//
// Methods below extend the vcache-ported surface with command families
// (hashes, sets, sorted sets, streams, admin/introspection) that
// django-cachex needs to expose. They use the file-local dispatch macros
// `dispatch_cmd!`, `conn_method!`, and `sentinel_retry!` defined above.
// =========================================================================

impl ConnInner {
    // ----- Strings / TTL / type / admin gap commands -----

    pub async fn ttl(&mut self, key: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("TTL");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn pttl(&mut self, key: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("PTTL");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    /// On Cluster, KEYS only hits one master and returns that node's keys —
    /// callers needing a full-cluster scan should fan out themselves or use
    /// `scan_all` (which has the same single-node limitation here, mirroring
    /// vcache's behavior; documented for both).
    pub async fn keys(&mut self, pattern: &str) -> RedisResult<Vec<String>> {
        let mut cmd = redis::cmd("KEYS");
        cmd.arg(pattern);
        dispatch_cmd!(self, cmd)
    }

    pub async fn key_type(&mut self, key: &str) -> RedisResult<String> {
        let mut cmd = redis::cmd("TYPE");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn lpop(&mut self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("LPOP");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn lpop_count(&mut self, key: &str, count: i64) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("LPOP");
        cmd.arg(key).arg(count);
        dispatch_cmd!(self, cmd)
    }

    pub async fn rpop_count(&mut self, key: &str, count: i64) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("RPOP");
        cmd.arg(key).arg(count);
        dispatch_cmd!(self, cmd)
    }

    pub async fn lmove(
        &mut self,
        source: &str,
        destination: &str,
        wherefrom: &str,
        whereto: &str,
    ) -> RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("LMOVE");
        cmd.arg(source).arg(destination).arg(wherefrom).arg(whereto);
        dispatch_cmd!(self, cmd)
    }

    pub async fn lpos(
        &mut self,
        key: &str,
        value: &[u8],
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("LPOS");
        cmd.arg(key).arg(value);
        if let Some(r) = rank {
            cmd.arg("RANK").arg(r);
        }
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        if let Some(m) = maxlen {
            cmd.arg("MAXLEN").arg(m);
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn lindex(&mut self, key: &str, index: i64) -> RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("LINDEX");
        cmd.arg(key).arg(index);
        dispatch_cmd!(self, cmd)
    }

    pub async fn lset(&mut self, key: &str, index: i64, value: &[u8]) -> RedisResult<()> {
        let mut cmd = redis::cmd("LSET");
        cmd.arg(key).arg(index).arg(value);
        dispatch_cmd!(self, cmd)
    }

    pub async fn linsert(
        &mut self,
        key: &str,
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("LINSERT");
        cmd.arg(key)
            .arg(if before { "BEFORE" } else { "AFTER" })
            .arg(pivot)
            .arg(value);
        dispatch_cmd!(self, cmd)
    }

    pub async fn info(&mut self) -> RedisResult<redis::Value> {
        dispatch_cmd!(self, redis::cmd("INFO"))
    }

    // ----- Hashes -----

    pub async fn hget(&mut self, key: &str, field: &str) -> RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("HGET");
        cmd.arg(key).arg(field);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hset(&mut self, key: &str, field: &str, value: &[u8]) -> RedisResult<i64> {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key).arg(field).arg(value);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hsetnx(&mut self, key: &str, field: &str, value: &[u8]) -> RedisResult<bool> {
        let mut cmd = redis::cmd("HSETNX");
        cmd.arg(key).arg(field).arg(value);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hgetall(&mut self, key: &str) -> RedisResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cmd = redis::cmd("HGETALL");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hdel(&mut self, key: &str, fields: &[String]) -> RedisResult<i64> {
        let mut cmd = redis::cmd("HDEL");
        cmd.arg(key);
        for f in fields {
            cmd.arg(f.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn hincrby(&mut self, key: &str, field: &str, delta: i64) -> RedisResult<i64> {
        let mut cmd = redis::cmd("HINCRBY");
        cmd.arg(key).arg(field).arg(delta);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hincrbyfloat(
        &mut self,
        key: &str,
        field: &str,
        delta: f64,
    ) -> RedisResult<f64> {
        let mut cmd = redis::cmd("HINCRBYFLOAT");
        cmd.arg(key).arg(field).arg(delta);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hkeys(&mut self, key: &str) -> RedisResult<Vec<String>> {
        let mut cmd = redis::cmd("HKEYS");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hvals(&mut self, key: &str) -> RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("HVALS");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hexists(&mut self, key: &str, field: &str) -> RedisResult<bool> {
        let mut cmd = redis::cmd("HEXISTS");
        cmd.arg(key).arg(field);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hlen(&mut self, key: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("HLEN");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn hmget(
        &mut self,
        key: &str,
        fields: &[String],
    ) -> RedisResult<Vec<Option<Vec<u8>>>> {
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(key);
        for f in fields {
            cmd.arg(f.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn hmset(
        &mut self,
        key: &str,
        fields: &[(String, Vec<u8>)],
    ) -> RedisResult<()> {
        // HMSET is technically deprecated; HSET with multiple field/value pairs is the modern equivalent.
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key);
        for (f, v) in fields {
            cmd.arg(f.as_str()).arg(v.as_slice());
        }
        let _: i64 = dispatch_cmd!(self, cmd)?;
        Ok(())
    }

    // ----- Sets -----

    pub async fn sadd(&mut self, key: &str, members: Vec<Vec<u8>>) -> RedisResult<i64> {
        let mut cmd = redis::cmd("SADD");
        cmd.arg(key);
        for m in &members {
            cmd.arg(m.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn srem(&mut self, key: &str, members: Vec<Vec<u8>>) -> RedisResult<i64> {
        let mut cmd = redis::cmd("SREM");
        cmd.arg(key);
        for m in &members {
            cmd.arg(m.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn smembers(&mut self, key: &str) -> RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("SMEMBERS");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    /// Single SSCAN iteration. Caller drives the cursor loop.
    pub async fn sscan_step(
        &mut self,
        key: &str,
        cursor: u64,
        match_pattern: Option<&str>,
        count: Option<i64>,
    ) -> RedisResult<(u64, Vec<Vec<u8>>)> {
        let mut cmd = redis::cmd("SSCAN");
        cmd.arg(key).arg(cursor);
        if let Some(p) = match_pattern {
            cmd.arg("MATCH").arg(p);
        }
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn sismember(&mut self, key: &str, member: &[u8]) -> RedisResult<bool> {
        let mut cmd = redis::cmd("SISMEMBER");
        cmd.arg(key).arg(member);
        dispatch_cmd!(self, cmd)
    }

    pub async fn scard(&mut self, key: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("SCARD");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    /// Multi-key. On Cluster all keys must hash to the same slot; redis-rs
    /// surfaces a CROSSSLOT error otherwise. Same applies to `sunion` / `sdiff`.
    pub async fn sinter(&mut self, keys: &[String]) -> RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("SINTER");
        for k in keys {
            cmd.arg(k.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn sunion(&mut self, keys: &[String]) -> RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("SUNION");
        for k in keys {
            cmd.arg(k.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn sdiff(&mut self, keys: &[String]) -> RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("SDIFF");
        for k in keys {
            cmd.arg(k.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn smove(
        &mut self,
        source: &str,
        destination: &str,
        member: &[u8],
    ) -> RedisResult<bool> {
        let mut cmd = redis::cmd("SMOVE");
        cmd.arg(source).arg(destination).arg(member);
        dispatch_cmd!(self, cmd)
    }

    pub async fn smismember(
        &mut self,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("SMISMEMBER");
        cmd.arg(key);
        for m in &members {
            cmd.arg(m.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn spop(&mut self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("SPOP");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn spop_count(&mut self, key: &str, count: i64) -> RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("SPOP");
        cmd.arg(key).arg(count);
        dispatch_cmd!(self, cmd)
    }

    pub async fn srandmember(&mut self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("SRANDMEMBER");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn srandmember_count(
        &mut self,
        key: &str,
        count: i64,
    ) -> RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("SRANDMEMBER");
        cmd.arg(key).arg(count);
        dispatch_cmd!(self, cmd)
    }

    pub async fn sdiffstore(
        &mut self,
        destination: &str,
        keys: &[String],
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("SDIFFSTORE");
        cmd.arg(destination);
        for k in keys {
            cmd.arg(k.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn sinterstore(
        &mut self,
        destination: &str,
        keys: &[String],
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("SINTERSTORE");
        cmd.arg(destination);
        for k in keys {
            cmd.arg(k.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn sunionstore(
        &mut self,
        destination: &str,
        keys: &[String],
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("SUNIONSTORE");
        cmd.arg(destination);
        for k in keys {
            cmd.arg(k.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    // ----- Sorted sets -----

    pub async fn zadd(&mut self, key: &str, members: Vec<(Vec<u8>, f64)>) -> RedisResult<i64> {
        let mut cmd = redis::cmd("ZADD");
        cmd.arg(key);
        for (member, score) in &members {
            cmd.arg(*score).arg(member.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    /// ZADD with NX/XX/GT/LT/CH flags. Mutually exclusive flag pairs (e.g.
    /// NX+XX) are rejected by Redis with a server error.
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    pub async fn zadd_with_flags(
        &mut self,
        key: &str,
        members: Vec<(Vec<u8>, f64)>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("ZADD");
        cmd.arg(key);
        if nx {
            cmd.arg("NX");
        }
        if xx {
            cmd.arg("XX");
        }
        if gt {
            cmd.arg("GT");
        }
        if lt {
            cmd.arg("LT");
        }
        if ch {
            cmd.arg("CH");
        }
        for (member, score) in &members {
            cmd.arg(*score).arg(member.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn zrem(&mut self, key: &str, members: Vec<Vec<u8>>) -> RedisResult<i64> {
        let mut cmd = redis::cmd("ZREM");
        cmd.arg(key);
        for m in &members {
            cmd.arg(m.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn zrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("ZRANGE");
        cmd.arg(key).arg(start).arg(stop);
        if with_scores {
            cmd.arg("WITHSCORES");
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn zrangebyscore(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        with_scores: bool,
        offset: Option<i64>,
        count: Option<i64>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("ZRANGEBYSCORE");
        cmd.arg(key).arg(min).arg(max);
        if with_scores {
            cmd.arg("WITHSCORES");
        }
        if offset.is_some() || count.is_some() {
            // ``LIMIT`` requires both arguments; default the missing one to
            // the natural extreme (no offset / no count).
            cmd.arg("LIMIT")
                .arg(offset.unwrap_or(0))
                .arg(count.unwrap_or(-1));
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn zrevrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("ZREVRANGE");
        cmd.arg(key).arg(start).arg(stop);
        if with_scores {
            cmd.arg("WITHSCORES");
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn zincrby(&mut self, key: &str, member: &[u8], delta: f64) -> RedisResult<f64> {
        let mut cmd = redis::cmd("ZINCRBY");
        cmd.arg(key).arg(delta).arg(member);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zcard(&mut self, key: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("ZCARD");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zscore(&mut self, key: &str, member: &[u8]) -> RedisResult<Option<f64>> {
        let mut cmd = redis::cmd("ZSCORE");
        cmd.arg(key).arg(member);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zrank(&mut self, key: &str, member: &[u8]) -> RedisResult<Option<i64>> {
        let mut cmd = redis::cmd("ZRANK");
        cmd.arg(key).arg(member);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zrevrank(&mut self, key: &str, member: &[u8]) -> RedisResult<Option<i64>> {
        let mut cmd = redis::cmd("ZREVRANK");
        cmd.arg(key).arg(member);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zmscore(
        &mut self,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("ZMSCORE");
        cmd.arg(key);
        for m in &members {
            cmd.arg(m.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn zremrangebyrank(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("ZREMRANGEBYRANK");
        cmd.arg(key).arg(start).arg(stop);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zremrangebyscore(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("ZREMRANGEBYSCORE");
        cmd.arg(key).arg(min).arg(max);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zrevrangebyscore(
        &mut self,
        key: &str,
        max: &str,
        min: &str,
        with_scores: bool,
        offset_count: Option<(i64, i64)>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("ZREVRANGEBYSCORE");
        cmd.arg(key).arg(max).arg(min);
        if with_scores {
            cmd.arg("WITHSCORES");
        }
        if let Some((offset, count)) = offset_count {
            cmd.arg("LIMIT").arg(offset).arg(count);
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn zcount(&mut self, key: &str, min: &str, max: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("ZCOUNT");
        cmd.arg(key).arg(min).arg(max);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zpopmin(
        &mut self,
        key: &str,
        count: i64,
    ) -> RedisResult<Vec<(Vec<u8>, f64)>> {
        let mut cmd = redis::cmd("ZPOPMIN");
        cmd.arg(key).arg(count);
        dispatch_cmd!(self, cmd)
    }

    pub async fn zpopmax(
        &mut self,
        key: &str,
        count: i64,
    ) -> RedisResult<Vec<(Vec<u8>, f64)>> {
        let mut cmd = redis::cmd("ZPOPMAX");
        cmd.arg(key).arg(count);
        dispatch_cmd!(self, cmd)
    }

    // ----- Streams -----

    pub async fn xadd(
        &mut self,
        key: &str,
        id: &str,
        fields: &[(String, Vec<u8>)],
    ) -> RedisResult<String> {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key).arg(id);
        for (f, v) in fields {
            cmd.arg(f.as_str()).arg(v.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    /// XADD with MAXLEN/MINID/NOMKSTREAM/LIMIT options. Returns
    /// ``None`` when ``NOMKSTREAM`` is set and the stream doesn't exist.
    #[allow(clippy::too_many_arguments)]
    pub async fn xadd_with_options(
        &mut self,
        key: &str,
        id: &str,
        fields: &[(String, Vec<u8>)],
        maxlen: Option<i64>,
        approximate: bool,
        nomkstream: bool,
        minid: Option<&str>,
        limit: Option<i64>,
    ) -> RedisResult<Option<String>> {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key);
        if nomkstream {
            cmd.arg("NOMKSTREAM");
        }
        if let Some(m) = maxlen {
            cmd.arg("MAXLEN");
            if approximate {
                cmd.arg("~");
            }
            cmd.arg(m);
            if let Some(l) = limit {
                cmd.arg("LIMIT").arg(l);
            }
        } else if let Some(mi) = minid {
            cmd.arg("MINID");
            if approximate {
                cmd.arg("~");
            }
            cmd.arg(mi);
            if let Some(l) = limit {
                cmd.arg("LIMIT").arg(l);
            }
        }
        cmd.arg(id);
        for (f, v) in fields {
            cmd.arg(f.as_str()).arg(v.as_slice());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn xlen(&mut self, key: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("XLEN");
        cmd.arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn xrange(
        &mut self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<i64>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XRANGE");
        cmd.arg(key).arg(start).arg(end);
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn xrevrange(
        &mut self,
        key: &str,
        end: &str,
        start: &str,
        count: Option<i64>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XREVRANGE");
        cmd.arg(key).arg(end).arg(start);
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        dispatch_cmd!(self, cmd)
    }

    /// Multi-stream. On Cluster all stream keys must hash to the same slot
    /// (CROSSSLOT otherwise). Same applies to `xreadgroup`.
    ///
    /// `block_ms` adds the BLOCK option. The caller is responsible for routing
    /// blocking variants through the dedicated blocking connection (see
    /// `Conn::xread`) — head-of-line blocking on the multiplexed
    /// connection would otherwise stall every other command.
    pub async fn xread(
        &mut self,
        keys: &[String],
        ids: &[String],
        count: Option<i64>,
        block_ms: Option<i64>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XREAD");
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        if let Some(b) = block_ms {
            cmd.arg("BLOCK").arg(b);
        }
        cmd.arg("STREAMS");
        for k in keys {
            cmd.arg(k.as_str());
        }
        for i in ids {
            cmd.arg(i.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn xreadgroup(
        &mut self,
        group: &str,
        consumer: &str,
        keys: &[String],
        ids: &[String],
        count: Option<i64>,
        block_ms: Option<i64>,
        noack: bool,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP").arg(group).arg(consumer);
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        if let Some(b) = block_ms {
            cmd.arg("BLOCK").arg(b);
        }
        if noack {
            cmd.arg("NOACK");
        }
        cmd.arg("STREAMS");
        for k in keys {
            cmd.arg(k.as_str());
        }
        for i in ids {
            cmd.arg(i.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn xack(&mut self, key: &str, group: &str, ids: &[String]) -> RedisResult<i64> {
        let mut cmd = redis::cmd("XACK");
        cmd.arg(key).arg(group);
        for i in ids {
            cmd.arg(i.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn xdel(&mut self, key: &str, ids: &[String]) -> RedisResult<i64> {
        let mut cmd = redis::cmd("XDEL");
        cmd.arg(key);
        for i in ids {
            cmd.arg(i.as_str());
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn xgroup_destroy(&mut self, key: &str, group: &str) -> RedisResult<i64> {
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("DESTROY").arg(key).arg(group);
        dispatch_cmd!(self, cmd)
    }

    pub async fn xgroup_setid(
        &mut self,
        key: &str,
        group: &str,
        id: &str,
        entries_read: Option<i64>,
    ) -> RedisResult<()> {
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("SETID").arg(key).arg(group).arg(id);
        if let Some(n) = entries_read {
            cmd.arg("ENTRIESREAD").arg(n);
        }
        let _: redis::Value = dispatch_cmd!(self, cmd)?;
        Ok(())
    }

    pub async fn xgroup_delconsumer(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("DELCONSUMER").arg(key).arg(group).arg(consumer);
        dispatch_cmd!(self, cmd)
    }

    pub async fn xinfo_stream(&mut self, key: &str, full: bool) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XINFO");
        cmd.arg("STREAM").arg(key);
        if full {
            cmd.arg("FULL");
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn xinfo_groups(&mut self, key: &str) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XINFO");
        cmd.arg("GROUPS").arg(key);
        dispatch_cmd!(self, cmd)
    }

    pub async fn xinfo_consumers(
        &mut self,
        key: &str,
        group: &str,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XINFO");
        cmd.arg("CONSUMERS").arg(key).arg(group);
        dispatch_cmd!(self, cmd)
    }

    /// XTRIM with MAXLEN or MINID + optional LIMIT.
    pub async fn xtrim_with_options(
        &mut self,
        key: &str,
        maxlen: Option<i64>,
        approximate: bool,
        minid: Option<&str>,
        limit: Option<i64>,
    ) -> RedisResult<i64> {
        let mut cmd = redis::cmd("XTRIM");
        cmd.arg(key);
        if let Some(m) = maxlen {
            cmd.arg("MAXLEN");
            if approximate {
                cmd.arg("~");
            }
            cmd.arg(m);
        } else if let Some(mi) = minid {
            cmd.arg("MINID");
            if approximate {
                cmd.arg("~");
            }
            cmd.arg(mi);
        }
        if let Some(l) = limit {
            cmd.arg("LIMIT").arg(l);
        }
        dispatch_cmd!(self, cmd)
    }

    pub async fn xgroup_create(
        &mut self,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
        entries_read: Option<i64>,
    ) -> RedisResult<()> {
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("CREATE").arg(key).arg(group).arg(id);
        if mkstream {
            cmd.arg("MKSTREAM");
        }
        if let Some(n) = entries_read {
            cmd.arg("ENTRIESREAD").arg(n);
        }
        let _: redis::Value = dispatch_cmd!(self, cmd)?;
        Ok(())
    }

    pub async fn xpending(&mut self, key: &str, group: &str) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XPENDING");
        cmd.arg(key).arg(group);
        dispatch_cmd!(self, cmd)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn xpending_range(
        &mut self,
        key: &str,
        group: &str,
        start: &str,
        end: &str,
        count: i64,
        consumer: Option<&str>,
        idle: Option<u64>,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XPENDING");
        cmd.arg(key).arg(group);
        if let Some(ms) = idle {
            cmd.arg("IDLE").arg(ms);
        }
        cmd.arg(start).arg(end).arg(count);
        if let Some(c) = consumer {
            cmd.arg(c);
        }
        dispatch_cmd!(self, cmd)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn xclaim(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        ids: &[String],
        idle: Option<i64>,
        time_ms: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XCLAIM");
        cmd.arg(key).arg(group).arg(consumer).arg(min_idle_time);
        for id in ids {
            cmd.arg(id.as_str());
        }
        if let Some(v) = idle {
            cmd.arg("IDLE").arg(v);
        }
        if let Some(v) = time_ms {
            cmd.arg("TIME").arg(v);
        }
        if let Some(v) = retrycount {
            cmd.arg("RETRYCOUNT").arg(v);
        }
        if force {
            cmd.arg("FORCE");
        }
        if justid {
            cmd.arg("JUSTID");
        }
        dispatch_cmd!(self, cmd)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn xautoclaim(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        start: &str,
        count: Option<i64>,
        justid: bool,
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd("XAUTOCLAIM");
        cmd.arg(key)
            .arg(group)
            .arg(consumer)
            .arg(min_idle_time)
            .arg(start);
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        if justid {
            cmd.arg("JUSTID");
        }
        dispatch_cmd!(self, cmd)
    }
}
