// PyO3 driver wrapper for the Rust I/O driver.
//
// Constructors + connection helpers are adapted from django-vcache (MIT,
// David Burke / GlitchTip): https://gitlab.com/glitchtip/django-vcache/-/blob/main/src/client.rs
//
// The full sync + async command surface (~85 commands × 2 variants) is
// django-cachex's extension. Each command pair follows the same shape:
//   * `a<cmd>` — async; returns a `RedisRsAwaitable`. Spawns the operation on the
//     tokio runtime and routes through `RawResult` for type conversion.
//   * `<cmd>` — sync; releases the GIL and blocks on the runtime,
//     returning the typed Python value directly.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::async_bridge::{RawResult, RedisRsAwaitable, get_runtime};
use crate::connection::{
    ClientCacheOpts, TlsOpts, ValkeyConn, connect_cluster, connect_sentinel, connect_standard,
};

#[pyclass]
pub struct RedisRsDriver {
    connection: ValkeyConn,
}

// =========================================================================
// Async / sync invocation macros (mirrors vcache pattern)
// =========================================================================

// Async: spawn a tokio task that sends RawResult through a oneshot. The tokio
// task never touches the GIL — zero Python interaction. RedisRsAwaitable handles
// the asyncio future-blocking protocol.
macro_rules! async_op {
    ($self:expr, $py:expr, $conn:ident, $body:expr) => {{
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = RedisRsAwaitable::new(rx);
        let mut $conn = $self.connection.clone();
        get_runtime().spawn(async move {
            let result: RawResult = async { $body }.await;
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject($py)?.into_any().unbind())
    }};
}

// Sync: release GIL, block on tokio runtime.
macro_rules! sync_op {
    ($py:expr, $self:expr, $conn:ident, $body:expr) => {{
        let mut $conn = $self.connection.clone();
        $py.detach(|| get_runtime().block_on(async { $body }))
    }};
}

// =========================================================================
// Error classification
// =========================================================================

fn classify(e: redis::RedisError) -> RawResult {
    if is_connection_error(&e) {
        RawResult::Error(e.to_string())
    } else {
        RawResult::ServerError(e.to_string())
    }
}

fn to_py_err(e: redis::RedisError) -> PyErr {
    if is_connection_error(&e) {
        pyo3::exceptions::PyConnectionError::new_err(e.to_string())
    } else {
        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
    }
}

fn is_connection_error(e: &redis::RedisError) -> bool {
    matches!(
        e.kind(),
        redis::ErrorKind::Io
            | redis::ErrorKind::Server(redis::ServerErrorKind::BusyLoading)
            | redis::ErrorKind::Server(redis::ServerErrorKind::TryAgain)
            | redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly)
    ) || e.is_connection_dropped()
        || e.is_connection_refusal()
        || e.is_timeout()
}

// =========================================================================
// Result-to-RawResult conversion helpers (used by `async_op!` bodies)
// =========================================================================

fn r_opt_bytes(r: Result<Option<Vec<u8>>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::OptBytes(v),
        Err(e) => classify(e),
    }
}

fn r_bool(r: Result<bool, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::Bool(v),
        Err(e) => classify(e),
    }
}

fn r_int(r: Result<i64, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::Int(v),
        Err(e) => classify(e),
    }
}

fn r_opt_int(r: Result<Option<i64>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::OptInt(v),
        Err(e) => classify(e),
    }
}

fn r_f64(r: Result<f64, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::F64(v),
        Err(e) => classify(e),
    }
}

fn r_opt_f64(r: Result<Option<f64>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::OptF64(v),
        Err(e) => classify(e),
    }
}

fn r_unit(r: Result<(), redis::RedisError>) -> RawResult {
    // Sync siblings of these calls return PyResult<()> → Python None;
    // emit Nil here so async resolves to None too (parity).
    match r {
        Ok(()) => RawResult::Nil,
        Err(e) => classify(e),
    }
}

fn r_opt_bytes_list(r: Result<Vec<Option<Vec<u8>>>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::OptBytesList(v),
        Err(e) => classify(e),
    }
}

fn r_bytes_list(r: Result<Vec<Vec<u8>>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::BytesList(v),
        Err(e) => classify(e),
    }
}

fn r_string_list(r: Result<Vec<String>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::StringList(v),
        Err(e) => classify(e),
    }
}

fn r_bytes_pairs(r: Result<Vec<(Vec<u8>, Vec<u8>)>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::BytesPairs(v),
        Err(e) => classify(e),
    }
}

fn r_scored_members(r: Result<Vec<(Vec<u8>, f64)>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::ScoredMembers(v),
        Err(e) => classify(e),
    }
}

fn r_value(r: Result<redis::Value, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::Value(v),
        Err(e) => classify(e),
    }
}

fn r_string(r: Result<String, redis::RedisError>) -> RawResult {
    // Renders as Python str (matches sync siblings that return PyResult<String>).
    match r {
        Ok(v) => RawResult::Str(v),
        Err(e) => classify(e),
    }
}

fn r_opt_string(r: Result<Option<String>, redis::RedisError>) -> RawResult {
    match r {
        Ok(v) => RawResult::OptStr(v),
        Err(e) => classify(e),
    }
}

fn r_opt_key_and_bytes_list(
    r: Result<Option<(String, Vec<Vec<u8>>)>, redis::RedisError>,
) -> RawResult {
    match r {
        Ok(v) => RawResult::OptKeyAndBytesList(v),
        Err(e) => classify(e),
    }
}

fn r_opt_key_and_bytes(
    r: Result<Option<(String, Vec<u8>)>, redis::RedisError>,
) -> RawResult {
    match r {
        Ok(v) => RawResult::OptKeyAndBytes(v),
        Err(e) => classify(e),
    }
}

fn r_cursor_strings(
    r: Result<(u64, Vec<String>), redis::RedisError>,
) -> RawResult {
    match r {
        Ok((cursor, keys)) => RawResult::CursorAndStrings(cursor, keys),
        Err(e) => classify(e),
    }
}

// =========================================================================
// Sync-side conversion helpers (translate Rust types to Python)
// =========================================================================

fn py_opt_bytes(py: Python<'_>, v: Option<Vec<u8>>) -> Py<PyAny> {
    match v {
        Some(b) => PyBytes::new(py, &b).into_any().unbind(),
        None => py.None(),
    }
}

fn py_bytes_list(py: Python<'_>, items: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
    let py_items: Vec<Py<PyAny>> = items
        .into_iter()
        .map(|b| PyBytes::new(py, &b).into_any().unbind())
        .collect();
    Ok(PyList::new(py, py_items)?.into_any().unbind())
}

fn py_opt_bytes_list(py: Python<'_>, items: Vec<Option<Vec<u8>>>) -> PyResult<Py<PyAny>> {
    let py_items: Vec<Py<PyAny>> = items
        .into_iter()
        .map(|r| py_opt_bytes(py, r))
        .collect();
    Ok(PyList::new(py, py_items)?.into_any().unbind())
}

fn py_string_list(py: Python<'_>, items: Vec<String>) -> PyResult<Py<PyAny>> {
    let py_items: Vec<Py<PyAny>> = items
        .into_iter()
        .map(|s| pyo3::types::PyString::new(py, &s).into_any().unbind())
        .collect();
    Ok(PyList::new(py, py_items)?.into_any().unbind())
}

fn py_bytes_pairs(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (k, v) in pairs {
        let k_py = PyBytes::new(py, &k).into_any().unbind();
        let v_py = PyBytes::new(py, &v).into_any().unbind();
        dict.set_item(k_py, v_py)?;
    }
    Ok(dict.into_any().unbind())
}

fn py_scored_members(py: Python<'_>, items: Vec<(Vec<u8>, f64)>) -> PyResult<Py<PyAny>> {
    let py_items: Vec<Py<PyAny>> = items
        .into_iter()
        .map(|(member, score)| {
            let m_py = PyBytes::new(py, &member).into_any().unbind();
            let s_py = score.into_pyobject(py)?.into_any().unbind();
            Ok(PyTuple::new(py, [m_py, s_py])?.into_any().unbind())
        })
        .collect::<PyResult<_>>()?;
    Ok(PyList::new(py, py_items)?.into_any().unbind())
}

fn py_redis_value(py: Python<'_>, v: redis::Value) -> PyResult<Py<PyAny>> {
    // Reuse the conversion path that RawResult::Value goes through.
    let raw = RawResult::Value(v);
    raw.into_py(py)
}

fn py_opt_key_and_bytes(
    py: Python<'_>,
    v: Option<(String, Vec<u8>)>,
) -> PyResult<Py<PyAny>> {
    match v {
        Some((key, value)) => {
            let py_key = pyo3::types::PyString::new(py, &key).into_any().unbind();
            let py_value = PyBytes::new(py, &value).into_any().unbind();
            Ok(PyTuple::new(py, [py_key, py_value])?.into_any().unbind())
        }
        None => Ok(py.None()),
    }
}

fn py_cursor_strings(
    py: Python<'_>,
    cursor: u64,
    keys: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let py_cursor = cursor.into_pyobject(py)?.into_any().unbind();
    let py_items: Vec<Py<PyAny>> = keys
        .into_iter()
        .map(|s| pyo3::types::PyString::new(py, &s).into_any().unbind())
        .collect();
    let py_list = PyList::new(py, py_items)?.into_any().unbind();
    Ok(PyTuple::new(py, [py_cursor, py_list])?.into_any().unbind())
}

fn py_opt_key_and_bytes_list(
    py: Python<'_>,
    v: Option<(String, Vec<Vec<u8>>)>,
) -> PyResult<Py<PyAny>> {
    match v {
        Some((key, values)) => {
            let py_values: Vec<Py<PyAny>> = values
                .into_iter()
                .map(|b| PyBytes::new(py, &b).into_any().unbind())
                .collect();
            let py_key = pyo3::types::PyString::new(py, &key).into_any().unbind();
            let py_list = PyList::new(py, py_values)?.into_any().unbind();
            Ok(PyTuple::new(py, [py_key, py_list])?.into_any().unbind())
        }
        None => Ok(py.None()),
    }
}

// =========================================================================
// Connection-config helpers
// =========================================================================

fn make_cache_opts(max_size: Option<usize>, ttl_secs: Option<u64>) -> Option<ClientCacheOpts> {
    if max_size.is_some() || ttl_secs.is_some() {
        Some(ClientCacheOpts {
            max_size: max_size.unwrap_or(10_000),
            ttl_secs: ttl_secs.unwrap_or(300),
        })
    } else {
        None
    }
}

fn read_pem_file(param: &str, path: &str) -> PyResult<Vec<u8>> {
    std::fs::read(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            pyo3::exceptions::PyFileNotFoundError::new_err(format!("{param} {path}: {e}"))
        } else {
            pyo3::exceptions::PyOSError::new_err(format!("{param} {path}: {e}"))
        }
    })
}

fn make_tls_opts(
    ssl_ca_certs: Option<String>,
    ssl_certfile: Option<String>,
    ssl_keyfile: Option<String>,
) -> PyResult<Option<TlsOpts>> {
    if ssl_ca_certs.is_none() && ssl_certfile.is_none() && ssl_keyfile.is_none() {
        return Ok(None);
    }
    if ssl_certfile.is_some() != ssl_keyfile.is_some() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "ssl_certfile and ssl_keyfile must both be provided for mTLS",
        ));
    }
    let root_cert = ssl_ca_certs
        .map(|p| read_pem_file("ssl_ca_certs", &p))
        .transpose()?;
    let client_cert = ssl_certfile
        .map(|p| read_pem_file("ssl_certfile", &p))
        .transpose()?;
    let client_key = ssl_keyfile
        .map(|p| read_pem_file("ssl_keyfile", &p))
        .transpose()?;
    Ok(Some(TlsOpts {
        root_cert,
        client_cert,
        client_key,
    }))
}

#[pymethods]
impl RedisRsDriver {
    // =====================================================================
    // Connection constructors
    // =====================================================================

    #[staticmethod]
    #[pyo3(signature = (url, cache_max_size=None, cache_ttl_secs=None, ssl_ca_certs=None, ssl_certfile=None, ssl_keyfile=None))]
    fn connect_standard(
        py: Python<'_>,
        url: &str,
        cache_max_size: Option<usize>,
        cache_ttl_secs: Option<u64>,
        ssl_ca_certs: Option<String>,
        ssl_certfile: Option<String>,
        ssl_keyfile: Option<String>,
    ) -> PyResult<Self> {
        let url = url.to_string();
        let cache_opts = make_cache_opts(cache_max_size, cache_ttl_secs);
        let tls_opts = make_tls_opts(ssl_ca_certs, ssl_certfile, ssl_keyfile)?;
        let conn = py
            .detach(|| get_runtime().block_on(connect_standard(&url, cache_opts, tls_opts)))
            .map_err(pyo3::exceptions::PyConnectionError::new_err)?;
        Ok(Self { connection: conn })
    }

    #[staticmethod]
    #[pyo3(signature = (urls, ssl_ca_certs=None, ssl_certfile=None, ssl_keyfile=None))]
    fn connect_cluster(
        py: Python<'_>,
        urls: Vec<String>,
        ssl_ca_certs: Option<String>,
        ssl_certfile: Option<String>,
        ssl_keyfile: Option<String>,
    ) -> PyResult<Self> {
        let tls_opts = make_tls_opts(ssl_ca_certs, ssl_certfile, ssl_keyfile)?;
        let conn = py
            .detach(|| get_runtime().block_on(connect_cluster(urls, tls_opts)))
            .map_err(pyo3::exceptions::PyConnectionError::new_err)?;
        Ok(Self { connection: conn })
    }

    #[staticmethod]
    #[pyo3(signature = (sentinel_urls, service_name, db, cache_max_size=None, cache_ttl_secs=None, ssl_ca_certs=None, ssl_certfile=None, ssl_keyfile=None))]
    fn connect_sentinel(
        py: Python<'_>,
        sentinel_urls: Vec<String>,
        service_name: &str,
        db: i64,
        cache_max_size: Option<usize>,
        cache_ttl_secs: Option<u64>,
        ssl_ca_certs: Option<String>,
        ssl_certfile: Option<String>,
        ssl_keyfile: Option<String>,
    ) -> PyResult<Self> {
        let service_name = service_name.to_string();
        let cache_opts = make_cache_opts(cache_max_size, cache_ttl_secs);
        let tls_opts = make_tls_opts(ssl_ca_certs, ssl_certfile, ssl_keyfile)?;
        let conn = py
            .detach(|| {
                get_runtime().block_on(connect_sentinel(
                    sentinel_urls,
                    &service_name,
                    db,
                    cache_opts,
                    tls_opts,
                ))
            })
            .map_err(pyo3::exceptions::PyConnectionError::new_err)?;
        Ok(Self { connection: conn })
    }

    /// Returns (hits, misses, invalidations) or None if client-side caching disabled.
    fn cache_statistics(&self) -> Option<(usize, usize, usize)> {
        self.connection
            .cache_statistics()
            .map(|s| (s.hit, s.miss, s.invalidate))
    }

    // =====================================================================
    // Strings / counters / TTL
    // =====================================================================

    #[pyo3(signature = (key))]
    fn aget(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_opt_bytes(conn.get_bytes(&key).await))
    }

    #[pyo3(signature = (key))]
    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> = sync_op!(py, self, conn, conn.get_bytes(key).await);
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn aset(&self, py: Python<'_>, key: &str, value: &[u8], ttl: Option<u64>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, r_unit(conn.set_bytes(&key, value, ttl).await))
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn set(
        &self,
        py: Python<'_>,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> PyResult<()> {
        let value = value.to_vec();
        sync_op!(py, self, conn, conn.set_bytes(key, value, ttl).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn aset_nx(
        &self,
        py: Python<'_>,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, r_bool(conn.set_nx(&key, value, ttl).await))
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn set_nx(
        &self,
        py: Python<'_>,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> PyResult<bool> {
        let value = value.to_vec();
        sync_op!(py, self, conn, conn.set_nx(key, value, ttl).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn adelete(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.del(&key).await))
    }

    #[pyo3(signature = (key))]
    fn delete(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.del(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (keys))]
    fn adelete_many(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_int(conn.del_many(&keys).await))
    }

    #[pyo3(signature = (keys))]
    fn delete_many(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.del_many(&keys).await).map_err(to_py_err)
    }

    #[pyo3(signature = (keys))]
    fn amget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_opt_bytes_list(conn.mget_bytes(&keys).await))
    }

    #[pyo3(signature = (keys))]
    fn mget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Option<Vec<u8>>>, _> =
            sync_op!(py, self, conn, conn.mget_bytes(&keys).await);
        py_opt_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (entries, ttl=None))]
    fn apipeline_set(
        &self,
        py: Python<'_>,
        entries: Vec<(String, Vec<u8>)>,
        ttl: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_unit(conn.pipeline_set(&entries, ttl).await))
    }

    #[pyo3(signature = (entries, ttl=None))]
    fn pipeline_set(
        &self,
        py: Python<'_>,
        entries: Vec<(String, Vec<u8>)>,
        ttl: Option<u64>,
    ) -> PyResult<()> {
        sync_op!(py, self, conn, conn.pipeline_set(&entries, ttl).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, delta))]
    fn aincr_by(&self, py: Python<'_>, key: &str, delta: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.incr_by(&key, delta).await))
    }

    #[pyo3(signature = (key, delta))]
    fn incr_by(&self, py: Python<'_>, key: &str, delta: i64) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.incr_by(key, delta).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn aexists(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_bool(conn.exists(&key).await))
    }

    #[pyo3(signature = (key))]
    fn exists(&self, py: Python<'_>, key: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, conn.exists(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, seconds))]
    fn aexpire(&self, py: Python<'_>, key: &str, seconds: u64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_bool(conn.expire(&key, seconds).await))
    }

    #[pyo3(signature = (key, seconds))]
    fn expire(&self, py: Python<'_>, key: &str, seconds: u64) -> PyResult<bool> {
        sync_op!(py, self, conn, conn.expire(key, seconds).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn apersist(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_bool(conn.persist(&key).await))
    }

    #[pyo3(signature = (key))]
    fn persist(&self, py: Python<'_>, key: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, conn.persist(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn attl(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.ttl(&key).await))
    }

    #[pyo3(signature = (key))]
    fn ttl(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.ttl(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn apttl(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.pttl(&key).await))
    }

    #[pyo3(signature = (key))]
    fn pttl(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.pttl(key).await).map_err(to_py_err)
    }

    fn aflushdb(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_unit(conn.flushdb().await))
    }

    fn flushdb(&self, py: Python<'_>) -> PyResult<()> {
        sync_op!(py, self, conn, conn.flushdb().await).map_err(to_py_err)
    }

    #[pyo3(signature = (pattern))]
    fn akeys(&self, py: Python<'_>, pattern: &str) -> PyResult<Py<PyAny>> {
        let pattern = pattern.to_string();
        async_op!(self, py, conn, r_string_list(conn.keys(&pattern).await))
    }

    #[pyo3(signature = (pattern))]
    fn keys(&self, py: Python<'_>, pattern: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<String>, _> = sync_op!(py, self, conn, conn.keys(pattern).await);
        py_string_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn atype(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_string(conn.key_type(&key).await))
    }

    #[pyo3(signature = (key))]
    #[pyo3(name = "type")]
    fn key_type(&self, py: Python<'_>, key: &str) -> PyResult<String> {
        sync_op!(py, self, conn, conn.key_type(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (pattern, count))]
    fn ascan(&self, py: Python<'_>, pattern: &str, count: i64) -> PyResult<Py<PyAny>> {
        let pattern = pattern.to_string();
        async_op!(self, py, conn, r_string_list(conn.scan_all(&pattern, count).await))
    }

    #[pyo3(signature = (pattern, count))]
    fn scan(&self, py: Python<'_>, pattern: &str, count: i64) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<String>, _> =
            sync_op!(py, self, conn, conn.scan_all(pattern, count).await);
        py_string_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (cursor, pattern, count))]
    fn ascan_one(
        &self,
        py: Python<'_>,
        cursor: u64,
        pattern: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        let pattern = pattern.to_string();
        async_op!(
            self,
            py,
            conn,
            r_cursor_strings(conn.scan_one(cursor, &pattern, count).await)
        )
    }

    #[pyo3(signature = (cursor, pattern, count))]
    fn scan_one(
        &self,
        py: Python<'_>,
        cursor: u64,
        pattern: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<(u64, Vec<String>), _> =
            sync_op!(py, self, conn, conn.scan_one(cursor, pattern, count).await);
        let (next_cursor, keys) = r.map_err(to_py_err)?;
        py_cursor_strings(py, next_cursor, keys)
    }

    fn adbsize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_int(conn.dbsize().await))
    }

    fn dbsize(&self, py: Python<'_>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.dbsize().await).map_err(to_py_err)
    }

    fn ainfo(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_value(conn.info().await))
    }

    fn info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, conn.info().await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    fn aclient_list(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_value(conn.client_list().await))
    }

    fn client_list(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, conn.client_list().await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (parameter))]
    fn aconfig_get(&self, py: Python<'_>, parameter: &str) -> PyResult<Py<PyAny>> {
        let parameter = parameter.to_string();
        async_op!(self, py, conn, r_value(conn.config_get(&parameter).await))
    }

    #[pyo3(signature = (parameter))]
    fn config_get(&self, py: Python<'_>, parameter: &str) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> =
            sync_op!(py, self, conn, conn.config_get(parameter).await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn aobject_encoding(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_opt_string(conn.object_encoding(&key).await))
    }

    #[pyo3(signature = (key))]
    fn object_encoding(&self, py: Python<'_>, key: &str) -> PyResult<Option<String>> {
        sync_op!(py, self, conn, conn.object_encoding(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn aobject_idletime(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_opt_int(conn.object_idletime(&key).await))
    }

    #[pyo3(signature = (key))]
    fn object_idletime(&self, py: Python<'_>, key: &str) -> PyResult<Option<i64>> {
        sync_op!(py, self, conn, conn.object_idletime(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn amemory_usage(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_opt_int(conn.memory_usage(&key).await))
    }

    #[pyo3(signature = (key))]
    fn memory_usage(&self, py: Python<'_>, key: &str) -> PyResult<Option<i64>> {
        sync_op!(py, self, conn, conn.memory_usage(key).await).map_err(to_py_err)
    }

    // =====================================================================
    // Locks
    // =====================================================================

    #[pyo3(signature = (key, token, timeout_ms=None))]
    fn alock_acquire(
        &self,
        py: Python<'_>,
        key: &str,
        token: &str,
        timeout_ms: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let token = token.to_string();
        async_op!(self, py, conn, {
            r_bool(conn.lock_acquire(&key, &token, timeout_ms).await)
        })
    }

    #[pyo3(signature = (key, token, timeout_ms=None))]
    fn lock_acquire(
        &self,
        py: Python<'_>,
        key: &str,
        token: &str,
        timeout_ms: Option<u64>,
    ) -> PyResult<bool> {
        sync_op!(py, self, conn, conn.lock_acquire(key, token, timeout_ms).await)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (key, token))]
    fn alock_release(&self, py: Python<'_>, key: &str, token: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let token = token.to_string();
        async_op!(self, py, conn, r_int(conn.lock_release(&key, &token).await))
    }

    #[pyo3(signature = (key, token))]
    fn lock_release(&self, py: Python<'_>, key: &str, token: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.lock_release(key, token).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, token, additional_ms))]
    fn alock_extend(
        &self,
        py: Python<'_>,
        key: &str,
        token: &str,
        additional_ms: u64,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let token = token.to_string();
        async_op!(self, py, conn, {
            r_int(conn.lock_extend(&key, &token, additional_ms).await)
        })
    }

    #[pyo3(signature = (key, token, additional_ms))]
    fn lock_extend(
        &self,
        py: Python<'_>,
        key: &str,
        token: &str,
        additional_ms: u64,
    ) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.lock_extend(key, token, additional_ms).await)
            .map_err(to_py_err)
    }

    // =====================================================================
    // Scripts
    // =====================================================================

    #[pyo3(signature = (script, keys, args))]
    fn aeval(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let script = script.to_string();
        async_op!(self, py, conn, r_value(conn.eval(&script, &keys, &args).await))
    }

    #[pyo3(signature = (script, keys, args))]
    fn eval(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> =
            sync_op!(py, self, conn, conn.eval(script, &keys, &args).await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (sha, keys, args))]
    fn aevalsha(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let sha = sha.to_string();
        async_op!(self, py, conn, r_value(conn.evalsha(&sha, &keys, &args).await))
    }

    #[pyo3(signature = (sha, keys, args))]
    fn evalsha(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> =
            sync_op!(py, self, conn, conn.evalsha(sha, &keys, &args).await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (script))]
    fn ascript_load(&self, py: Python<'_>, script: &str) -> PyResult<Py<PyAny>> {
        let script = script.to_string();
        async_op!(self, py, conn, r_string(conn.script_load(&script).await))
    }

    #[pyo3(signature = (script))]
    fn script_load(&self, py: Python<'_>, script: &str) -> PyResult<String> {
        sync_op!(py, self, conn, conn.script_load(script).await).map_err(to_py_err)
    }

    #[pyo3(signature = (sha))]
    fn ascript_exists(&self, py: Python<'_>, sha: &str) -> PyResult<Py<PyAny>> {
        let sha = sha.to_string();
        async_op!(self, py, conn, r_bool(conn.script_exists(&sha).await))
    }

    #[pyo3(signature = (sha))]
    fn script_exists(&self, py: Python<'_>, sha: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, conn.script_exists(sha).await).map_err(to_py_err)
    }

    // =====================================================================
    // Pipeline (arbitrary commands)
    // =====================================================================

    #[pyo3(signature = (commands, transaction = false))]
    fn apipeline_exec(
        &self,
        py: Python<'_>,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, {
            match conn.pipeline_exec(commands, transaction).await {
                Ok(items) => RawResult::Value(redis::Value::Array(items)),
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (commands, transaction = false))]
    fn pipeline_exec(
        &self,
        py: Python<'_>,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<redis::Value>, _> =
            sync_op!(py, self, conn, conn.pipeline_exec(commands, transaction).await);
        py_redis_value(py, redis::Value::Array(r.map_err(to_py_err)?))
    }

    // =====================================================================
    // Lists
    // =====================================================================

    #[pyo3(signature = (key, values))]
    fn alpush(&self, py: Python<'_>, key: &str, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.lpush(&key, values).await))
    }

    #[pyo3(signature = (key, values))]
    fn lpush(&self, py: Python<'_>, key: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.lpush(key, values).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, values))]
    fn arpush(&self, py: Python<'_>, key: &str, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.rpush(&key, values).await))
    }

    #[pyo3(signature = (key, values))]
    fn rpush(&self, py: Python<'_>, key: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.rpush(key, values).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn alpop(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_opt_bytes(conn.lpop(&key).await))
    }

    #[pyo3(signature = (key))]
    fn lpop(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> = sync_op!(py, self, conn, conn.lpop(key).await);
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (key))]
    fn arpop(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_opt_bytes(conn.rpop(&key).await))
    }

    #[pyo3(signature = (key))]
    fn rpop(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> = sync_op!(py, self, conn, conn.rpop(key).await);
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (key, start, stop))]
    fn alrange(&self, py: Python<'_>, key: &str, start: i64, stop: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_bytes_list(conn.lrange(&key, start, stop).await))
    }

    #[pyo3(signature = (key, start, stop))]
    fn lrange(&self, py: Python<'_>, key: &str, start: i64, stop: i64) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn.lrange(key, start, stop).await);
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn allen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.llen(&key).await))
    }

    #[pyo3(signature = (key))]
    fn llen(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.llen(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, count, value))]
    fn alrem(&self, py: Python<'_>, key: &str, count: i64, value: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, r_int(conn.lrem(&key, count, &value).await))
    }

    #[pyo3(signature = (key, count, value))]
    fn lrem(&self, py: Python<'_>, key: &str, count: i64, value: &[u8]) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.lrem(key, count, value).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, index))]
    fn alindex(&self, py: Python<'_>, key: &str, index: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_opt_bytes(conn.lindex(&key, index).await))
    }

    #[pyo3(signature = (key, index))]
    fn lindex(&self, py: Python<'_>, key: &str, index: i64) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> = sync_op!(py, self, conn, conn.lindex(key, index).await);
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (key, index, value))]
    fn alset(&self, py: Python<'_>, key: &str, index: i64, value: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, r_unit(conn.lset(&key, index, &value).await))
    }

    #[pyo3(signature = (key, index, value))]
    fn lset(&self, py: Python<'_>, key: &str, index: i64, value: &[u8]) -> PyResult<()> {
        sync_op!(py, self, conn, conn.lset(key, index, value).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, before, pivot, value))]
    fn alinsert(
        &self,
        py: Python<'_>,
        key: &str,
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let pivot = pivot.to_vec();
        let value = value.to_vec();
        async_op!(self, py, conn, r_int(conn.linsert(&key, before, &pivot, &value).await))
    }

    #[pyo3(signature = (key, before, pivot, value))]
    fn linsert(
        &self,
        py: Python<'_>,
        key: &str,
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.linsert(key, before, pivot, value).await)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (key, start, stop))]
    fn altrim(&self, py: Python<'_>, key: &str, start: i64, stop: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_unit(conn.ltrim(&key, start, stop).await))
    }

    #[pyo3(signature = (key, start, stop))]
    fn ltrim(&self, py: Python<'_>, key: &str, start: i64, stop: i64) -> PyResult<()> {
        sync_op!(py, self, conn, conn.ltrim(key, start, stop).await).map_err(to_py_err)
    }

    #[pyo3(signature = (source, destination, wherefrom, whereto, timeout))]
    fn ablmove(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        wherefrom: &str,
        whereto: &str,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let source = source.to_string();
        let destination = destination.to_string();
        let wherefrom = wherefrom.to_string();
        let whereto = whereto.to_string();
        async_op!(self, py, conn, {
            r_opt_bytes(conn.blmove(&source, &destination, &wherefrom, &whereto, timeout).await)
        })
    }

    #[pyo3(signature = (source, destination, wherefrom, whereto, timeout))]
    fn blmove(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        wherefrom: &str,
        whereto: &str,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> = sync_op!(
            py,
            self,
            conn,
            conn.blmove(source, destination, wherefrom, whereto, timeout).await
        );
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (timeout, keys, direction, count))]
    fn ablmpop(
        &self,
        py: Python<'_>,
        timeout: f64,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        let direction = direction.to_string();
        async_op!(self, py, conn, {
            r_opt_key_and_bytes_list(conn.blmpop(timeout, &keys, &direction, count).await)
        })
    }

    #[pyo3(signature = (timeout, keys, direction, count))]
    fn blmpop(
        &self,
        py: Python<'_>,
        timeout: f64,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Option<(String, Vec<Vec<u8>>)>, _> = sync_op!(
            py,
            self,
            conn,
            conn.blmpop(timeout, &keys, direction, count).await
        );
        py_opt_key_and_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (keys, timeout))]
    fn ablpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_opt_key_and_bytes(conn.blpop(&keys, timeout).await))
    }

    #[pyo3(signature = (keys, timeout))]
    fn blpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Option<(String, Vec<u8>)>, _> = sync_op!(
            py,
            self,
            conn,
            conn.blpop(&keys, timeout).await
        );
        py_opt_key_and_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (keys, timeout))]
    fn abrpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_opt_key_and_bytes(conn.brpop(&keys, timeout).await))
    }

    #[pyo3(signature = (keys, timeout))]
    fn brpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Option<(String, Vec<u8>)>, _> = sync_op!(
            py,
            self,
            conn,
            conn.brpop(&keys, timeout).await
        );
        py_opt_key_and_bytes(py, r.map_err(to_py_err)?)
    }

    // =====================================================================
    // Hashes
    // =====================================================================

    #[pyo3(signature = (key, field))]
    fn ahget(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, r_opt_bytes(conn.hget(&key, &field).await))
    }

    #[pyo3(signature = (key, field))]
    fn hget(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> = sync_op!(py, self, conn, conn.hget(key, field).await);
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (key, field, value))]
    fn ahset(&self, py: Python<'_>, key: &str, field: &str, value: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, r_int(conn.hset(&key, &field, &value).await))
    }

    #[pyo3(signature = (key, field, value))]
    fn hset(&self, py: Python<'_>, key: &str, field: &str, value: &[u8]) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.hset(key, field, value).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn ahgetall(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_bytes_pairs(conn.hgetall(&key).await))
    }

    #[pyo3(signature = (key))]
    fn hgetall(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<(Vec<u8>, Vec<u8>)>, _> =
            sync_op!(py, self, conn, conn.hgetall(key).await);
        py_bytes_pairs(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields))]
    fn ahdel(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.hdel(&key, &fields).await))
    }

    #[pyo3(signature = (key, fields))]
    fn hdel(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.hdel(key, &fields).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, field, delta))]
    fn ahincrby(&self, py: Python<'_>, key: &str, field: &str, delta: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, r_int(conn.hincrby(&key, &field, delta).await))
    }

    #[pyo3(signature = (key, field, delta))]
    fn hincrby(&self, py: Python<'_>, key: &str, field: &str, delta: i64) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.hincrby(key, field, delta).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn ahkeys(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_string_list(conn.hkeys(&key).await))
    }

    #[pyo3(signature = (key))]
    fn hkeys(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<String>, _> = sync_op!(py, self, conn, conn.hkeys(key).await);
        py_string_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahvals(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_bytes_list(conn.hvals(&key).await))
    }

    #[pyo3(signature = (key))]
    fn hvals(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> = sync_op!(py, self, conn, conn.hvals(key).await);
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, field))]
    fn ahexists(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, r_bool(conn.hexists(&key, &field).await))
    }

    #[pyo3(signature = (key, field))]
    fn hexists(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, conn.hexists(key, field).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn ahlen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.hlen(&key).await))
    }

    #[pyo3(signature = (key))]
    fn hlen(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.hlen(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, fields))]
    fn ahmget(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_opt_bytes_list(conn.hmget(&key, &fields).await))
    }

    #[pyo3(signature = (key, fields))]
    fn hmget(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Option<Vec<u8>>>, _> =
            sync_op!(py, self, conn, conn.hmget(key, &fields).await);
        py_opt_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields))]
    fn ahmset(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<(String, Vec<u8>)>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_unit(conn.hmset(&key, &fields).await))
    }

    #[pyo3(signature = (key, fields))]
    fn hmset(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<(String, Vec<u8>)>,
    ) -> PyResult<()> {
        sync_op!(py, self, conn, conn.hmset(key, &fields).await).map_err(to_py_err)
    }

    // =====================================================================
    // Sets
    // =====================================================================

    #[pyo3(signature = (key, members))]
    fn asadd(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.sadd(&key, members).await))
    }

    #[pyo3(signature = (key, members))]
    fn sadd(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.sadd(key, members).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, members))]
    fn asrem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.srem(&key, members).await))
    }

    #[pyo3(signature = (key, members))]
    fn srem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.srem(key, members).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn asmembers(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_bytes_list(conn.smembers(&key).await))
    }

    #[pyo3(signature = (key))]
    fn smembers(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> = sync_op!(py, self, conn, conn.smembers(key).await);
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, member))]
    fn asismember(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, r_bool(conn.sismember(&key, &member).await))
    }

    #[pyo3(signature = (key, member))]
    fn sismember(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<bool> {
        sync_op!(py, self, conn, conn.sismember(key, member).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn ascard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.scard(&key).await))
    }

    #[pyo3(signature = (key))]
    fn scard(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.scard(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (keys))]
    fn asinter(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_bytes_list(conn.sinter(&keys).await))
    }

    #[pyo3(signature = (keys))]
    fn sinter(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> = sync_op!(py, self, conn, conn.sinter(&keys).await);
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (keys))]
    fn asunion(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_bytes_list(conn.sunion(&keys).await))
    }

    #[pyo3(signature = (keys))]
    fn sunion(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> = sync_op!(py, self, conn, conn.sunion(&keys).await);
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (keys))]
    fn asdiff(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_bytes_list(conn.sdiff(&keys).await))
    }

    #[pyo3(signature = (keys))]
    fn sdiff(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> = sync_op!(py, self, conn, conn.sdiff(&keys).await);
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    // =====================================================================
    // Sorted sets
    // =====================================================================

    #[pyo3(signature = (key, members))]
    fn azadd(
        &self,
        py: Python<'_>,
        key: &str,
        members: Vec<(Vec<u8>, f64)>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.zadd(&key, members).await))
    }

    #[pyo3(signature = (key, members))]
    fn zadd(
        &self,
        py: Python<'_>,
        key: &str,
        members: Vec<(Vec<u8>, f64)>,
    ) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.zadd(key, members).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, members))]
    fn azrem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.zrem(&key, members).await))
    }

    #[pyo3(signature = (key, members))]
    fn zrem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.zrem(key, members).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, start, stop, with_scores=false))]
    fn azrange(
        &self,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, {
            r_value(conn.zrange(&key, start, stop, with_scores).await)
        })
    }

    #[pyo3(signature = (key, start, stop, with_scores=false))]
    fn zrange(
        &self,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> =
            sync_op!(py, self, conn, conn.zrange(key, start, stop, with_scores).await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, min, max, with_scores=false))]
    fn azrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        with_scores: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let min = min.to_string();
        let max = max.to_string();
        async_op!(self, py, conn, {
            r_value(conn.zrangebyscore(&key, &min, &max, with_scores).await)
        })
    }

    #[pyo3(signature = (key, min, max, with_scores=false))]
    fn zrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        with_scores: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(
            py,
            self,
            conn,
            conn.zrangebyscore(key, min, max, with_scores).await
        );
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, start, stop, with_scores=false))]
    fn azrevrange(
        &self,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, {
            r_value(conn.zrevrange(&key, start, stop, with_scores).await)
        })
    }

    #[pyo3(signature = (key, start, stop, with_scores=false))]
    fn zrevrange(
        &self,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(
            py,
            self,
            conn,
            conn.zrevrange(key, start, stop, with_scores).await
        );
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, member, delta))]
    fn azincrby(&self, py: Python<'_>, key: &str, member: &[u8], delta: f64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, r_f64(conn.zincrby(&key, &member, delta).await))
    }

    #[pyo3(signature = (key, member, delta))]
    fn zincrby(&self, py: Python<'_>, key: &str, member: &[u8], delta: f64) -> PyResult<f64> {
        sync_op!(py, self, conn, conn.zincrby(key, member, delta).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn azcard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.zcard(&key).await))
    }

    #[pyo3(signature = (key))]
    fn zcard(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.zcard(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, member))]
    fn azscore(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, r_opt_f64(conn.zscore(&key, &member).await))
    }

    #[pyo3(signature = (key, member))]
    fn zscore(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Option<f64>> {
        sync_op!(py, self, conn, conn.zscore(key, member).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, member))]
    fn azrank(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, r_opt_int(conn.zrank(&key, &member).await))
    }

    #[pyo3(signature = (key, member))]
    fn zrank(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Option<i64>> {
        sync_op!(py, self, conn, conn.zrank(key, member).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, min, max))]
    fn azcount(&self, py: Python<'_>, key: &str, min: &str, max: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let min = min.to_string();
        let max = max.to_string();
        async_op!(self, py, conn, r_int(conn.zcount(&key, &min, &max).await))
    }

    #[pyo3(signature = (key, min, max))]
    fn zcount(&self, py: Python<'_>, key: &str, min: &str, max: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.zcount(key, min, max).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, count=1))]
    fn azpopmin(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_scored_members(conn.zpopmin(&key, count).await))
    }

    #[pyo3(signature = (key, count=1))]
    fn zpopmin(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<(Vec<u8>, f64)>, _> =
            sync_op!(py, self, conn, conn.zpopmin(key, count).await);
        py_scored_members(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, count=1))]
    fn azpopmax(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_scored_members(conn.zpopmax(&key, count).await))
    }

    #[pyo3(signature = (key, count=1))]
    fn zpopmax(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<(Vec<u8>, f64)>, _> =
            sync_op!(py, self, conn, conn.zpopmax(key, count).await);
        py_scored_members(py, r.map_err(to_py_err)?)
    }

    // =====================================================================
    // Streams
    // =====================================================================

    #[pyo3(signature = (key, id, fields))]
    fn axadd(
        &self,
        py: Python<'_>,
        key: &str,
        id: &str,
        fields: Vec<(String, Vec<u8>)>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let id = id.to_string();
        async_op!(self, py, conn, r_string(conn.xadd(&key, &id, &fields).await))
    }

    #[pyo3(signature = (key, id, fields))]
    fn xadd(
        &self,
        py: Python<'_>,
        key: &str,
        id: &str,
        fields: Vec<(String, Vec<u8>)>,
    ) -> PyResult<String> {
        sync_op!(py, self, conn, conn.xadd(key, id, &fields).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn axlen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.xlen(&key).await))
    }

    #[pyo3(signature = (key))]
    fn xlen(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.xlen(key).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, start, end, count=None))]
    fn axrange(
        &self,
        py: Python<'_>,
        key: &str,
        start: &str,
        end: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let start = start.to_string();
        let end = end.to_string();
        async_op!(self, py, conn, r_value(conn.xrange(&key, &start, &end, count).await))
    }

    #[pyo3(signature = (key, start, end, count=None))]
    fn xrange(
        &self,
        py: Python<'_>,
        key: &str,
        start: &str,
        end: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> =
            sync_op!(py, self, conn, conn.xrange(key, start, end, count).await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (keys, ids, count=None))]
    fn axread(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        ids: Vec<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, r_value(conn.xread(&keys, &ids, count).await))
    }

    #[pyo3(signature = (keys, ids, count=None))]
    fn xread(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        ids: Vec<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> =
            sync_op!(py, self, conn, conn.xread(&keys, &ids, count).await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (group, consumer, keys, ids, count=None))]
    fn axreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        keys: Vec<String>,
        ids: Vec<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let group = group.to_string();
        let consumer = consumer.to_string();
        async_op!(self, py, conn, {
            r_value(conn.xreadgroup(&group, &consumer, &keys, &ids, count).await)
        })
    }

    #[pyo3(signature = (group, consumer, keys, ids, count=None))]
    fn xreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        keys: Vec<String>,
        ids: Vec<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(
            py,
            self,
            conn,
            conn.xreadgroup(group, consumer, &keys, &ids, count).await
        );
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, group, ids))]
    fn axack(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        ids: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        async_op!(self, py, conn, r_int(conn.xack(&key, &group, &ids).await))
    }

    #[pyo3(signature = (key, group, ids))]
    fn xack(&self, py: Python<'_>, key: &str, group: &str, ids: Vec<String>) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.xack(key, group, &ids).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, max_len, approximate=false))]
    fn axtrim(
        &self,
        py: Python<'_>,
        key: &str,
        max_len: i64,
        approximate: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, r_int(conn.xtrim(&key, max_len, approximate).await))
    }

    #[pyo3(signature = (key, max_len, approximate=false))]
    fn xtrim(
        &self,
        py: Python<'_>,
        key: &str,
        max_len: i64,
        approximate: bool,
    ) -> PyResult<i64> {
        sync_op!(py, self, conn, conn.xtrim(key, max_len, approximate).await).map_err(to_py_err)
    }

    #[pyo3(signature = (key, group, id, mkstream=false))]
    fn axgroup_create(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let id = id.to_string();
        async_op!(self, py, conn, {
            r_unit(conn.xgroup_create(&key, &group, &id, mkstream).await)
        })
    }

    #[pyo3(signature = (key, group, id, mkstream=false))]
    fn xgroup_create(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> PyResult<()> {
        sync_op!(py, self, conn, conn.xgroup_create(key, group, id, mkstream).await)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (key, group))]
    fn axpending(&self, py: Python<'_>, key: &str, group: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        async_op!(self, py, conn, r_value(conn.xpending(&key, &group).await))
    }

    #[pyo3(signature = (key, group))]
    fn xpending(&self, py: Python<'_>, key: &str, group: &str) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, conn.xpending(key, group).await);
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, group, start, end, count, consumer=None, idle=None))]
    #[allow(clippy::too_many_arguments)]
    fn axpending_range(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        start: &str,
        end: &str,
        count: i64,
        consumer: Option<String>,
        idle: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let start = start.to_string();
        let end = end.to_string();
        async_op!(self, py, conn, {
            r_value(
                conn.xpending_range(&key, &group, &start, &end, count, consumer.as_deref(), idle)
                    .await,
            )
        })
    }

    #[pyo3(signature = (key, group, start, end, count, consumer=None, idle=None))]
    #[allow(clippy::too_many_arguments)]
    fn xpending_range(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        start: &str,
        end: &str,
        count: i64,
        consumer: Option<String>,
        idle: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(
            py,
            self,
            conn,
            conn.xpending_range(key, group, start, end, count, consumer.as_deref(), idle)
                .await
        );
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, ids, idle=None, time_ms=None, retrycount=None, force=false, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn axclaim(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        ids: Vec<String>,
        idle: Option<i64>,
        time_ms: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        async_op!(self, py, conn, {
            r_value(
                conn.xclaim(
                    &key,
                    &group,
                    &consumer,
                    min_idle_time,
                    &ids,
                    idle,
                    time_ms,
                    retrycount,
                    force,
                    justid,
                )
                .await,
            )
        })
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, ids, idle=None, time_ms=None, retrycount=None, force=false, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn xclaim(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        ids: Vec<String>,
        idle: Option<i64>,
        time_ms: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(
            py,
            self,
            conn,
            conn.xclaim(
                key,
                group,
                consumer,
                min_idle_time,
                &ids,
                idle,
                time_ms,
                retrycount,
                force,
                justid,
            )
            .await
        );
        py_redis_value(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, start, count=None, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn axautoclaim(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        start: &str,
        count: Option<i64>,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        let start = start.to_string();
        async_op!(self, py, conn, {
            r_value(
                conn.xautoclaim(&key, &group, &consumer, min_idle_time, &start, count, justid)
                    .await,
            )
        })
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, start, count=None, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn xautoclaim(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        start: &str,
        count: Option<i64>,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(
            py,
            self,
            conn,
            conn.xautoclaim(key, group, consumer, min_idle_time, start, count, justid)
                .await
        );
        py_redis_value(py, r.map_err(to_py_err)?)
    }
}
