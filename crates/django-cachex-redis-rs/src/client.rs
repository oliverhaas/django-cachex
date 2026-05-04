// Shared error / result plumbing for the Rust adapter.
//
// Originally a PyO3 driver wrapper (kept the GIL-aware sync/async
// macro layer) adapted from django-vcache (MIT, David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache/-/blob/main/src/client.rs
//
// Now reduced to the cross-cutting helpers ``adapter.rs`` uses:
//   * Connection-config types (``ClientCacheOpts``, ``TlsOpts``) and
//     the ``make_*`` factories that translate Python OPTIONS into them.
//   * Error classification — ``classify``, ``to_py_err``,
//     ``is_connection_error`` — splits Redis errors into
//     ``ConnectionError`` (retryable / transport-level) vs
//     ``RuntimeError`` (server-side).
//   * ``IntoRawResult`` + ``From`` impls — converts every typed
//     ``RedisResult<T>`` into a ``RawResult`` variant for the
//     awaitable bridge.
//   * Sync-side conversion helpers (``py_redis_value``,
//     ``py_scored_members``, ``py_cursor_strings``) used by adapter
//     methods on the sync return path.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

use crate::async_bridge::RawResult;
use crate::connection::{ClientCacheOpts, TlsOpts};

// =========================================================================
// Error classification
// =========================================================================

pub(crate) fn classify(e: redis::RedisError) -> RawResult {
    if is_connection_error(&e) {
        RawResult::Error(e.to_string())
    } else {
        RawResult::ServerError(e.to_string())
    }
}

pub(crate) fn to_py_err(e: redis::RedisError) -> PyErr {
    if is_connection_error(&e) {
        pyo3::exceptions::PyConnectionError::new_err(e.to_string())
    } else {
        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
    }
}

pub(crate) fn is_connection_error(e: &redis::RedisError) -> bool {
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
// Result → RawResult conversion
// =========================================================================
//
// `async_op!` bodies emit `RawResult`. Every command returns
// `Result<T, redis::RedisError>` for some `T` matching a `RawResult` variant;
// the trait below routes either side: `Ok(v)` goes through a per-`T` `From`
// impl into the corresponding variant, `Err(e)` flows through `classify`.

pub(crate) trait IntoRawResult {
    fn into_raw_result(self) -> RawResult;
}

impl<T: Into<RawResult>> IntoRawResult for Result<T, redis::RedisError> {
    fn into_raw_result(self) -> RawResult {
        match self {
            Ok(v) => v.into(),
            Err(e) => classify(e),
        }
    }
}

// `()` maps to `Nil` so async siblings of sync `-> PyResult<()>` calls render
// as Python `None` (parity).
impl From<()> for RawResult {
    fn from((): ()) -> Self {
        Self::Nil
    }
}
impl From<bool> for RawResult {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<i64> for RawResult {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<f64> for RawResult {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}
impl From<String> for RawResult {
    fn from(v: String) -> Self {
        Self::Str(v)
    }
}
impl From<Option<i64>> for RawResult {
    fn from(v: Option<i64>) -> Self {
        Self::OptInt(v)
    }
}
impl From<Option<f64>> for RawResult {
    fn from(v: Option<f64>) -> Self {
        Self::OptF64(v)
    }
}
impl From<Option<Vec<u8>>> for RawResult {
    fn from(v: Option<Vec<u8>>) -> Self {
        Self::OptBytes(v)
    }
}
impl From<Option<String>> for RawResult {
    fn from(v: Option<String>) -> Self {
        Self::OptStr(v)
    }
}
impl From<Vec<Option<Vec<u8>>>> for RawResult {
    fn from(v: Vec<Option<Vec<u8>>>) -> Self {
        Self::OptBytesList(v)
    }
}
impl From<Vec<Vec<u8>>> for RawResult {
    fn from(v: Vec<Vec<u8>>) -> Self {
        Self::BytesList(v)
    }
}
impl From<Vec<String>> for RawResult {
    fn from(v: Vec<String>) -> Self {
        Self::StringList(v)
    }
}
impl From<Vec<(Vec<u8>, Vec<u8>)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self::BytesPairs(v)
    }
}
impl From<Vec<(Vec<u8>, f64)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, f64)>) -> Self {
        Self::ScoredMembers(v)
    }
}
impl From<Option<(String, Vec<Vec<u8>>)>> for RawResult {
    fn from(v: Option<(String, Vec<Vec<u8>>)>) -> Self {
        Self::OptKeyAndBytesList(v)
    }
}
impl From<Option<(String, Vec<u8>)>> for RawResult {
    fn from(v: Option<(String, Vec<u8>)>) -> Self {
        Self::OptKeyAndBytes(v)
    }
}
impl From<(u64, Vec<String>)> for RawResult {
    fn from((cursor, keys): (u64, Vec<String>)) -> Self {
        Self::CursorAndStrings(cursor, keys)
    }
}
impl From<(u64, Vec<Vec<u8>>)> for RawResult {
    fn from((cursor, members): (u64, Vec<Vec<u8>>)) -> Self {
        Self::CursorAndBytes(cursor, members)
    }
}
impl From<redis::Value> for RawResult {
    fn from(v: redis::Value) -> Self {
        Self::Value(v)
    }
}

// =========================================================================
// Sync-side conversion helpers (translate Rust types to Python)
// =========================================================================

pub(crate) fn py_scored_members(py: Python<'_>, items: Vec<(Vec<u8>, f64)>) -> PyResult<Py<PyAny>> {
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

pub(crate) fn py_redis_value(py: Python<'_>, v: redis::Value) -> PyResult<Py<PyAny>> {
    // Reuse the conversion path that RawResult::Value goes through.
    let raw = RawResult::Value(v);
    raw.into_py(py)
}


pub(crate) fn py_cursor_strings(
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


// =========================================================================
// Connection-config helpers
// =========================================================================

pub(crate) fn make_cache_opts(max_size: Option<usize>, ttl_secs: Option<u64>) -> Option<ClientCacheOpts> {
    if max_size.is_some() || ttl_secs.is_some() {
        Some(ClientCacheOpts {
            max_size: max_size.unwrap_or(10_000),
            ttl_secs: ttl_secs.unwrap_or(300),
        })
    } else {
        None
    }
}

pub(crate) fn read_pem_file(param: &str, path: &str) -> PyResult<Vec<u8>> {
    std::fs::read(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            pyo3::exceptions::PyFileNotFoundError::new_err(format!("{param} {path}: {e}"))
        } else {
            pyo3::exceptions::PyOSError::new_err(format!("{param} {path}: {e}"))
        }
    })
}

pub(crate) fn make_tls_opts(
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
