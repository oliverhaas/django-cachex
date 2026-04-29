// PyO3 driver wrapper for the Rust I/O driver.
//
// Adapted from django-vcache (MIT, by David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache/-/blob/main/src/client.rs
//
// Issue #65 lands only the connection layer + a slim sync surface
// (`set_sync`, `get_sync`, `flushdb_sync`) needed to verify connectivity.
// The full sync + async command surface is added in issue #66.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::async_bridge::get_runtime;
use crate::connection::{
    ClientCacheOpts, TlsOpts, ValkeyConn, connect_cluster, connect_sentinel, connect_standard,
};

#[pyclass]
pub struct RustValkeyDriver {
    connection: ValkeyConn,
}

// Sync: release GIL, block on tokio runtime.
macro_rules! sync_op {
    ($py:expr, $self:expr, $conn:ident, $body:expr) => {{
        let mut $conn = $self.connection.clone();
        $py.detach(|| get_runtime().block_on(async { $body }))
    }};
}

/// Classify a Redis error: connection/transient → PyConnectionError
/// (swallowable by IGNORE_EXCEPTIONS), everything else → PyRuntimeError.
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
impl RustValkeyDriver {
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

    // =========================================================================
    // Slim test surface — full command surface comes in issue #66.
    // =========================================================================

    #[pyo3(signature = (key))]
    fn get_sync(&self, py: Python<'_>, key: &str) -> PyResult<Option<Py<PyAny>>> {
        let result: Result<Option<Vec<u8>>, _> = sync_op!(py, self, conn, conn.get_bytes(key).await);
        match result.map_err(to_py_err)? {
            Some(bytes) => Ok(Some(PyBytes::new(py, &bytes).into_any().unbind())),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn set_sync(
        &self,
        py: Python<'_>,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> PyResult<()> {
        let value = value.to_vec();
        sync_op!(py, self, conn, conn.set_bytes(key, value, ttl).await).map_err(to_py_err)
    }

    fn flushdb_sync(&self, py: Python<'_>) -> PyResult<()> {
        sync_op!(py, self, conn, conn.flushdb().await).map_err(to_py_err)
    }
}
