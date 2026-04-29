// Rust I/O driver for django-cachex.
//
// Heavily inspired by django-vcache (MIT, by David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache

mod async_bridge;
mod client;
// Full ValkeyConn surface comes online in issue #66; suppress dead-code warnings
// for methods/fields wired up there (lock_*, eval, list/stream ops, blocking conn).
#[allow(dead_code)]
mod connection;
mod test_helpers;

use pyo3::prelude::*;

#[pymodule]
fn _driver(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<async_bridge::RustAwaitable>()?;
    m.add_class::<client::RustValkeyDriver>()?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_none, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_int, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_delayed_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_pending, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_dropped, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_error, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_server_error, m)?)?;
    Ok(())
}
