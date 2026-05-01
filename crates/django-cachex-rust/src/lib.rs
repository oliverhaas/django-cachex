// Rust I/O driver for django-cachex.
//
// Heavily inspired by django-vcache (MIT, by David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache

mod async_bridge;
mod client;
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
