// Rust I/O driver for django-cachex.
//
// Heavily inspired by django-vcache (MIT, by David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache

mod async_bridge;

use pyo3::prelude::*;

#[pymodule]
fn _driver(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<async_bridge::RustAwaitable>()?;
    Ok(())
}
