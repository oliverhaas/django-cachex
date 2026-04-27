// Rust I/O driver for django-cachex.
//
// This file is intentionally a stub. The async bridge, connection layer,
// and command bindings land in follow-up PRs (issues #64, #65, #66).
// What ships here is just the build pipeline and an importable extension
// module so subsequent PRs have something to attach code to.
//
// Heavily inspired by django-vcache (MIT, by David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache

use pyo3::prelude::*;

#[pymodule]
fn _driver(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
