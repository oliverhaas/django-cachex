// Rust adapter for django-cachex.
//
// Heavily inspired by django-vcache (MIT, by David Burke / GlitchTip):
// https://gitlab.com/glitchtip/django-vcache

mod adapter;
mod async_bridge;
mod client;
mod connection;
mod pipeline;
mod stream_decode;

use pyo3::prelude::*;

#[pymodule]
fn _redis_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<async_bridge::RedisRsAwaitable>()?;
    m.add_class::<adapter::RedisRsAdapter>()?;
    m.add_class::<adapter::RedisRsClusterAdapter>()?;
    m.add_class::<adapter::RedisRsSentinelAdapter>()?;
    m.add_class::<pipeline::RedisRsPipelineAdapter>()?;
    m.add_class::<pipeline::RedisRsAsyncPipelineAdapter>()?;
    Ok(())
}
