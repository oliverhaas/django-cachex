// Test-only PyO3 functions for driving RedisRsAwaitable from Python tests.
// Always compiled in (PyO3 doesn't expose Rust #[cfg(test)] to Python).
// All exported names start with `_test_` to mark them as internal.

use crate::async_bridge::{RawResult, RedisRsAwaitable, get_runtime};
use pyo3::prelude::*;
use std::time::Duration;
use tokio::sync::oneshot;

/// Resolved-bytes awaitable: sender fires synchronously before returning.
#[pyfunction]
pub fn _test_resolved_bytes(b: Vec<u8>) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::OptBytes(Some(b)));
    RedisRsAwaitable::new(rx)
}

/// Resolved-None awaitable.
#[pyfunction]
pub fn _test_resolved_none() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::OptBytes(None));
    RedisRsAwaitable::new(rx)
}

/// Resolved-int awaitable.
#[pyfunction]
pub fn _test_resolved_int(n: i64) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::Int(n));
    RedisRsAwaitable::new(rx)
}

/// Delayed-bytes awaitable: tokio task sleeps `delay_ms` then sends.
/// Used to force callback mode (delay > 5 polls' worth of event-loop ticks).
#[pyfunction]
pub fn _test_delayed_bytes(b: Vec<u8>, delay_ms: u64) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    get_runtime().spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let _ = tx.send(RawResult::OptBytes(Some(b)));
    });
    RedisRsAwaitable::new(rx)
}

/// Pending-forever awaitable: sender is leaked, never resolves. For cancel tests.
#[pyfunction]
pub fn _test_pending() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel::<RawResult>();
    Box::leak(Box::new(tx));
    RedisRsAwaitable::new(rx)
}

/// Dropped-sender awaitable: oneshot closes immediately → "operation was dropped".
#[pyfunction]
pub fn _test_dropped() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel::<RawResult>();
    drop(tx);
    RedisRsAwaitable::new(rx)
}

/// Connection-error awaitable: resolves with PyConnectionError.
#[pyfunction]
pub fn _test_error(msg: String) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::Error(msg));
    RedisRsAwaitable::new(rx)
}

/// Server-error awaitable: resolves with PyRuntimeError.
#[pyfunction]
pub fn _test_server_error(msg: String) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::ServerError(msg));
    RedisRsAwaitable::new(rx)
}
