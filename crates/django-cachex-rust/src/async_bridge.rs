// Async bridge: tokio runtime + RustAwaitable Python class.
//
// Verbatim port of django-vcache's `src/async_bridge.rs` (MIT,
// David Burke / GlitchTip). Upstream:
// https://gitlab.com/glitchtip/django-vcache/-/blob/main/src/async_bridge.rs
//
// Keep this file in lockstep with upstream. If you want to diverge,
// open a discussion first — the design (5-poll busy-yield, OnceLock
// runtime with PID fork detection, oneshot channel + watcher task) is
// the load-bearing part and must not drift accidentally.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

/// Fork-safe tokio runtime with zero-cost fast path.
///
/// Fast path (99.99% of calls): atomic PID check + OnceLock::get() → &'static.
/// Slow path (first call or after fork): OnceLock init or Mutex-protected
/// runtime creation. After fork(), the parent's dead runtime is leaked via
/// Box::leak to avoid joining dead threads.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static RUNTIME_PID: AtomicU32 = AtomicU32::new(0);
static FORK_RUNTIME: Mutex<Option<(u32, &'static Runtime)>> = Mutex::new(None);

#[inline]
pub fn get_runtime() -> &'static Runtime {
    let pid = std::process::id();
    if RUNTIME_PID.load(Ordering::Relaxed) == pid {
        // Fast path: same process, no fork. Zero-cost OnceLock get.
        return RUNTIME.get().unwrap();
    }
    init_or_fork_runtime(pid)
}

#[cold]
fn init_or_fork_runtime(pid: u32) -> &'static Runtime {
    let stored = RUNTIME_PID.load(Ordering::Relaxed);

    if stored == 0 {
        // First call: initialize via OnceLock (handles concurrent init safely)
        let rt = RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime")
        });
        RUNTIME_PID.store(pid, Ordering::Relaxed);
        return rt;
    }

    // Fork detected: parent's tokio threads are dead in this child.
    // Create a fresh runtime and leak it (can't drop a dead runtime safely).
    let mut guard = FORK_RUNTIME.lock().unwrap();
    if let Some((stored_pid, rt)) = *guard {
        if stored_pid == pid {
            return rt;
        }
    }
    let rt: &'static Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime"),
    ));
    *guard = Some((pid, rt));
    rt
}

// =========================================================================
// Result types — Rust-native, no GIL needed to construct
// =========================================================================

pub enum RawResult {
    /// Successful operation with no return value — renders as Python None.
    Nil,
    OptBytes(Option<Vec<u8>>),
    Bool(bool),
    Int(i64),
    OptInt(Option<i64>),
    F64(f64),
    OptF64(Option<f64>),
    /// String result rendered as Python str (e.g. XADD id, SCRIPT LOAD sha, TYPE).
    Str(String),
    OptStr(Option<String>),
    OptBytesList(Vec<Option<Vec<u8>>>),
    BytesList(Vec<Vec<u8>>),
    StringList(Vec<String>),
    /// Field/value pairs for HGETALL/HMSET-style results (bytes value).
    BytesPairs(Vec<(Vec<u8>, Vec<u8>)>),
    /// Member/score pairs for ZRANGE WITHSCORES, ZPOPMIN/MAX.
    ScoredMembers(Vec<(Vec<u8>, f64)>),
    OptKeyAndBytesList(Option<(String, Vec<Vec<u8>>)>),
    /// Key + single bytes value for BLPOP/BRPOP-style results.
    OptKeyAndBytes(Option<(String, Vec<u8>)>),
    /// Generic redis::Value, recursively converted to Python.
    /// Used for EVAL/EVALSHA, INFO, CLIENT LIST, XREAD, XRANGE, and other
    /// commands whose return shape varies enough that a typed variant doesn't help.
    Value(redis::Value),
    /// Connection/IO error → PyConnectionError (swallowed by IGNORE_EXCEPTIONS)
    Error(String),
    /// Data/server error → PyRuntimeError (NOT swallowed, indicates real problems)
    ServerError(String),
}

/// Recursively convert a `redis::Value` to a Python object.
/// Bulk strings and simple strings → bytes. Integers → int. Booleans → bool.
/// Arrays → list. Maps → dict (with bytes/str keys). Nil → None. Doubles → float.
fn redis_value_to_py(py: Python<'_>, v: redis::Value) -> PyResult<Py<PyAny>> {
    match v {
        redis::Value::Nil => Ok(py.None()),
        redis::Value::Int(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        redis::Value::BulkString(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        redis::Value::SimpleString(s) => Ok(PyBytes::new(py, s.as_bytes()).into_any().unbind()),
        redis::Value::Boolean(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::Double(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        redis::Value::Okay => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::Array(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::Map(pairs) => {
            let dict = PyDict::new(py);
            for (k, val) in pairs {
                let k_py = redis_value_to_py(py, k)?;
                let v_py = redis_value_to_py(py, val)?;
                dict.set_item(k_py, v_py)?;
            }
            Ok(dict.into_any().unbind())
        }
        redis::Value::Set(items) => {
            // Redis sets via RESP3 — return as list to preserve order; Python can `set(...)` if needed.
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::Attribute { data, .. } => redis_value_to_py(py, *data),
        redis::Value::Push { kind: _, data } => {
            let py_items: Vec<Py<PyAny>> = data
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::BigNumber(n) => Ok(PyString::new(py, &n.to_string()).into_any().unbind()),
        redis::Value::VerbatimString { text, .. } => {
            Ok(PyBytes::new(py, text.as_bytes()).into_any().unbind())
        }
        redis::Value::ServerError(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "{e:?}"
        ))),
        // redis::Value is marked non_exhaustive — fall back to the Debug repr.
        other => Ok(PyString::new(py, &format!("{other:?}")).into_any().unbind()),
    }
}

impl RawResult {
    pub fn into_py(self, py: Python<'_>) -> Result<Py<PyAny>, PyErr> {
        match self {
            RawResult::Nil => Ok(py.None()),
            RawResult::OptBytes(Some(b)) => Ok(PyBytes::new(py, &b).into_any().unbind()),
            RawResult::OptBytes(None) => Ok(py.None()),
            RawResult::Bool(b) => {
                Ok(b.into_pyobject(py).unwrap().to_owned().into_any().unbind())
            }
            RawResult::Int(n) => Ok(n.into_pyobject(py).unwrap().into_any().unbind()),
            RawResult::Str(s) => Ok(PyString::new(py, &s).into_any().unbind()),
            RawResult::OptStr(Some(s)) => Ok(PyString::new(py, &s).into_any().unbind()),
            RawResult::OptStr(None) => Ok(py.None()),
            RawResult::OptBytesList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|r| match r {
                        Some(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                        None => py.None(),
                    })
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::BytesList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .iter()
                    .map(|b| PyBytes::new(py, b).into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::StringList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .iter()
                    .map(|s| PyString::new(py, s).into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::OptKeyAndBytesList(Some((key, values))) => {
                let py_values: Vec<Py<PyAny>> = values
                    .iter()
                    .map(|b| PyBytes::new(py, b).into_any().unbind())
                    .collect();
                let py_key = PyString::new(py, &key).into_any().unbind();
                let py_list = PyList::new(py, py_values)?.into_any().unbind();
                Ok(PyTuple::new(py, [py_key, py_list])?.into_any().unbind())
            }
            RawResult::OptKeyAndBytesList(None) => Ok(py.None()),
            RawResult::OptKeyAndBytes(Some((key, value))) => {
                let py_key = PyString::new(py, &key).into_any().unbind();
                let py_value = PyBytes::new(py, &value).into_any().unbind();
                Ok(PyTuple::new(py, [py_key, py_value])?.into_any().unbind())
            }
            RawResult::OptKeyAndBytes(None) => Ok(py.None()),
            RawResult::OptInt(Some(n)) => Ok(n.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptInt(None) => Ok(py.None()),
            RawResult::F64(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptF64(Some(f)) => Ok(f.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptF64(None) => Ok(py.None()),
            RawResult::BytesPairs(pairs) => {
                // Returned as a dict {bytes: bytes} so async HGETALL matches sync.
                let dict = PyDict::new(py);
                for (k, v) in pairs {
                    let k_py = PyBytes::new(py, &k).into_any().unbind();
                    let v_py = PyBytes::new(py, &v).into_any().unbind();
                    dict.set_item(k_py, v_py)?;
                }
                Ok(dict.into_any().unbind())
            }
            RawResult::ScoredMembers(items) => {
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
            RawResult::Value(v) => redis_value_to_py(py, v),
            RawResult::Error(e) => Err(pyo3::exceptions::PyConnectionError::new_err(e)),
            RawResult::ServerError(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}

// =========================================================================
// RustAwaitable — deferred-callback async bridge
//
// The tokio task sends its result via a oneshot channel — no GIL needed.
//
// __next__ polls try_recv(). For fast local operations, the result is
// usually ready on the first call → zero overhead, identical to the old
// busy-yield approach.
//
// If the result isn't ready after 5 polls, we switch to callback mode:
// get the event loop, spawn a lightweight watcher task, and yield self
// with _asyncio_future_blocking=True. The watcher awaits the oneshot
// and calls call_soon_threadsafe(_wake).
//
// This gives us the best of both worlds:
// - Fast path: zero GIL from tokio threads, minimal per-op overhead
// - Slow path: proper callback-based suspend (no CPU burn)
// - Idle: zero CPU (no busy-yield loops)
// =========================================================================

/// A done-callback with its associated context, matching asyncio's
/// `add_done_callback(fn, *, context=ctx)` protocol. Storing the context
/// ensures callbacks run in the correct `contextvars.Context`, which is
/// critical for middleware that uses `ContextVar.reset(token)`.
struct DoneCallback {
    callback: Py<PyAny>,
    context: Option<Py<PyAny>>,
}

/// Callback-mode state — only allocated when an operation doesn't resolve
/// within the busy-yield window (5 polls). Most fast ops never need this,
/// so keeping it boxed avoids bloating every RustAwaitable allocation.
struct CallbackState {
    event_loop: Py<PyAny>,
    callbacks: Vec<DoneCallback>,
    result_slot: Arc<Mutex<Option<Result<RawResult, ()>>>>,
}

#[pyclass]
pub struct RustAwaitable {
    rx: Option<oneshot::Receiver<RawResult>>,
    /// Successful result value — stored for result() after StopIteration delivery.
    value: Option<Py<PyAny>>,
    /// Error exception object — raised by result() for the Task to propagate.
    error: Option<Py<PyAny>>,
    /// Whether we have a stored result (value or error).
    resolved: bool,
    /// Whether cancel() was called.
    cancelled: bool,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
    /// Number of times __next__ has been called without a result.
    polls: u8,
    /// Callback mode state — allocated lazily on 6th poll miss.
    cb: Option<Box<CallbackState>>,
}

/// Helper: raise asyncio.CancelledError.
fn cancelled_error(py: Python<'_>) -> PyErr {
    if let Ok(asyncio) = py.import("asyncio") {
        if let Ok(cls) = asyncio.getattr("CancelledError") {
            if let Ok(exc) = cls.call0() {
                return PyErr::from_value(exc.into_any());
            }
        }
    }
    pyo3::exceptions::PyRuntimeError::new_err("cancelled")
}

/// Helper: deliver a successful result via StopIteration and store in self.value.
fn deliver_value(
    this: &mut RustAwaitable,
    py: Python<'_>,
    val: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    this.resolved = true;
    this.value = Some(val.clone_ref(py));
    let stop = py
        .get_type::<pyo3::exceptions::PyStopIteration>()
        .call1((val,))?;
    Err(PyErr::from_value(stop.into_any()))
}

/// Helper: store an error and raise it.
fn deliver_error(this: &mut RustAwaitable, py: Python<'_>, err: PyErr) -> PyResult<Py<PyAny>> {
    this.resolved = true;
    this.error = Some(err.value(py).clone().into_any().unbind());
    Err(err)
}

#[pymethods]
impl RustAwaitable {
    fn __await__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[getter]
    fn _loop(&self) -> Option<&Py<PyAny>> {
        self.cb.as_ref().map(|cb| &cb.event_loop)
    }

    fn __next__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut this = slf.borrow_mut(py);

        // Cancelled — raise CancelledError.
        if this.cancelled {
            return Err(cancelled_error(py));
        }

        // Already resolved — re-deliver stored result.
        if this.resolved {
            if let Some(ref exc) = this.error {
                return Err(PyErr::from_value(exc.bind(py).clone()));
            }
            if let Some(ref value) = this.value {
                let stop = py
                    .get_type::<pyo3::exceptions::PyStopIteration>()
                    .call1((value,))?;
                return Err(PyErr::from_value(stop.into_any()));
            }
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "awaitable already consumed",
            ));
        }

        // Check result_slot (set by watcher task via Rust mutex, no GIL needed).
        if let Some(ref cb) = this.cb {
            let maybe = cb.result_slot.lock().unwrap().take();
            if let Some(raw_result) = maybe {
                this.cb = None;
                return match raw_result {
                    Ok(raw) => match raw.into_py(py) {
                        Ok(val) => deliver_value(&mut this, py, val),
                        Err(e) => deliver_error(&mut this, py, e),
                    },
                    Err(()) => deliver_error(
                        &mut this,
                        py,
                        pyo3::exceptions::PyRuntimeError::new_err("operation was dropped"),
                    ),
                };
            }
        }

        // Try to read result directly from the oneshot channel.
        if let Some(rx) = this.rx.as_mut() {
            match rx.try_recv() {
                Ok(raw) => {
                    this.rx = None;
                    return match raw.into_py(py) {
                        Ok(val) => deliver_value(&mut this, py, val),
                        Err(e) => deliver_error(&mut this, py, e),
                    };
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    this.rx = None;
                    return deliver_error(
                        &mut this,
                        py,
                        pyo3::exceptions::PyRuntimeError::new_err("operation was dropped"),
                    );
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    // Not ready yet
                }
            }
        } else if this.resolved {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "awaitable already consumed",
            ));
        }

        this.polls += 1;

        if this.polls <= 5 {
            // Busy-yield for up to 5 iterations — covers nearly all fast ops
            // (sub-ms driver operations resolve within 1-3 event loop ticks).
            // Callback mode has high fixed cost (get_running_loop + watcher
            // spawn + spawn_blocking + GIL acquisition), so busy-yield is
            // cheaper even when all 5 polls miss.
            drop(this);
            return Ok(py.None());
        }

        // Sixth poll miss: switch to callback mode (for genuinely slow ops).
        let rx = this.rx.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("awaitable already consumed")
        })?;

        let asyncio = py.import("asyncio")?;
        let event_loop = asyncio.call_method0("get_running_loop")?;
        this._asyncio_future_blocking = true;

        // Spawn a lightweight watcher that awaits the result and wakes us.
        let event_loop_ref = event_loop.clone().into_any().unbind();
        let awaitable_ref = slf.clone_ref(py).into_any();
        let result_slot = Arc::new(Mutex::new(None));
        this.cb = Some(Box::new(CallbackState {
            event_loop: event_loop.into_any().unbind(),
            callbacks: Vec::new(),
            result_slot: result_slot.clone(),
        }));
        get_runtime().spawn(async move {
            let raw = rx.await;
            let raw_result = match raw {
                Ok(r) => Ok(r),
                Err(_) => Err(()),
            };
            *result_slot.lock().unwrap() = Some(raw_result);
            tokio::task::spawn_blocking(move || {
                Python::try_attach(|py| {
                    if let Ok(wake) = awaitable_ref.getattr(py, "_wake") {
                        let _ =
                            event_loop_ref.call_method1(py, "call_soon_threadsafe", (wake,));
                    }
                });
            });
        });

        drop(this);
        Ok(slf.into_any())
    }

    /// Called on the event loop thread via call_soon_threadsafe.
    /// Fires done-callbacks so the Task can resume.
    ///
    /// Each callback is invoked via `context.run(callback, self)` when a
    /// context was provided by `add_done_callback`, matching asyncio.Future's
    /// contract.  This ensures the callback (typically `Task.__step`) runs
    /// in the correct `contextvars.Context`.
    fn _wake(slf: Py<Self>, py: Python<'_>) {
        let callbacks = {
            let mut this = slf.borrow_mut(py);
            this.cb
                .as_mut()
                .map(|cb| std::mem::take(&mut cb.callbacks))
                .unwrap_or_default()
        };
        for done_cb in callbacks {
            if let Some(ref ctx) = done_cb.context {
                let _ = ctx.call_method1(py, "run", (&done_cb.callback, &slf));
            } else {
                let _ = done_cb.callback.call1(py, (&slf,));
            }
        }
    }

    #[pyo3(signature = (fn_cb, *, context=None))]
    fn add_done_callback(&mut self, fn_cb: Py<PyAny>, context: Option<Py<PyAny>>) {
        if let Some(ref mut cb) = self.cb {
            cb.callbacks.push(DoneCallback {
                callback: fn_cb,
                context,
            });
        }
        // If no callback state yet (still in busy-yield), the callback
        // will be added when we enter callback mode. In practice, asyncio
        // only calls add_done_callback after we yield with future_blocking=True.
    }

    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.cancelled {
            return Err(cancelled_error(py));
        }
        if let Some(ref exc) = self.error {
            return Err(PyErr::from_value(exc.bind(py).clone()));
        }
        match &self.value {
            Some(v) => Ok(v.clone_ref(py)),
            None => Ok(py.None()),
        }
    }

    fn exception(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.cancelled {
            let asyncio = py.import("asyncio")?;
            let exc = asyncio.getattr("CancelledError")?.call0()?;
            return Ok(exc.into_any().unbind());
        }
        match &self.error {
            Some(exc) => Ok(exc.clone_ref(py)),
            None => Ok(py.None()),
        }
    }

    /// Cancel the awaitable. Drops the oneshot receiver so the tokio task
    /// result is discarded. Returns True if successfully cancelled.
    ///
    /// In callback mode, fires done-callbacks via loop.call_soon() so the
    /// Task can resume and throw CancelledError. Without this, a BLMOVE
    /// with infinite timeout would hang on SIGTERM.
    #[pyo3(signature = (msg=None))]
    fn cancel(slf: Py<Self>, py: Python<'_>, msg: Option<Py<PyAny>>) -> bool {
        let mut this = slf.borrow_mut(py);
        let _ = msg;
        if this.resolved || this.cancelled {
            return false;
        }
        this.cancelled = true;
        this.rx = None;
        let cb_state = this.cb.take();
        drop(this);
        if let Some(cb) = cb_state {
            for done_cb in cb.callbacks {
                let kwargs = pyo3::types::PyDict::new(py);
                if let Some(ref ctx) = done_cb.context {
                    let _ = kwargs.set_item("context", ctx);
                }
                let _ = cb.event_loop.call_method(
                    py,
                    "call_soon",
                    (&done_cb.callback, slf.bind(py)),
                    Some(&kwargs),
                );
            }
        }
        true
    }

    fn cancelled(&self) -> bool {
        self.cancelled
    }

    fn done(&self) -> bool {
        self.resolved || self.cancelled
    }
}

impl RustAwaitable {
    pub fn new(rx: oneshot::Receiver<RawResult>) -> Self {
        RustAwaitable {
            rx: Some(rx),
            value: None,
            error: None,
            resolved: false,
            cancelled: false,
            _asyncio_future_blocking: false,
            polls: 0,
            cb: None,
        }
    }
}
