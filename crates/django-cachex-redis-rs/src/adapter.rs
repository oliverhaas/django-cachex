// High-level cachex adapter, defined in Rust.
//
// The Python ``RedisRsAdapter`` (in django_cachex/adapters/redis_rs.py)
// inherits from this Rust class and progressively delegates more methods
// to it; the final state is a one-line Python re-export.
//
// Phase 2 establishes only the skeleton: lazy driver resolution via the
// Python ``_redis_rs_clients`` registry, and the basic ``get_client`` /
// ``get_async_client`` / ``get_raw_client`` accessors. Stampede math
// stays in ``django_cachex/stampede.py`` for now; later phases migrate it.

use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyDelta, PyDeltaAccess, PyDict};

// =========================================================================
// Argument types — names mirror redis-py's ``redis.typing`` for vocabulary
// consistency. Each derives ``FromPyObject`` so PyO3 raises ``TypeError``
// for invalid input shapes, replacing the old runtime-polymorphism helpers
// (``value_to_bytes`` / ``to_seconds`` / ``to_unix`` / ...).
// =========================================================================

/// ``int | timedelta`` — TTL inputs to EXPIRE / PEXPIRE / TOUCH / SET ... EX.
/// Matches redis-py's ``ExpiryT``; cachex narrows the int side to a signed
/// integer (negative timeouts collapse to "delete immediately" elsewhere).
#[derive(FromPyObject)]
pub(crate) enum ExpiryT<'py> {
    Int(i64),
    Delta(Bound<'py, PyDelta>),
}

impl ExpiryT<'_> {
    /// Total seconds, truncating fractional microseconds. Negatives are
    /// passed through; the caller decides whether to clamp.
    pub(crate) fn to_seconds(&self) -> i64 {
        match self {
            Self::Int(n) => *n,
            Self::Delta(d) => {
                let days = i64::from(d.get_days());
                let seconds = i64::from(d.get_seconds());
                days * 86_400 + seconds
            }
        }
    }

    /// Total milliseconds, truncating sub-millisecond microseconds.
    pub(crate) fn to_milliseconds(&self) -> i64 {
        match self {
            Self::Int(n) => *n,
            Self::Delta(d) => {
                let days = i64::from(d.get_days());
                let seconds = i64::from(d.get_seconds());
                let micros = i64::from(d.get_microseconds());
                (days * 86_400 + seconds) * 1_000 + micros / 1_000
            }
        }
    }
}

/// ``int | datetime`` — absolute expiry inputs to EXPIREAT / PEXPIREAT.
/// Matches redis-py's ``AbsExpiryT``.
#[derive(FromPyObject)]
pub(crate) enum AbsExpiryT<'py> {
    Int(i64),
    DateTime(Bound<'py, PyDateTime>),
}

impl AbsExpiryT<'_> {
    /// Unix timestamp in seconds. Calls Python's ``datetime.timestamp()``
    /// so timezone-aware values resolve correctly via the stdlib.
    pub(crate) fn to_unix(&self) -> PyResult<i64> {
        match self {
            Self::Int(n) => Ok(*n),
            Self::DateTime(dt) => {
                let ts: f64 = dt.call_method0("timestamp")?.extract()?;
                Ok(ts as i64)
            }
        }
    }

    /// Unix timestamp in milliseconds.
    pub(crate) fn to_unix_milliseconds(&self) -> PyResult<i64> {
        match self {
            Self::Int(n) => Ok(*n),
            Self::DateTime(dt) => {
                let ts: f64 = dt.call_method0("timestamp")?.extract()?;
                Ok((ts * 1_000.0) as i64)
            }
        }
    }
}

/// ``bytes | int`` — values written to / matched against Redis. Matches
/// redis-py's ``EncodableT`` *vocabulary*; cachex narrows the actual set
/// of accepted Python types because the cache layer's serializer always
/// produces ``bytes`` and ``INCR`` expects ints. The int branch encodes
/// to its decimal ASCII form (the wire shape redis-py uses).
#[derive(FromPyObject)]
pub(crate) enum EncodableT {
    // Order matters: Bytes first so a ``bytes`` input is taken verbatim
    // (an int can't extract as ``Vec<u8>``, so it falls through to Int).
    Bytes(Vec<u8>),
    Int(i64),
}

impl EncodableT {
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Bytes(b) => b,
            Self::Int(n) => n.to_string().into_bytes(),
        }
    }
}

/// Sync command — resolve connection (cached after first call), release the
/// GIL, block on the tokio runtime. Returns the raw redis-rs result; caller
/// uses `.map_err(crate::client::to_py_err)` to convert.
macro_rules! adapter_sync {
    ($slf:expr, $conn:ident, $body:expr) => {{
        let mut $conn = $crate::adapter::connection($slf)?;
        $slf.py().detach(|| {
            $crate::async_bridge::get_runtime().block_on(async { $body })
        })
    }};
}

/// Async command — resolve connection (cached after first call), spawn the
/// op on the tokio runtime, return an awaitable. Body must produce a
/// `RawResult` (use `.into_raw_result()` from `crate::client::IntoRawResult`).
///
/// Two forms:
/// - `adapter_async!(slf, conn, body)` — no post-await transform.
/// - `adapter_async!(slf, conn, body; Transform::Variant)` — apply transform
///   to the resolved value (e.g., `ToBool`, `NormalizeTtl`) before delivery.
///   Replaces the older Python `_async_helpers` coroutine wrap pattern.
macro_rules! adapter_async {
    ($slf:expr, $conn:ident, $body:expr) => {
        adapter_async!($slf, $conn, $body; $crate::async_bridge::AwaitTransform::None)
    };
    ($slf:expr, $conn:ident, $body:expr; $transform:expr) => {{
        let py = $slf.py();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = $crate::async_bridge::RedisRsAwaitable::with_transform(rx, $transform);
        let mut $conn = $crate::adapter::connection($slf)?;
        $crate::async_bridge::get_runtime().spawn(async move {
            let result: $crate::async_bridge::RawResult = async { $body }.await;
            let _ = tx.send(result);
        });
        Ok::<Py<$crate::async_bridge::RedisRsAwaitable>, pyo3::PyErr>(
            awaitable.into_pyobject(py)?.unbind(),
        )
    }};
}

// =========================================================================
// Helpers — Python value conversions used by adapter methods
// =========================================================================

/// Coerce ``KeyT`` (str | bytes | int) to a Rust ``String``.
fn str_key(key: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = key.extract::<String>() {
        return Ok(s);
    }
    if let Ok(b) = key.extract::<Vec<u8>>() {
        return Ok(String::from_utf8_lossy(&b).into_owned());
    }
    Ok(key.str()?.to_str()?.to_owned())
}

/// Coerce a Python value to ``bytes`` for storage when iterating a
/// container element-by-element. Pymethod-level args use ``EncodableT``
/// directly; this is for collection walks (mappings / lists / dicts)
/// where the inner item is still a ``Bound<PyAny>``.
fn value_to_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    Ok(value.extract::<EncodableT>()?.into_bytes())
}

/// Decode a ``bytes | str`` value to ``str`` (lossy on invalid UTF-8).
fn decode_str(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = value.extract::<String>() {
        return Ok(s);
    }
    if let Ok(b) = value.extract::<Vec<u8>>() {
        return Ok(String::from_utf8_lossy(&b).into_owned());
    }
    value.str()?.extract()
}

/// Encode an EVAL ARGV value the way redis-py does on the wire: bytes
/// pass through, bool serializes as 0/1 (not "True"/"False"), int as ASCII
/// decimal, anything else via ``str()``.
fn eval_arg(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = value.extract::<Vec<u8>>() {
        return Ok(b);
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(if b { b"1".to_vec() } else { b"0".to_vec() });
    }
    if let Ok(n) = value.extract::<i64>() {
        return Ok(n.to_string().into_bytes());
    }
    let s: String = value.str()?.extract()?;
    Ok(s.into_bytes())
}

/// Parse a Redis INFO bulk-string into a ``dict[str, int | float | str]``.
/// Skips comment / blank lines; coerces values to ``int`` then ``float``
/// then leaves them as ``str``. Mirrors redis-py's ``parse_info``.
fn parse_info<'py>(py: Python<'py>, raw: &str) -> PyResult<Bound<'py, PyDict>> {
    let info = PyDict::new(py);
    for line in raw.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            if let Ok(n) = value.parse::<i64>() {
                info.set_item(name, n)?;
            } else if let Ok(f) = value.parse::<f64>() {
                info.set_item(name, f)?;
            } else {
                info.set_item(name, value)?;
            }
        }
    }
    Ok(info)
}

/// Slice a single ``# <Section>`` block out of an INFO bulk string.
fn select_info_section(raw: &str, section: &str) -> String {
    let target = section.trim().to_lowercase();
    let mut out = Vec::new();
    let mut in_section = false;
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix('#') {
            in_section = rest.trim().to_lowercase() == target;
            continue;
        }
        if in_section {
            out.push(line);
        }
    }
    out.join("\n")
}

/// Split ``[k1, k2, ..., a1, a2, ...]`` (the ``*keys_and_args`` cachex
/// passes to ``eval``) into key strings + EVAL-encoded ARGV bytes, using
/// ``numkeys`` as the split point.
fn split_eval_args(
    keys_and_args: &[Bound<'_, PyAny>],
    numkeys: usize,
) -> PyResult<(Vec<String>, Vec<Vec<u8>>)> {
    let (keys_part, args_part) = keys_and_args.split_at(numkeys.min(keys_and_args.len()));
    let keys: Vec<String> = keys_part.iter().map(str_key).collect::<PyResult<_>>()?;
    let args: Vec<Vec<u8>> = args_part.iter().map(eval_arg).collect::<PyResult<_>>()?;
    Ok((keys, args))
}

/// Translate Redis's ``ERR no such key`` into the cachex contract's
/// ``ValueError(f"Key {src!r} not found")``. Other errors pass through.
fn translate_rename_error_str(e: PyErr, src_key: &str) -> PyErr {
    if e.to_string().to_lowercase().contains("no such key") {
        pyo3::exceptions::PyValueError::new_err(format!("Key '{src_key}' not found"))
    } else {
        e
    }
}

/// PyO3 ``from_py_with`` helper: extract ``Option<String>`` while letting
/// ``None`` and ``""`` both surface as ``None`` (the cachex contract treats
/// them equivalently for SCAN's MATCH argument).
fn py_optional_str(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    if value.is_none() {
        return Ok(None);
    }
    Ok(Some(value.extract()?))
}

/// Encode ``Mapping[str, bytes | int]`` (XADD / XADD-options field list) to
/// the ``Vec<(String, Vec<u8>)>`` shape the driver expects.
fn encode_str_field_pairs(fields: &Bound<'_, PyAny>) -> PyResult<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    let items = fields.call_method0("items")?;
    for item in items.try_iter()? {
        let item = item?;
        let pair = item.cast::<pyo3::types::PyTuple>()?;
        let f = pair.get_item(0)?;
        let v = pair.get_item(1)?;
        out.push((decode_str(&f)?, value_to_bytes(&v)?));
    }
    Ok(out)
}

/// Split a ``Mapping[KeyT, str]`` (XREAD's ``streams=`` arg) into
/// ``(keys: Vec<String>, ids: Vec<String>)``.
fn split_streams_dict(streams: &Bound<'_, PyAny>) -> PyResult<(Vec<String>, Vec<String>)> {
    let mut keys = Vec::new();
    let mut ids = Vec::new();
    let items = streams.call_method0("items")?;
    for item in items.try_iter()? {
        let item = item?;
        let pair = item.cast::<pyo3::types::PyTuple>()?;
        let k = pair.get_item(0)?;
        let v = pair.get_item(1)?;
        keys.push(str_key(&k)?);
        ids.push(decode_str(&v)?);
    }
    Ok((keys, ids))
}

/// Iterate over ``data`` (any ``Mapping[KeyT, bytes | int]``) producing the
/// ``[(str_key, bytes_value), ...]`` shape the driver's ``pipeline_set``
/// expects. Used by ``set_many`` / ``aset_many``.
fn prepare_set_many(data: &Bound<'_, PyAny>) -> PyResult<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    let items = data.call_method0("items")?;
    for item in items.try_iter()? {
        let item = item?;
        let pair = item.cast::<pyo3::types::PyTuple>()?;
        let k = pair.get_item(0)?;
        let v = pair.get_item(1)?;
        out.push((str_key(&k)?, value_to_bytes(&v)?));
    }
    Ok(out)
}

/// Coerce a Python value (typically `bytes` or `str`) to ``float`` via
/// the Python builtin. Used by the ZRANGE-family decoders, where the
/// driver hands back bytes-encoded scores in cluster mode.
fn to_py_float<'py>(py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    let float_cls = py.import("builtins")?.getattr("float")?;
    Ok(float_cls.call1((value,))?.unbind())
}

/// Mirror of Python ``_decode_zrange``: normalize ZRANGE-family results.
///
/// Returns the raw list unchanged when ``withscores=False``. With scores,
/// accepts either nested ``[[m, s], ...]`` (RESP3) or flat ``[m, s, m, s,
/// ...]`` (RESP2) and returns ``[(m, float(s)), ...]``.
fn decode_zrange<'py>(
    py: Python<'py>,
    raw: &Bound<'py, PyAny>,
    withscores: bool,
) -> PyResult<Py<PyAny>> {
    use pyo3::types::{PyList, PyTuple};
    if !withscores {
        return Ok(PyList::new(py, raw.try_iter()?.collect::<PyResult<Vec<_>>>()?)?
            .into_any()
            .unbind());
    }
    let raw_len = raw.len()?;
    if raw_len == 0 {
        return Ok(PyList::empty(py).into_any().unbind());
    }
    let first = raw.get_item(0)?;
    let nested = first.cast::<PyList>().is_ok() || first.cast::<PyTuple>().is_ok();
    let mut out: Vec<Py<PyAny>> = Vec::with_capacity(raw_len);
    if nested {
        for pair in raw.try_iter()? {
            let pair = pair?;
            let member = pair.get_item(0)?;
            let score = to_py_float(py, pair.get_item(1)?)?;
            let tup = PyTuple::new(py, [member.unbind(), score])?;
            out.push(tup.into_any().unbind());
        }
    } else {
        let mut iter = raw.try_iter()?;
        while let (Some(m), Some(s)) = (iter.next(), iter.next()) {
            let member = m?;
            let score = to_py_float(py, s?)?;
            let tup = PyTuple::new(py, [member.unbind(), score])?;
            out.push(tup.into_any().unbind());
        }
    }
    Ok(PyList::new(py, out)?.into_any().unbind())
}

/// Mirror of Python ``_decode_zrevrangebyscore``: ZREVRANGEBYSCORE
/// WITHSCORES returns a flat ``[m1, s1, m2, s2, ...]`` list.
fn decode_zrevrangebyscore<'py>(
    py: Python<'py>,
    raw: &Bound<'py, PyAny>,
    withscores: bool,
) -> PyResult<Py<PyAny>> {
    use pyo3::types::{PyList, PyTuple};
    if !withscores {
        return Ok(PyList::new(py, raw.try_iter()?.collect::<PyResult<Vec<_>>>()?)?
            .into_any()
            .unbind());
    }
    let mut out: Vec<Py<PyAny>> = Vec::new();
    let mut iter = raw.try_iter()?;
    while let (Some(m), Some(s)) = (iter.next(), iter.next()) {
        let member = m?;
        let score = to_py_float(py, s?)?;
        let tup = PyTuple::new(py, [member.unbind(), score])?;
        out.push(tup.into_any().unbind());
    }
    Ok(PyList::new(py, out)?.into_any().unbind())
}

/// Convert a flat XINFO field-pair response into a ``dict[str, Any]``.
///
/// XINFO returns alternating ``[name, value, ...]`` arrays in RESP2; RESP3
/// servers may return a map directly. Both shapes resolve to the same dict.
pub(crate) fn parse_xinfo_pairs<'py>(py: Python<'py>, raw: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyDict;
    let out = PyDict::new(py);
    if raw.is_none() {
        return Ok(out.into_any().unbind());
    }
    if let Ok(d) = raw.cast::<PyDict>() {
        for (k, v) in d.iter() {
            out.set_item(decode_str(&k)?, v)?;
        }
        return Ok(out.into_any().unbind());
    }
    let mut it = raw.try_iter()?;
    while let (Some(k), Some(v)) = (it.next(), it.next()) {
        out.set_item(decode_str(&k?)?, v?)?;
    }
    Ok(out.into_any().unbind())
}

/// Convert a list of XINFO entries (each a flat pair-list or dict) to
/// ``list[dict[str, Any]]``.
pub(crate) fn parse_xinfo_pairs_list<'py>(py: Python<'py>, raw: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyList;
    let out = PyList::empty(py);
    if raw.is_none() {
        return Ok(out.into_any().unbind());
    }
    for item in raw.try_iter()? {
        out.append(parse_xinfo_pairs(py, &item?)?)?;
    }
    Ok(out.into_any().unbind())
}

/// Coerce a Python value to ``i64`` via the builtin ``int(...)`` — tolerates
/// ``int``, ``bytes`` (b"123"), and ``str``. Used by the XPENDING decoders
/// where Redis returns numeric fields as bulk strings depending on protocol.
fn to_py_int(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if let Ok(n) = value.extract::<i64>() {
        return Ok(n);
    }
    let int_cls = value.py().import("builtins")?.getattr("int")?;
    int_cls.call1((value,))?.extract()
}

/// Decode the XPENDING summary form: ``[count, min, max, [[consumer, n], ...]]``
/// → ``{"pending": ..., "min": ..., "max": ..., "consumers": [...]}``.
pub(crate) fn decode_xpending_summary<'py>(py: Python<'py>, raw: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    use pyo3::types::{PyDict, PyList};
    let out = PyDict::new(py);
    let consumers = PyList::empty(py);
    let len = if raw.is_none() {
        0
    } else {
        raw.len().unwrap_or(0)
    };
    if len == 0 {
        out.set_item("pending", 0i64)?;
        out.set_item("min", py.None())?;
        out.set_item("max", py.None())?;
        out.set_item("consumers", consumers)?;
        return Ok(out.into_any().unbind());
    }
    let count_item = raw.get_item(0)?;
    let count: i64 = if count_item.is_none() { 0 } else { to_py_int(&count_item)? };
    let min_item = raw.get_item(1)?;
    let max_item = raw.get_item(2)?;
    let min_v: Py<PyAny> = if min_item.is_none() {
        py.None()
    } else {
        decode_str(&min_item)?
            .into_pyobject(py)?
            .into_any()
            .unbind()
    };
    let max_v: Py<PyAny> = if max_item.is_none() {
        py.None()
    } else {
        decode_str(&max_item)?
            .into_pyobject(py)?
            .into_any()
            .unbind()
    };
    out.set_item("pending", count)?;
    out.set_item("min", min_v)?;
    out.set_item("max", max_v)?;
    if len > 3 {
        let third = raw.get_item(3)?;
        if !third.is_none() {
            for entry in third.try_iter()? {
                let entry = entry?;
                let name = decode_str(&entry.get_item(0)?)?;
                let pending = to_py_int(&entry.get_item(1)?)?;
                let d = PyDict::new(py);
                d.set_item("name", name)?;
                d.set_item("pending", pending)?;
                consumers.append(d)?;
            }
        }
    }
    out.set_item("consumers", consumers)?;
    Ok(out.into_any().unbind())
}

/// Decode the XPENDING range form: ``[[id, consumer, idle_ms, deliveries], ...]``.
pub(crate) fn decode_xpending_range<'py>(py: Python<'py>, raw: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    use pyo3::types::{PyDict, PyList};
    let out = PyList::empty(py);
    if raw.is_none() {
        return Ok(out.into_any().unbind());
    }
    for entry in raw.try_iter()? {
        let entry = entry?;
        let d = PyDict::new(py);
        d.set_item("message_id", decode_str(&entry.get_item(0)?)?)?;
        d.set_item("consumer", decode_str(&entry.get_item(1)?)?)?;
        let idle = to_py_int(&entry.get_item(2)?)?;
        let delivered = to_py_int(&entry.get_item(3)?)?;
        d.set_item("time_since_delivered", idle)?;
        d.set_item("times_delivered", delivered)?;
        out.append(d)?;
    }
    Ok(out.into_any().unbind())
}

/// Resolve the adapter's connection. Cheap clone on the fast path
/// (``Conn`` is ``Arc``-backed). After fork, the cached connection
/// is invalidated and rebuilt with the captured ``AdapterConnConfig``.
pub(crate) fn connection(slf: &Bound<'_, RedisRsAdapter>) -> PyResult<crate::connection::Conn> {
    let cur_pid = std::process::id();
    let this = slf.borrow();
    {
        let state = this.state.lock().unwrap();
        if state.0 == cur_pid {
            if let Some(c) = state.1.as_ref() {
                return Ok(c.clone());
            }
        }
    }
    // Slow path: reconnect (post-fork or after explicit close()).
    let config = this.config.clone();
    let new_conn = slf
        .py()
        .detach(|| crate::async_bridge::get_runtime().block_on(config.connect()))
        .map_err(pyo3::exceptions::PyConnectionError::new_err)?;
    let mut state = this.state.lock().unwrap();
    *state = (cur_pid, Some(new_conn.clone()));
    Ok(new_conn)
}

/// Build a pre-resolved ``RedisRsAwaitable`` that delivers ``value`` on
/// the caller's first poll. Used for empty-args short-circuits
/// (``ahdel``/``ahmget``/``asadd``/… return ``0``/``[]`` without hitting
/// the driver but the caller does ``await ...``). Lets the async
/// pymethods all return the same typed ``Py<RedisRsAwaitable>`` rather
/// than splitting between ``RedisRsAwaitable`` and a Python coroutine.
fn await_constant(
    py: Python<'_>,
    value: Py<PyAny>,
) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
    Ok(crate::async_bridge::RedisRsAwaitable::ready(value)
        .into_pyobject(py)?
        .unbind())
}

// =========================================================================
// Stampede helpers — call into ``django_cachex.stampede``
// =========================================================================
//
// Stampede math (XFetch decision, config merging, buffer arithmetic) lives
// in the Python module so it stays shared with the other adapters
// (valkey-py, valkey-glide, redis-py). The Rust adapter holds the per-
// instance ``StampedeConfig`` as an opaque ``Py<PyAny>`` and dispatches
// each call through the module-level helpers.

/// ``stampede.resolve_stampede(instance_config, override)`` →
/// effective config or ``None``.
fn call_resolve_stampede<'py>(
    py: Python<'py>,
    instance_config: &Py<PyAny>,
    override_arg: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let module = py.import("django_cachex.stampede")?;
    let override_arg = match override_arg {
        Some(v) => v.clone(),
        None => py.None().into_bound(py),
    };
    module
        .getattr("resolve_stampede")?
        .call1((instance_config.bind(py), override_arg))
}

/// ``stampede.get_timeout_with_buffer(timeout, instance_config, override)`` →
/// timeout (possibly with buffer added) as ``Option<i64>``.
fn call_get_timeout_with_buffer<'py>(
    py: Python<'py>,
    timeout: Option<i64>,
    instance_config: &Py<PyAny>,
    override_arg: Option<&Bound<'py, PyAny>>,
) -> PyResult<Option<i64>> {
    let module = py.import("django_cachex.stampede")?;
    let timeout_arg: Py<PyAny> = match timeout {
        Some(n) => n.into_pyobject(py)?.into_any().unbind(),
        None => py.None(),
    };
    let override_arg = match override_arg {
        Some(v) => v.clone(),
        None => py.None().into_bound(py),
    };
    let result = module.getattr("get_timeout_with_buffer")?.call1((
        timeout_arg,
        instance_config.bind(py),
        override_arg,
    ))?;
    if result.is_none() {
        Ok(None)
    } else {
        Ok(Some(result.extract()?))
    }
}

/// ``stampede.should_recompute(ttl, config)`` → bool.
fn call_should_recompute<'py>(
    py: Python<'py>,
    ttl: i64,
    config: &Bound<'py, PyAny>,
) -> PyResult<bool> {
    let module = py.import("django_cachex.stampede")?;
    let result = module.getattr("should_recompute")?.call1((ttl, config))?;
    result.extract()
}

/// Convert ``Option<i64>`` timeout to the driver's ``Option<u64>`` ttl arg
/// (clamping negatives to ``Some(0)`` for parity with Python's redis-py
/// behavior — ``timeout <= 0`` means "delete").
fn timeout_to_ttl(timeout: Option<i64>) -> Option<u64> {
    timeout.map(|t| if t < 0 { 0 } else { t as u64 })
}

// =========================================================================
// RedisRsAdapter — high-level cachex adapter, subclassable from Python
// =========================================================================

/// Captured connection config so the adapter can re-connect on demand
/// (after fork, or after an explicit ``close()``). Each topology stores
/// the parameters it needs for its ``connect_*`` factory.
#[derive(Clone)]
enum AdapterConnConfig {
    Standard {
        url: String,
        cache_opts: Option<crate::connection::ClientCacheOpts>,
        tls_opts: Option<crate::connection::TlsOpts>,
    },
    Cluster {
        urls: Vec<String>,
        tls_opts: Option<crate::connection::TlsOpts>,
    },
    Sentinel {
        sentinel_urls: Vec<String>,
        service_name: String,
        db: i64,
        cache_opts: Option<crate::connection::ClientCacheOpts>,
        tls_opts: Option<crate::connection::TlsOpts>,
    },
}

impl AdapterConnConfig {
    async fn connect(&self) -> Result<crate::connection::Conn, String> {
        match self {
            Self::Standard {
                url,
                cache_opts,
                tls_opts,
            } => crate::connection::connect_standard(url, cache_opts.clone(), tls_opts.clone())
                .await,
            Self::Cluster { urls, tls_opts } => {
                crate::connection::connect_cluster(urls.clone(), tls_opts.clone()).await
            }
            Self::Sentinel {
                sentinel_urls,
                service_name,
                db,
                cache_opts,
                tls_opts,
            } => crate::connection::connect_sentinel(
                sentinel_urls.clone(),
                service_name,
                *db,
                cache_opts.clone(),
                tls_opts.clone(),
            )
            .await,
        }
    }
}

/// Build the adapter-side ``Standard`` config from the cachex options dict.
fn standard_config_from_options(
    py: Python<'_>,
    servers: &Bound<'_, PyAny>,
    options: &Bound<'_, PyDict>,
) -> PyResult<AdapterConnConfig> {
    let _ = py;
    let url: String = servers.get_item(0)?.str()?.extract()?;
    let (cache_max_size, cache_ttl_secs) = read_cache_opts(options)?;
    let cache_opts = crate::client::make_cache_opts(cache_max_size, cache_ttl_secs);
    let (ca, cert, key) = read_tls_opts(options)?;
    let tls_opts = crate::client::make_tls_opts(ca, cert, key)?;
    Ok(AdapterConnConfig::Standard {
        url,
        cache_opts,
        tls_opts,
    })
}

/// Build the adapter-side ``Cluster`` config — every server in the list
/// is a cluster node URL.
fn cluster_config_from_options(
    py: Python<'_>,
    servers: &Bound<'_, PyAny>,
    options: &Bound<'_, PyDict>,
) -> PyResult<AdapterConnConfig> {
    let _ = py;
    let mut urls: Vec<String> = Vec::new();
    for entry in servers.try_iter()? {
        urls.push(entry?.str()?.extract()?);
    }
    if urls.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "RedisRsClusterAdapter requires at least one server URL",
        ));
    }
    let (ca, cert, key) = read_tls_opts(options)?;
    let tls_opts = crate::client::make_tls_opts(ca, cert, key)?;
    Ok(AdapterConnConfig::Cluster { urls, tls_opts })
}

/// Build the adapter-side ``Sentinel`` config from
/// ``OPTIONS['sentinels'] = [(host, port), ...]`` plus the LOCATION URL
/// (whose hostname is the Redis service name and ``?db=N`` selects the
/// database).
fn sentinel_config_from_options(
    py: Python<'_>,
    servers: &Bound<'_, PyAny>,
    options: &Bound<'_, PyDict>,
) -> PyResult<AdapterConnConfig> {
    let sentinels = options.get_item("sentinels")?.filter(|v| !v.is_none()).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(
            "Sentinel client requires OPTIONS['sentinels'] = [(host, port), ...]",
        )
    })?;
    let mut sentinel_urls: Vec<String> = Vec::new();
    for entry in sentinels.try_iter()? {
        let entry = entry?;
        let host: String = entry.get_item(0)?.str()?.extract()?;
        let port: String = entry.get_item(1)?.str()?.extract()?;
        sentinel_urls.push(format!("redis://{host}:{port}"));
    }
    if sentinel_urls.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Sentinel client requires OPTIONS['sentinels'] = [(host, port), ...]",
        ));
    }
    let first: String = servers.get_item(0)?.str()?.extract()?;
    let urllib = py.import("urllib.parse")?;
    let parsed = urllib.getattr("urlparse")?.call1((first.as_str(),))?;
    let service_name: String = parsed.getattr("hostname")?.extract()?;
    let query: String = parsed.getattr("query")?.extract()?;
    let qs = urllib.getattr("parse_qs")?.call1((query,))?;
    let db_opt = qs.cast::<PyDict>()?.get_item("db")?;
    let db: i64 = match db_opt {
        Some(v) if !v.is_none() => v.get_item(0)?.extract::<String>()?.parse().unwrap_or(0),
        _ => 0,
    };
    let (cache_max_size, cache_ttl_secs) = read_cache_opts(options)?;
    let cache_opts = crate::client::make_cache_opts(cache_max_size, cache_ttl_secs);
    let (ca, cert, key) = read_tls_opts(options)?;
    let tls_opts = crate::client::make_tls_opts(ca, cert, key)?;
    Ok(AdapterConnConfig::Sentinel {
        sentinel_urls,
        service_name,
        db,
        cache_opts,
        tls_opts,
    })
}

fn read_cache_opts(options: &Bound<'_, PyDict>) -> PyResult<(Option<usize>, Option<u64>)> {
    let max_size: Option<usize> = match options.get_item("cache_max_size")? {
        Some(v) if !v.is_none() => Some(v.extract()?),
        _ => None,
    };
    let ttl_secs: Option<u64> = match options.get_item("cache_ttl_secs")? {
        Some(v) if !v.is_none() => Some(v.extract()?),
        _ => None,
    };
    Ok((max_size, ttl_secs))
}

fn read_tls_opts(
    options: &Bound<'_, PyDict>,
) -> PyResult<(Option<String>, Option<String>, Option<String>)> {
    let ca: Option<String> = match options.get_item("ssl_ca_certs")? {
        Some(v) if !v.is_none() => Some(v.extract()?),
        _ => None,
    };
    let cert: Option<String> = match options.get_item("ssl_certfile")? {
        Some(v) if !v.is_none() => Some(v.extract()?),
        _ => None,
    };
    let key: Option<String> = match options.get_item("ssl_keyfile")? {
        Some(v) if !v.is_none() => Some(v.extract()?),
        _ => None,
    };
    Ok((ca, cert, key))
}

#[pyclass(subclass, module = "django_cachex.adapters._redis_rs")]
pub struct RedisRsAdapter {
    #[pyo3(get, name = "_servers")]
    servers: Py<PyAny>,
    #[pyo3(get, name = "_options")]
    options: Py<PyDict>,
    /// Python ``StampedeConfig | None`` (via
    /// ``django_cachex.stampede.make_stampede_config``). Held opaquely so
    /// ``get/set/get_many/...`` can pass it back into the Python helpers
    /// without re-parsing the config dict on every call.
    #[pyo3(get, name = "_stampede_config")]
    stampede_config: Py<PyAny>,
    /// Connection parameters captured at construction. Re-used by
    /// ``connection()`` to reconnect after fork or an explicit ``close()``.
    config: AdapterConnConfig,
    /// (creation pid, current connection). The mutex protects the slow
    /// reconnect path; clones of the inner ``Conn`` (which is itself
    /// ``Arc``-backed) are cheap on the fast path.
    state: std::sync::Mutex<(u32, Option<crate::connection::Conn>)>,
}

impl RedisRsAdapter {
    /// Construct an adapter from a pre-built ``AdapterConnConfig``.
    /// Eagerly opens the connection so callers see configuration errors
    /// (bad URL, TLS misconfig, …) at construction time.
    fn build(
        py: Python<'_>,
        servers: Py<PyAny>,
        options: Bound<'_, PyDict>,
        config: AdapterConnConfig,
    ) -> PyResult<Self> {
        let stampede_option = options
            .get_item("stampede_prevention")?
            .map(|v| v.unbind())
            .unwrap_or_else(|| py.None());
        let stampede_config = py
            .import("django_cachex.stampede")?
            .getattr("make_stampede_config")?
            .call1((stampede_option,))?
            .unbind();
        let conn = py
            .detach(|| crate::async_bridge::get_runtime().block_on(config.connect()))
            .map_err(pyo3::exceptions::PyConnectionError::new_err)?;
        let pid = std::process::id();
        Ok(Self {
            servers,
            options: options.unbind(),
            stampede_config,
            config,
            state: std::sync::Mutex::new((pid, Some(conn))),
        })
    }
}

#[pymethods]
impl RedisRsAdapter {
    #[new]
    #[pyo3(signature = (servers, **options))]
    fn new(
        py: Python<'_>,
        servers: Py<PyAny>,
        options: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let options = options.unwrap_or_else(|| PyDict::new(py));
        let servers_bound = servers.bind(py);
        let config = standard_config_from_options(py, servers_bound, &options)?;
        Self::build(py, servers, options, config)
    }

    /// Returns ``(hits, misses, invalidations)`` when client-side caching
    /// is enabled; ``None`` otherwise. Reads the live connection's stats.
    fn cache_statistics(slf: &Bound<'_, Self>) -> PyResult<Option<(usize, usize, usize)>> {
        let conn = connection(slf)?;
        Ok(conn
            .cache_statistics()
            .map(|s| (s.hit, s.miss, s.invalidate)))
    }

    /// Cross-driver convenience: ``cache.get_client()`` returns "the
    /// underlying client object" for tests / debugging. For the redis-rs
    /// adapter, that's the adapter itself — its command surface mirrors
    /// the redis-py client closely enough for shared cross-driver tests.
    /// ``key`` and ``write`` are accepted for signature parity with the
    /// other adapters (sentinel/cluster route by key) but ignored here.
    #[pyo3(signature = (key=None, *, write=false))]
    fn get_client(slf: &Bound<'_, Self>, key: Option<Py<PyAny>>, write: bool) -> Py<PyAny> {
        let _ = (key, write);
        slf.clone().unbind().into_any()
    }

    #[pyo3(signature = (key=None, *, write=false))]
    fn get_async_client(slf: &Bound<'_, Self>, key: Option<Py<PyAny>>, write: bool) -> Py<PyAny> {
        let _ = (key, write);
        slf.clone().unbind().into_any()
    }

    fn get_raw_client(slf: &Bound<'_, Self>) -> Py<PyAny> {
        slf.clone().unbind().into_any()
    }

    /// Normalize Redis TTL/PTTL/EXPIRETIME results: -1 → None, others as-is.
    ///
    /// `expiretime` callers want to distinguish `-2` (key missing) from
    /// `None` (no expiry); `ttl`/`pttl` callers don't, but they pre-handle
    /// the missing-key case via `has_key`. So the rule is just `-1 → None`.
    #[staticmethod]
    fn _normalize_ttl(result: i64) -> Option<i64> {
        if result == -1 { None } else { Some(result) }
    }

    #[pyo3(signature = (*, transaction = true))]
    fn pipeline(slf: &Bound<'_, Self>, transaction: bool) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let kwargs = PyDict::new(py);
        kwargs.set_item("transaction", transaction)?;
        Ok(py
            .import("django_cachex.adapters._redis_rs")?
            .getattr("RedisRsPipelineAdapter")?
            .call((slf.clone(),), Some(&kwargs))?
            .unbind())
    }

    // ``apipeline`` returns a Python ``RedisRsAsyncPipelineAdapter`` class
    // instance — not a redis-rs awaitable — so the return type is
    // genuinely ``Py<PyAny>``.
    #[pyo3(signature = (*, transaction = true))]
    fn apipeline(slf: &Bound<'_, Self>, transaction: bool) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let kwargs = PyDict::new(py);
        kwargs.set_item("transaction", transaction)?;
        Ok(py
            .import("django_cachex.adapters._redis_rs")?
            .getattr("RedisRsAsyncPipelineAdapter")?
            .call((slf.clone(),), Some(&kwargs))?
            .unbind())
    }

    // =====================================================================
    // Pipeline execution
    //
    // ``pipeline_exec`` / ``apipeline_exec`` execute a buffered list of
    // commands in one round trip. Called from the ``RedisRsPipelineAdapter``
    // pyclass (see ``pipeline.rs``) — the pipeline holds an adapter
    // reference and dispatches to these methods on ``execute()``.
    // =====================================================================

    #[pyo3(signature = (commands, transaction = false))]
    fn pipeline_exec(
        slf: &Bound<'_, Self>,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Vec<redis::Value>, _> =
            adapter_sync!(slf, conn, conn.pipeline_exec(commands, transaction).await);
        crate::client::py_redis_value(py, redis::Value::Array(r.map_err(crate::client::to_py_err)?))
    }

    #[pyo3(signature = (commands, transaction = false))]
    fn apipeline_exec(
        slf: &Bound<'_, Self>,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        adapter_async!(slf, conn, {
            match conn.pipeline_exec(commands, transaction).await {
                Ok(items) => crate::async_bridge::RawResult::Value(redis::Value::Array(items)),
                Err(e) => crate::client::classify(e),
            }
        })
    }

    // =====================================================================
    // Lock primitives
    //
    // Distributed lock built on Lua scripts — atomic acquire / release /
    // extend keyed by an adapter-owned token. Called from the Python
    // ``ValkeyLock`` / ``AsyncValkeyLock`` wrappers (see
    // ``django_cachex.lock``); the wrappers handle blocking, retry, and
    // context-manager semantics on top of these primitives.
    // =====================================================================

    #[pyo3(signature = (key, token, timeout_ms=None))]
    fn lock_acquire(
        slf: &Bound<'_, Self>,
        key: &str,
        token: &str,
        timeout_ms: Option<u64>,
    ) -> PyResult<bool> {
        adapter_sync!(slf, conn, conn.lock_acquire(key, token, timeout_ms).await)
            .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, token, timeout_ms=None))]
    fn alock_acquire(
        slf: &Bound<'_, Self>,
        key: &str,
        token: &str,
        timeout_ms: Option<u64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let token = token.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lock_acquire(&key, &token, timeout_ms).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, token))]
    fn lock_release(slf: &Bound<'_, Self>, key: &str, token: &str) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.lock_release(key, token).await)
            .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, token))]
    fn alock_release(
        slf: &Bound<'_, Self>,
        key: &str,
        token: &str,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let token = token.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lock_release(&key, &token).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, token, additional_ms))]
    fn lock_extend(
        slf: &Bound<'_, Self>,
        key: &str,
        token: &str,
        additional_ms: u64,
    ) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.lock_extend(key, token, additional_ms).await)
            .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, token, additional_ms))]
    fn alock_extend(
        slf: &Bound<'_, Self>,
        key: &str,
        token: &str,
        additional_ms: u64,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let token = token.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lock_extend(&key, &token, additional_ms).await.into_raw_result()
        })
    }

    // =====================================================================
    // Stampede-aware get/set surface (Phase 4e)
    //
    // The Cache layer treats these as the public adapter API and threads
    // ``stampede_prevention`` through. Sync paths inline the XFetch check;
    // async paths build a coroutine in ``_async_helpers`` so the multi-step
    // logic (TTL probe + early-return) stays expressible.
    // =====================================================================

    /// Resolve a per-call ``stampede_prevention`` override against the
    /// instance config — returns the effective ``StampedeConfig`` or ``None``.
    /// Used by :class:`RespCache` to decide whether the per-call buffer applies.
    #[pyo3(signature = (stampede_prevention=None))]
    fn _resolve_stampede(
        slf: &Bound<'_, Self>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let cfg = &slf.borrow().stampede_config;
        Ok(call_resolve_stampede(py, cfg, stampede_prevention)?.unbind())
    }

    /// Add the stampede buffer to ``timeout`` when prevention is enabled.
    #[pyo3(signature = (timeout, stampede_prevention=None))]
    fn _get_timeout_with_buffer(
        slf: &Bound<'_, Self>,
        timeout: Option<i64>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<i64>> {
        let py = slf.py();
        let cfg = &slf.borrow().stampede_config;
        call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)
    }

    #[pyo3(signature = (key, value, timeout, *, stampede_prevention=None))]
    fn add(
        slf: &Bound<'_, Self>,
        key: &str,
        value: EncodableT,
        timeout: Option<i64>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let py = slf.py();
        let nvalue = value.into_bytes();
        let cfg = &slf.borrow().stampede_config;
        let actual = call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)?;
        if actual == Some(0) {
            // ``timeout=0`` is "set then immediately delete" — used by Django
            // backends to express "keys with non-positive timeout expire now."
            let set: bool = adapter_sync!(slf, conn, conn.set_nx(&key, nvalue, None).await)
                .map_err(crate::client::to_py_err)?;
            if set {
                let _: i64 = adapter_sync!(slf, conn, conn.del(&key).await)
                    .map_err(crate::client::to_py_err)?;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            let ttl = timeout_to_ttl(actual);
            adapter_sync!(slf, conn, conn.set_nx(&key, nvalue, ttl).await)
                .map_err(crate::client::to_py_err)
        }
    }

    #[pyo3(signature = (key, value, timeout, *, stampede_prevention=None))]
    fn aadd(
        slf: &Bound<'_, Self>,
        key: &str,
        value: EncodableT,
        timeout: Option<i64>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        let key = key.to_string();
        let nvalue = value.into_bytes();
        let cfg = &slf.borrow().stampede_config;
        let actual = call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)?;
        if actual == Some(0) {
            // ``timeout=0`` async variant: SET NX, then if it succeeded, DELETE.
            // Returns True if SET NX won the race, else False.
            adapter_async!(slf, conn, {
                match conn.set_nx(&key, nvalue, None).await {
                    Ok(true) => {
                        let _ = conn.del(&key).await; // best-effort
                        crate::async_bridge::RawResult::from(true)
                    }
                    Ok(false) => crate::async_bridge::RawResult::from(false),
                    Err(e) => crate::client::classify(e),
                }
            })
        } else {
            let ttl = timeout_to_ttl(actual);
            adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.set_nx(&key, nvalue, ttl).await.into_raw_result()
            })
        }
    }

    #[pyo3(signature = (key, *, stampede_prevention=None))]
    fn get(
        slf: &Bound<'_, Self>,
        key: &str,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Option<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.get_bytes(&key).await);
        let val_opt = r.map_err(crate::client::to_py_err)?;
        let Some(val) = val_opt else {
            return Ok(py.None());
        };
        let cfg = &slf.borrow().stampede_config;
        let resolved = call_resolve_stampede(py, cfg, stampede_prevention)?;
        if !resolved.is_none() {
            // GET returns bytes (Redis is type-strict here). Stampede check
            // applies to TTL'd entries only.
            let ttl_r: Result<i64, _> = adapter_sync!(slf, conn, conn.ttl(&key).await);
            let ttl = ttl_r.map_err(crate::client::to_py_err)?;
            if ttl > 0 && call_should_recompute(py, ttl, &resolved)? {
                return Ok(py.None());
            }
        }
        Ok(pyo3::types::PyBytes::new(py, &val).into_any().unbind())
    }

    #[pyo3(signature = (key, *, stampede_prevention=None))]
    fn aget(
        slf: &Bound<'_, Self>,
        key: &str,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        let key = key.to_string();
        let cfg = &slf.borrow().stampede_config;
        let resolved_bound = call_resolve_stampede(py, cfg, stampede_prevention)?;
        let resolved: Option<Py<PyAny>> = if resolved_bound.is_none() {
            None
        } else {
            Some(resolved_bound.unbind())
        };
        adapter_async!(slf, conn, {
            let bytes_opt = match conn.get_bytes(&key).await {
                Ok(v) => v,
                Err(e) => return crate::client::classify(e),
            };
            match bytes_opt {
                None => crate::async_bridge::RawResult::Nil,
                Some(b) => {
                    if let Some(cfg_py) = resolved.as_ref() {
                        let ttl = match conn.ttl(&key).await {
                            Ok(t) => t,
                            Err(e) => return crate::client::classify(e),
                        };
                        if ttl > 0
                            && pyo3::Python::try_attach(|py| {
                                call_should_recompute(py, ttl, cfg_py.bind(py)).unwrap_or(false)
                            })
                            .unwrap_or(false)
                        {
                            crate::async_bridge::RawResult::Nil
                        } else {
                            crate::async_bridge::RawResult::OptBytes(Some(b))
                        }
                    } else {
                        crate::async_bridge::RawResult::OptBytes(Some(b))
                    }
                }
            }
        })
    }

    #[pyo3(signature = (key, value, timeout, *, stampede_prevention=None))]
    fn set(
        slf: &Bound<'_, Self>,
        key: &str,
        value: EncodableT,
        timeout: Option<i64>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let py = slf.py();
        let nvalue = value.into_bytes();
        let cfg = &slf.borrow().stampede_config;
        let actual = call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)?;
        if actual == Some(0) {
            let _: i64 = adapter_sync!(slf, conn, conn.del(&key).await)
                .map_err(crate::client::to_py_err)?;
        } else {
            let ttl = timeout_to_ttl(actual);
            let r: Result<(), _> = adapter_sync!(slf, conn, conn.set_bytes(&key, nvalue, ttl).await);
            r.map_err(crate::client::to_py_err)?;
        }
        Ok(())
    }

    #[pyo3(signature = (key, value, timeout, *, stampede_prevention=None))]
    fn aset(
        slf: &Bound<'_, Self>,
        key: &str,
        value: EncodableT,
        timeout: Option<i64>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        let key = key.to_string();
        let nvalue = value.into_bytes();
        let cfg = &slf.borrow().stampede_config;
        let actual = call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)?;
        let ttl = timeout_to_ttl(actual);
        if actual == Some(0) {
            adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.del(&key).await.into_raw_result()
                };
                crate::async_bridge::AwaitTransform::NilAfter
            )
        } else {
            adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.set_bytes(&key, nvalue, ttl).await.into_raw_result()
                };
                crate::async_bridge::AwaitTransform::NilAfter
            )
        }
    }

    #[pyo3(signature = (key, value, timeout, *, nx=false, xx=false, get=false, stampede_prevention=None))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn set_with_flags(
        slf: &Bound<'_, Self>,
        key: &str,
        value: EncodableT,
        timeout: Option<i64>,
        nx: bool,
        xx: bool,
        get: bool,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let nvalue = value.into_bytes();
        let cfg = &slf.borrow().stampede_config;
        let actual = call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)?;
        if actual == Some(0) {
            // ``timeout=0`` short-circuit: GET branch returns None (no value
            // to fetch); NX/XX branch returns False.
            return Ok(if get { py.None() } else { false.into_pyobject(py)?.to_owned().into_any().unbind() });
        }
        let ttl = timeout_to_ttl(actual);
        let r: Result<redis::Value, _> = adapter_sync!(
            slf,
            conn,
            conn.set_with_flags(&key, nvalue, ttl, nx, xx, get).await
        );
        let result = r.map_err(crate::client::to_py_err)?;
        if get {
            // SET ... GET: ``Nil`` → None, otherwise the prior value bytes.
            match result {
                redis::Value::Nil => Ok(py.None()),
                redis::Value::BulkString(b) => Ok(pyo3::types::PyBytes::new(py, &b).into_any().unbind()),
                other => Ok(crate::client::py_redis_value(py, other)?),
            }
        } else {
            // Without GET: ``OK`` (Okay/SimpleString) → True; NX/XX rejection
            // surfaces as Nil → False.
            let is_true = matches!(result, redis::Value::Okay | redis::Value::SimpleString(_));
            Ok(is_true.into_pyobject(py)?.to_owned().into_any().unbind())
        }
    }

    #[pyo3(signature = (key, value, timeout, *, nx=false, xx=false, get=false, stampede_prevention=None))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn aset_with_flags(
        slf: &Bound<'_, Self>,
        key: &str,
        value: EncodableT,
        timeout: Option<i64>,
        nx: bool,
        xx: bool,
        get: bool,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        let key = key.to_string();
        let nvalue = value.into_bytes();
        let cfg = &slf.borrow().stampede_config;
        let actual = call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)?;
        if actual == Some(0) {
            let value: Py<PyAny> = if get {
                py.None()
            } else {
                false.into_pyobject(py)?.to_owned().into_any().unbind()
            };
            return await_constant(py, value);
        }
        let ttl = timeout_to_ttl(actual);
        let transform = if get {
            crate::async_bridge::AwaitTransform::SetWithFlagsGet
        } else {
            crate::async_bridge::AwaitTransform::SetWithFlagsBool
        };
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.set_with_flags(&key, nvalue, ttl, nx, xx, get).await.into_raw_result()
            };
            transform
        )
    }

    #[pyo3(signature = (keys, *, stampede_prevention=None))]
    fn get_many(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        use pyo3::types::PyDict;
        let py = slf.py();
        let out = PyDict::new(py);
        if keys.is_empty() {
            return Ok(out.into_any().unbind());
        }
        let r: Result<Vec<Option<Vec<u8>>>, _> =
            adapter_sync!(slf, conn, conn.mget_bytes(&keys).await);
        let results = r.map_err(crate::client::to_py_err)?;
        for (k, v) in keys.iter().zip(results.into_iter()) {
            if let Some(b) = v {
                out.set_item(k.as_str(), pyo3::types::PyBytes::new(py, &b))?;
            }
        }
        let cfg = &slf.borrow().stampede_config;
        let resolved = call_resolve_stampede(py, cfg, stampede_prevention)?;
        if !resolved.is_none() && !out.is_empty() {
            // All present entries are bytes (MGET semantics); apply stampede
            // check to each. Per-key TTL roundtrip — same shape as before.
            let present_keys: Vec<String> = out
                .keys()
                .iter()
                .map(|k| k.extract::<String>())
                .collect::<PyResult<_>>()?;
            for k in &present_keys {
                let ttl_r: Result<i64, _> = adapter_sync!(slf, conn, conn.ttl(k).await);
                let ttl = ttl_r.map_err(crate::client::to_py_err)?;
                if ttl > 0 && call_should_recompute(py, ttl, &resolved)? {
                    out.del_item(k.as_str())?;
                }
            }
        }
        Ok(out.into_any().unbind())
    }

    #[pyo3(signature = (keys, *, stampede_prevention=None))]
    fn aget_many(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if keys.is_empty() {
            let empty = pyo3::types::PyDict::new(py).into_any().unbind();
            return await_constant(py, empty);
        }
        let cfg = &slf.borrow().stampede_config;
        let resolved_bound = call_resolve_stampede(py, cfg, stampede_prevention)?;
        let resolved: Option<Py<PyAny>> = if resolved_bound.is_none() {
            None
        } else {
            Some(resolved_bound.unbind())
        };
        adapter_async!(slf, conn, {
            let mget_r = match conn.mget_bytes(&keys).await {
                Ok(v) => v,
                Err(e) => return crate::client::classify(e),
            };
            // For each present key, optionally check TTL/stampede.
            let mut survivors: Vec<(String, Vec<u8>)> = Vec::new();
            for (k, v) in keys.iter().zip(mget_r.into_iter()) {
                let Some(b) = v else { continue };
                if let Some(cfg_py) = resolved.as_ref() {
                    let ttl = match conn.ttl(k).await {
                        Ok(t) => t,
                        Err(e) => return crate::client::classify(e),
                    };
                    if ttl > 0
                        && pyo3::Python::try_attach(|py| {
                            call_should_recompute(py, ttl, cfg_py.bind(py)).unwrap_or(false)
                        })
                        .unwrap_or(false)
                    {
                        continue;
                    }
                }
                survivors.push((k.clone(), b));
            }
            crate::async_bridge::RawResult::StringBytesPairs(survivors)
        })
    }

    #[pyo3(signature = (data, timeout, *, stampede_prevention=None))]
    fn set_many(
        slf: &Bound<'_, Self>,
        data: &Bound<'_, PyAny>,
        timeout: Option<i64>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let prepared = prepare_set_many(data)?;
        let empty_list = pyo3::types::PyList::empty(py).into_any().unbind();
        if prepared.is_empty() {
            return Ok(empty_list);
        }
        let cfg = &slf.borrow().stampede_config;
        let actual = call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)?;
        if actual == Some(0) {
            let keys: Vec<String> = prepared.into_iter().map(|(k, _)| k).collect();
            let _: i64 = adapter_sync!(slf, conn, conn.del_many(&keys).await)
                .map_err(crate::client::to_py_err)?;
        } else {
            let ttl = timeout_to_ttl(actual);
            let r: Result<(), _> =
                adapter_sync!(slf, conn, conn.pipeline_set(&prepared, ttl).await);
            r.map_err(crate::client::to_py_err)?;
        }
        Ok(empty_list)
    }

    #[pyo3(signature = (data, timeout, *, stampede_prevention=None))]
    fn aset_many(
        slf: &Bound<'_, Self>,
        data: &Bound<'_, PyAny>,
        timeout: Option<i64>,
        stampede_prevention: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        let prepared = prepare_set_many(data)?;
        let empty_list = pyo3::types::PyList::empty(py).into_any().unbind();
        if prepared.is_empty() {
            return await_constant(py, empty_list);
        }
        let cfg = &slf.borrow().stampede_config;
        let actual = call_get_timeout_with_buffer(py, timeout, cfg, stampede_prevention)?;
        let ttl = timeout_to_ttl(actual);
        if actual == Some(0) {
            let keys: Vec<String> = prepared.into_iter().map(|(k, _)| k).collect();
            adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.del_many(&keys).await.into_raw_result()
                };
                crate::async_bridge::AwaitTransform::EmptyListAfter
            )
        } else {
            adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.pipeline_set(&prepared, ttl).await.into_raw_result()
                };
                crate::async_bridge::AwaitTransform::EmptyListAfter
            )
        }
    }

    // =====================================================================
    // Trivial passthrough commands (Phase 3a: TTL / delete / exists / incr / touch)
    //
    // All methods dispatch through the driver's Python-exposed methods
    // (`call_method1`). One method-lookup per call vs. typed inherent
    // calls — but the driver's `#[pymethods]` are private to its module,
    // and Python dispatch keeps the adapter independent of changes in
    // the driver's Rust API surface.
    // =====================================================================

    fn delete(slf: &Bound<'_, Self>, key: &str) -> PyResult<bool> {
        let n: i64 = adapter_sync!(slf, conn, conn.del(key).await)
            .map_err(crate::client::to_py_err)?;
        Ok(n > 0)
    }

    fn adelete(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.del(&key).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::ToBool
        )
    }

    fn has_key(slf: &Bound<'_, Self>, key: &str) -> PyResult<bool> {
        adapter_sync!(slf, conn, conn.exists(key).await).map_err(crate::client::to_py_err)
    }

    fn ahas_key(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.exists(&key).await.into_raw_result()
        })
    }

    /// Redis-py-style alias for ``has_key`` — kept so cross-driver tests
    /// that use ``cache.get_client().exists(...)`` work against the
    /// adapter (which is what ``get_client()`` returns here).
    fn exists(slf: &Bound<'_, Self>, key: &str) -> PyResult<bool> {
        Self::has_key(slf, key)
    }

    fn aexists(
        slf: &Bound<'_, Self>,
        key: &str,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        Self::ahas_key(slf, key)
    }

    #[pyo3(signature = (key, delta=1))]
    fn incr(slf: &Bound<'_, Self>, key: &str, delta: i64) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.incr_by(key, delta).await)
            .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, delta=1))]
    fn aincr(slf: &Bound<'_, Self>, key: &str, delta: i64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.incr_by(&key, delta).await.into_raw_result()
        })
    }

    fn expire(slf: &Bound<'_, Self>, key: &str, timeout: ExpiryT<'_>) -> PyResult<bool> {
        let secs: u64 = timeout.to_seconds().max(0).try_into().unwrap_or(0);
        adapter_sync!(slf, conn, conn.expire(key, secs).await).map_err(crate::client::to_py_err)
    }

    fn aexpire(slf: &Bound<'_, Self>, key: &str, timeout: ExpiryT<'_>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let secs: u64 = timeout.to_seconds().max(0).try_into().unwrap_or(0);
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.expire(&key, secs).await.into_raw_result()
        })
    }

    fn persist(slf: &Bound<'_, Self>, key: &str) -> PyResult<bool> {
        adapter_sync!(slf, conn, conn.persist(key).await).map_err(crate::client::to_py_err)
    }

    fn apersist(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.persist(&key).await.into_raw_result()
        })
    }

    fn ttl(slf: &Bound<'_, Self>, key: &str) -> PyResult<Option<i64>> {
        let raw: i64 = adapter_sync!(slf, conn, conn.ttl(key).await)
            .map_err(crate::client::to_py_err)?;
        Ok(Self::_normalize_ttl(raw))
    }

    fn attl(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.ttl(&key).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::NormalizeTtl
        )
    }

    fn pttl(slf: &Bound<'_, Self>, key: &str) -> PyResult<Option<i64>> {
        let raw: i64 = adapter_sync!(slf, conn, conn.pttl(key).await)
            .map_err(crate::client::to_py_err)?;
        Ok(Self::_normalize_ttl(raw))
    }

    fn apttl(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.pttl(&key).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::NormalizeTtl
        )
    }

    fn pexpire(slf: &Bound<'_, Self>, key: &str, timeout: ExpiryT<'_>) -> PyResult<bool> {
        let ms = timeout.to_milliseconds();
        adapter_sync!(slf, conn, conn.pexpire(key, ms).await).map_err(crate::client::to_py_err)
    }

    fn apexpire(slf: &Bound<'_, Self>, key: &str, timeout: ExpiryT<'_>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let ms = timeout.to_milliseconds();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.pexpire(&key, ms).await.into_raw_result()
        })
    }

    fn expireat(slf: &Bound<'_, Self>, key: &str, when: AbsExpiryT<'_>) -> PyResult<bool> {
        let ts = when.to_unix()?;
        adapter_sync!(slf, conn, conn.expireat(key, ts).await).map_err(crate::client::to_py_err)
    }

    fn aexpireat(slf: &Bound<'_, Self>, key: &str, when: AbsExpiryT<'_>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let ts = when.to_unix()?;
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.expireat(&key, ts).await.into_raw_result()
        })
    }

    fn pexpireat(slf: &Bound<'_, Self>, key: &str, when: AbsExpiryT<'_>) -> PyResult<bool> {
        let ms = when.to_unix_milliseconds()?;
        adapter_sync!(slf, conn, conn.pexpireat(key, ms).await).map_err(crate::client::to_py_err)
    }

    fn apexpireat(slf: &Bound<'_, Self>, key: &str, when: AbsExpiryT<'_>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let ms = when.to_unix_milliseconds()?;
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.pexpireat(&key, ms).await.into_raw_result()
        })
    }

    fn expiretime(slf: &Bound<'_, Self>, key: &str) -> PyResult<Option<i64>> {
        let raw: i64 = adapter_sync!(slf, conn, conn.expiretime(key).await)
            .map_err(crate::client::to_py_err)?;
        Ok(Self::_normalize_ttl(raw))
    }

    fn aexpiretime(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.expiretime(&key).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::NormalizeTtl
        )
    }

    #[pyo3(signature = (key, timeout=None))]
    fn touch(
        slf: &Bound<'_, Self>,
        key: &str,
        timeout: Option<ExpiryT<'_>>,
    ) -> PyResult<bool> {
        match timeout {
            None => adapter_sync!(slf, conn, conn.persist(key).await),
            Some(t) => {
                let secs: u64 = t.to_seconds().max(0).try_into().unwrap_or(0);
                adapter_sync!(slf, conn, conn.expire(key, secs).await)
            }
        }
        .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, timeout=None))]
    fn atouch(
        slf: &Bound<'_, Self>,
        key: &str,
        timeout: Option<ExpiryT<'_>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        match timeout {
            None => adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.persist(&key).await.into_raw_result()
            }),
            Some(t) => {
                let secs: u64 = t.to_seconds().max(0).try_into().unwrap_or(0);
                adapter_async!(slf, conn, {
                    use crate::client::IntoRawResult;
                    conn.expire(&key, secs).await.into_raw_result()
                })
            }
        }
    }

    // =====================================================================
    // Phase 3b: Hash commands
    // =====================================================================

    #[pyo3(signature = (key, field=None, value=None, mapping=None, items=None))]
    fn hset(
        slf: &Bound<'_, Self>,
        key: &str,
        field: Option<String>,
        value: Option<&Bound<'_, PyAny>>,
        mapping: Option<&Bound<'_, PyDict>>,
        items: Option<Vec<Bound<'_, PyAny>>>,
    ) -> PyResult<i64> {
        let mut pairs: Vec<(String, Vec<u8>)> = Vec::new();
        if let Some(f) = field {
            let v = value.ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("hset: value required when field provided")
            })?;
            pairs.push((f, value_to_bytes(v)?));
        }
        if let Some(m) = mapping {
            for (k, v) in m.iter() {
                pairs.push((decode_str(&k)?, value_to_bytes(&v)?));
            }
        }
        if let Some(its) = items {
            let mut it = its.into_iter();
            while let (Some(f), Some(v)) = (it.next(), it.next()) {
                pairs.push((decode_str(&f)?, value_to_bytes(&v)?));
            }
        }
        if pairs.is_empty() {
            return Ok(0);
        }
        if pairs.len() == 1 {
            let (f, v) = pairs.into_iter().next().unwrap();
            return adapter_sync!(slf, conn, conn.hset(key, &f, &v).await)
                .map_err(crate::client::to_py_err);
        }
        // Multi-field: HMSET returns OK; we report new-field count via HLEN delta.
        let before: i64 = adapter_sync!(slf, conn, conn.hlen(key).await)
            .map_err(crate::client::to_py_err)?;
        adapter_sync!(slf, conn, conn.hmset(key, &pairs).await)
            .map_err(crate::client::to_py_err)?;
        let after: i64 = adapter_sync!(slf, conn, conn.hlen(key).await)
            .map_err(crate::client::to_py_err)?;
        Ok(after - before)
    }

    fn hsetnx(slf: &Bound<'_, Self>, key: &str, field: &str, value: EncodableT) -> PyResult<bool> {
        let v = value.into_bytes();
        adapter_sync!(slf, conn, conn.hsetnx(key, field, &v).await)
            .map_err(crate::client::to_py_err)
    }

    fn ahsetnx(slf: &Bound<'_, Self>, key: &str, field: &str, value: EncodableT) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let field = field.to_string();
        let v = value.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hsetnx(&key, &field, &v).await.into_raw_result()
        })
    }

    fn hget(slf: &Bound<'_, Self>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Option<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.hget(key, field).await);
        let bytes = r.map_err(crate::client::to_py_err)?;
        Ok(match bytes {
            Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
            None => py.None(),
        })
    }

    fn ahget(slf: &Bound<'_, Self>, key: &str, field: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let field = field.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hget(&key, &field).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *fields))]
    fn hmget(slf: &Bound<'_, Self>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        if fields.is_empty() {
            return Ok(pyo3::types::PyList::empty(py).into_any().unbind());
        }
        let r: Result<Vec<Option<Vec<u8>>>, _> =
            adapter_sync!(slf, conn, conn.hmget(key, &fields).await);
        let items = r.map_err(crate::client::to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|opt| match opt {
                Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
                None => py.None(),
            })
            .collect();
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    fn hgetall(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Vec<(Vec<u8>, Vec<u8>)>, _> =
            adapter_sync!(slf, conn, conn.hgetall(key).await);
        let pairs = r.map_err(crate::client::to_py_err)?;
        let out = PyDict::new(py);
        for (k, v) in pairs {
            let k_str = String::from_utf8_lossy(&k).into_owned();
            out.set_item(k_str, pyo3::types::PyBytes::new(py, &v))?;
        }
        Ok(out.into_any().unbind())
    }

    #[pyo3(signature = (key, *fields))]
    fn hdel(slf: &Bound<'_, Self>, key: &str, fields: Vec<String>) -> PyResult<i64> {
        if fields.is_empty() {
            return Ok(0);
        }
        adapter_sync!(slf, conn, conn.hdel(key, &fields).await).map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, *fields))]
    fn ahdel(slf: &Bound<'_, Self>, key: &str, fields: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if fields.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hdel(&key, &fields).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *fields))]
    fn ahmget(slf: &Bound<'_, Self>, key: &str, fields: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if fields.is_empty() {
            let empty = pyo3::types::PyList::empty(py).into_any().unbind();
            return await_constant(py, empty);
        }
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hmget(&key, &fields).await.into_raw_result()
        })
    }

    fn hexists(slf: &Bound<'_, Self>, key: &str, field: &str) -> PyResult<bool> {
        adapter_sync!(slf, conn, conn.hexists(key, field).await).map_err(crate::client::to_py_err)
    }

    fn ahexists(slf: &Bound<'_, Self>, key: &str, field: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let field = field.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hexists(&key, &field).await.into_raw_result()
        })
    }

    fn hlen(slf: &Bound<'_, Self>, key: &str) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.hlen(key).await).map_err(crate::client::to_py_err)
    }

    fn ahlen(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hlen(&key).await.into_raw_result()
        })
    }

    fn hkeys(slf: &Bound<'_, Self>, key: &str) -> PyResult<Vec<String>> {
        adapter_sync!(slf, conn, conn.hkeys(key).await).map_err(crate::client::to_py_err)
    }

    fn hvals(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Vec<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.hvals(key).await);
        let items = r.map_err(crate::client::to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|b| pyo3::types::PyBytes::new(py, &b).into_any().unbind())
            .collect();
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    fn ahvals(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hvals(&key).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, field, amount=1))]
    fn hincrby(slf: &Bound<'_, Self>, key: &str, field: &str, amount: i64) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.hincrby(key, field, amount).await)
            .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, field, amount=1))]
    fn ahincrby(slf: &Bound<'_, Self>, key: &str, field: &str, amount: i64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let field = field.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hincrby(&key, &field, amount).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, field, amount=1.0))]
    fn hincrbyfloat(slf: &Bound<'_, Self>, key: &str, field: &str, amount: f64) -> PyResult<f64> {
        adapter_sync!(slf, conn, conn.hincrbyfloat(key, field, amount).await)
            .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, field, amount=1.0))]
    fn ahincrbyfloat(slf: &Bound<'_, Self>, key: &str, field: &str, amount: f64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let field = field.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hincrbyfloat(&key, &field, amount).await.into_raw_result()
        })
    }

    // =====================================================================
    // Phase 3c: List commands
    // =====================================================================

    #[pyo3(signature = (key, *values))]
    fn lpush(slf: &Bound<'_, Self>, key: &str, values: Vec<EncodableT>) -> PyResult<i64> {
        if values.is_empty() {
            return adapter_sync!(slf, conn, conn.llen(key).await).map_err(crate::client::to_py_err);
        }
        let v: Vec<Vec<u8>> = values.into_iter().map(EncodableT::into_bytes).collect();
        adapter_sync!(slf, conn, conn.lpush(key, v.clone()).await).map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, *values))]
    fn rpush(slf: &Bound<'_, Self>, key: &str, values: Vec<EncodableT>) -> PyResult<i64> {
        if values.is_empty() {
            return adapter_sync!(slf, conn, conn.llen(key).await).map_err(crate::client::to_py_err);
        }
        let v: Vec<Vec<u8>> = values.into_iter().map(EncodableT::into_bytes).collect();
        adapter_sync!(slf, conn, conn.rpush(key, v.clone()).await).map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, *values))]
    fn alpush(slf: &Bound<'_, Self>, key: &str, values: Vec<EncodableT>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        if values.is_empty() {
            return adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.llen(&key).await.into_raw_result()
            });
        }
        let v: Vec<Vec<u8>> = values.into_iter().map(EncodableT::into_bytes).collect();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lpush(&key, v).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *values))]
    fn arpush(slf: &Bound<'_, Self>, key: &str, values: Vec<EncodableT>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        if values.is_empty() {
            return adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.llen(&key).await.into_raw_result()
            });
        }
        let v: Vec<Vec<u8>> = values.into_iter().map(EncodableT::into_bytes).collect();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.rpush(&key, v).await.into_raw_result()
        })
    }

    fn llen(slf: &Bound<'_, Self>, key: &str) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.llen(key).await).map_err(crate::client::to_py_err)
    }

    fn allen(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.llen(&key).await.into_raw_result()
        })
    }

    fn lrange(slf: &Bound<'_, Self>, key: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Vec<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.lrange(key, start, end).await);
        let items: Vec<Vec<u8>> = r.map_err(crate::client::to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .iter()
            .map(|b| pyo3::types::PyBytes::new(py, b.as_slice()).into_any().unbind())
            .collect();
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    fn alrange(slf: &Bound<'_, Self>, key: &str, start: i64, end: i64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lrange(&key, start, end).await.into_raw_result()
        })
    }

    fn lindex(slf: &Bound<'_, Self>, key: &str, index: i64) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Option<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.lindex(key, index).await);
        let bytes = r.map_err(crate::client::to_py_err)?;
        Ok(match bytes {
            Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
            None => py.None(),
        })
    }

    fn alindex(slf: &Bound<'_, Self>, key: &str, index: i64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lindex(&key, index).await.into_raw_result()
        })
    }

    fn lset(slf: &Bound<'_, Self>, key: &str, index: i64, value: EncodableT) -> PyResult<bool> {
        let v = value.into_bytes();
        adapter_sync!(slf, conn, conn.lset(key, index, &v).await)
            .map_err(crate::client::to_py_err)?;
        Ok(true)
    }

    fn alset(slf: &Bound<'_, Self>, key: &str, index: i64, value: EncodableT) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let v = value.into_bytes();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.lset(&key, index, &v).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::TrueAfter
        )
    }

    fn lrem(slf: &Bound<'_, Self>, key: &str, count: i64, value: EncodableT) -> PyResult<i64> {
        let v = value.into_bytes();
        adapter_sync!(slf, conn, conn.lrem(key, count, &v).await).map_err(crate::client::to_py_err)
    }

    fn alrem(slf: &Bound<'_, Self>, key: &str, count: i64, value: EncodableT) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let v = value.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lrem(&key, count, &v).await.into_raw_result()
        })
    }

    fn ltrim(slf: &Bound<'_, Self>, key: &str, start: i64, end: i64) -> PyResult<bool> {
        adapter_sync!(slf, conn, conn.ltrim(key, start, end).await)
            .map_err(crate::client::to_py_err)?;
        Ok(true)
    }

    fn altrim(slf: &Bound<'_, Self>, key: &str, start: i64, end: i64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.ltrim(&key, start, end).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::TrueAfter
        )
    }

    fn linsert(
        slf: &Bound<'_, Self>,
        key: &str,
        whence: &str,
        pivot: EncodableT,
        value: EncodableT,
    ) -> PyResult<i64> {
        let before = whence.eq_ignore_ascii_case("BEFORE");
        let pivot = pivot.into_bytes();
        let v = value.into_bytes();
        adapter_sync!(slf, conn, conn.linsert(key, before, &pivot, &v).await)
            .map_err(crate::client::to_py_err)
    }

    fn alinsert(
        slf: &Bound<'_, Self>,
        key: &str,
        whence: &str,
        pivot: EncodableT,
        value: EncodableT,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let before = whence.eq_ignore_ascii_case("BEFORE");
        let pivot = pivot.into_bytes();
        let v = value.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.linsert(&key, before, &pivot, &v).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, count=None))]
    fn lpop(slf: &Bound<'_, Self>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        match count {
            None => {
                let r: Result<Option<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.lpop(key).await);
                let bytes = r.map_err(crate::client::to_py_err)?;
                Ok(match bytes {
                    Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
                    None => py.None(),
                })
            }
            Some(n) => {
                let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.lpop_count(key, n).await);
                crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)
            }
        }
    }

    #[pyo3(signature = (key, count=None))]
    fn rpop(slf: &Bound<'_, Self>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        match count {
            None => {
                let r: Result<Option<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.rpop(key).await);
                let bytes = r.map_err(crate::client::to_py_err)?;
                Ok(match bytes {
                    Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
                    None => py.None(),
                })
            }
            Some(n) => {
                let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.rpop_count(key, n).await);
                crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)
            }
        }
    }

    #[pyo3(signature = (key, count=None))]
    fn alpop(slf: &Bound<'_, Self>, key: &str, count: Option<i64>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        match count {
            None => adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.lpop(&key).await.into_raw_result()
            }),
            Some(n) => adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.lpop_count(&key, n).await.into_raw_result()
                };
                crate::async_bridge::AwaitTransform::ToList
            ),
        }
    }

    #[pyo3(signature = (key, count=None))]
    fn arpop(slf: &Bound<'_, Self>, key: &str, count: Option<i64>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        match count {
            None => adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.rpop(&key).await.into_raw_result()
            }),
            Some(n) => adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.rpop_count(&key, n).await.into_raw_result()
                };
                crate::async_bridge::AwaitTransform::ToList
            ),
        }
    }

    #[pyo3(signature = (src, dst, wherefrom="LEFT", whereto="RIGHT"))]
    fn lmove(
        slf: &Bound<'_, Self>,
        src: &str,
        dst: &str,
        wherefrom: &str,
        whereto: &str,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Option<Vec<u8>>, _> =
            adapter_sync!(slf, conn, conn.lmove(src, dst, wherefrom, whereto).await);
        let bytes = r.map_err(crate::client::to_py_err)?;
        Ok(match bytes {
            Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
            None => py.None(),
        })
    }

    #[pyo3(signature = (src, dst, wherefrom="LEFT", whereto="RIGHT"))]
    fn almove(
        slf: &Bound<'_, Self>,
        src: &str,
        dst: &str,
        wherefrom: &str,
        whereto: &str,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let src = src.to_string();
        let dst = dst.to_string();
        let wherefrom = wherefrom.to_string();
        let whereto = whereto.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lmove(&src, &dst, &wherefrom, &whereto).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, value, rank=None, count=None, maxlen=None))]
    fn lpos(
        slf: &Bound<'_, Self>,
        key: &str,
        value: EncodableT,
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let v = value.into_bytes();
        let r: Result<redis::Value, _> =
            adapter_sync!(slf, conn, conn.lpos(key, &v, rank, count, maxlen).await);
        crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)
    }

    #[pyo3(signature = (key, value, rank=None, count=None, maxlen=None))]
    fn alpos(
        slf: &Bound<'_, Self>,
        key: &str,
        value: EncodableT,
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let v = value.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.lpos(&key, &v, rank, count, maxlen).await.into_raw_result()
        })
    }

    // =====================================================================
    // Phase 3d: Set commands
    // =====================================================================

    #[pyo3(signature = (key, *members))]
    fn sadd(slf: &Bound<'_, Self>, key: &str, members: Vec<EncodableT>) -> PyResult<i64> {
        if members.is_empty() {
            return Ok(0);
        }
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        adapter_sync!(slf, conn, conn.sadd(key, m.clone()).await).map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, *members))]
    fn srem(slf: &Bound<'_, Self>, key: &str, members: Vec<EncodableT>) -> PyResult<i64> {
        if members.is_empty() {
            return Ok(0);
        }
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        adapter_sync!(slf, conn, conn.srem(key, m.clone()).await).map_err(crate::client::to_py_err)
    }

    fn smembers(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Vec<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.smembers(key).await);
        let items: Vec<Vec<u8>> = r.map_err(crate::client::to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .iter()
            .map(|b| pyo3::types::PyBytes::new(py, b.as_slice()).into_any().unbind())
            .collect();
        let set_cls = py.import("builtins")?.getattr("set")?;
        Ok(set_cls.call1((pyo3::types::PyList::new(py, py_items)?,))?.unbind())
    }

    fn asmembers(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.smembers(&key).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::ToSet
        )
    }

    #[pyo3(signature = (key, *members))]
    fn asadd(slf: &Bound<'_, Self>, key: &str, members: Vec<EncodableT>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if members.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        let key = key.to_string();
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.sadd(&key, m).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn asrem(slf: &Bound<'_, Self>, key: &str, members: Vec<EncodableT>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if members.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        let key = key.to_string();
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.srem(&key, m).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn asmismember(slf: &Bound<'_, Self>, key: &str, members: Vec<EncodableT>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if members.is_empty() {
            let empty = pyo3::types::PyList::empty(py).into_any().unbind();
            return await_constant(py, empty);
        }
        let key = key.to_string();
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.smismember(&key, m).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::MapIntToBool
        )
    }

    #[pyo3(signature = (key, count=None))]
    fn aspop(slf: &Bound<'_, Self>, key: &str, count: Option<i64>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        match count {
            None => adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.spop(&key).await.into_raw_result()
            }),
            Some(n) => adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.spop_count(&key, n).await.into_raw_result()
                };
                crate::async_bridge::AwaitTransform::ToList
            ),
        }
    }

    #[pyo3(signature = (key, count=None))]
    fn asrandmember(slf: &Bound<'_, Self>, key: &str, count: Option<i64>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        match count {
            None => adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.srandmember(&key).await.into_raw_result()
            }),
            Some(n) => adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.srandmember_count(&key, n).await.into_raw_result()
                };
                crate::async_bridge::AwaitTransform::ToList
            ),
        }
    }

    fn asinter(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.sinter(&keys).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::ToSet
        )
    }

    fn asunion(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.sunion(&keys).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::ToSet
        )
    }

    fn asdiff(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.sdiff(&keys).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::ToSet
        )
    }

    fn sismember(slf: &Bound<'_, Self>, key: &str, member: EncodableT) -> PyResult<bool> {
        let m = member.into_bytes();
        adapter_sync!(slf, conn, conn.sismember(key, &m).await).map_err(crate::client::to_py_err)
    }

    fn asismember(slf: &Bound<'_, Self>, key: &str, member: EncodableT) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let m = member.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.sismember(&key, &m).await.into_raw_result()
        })
    }

    fn scard(slf: &Bound<'_, Self>, key: &str) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.scard(key).await).map_err(crate::client::to_py_err)
    }

    fn ascard(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.scard(&key).await.into_raw_result()
        })
    }

    fn sinter(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Vec<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.sinter(&keys).await);
        let items: Vec<Vec<u8>> = r.map_err(crate::client::to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .iter()
            .map(|b| pyo3::types::PyBytes::new(py, b.as_slice()).into_any().unbind())
            .collect();
        let set_cls = py.import("builtins")?.getattr("set")?;
        Ok(set_cls.call1((pyo3::types::PyList::new(py, py_items)?,))?.unbind())
    }

    fn sunion(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Vec<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.sunion(&keys).await);
        let items: Vec<Vec<u8>> = r.map_err(crate::client::to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .iter()
            .map(|b| pyo3::types::PyBytes::new(py, b.as_slice()).into_any().unbind())
            .collect();
        let set_cls = py.import("builtins")?.getattr("set")?;
        Ok(set_cls.call1((pyo3::types::PyList::new(py, py_items)?,))?.unbind())
    }

    fn sdiff(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Vec<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.sdiff(&keys).await);
        let items: Vec<Vec<u8>> = r.map_err(crate::client::to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .iter()
            .map(|b| pyo3::types::PyBytes::new(py, b.as_slice()).into_any().unbind())
            .collect();
        let set_cls = py.import("builtins")?.getattr("set")?;
        Ok(set_cls.call1((pyo3::types::PyList::new(py, py_items)?,))?.unbind())
    }

    fn smove(slf: &Bound<'_, Self>, src: &str, dst: &str, member: EncodableT) -> PyResult<bool> {
        let m = member.into_bytes();
        adapter_sync!(slf, conn, conn.smove(src, dst, &m).await).map_err(crate::client::to_py_err)
    }

    fn asmove(slf: &Bound<'_, Self>, src: &str, dst: &str, member: EncodableT) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let src = src.to_string();
        let dst = dst.to_string();
        let m = member.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.smove(&src, &dst, &m).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn smismember(slf: &Bound<'_, Self>, key: &str, members: Vec<EncodableT>) -> PyResult<Vec<bool>> {
        if members.is_empty() {
            return Ok(Vec::new());
        }
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.smismember(key, m.clone()).await);
        let value = r.map_err(crate::client::to_py_err)?;
        // SMISMEMBER returns array of 0/1 ints; convert to bools.
        let py = slf.py();
        let py_val = crate::client::py_redis_value(py, value)?;
        let mut out = Vec::new();
        for item in py_val.bind(py).try_iter()? {
            out.push(item?.is_truthy()?);
        }
        Ok(out)
    }

    #[pyo3(signature = (key, count=None))]
    fn spop(slf: &Bound<'_, Self>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        match count {
            None => {
                let r: Result<Option<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.spop(key).await);
                let bytes = r.map_err(crate::client::to_py_err)?;
                Ok(match bytes {
                    Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
                    None => py.None(),
                })
            }
            Some(n) => {
                let r: Result<Vec<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.spop_count(key, n).await);
                let items: Vec<Vec<u8>> = r.map_err(crate::client::to_py_err)?;
                let py_items: Vec<Py<PyAny>> = items
                    .iter()
                    .map(|b| pyo3::types::PyBytes::new(py, b.as_slice()).into_any().unbind())
                    .collect();
                Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
            }
        }
    }

    #[pyo3(signature = (key, count=None))]
    fn srandmember(slf: &Bound<'_, Self>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        match count {
            None => {
                let r: Result<Option<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.srandmember(key).await);
                let bytes = r.map_err(crate::client::to_py_err)?;
                Ok(match bytes {
                    Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
                    None => py.None(),
                })
            }
            Some(n) => {
                let r: Result<Vec<Vec<u8>>, _> = adapter_sync!(slf, conn, conn.srandmember_count(key, n).await);
                let items: Vec<Vec<u8>> = r.map_err(crate::client::to_py_err)?;
                let py_items: Vec<Py<PyAny>> = items
                    .iter()
                    .map(|b| pyo3::types::PyBytes::new(py, b.as_slice()).into_any().unbind())
                    .collect();
                Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
            }
        }
    }

    fn sdiffstore(slf: &Bound<'_, Self>, dest: &str, keys: Vec<String>) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.sdiffstore(dest, &keys).await).map_err(crate::client::to_py_err)
    }

    fn asdiffstore(slf: &Bound<'_, Self>, dest: &str, keys: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let dest = dest.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.sdiffstore(&dest, &keys).await.into_raw_result()
        })
    }

    fn sinterstore(slf: &Bound<'_, Self>, dest: &str, keys: Vec<String>) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.sinterstore(dest, &keys).await).map_err(crate::client::to_py_err)
    }

    fn asinterstore(slf: &Bound<'_, Self>, dest: &str, keys: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let dest = dest.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.sinterstore(&dest, &keys).await.into_raw_result()
        })
    }

    fn sunionstore(slf: &Bound<'_, Self>, dest: &str, keys: Vec<String>) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.sunionstore(dest, &keys).await).map_err(crate::client::to_py_err)
    }

    fn asunionstore(slf: &Bound<'_, Self>, dest: &str, keys: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let dest = dest.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.sunionstore(&dest, &keys).await.into_raw_result()
        })
    }

    // =====================================================================
    // Phase 3e: Sorted-set commands
    // =====================================================================

    #[pyo3(signature = (key, *members))]
    fn zrem(slf: &Bound<'_, Self>, key: &str, members: Vec<EncodableT>) -> PyResult<i64> {
        if members.is_empty() {
            return Ok(0);
        }
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        adapter_sync!(slf, conn, conn.zrem(key, m.clone()).await).map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, *members))]
    fn azrem(slf: &Bound<'_, Self>, key: &str, members: Vec<EncodableT>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if members.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        let key = key.to_string();
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zrem(&key, m).await.into_raw_result()
        })
    }

    fn zscore(slf: &Bound<'_, Self>, key: &str, member: EncodableT) -> PyResult<Option<f64>> {
        let m = member.into_bytes();
        adapter_sync!(slf, conn, conn.zscore(key, &m).await).map_err(crate::client::to_py_err)
    }

    fn azscore(slf: &Bound<'_, Self>, key: &str, member: EncodableT) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let m = member.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zscore(&key, &m).await.into_raw_result()
        })
    }

    fn zrank(slf: &Bound<'_, Self>, key: &str, member: EncodableT) -> PyResult<Option<i64>> {
        let m = member.into_bytes();
        adapter_sync!(slf, conn, conn.zrank(key, &m).await).map_err(crate::client::to_py_err)
    }

    fn azrank(slf: &Bound<'_, Self>, key: &str, member: EncodableT) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let m = member.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zrank(&key, &m).await.into_raw_result()
        })
    }

    fn zcard(slf: &Bound<'_, Self>, key: &str) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.zcard(key).await).map_err(crate::client::to_py_err)
    }

    fn azcard(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zcard(&key).await.into_raw_result()
        })
    }

    fn zcount(
        slf: &Bound<'_, Self>,
        key: &str,
        min_score: &Bound<'_, PyAny>,
        max_score: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let mn: String = min_score.str()?.extract()?;
        let mx: String = max_score.str()?.extract()?;
        adapter_sync!(slf, conn, conn.zcount(key, &mn, &mx).await).map_err(crate::client::to_py_err)
    }

    fn azcount(
        slf: &Bound<'_, Self>,
        key: &str,
        min_score: &Bound<'_, PyAny>,
        max_score: &Bound<'_, PyAny>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let mn: String = min_score.str()?.extract()?;
        let mx: String = max_score.str()?.extract()?;
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zcount(&key, &mn, &mx).await.into_raw_result()
        })
    }

    fn zincrby(
        slf: &Bound<'_, Self>,
        key: &str,
        amount: f64,
        member: EncodableT,
    ) -> PyResult<f64> {
        let m = member.into_bytes();
        adapter_sync!(slf, conn, conn.zincrby(key, &m, amount).await).map_err(crate::client::to_py_err)
    }

    fn azincrby(
        slf: &Bound<'_, Self>,
        key: &str,
        amount: f64,
        member: EncodableT,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let m = member.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zincrby(&key, &m, amount).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, start, end, *, withscores=false))]
    fn zrange(
        slf: &Bound<'_, Self>,
        key: &str,
        start: i64,
        end: i64,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<redis::Value, _> =
            adapter_sync!(slf, conn, conn.zrange(key, start, end, withscores).await);
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        decode_zrange(py, raw.bind(py), withscores)
    }

    #[pyo3(signature = (key, start, end, *, withscores=false))]
    fn zrevrange(
        slf: &Bound<'_, Self>,
        key: &str,
        start: i64,
        end: i64,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<redis::Value, _> =
            adapter_sync!(slf, conn, conn.zrevrange(key, start, end, withscores).await);
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        decode_zrange(py, raw.bind(py), withscores)
    }

    #[pyo3(signature = (key, min_score, max_score, *, start=None, num=None, withscores=false))]
    fn zrangebyscore(
        slf: &Bound<'_, Self>,
        key: &str,
        min_score: &Bound<'_, PyAny>,
        max_score: &Bound<'_, PyAny>,
        start: Option<i64>,
        num: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let mn: String = min_score.str()?.extract()?;
        let mx: String = max_score.str()?.extract()?;
        let py = slf.py();
        let r: Result<redis::Value, _> = adapter_sync!(
            slf,
            conn,
            conn.zrangebyscore(key, &mn, &mx, withscores, start, num).await
        );
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        decode_zrange(py, raw.bind(py), withscores)
    }

    #[pyo3(signature = (key, count=None))]
    fn zpopmin(
        slf: &Bound<'_, Self>,
        key: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let n = count.unwrap_or(1);
        let r: Result<Vec<(Vec<u8>, f64)>, _> = adapter_sync!(slf, conn, conn.zpopmin(key, n).await);
        crate::client::py_scored_members(slf.py(), r.map_err(crate::client::to_py_err)?)
    }

    #[pyo3(signature = (key, count=None))]
    fn zpopmax(
        slf: &Bound<'_, Self>,
        key: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let n = count.unwrap_or(1);
        let r: Result<Vec<(Vec<u8>, f64)>, _> = adapter_sync!(slf, conn, conn.zpopmax(key, n).await);
        crate::client::py_scored_members(slf.py(), r.map_err(crate::client::to_py_err)?)
    }

    fn zrevrank(slf: &Bound<'_, Self>, key: &str, member: EncodableT) -> PyResult<Option<i64>> {
        let m = member.into_bytes();
        adapter_sync!(slf, conn, conn.zrevrank(key, &m).await).map_err(crate::client::to_py_err)
    }

    fn azrevrank(slf: &Bound<'_, Self>, key: &str, member: EncodableT) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let m = member.into_bytes();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zrevrank(&key, &m).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn zmscore(
        slf: &Bound<'_, Self>,
        key: &str,
        members: Vec<EncodableT>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        if members.is_empty() {
            return Ok(pyo3::types::PyList::empty(py).into_any().unbind());
        }
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.zmscore(key, m.clone()).await);
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        let float_cls = py.import("builtins")?.getattr("float")?;
        let raw_b = raw.bind(py);
        let mut out: Vec<Py<PyAny>> = Vec::with_capacity(raw_b.len()?);
        for item in raw_b.try_iter()? {
            let item = item?;
            if item.is_none() {
                out.push(py.None());
            } else {
                // Server returns bytes (encoded score); convert via float().
                out.push(float_cls.call1((item,))?.unbind());
            }
        }
        Ok(pyo3::types::PyList::new(py, out)?.into_any().unbind())
    }

    #[pyo3(signature = (key, *members))]
    fn azmscore(
        slf: &Bound<'_, Self>,
        key: &str,
        members: Vec<EncodableT>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if members.is_empty() {
            let empty = pyo3::types::PyList::empty(py).into_any().unbind();
            return await_constant(py, empty);
        }
        let key = key.to_string();
        let m: Vec<Vec<u8>> = members.into_iter().map(EncodableT::into_bytes).collect();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.zmscore(&key, m).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::MapOptionalFloat
        )
    }

    fn zremrangebyrank(slf: &Bound<'_, Self>, key: &str, start: i64, end: i64) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.zremrangebyrank(key, start, end).await)
            .map_err(crate::client::to_py_err)
    }

    fn azremrangebyrank(slf: &Bound<'_, Self>, key: &str, start: i64, end: i64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zremrangebyrank(&key, start, end).await.into_raw_result()
        })
    }

    fn zremrangebyscore(
        slf: &Bound<'_, Self>,
        key: &str,
        min_score: &Bound<'_, PyAny>,
        max_score: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let mn: String = min_score.str()?.extract()?;
        let mx: String = max_score.str()?.extract()?;
        adapter_sync!(slf, conn, conn.zremrangebyscore(key, &mn, &mx).await)
            .map_err(crate::client::to_py_err)
    }

    fn azremrangebyscore(
        slf: &Bound<'_, Self>,
        key: &str,
        min_score: &Bound<'_, PyAny>,
        max_score: &Bound<'_, PyAny>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let mn: String = min_score.str()?.extract()?;
        let mx: String = max_score.str()?.extract()?;
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zremrangebyscore(&key, &mn, &mx).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, max_score, min_score, *, start=None, num=None, withscores=false))]
    fn zrevrangebyscore(
        slf: &Bound<'_, Self>,
        key: &str,
        max_score: &Bound<'_, PyAny>,
        min_score: &Bound<'_, PyAny>,
        start: Option<i64>,
        num: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let mx: String = max_score.str()?.extract()?;
        let mn: String = min_score.str()?.extract()?;
        let py = slf.py();
        let offset_count = match (start, num) {
            (Some(s), Some(n)) => Some((s, n)),
            _ => None,
        };
        let r: Result<redis::Value, _> = adapter_sync!(
            slf,
            conn,
            conn.zrevrangebyscore(key, &mx, &mn, withscores, offset_count).await
        );
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        decode_zrevrangebyscore(py, raw.bind(py), withscores)
    }

    // =====================================================================
    // Phase 3f: Stream + blocking-list trivial passthroughs
    //
    // The complex stream methods (xadd, xrange, xrevrange, xread,
    // xreadgroup, xtrim, xpending, xclaim, xautoclaim, xinfo_*) need the
    // _decode_x* helpers and stay in Python for now; they migrate in
    // Phase 4 alongside the awaitable-wrapping work.
    // =====================================================================

    #[pyo3(signature = (key, *entry_ids))]
    fn xdel(
        slf: &Bound<'_, Self>,
        key: &str,
        entry_ids: Vec<String>,
    ) -> PyResult<i64> {
        if entry_ids.is_empty() {
            return Ok(0);
        }
        adapter_sync!(slf, conn, conn.xdel(key, &entry_ids).await).map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, *entry_ids))]
    fn axdel(
        slf: &Bound<'_, Self>,
        key: &str,
        entry_ids: Vec<String>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if entry_ids.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.xdel(&key, &entry_ids).await.into_raw_result()
        })
    }

    fn xlen(slf: &Bound<'_, Self>, key: &str) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.xlen(key).await).map_err(crate::client::to_py_err)
    }

    fn axlen(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.xlen(&key).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, group, *entry_ids))]
    fn xack(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        entry_ids: Vec<String>,
    ) -> PyResult<i64> {
        if entry_ids.is_empty() {
            return Ok(0);
        }
        adapter_sync!(slf, conn, conn.xack(key, group, &entry_ids).await)
            .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, group, *entry_ids))]
    fn axack(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        entry_ids: Vec<String>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if entry_ids.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        let key = key.to_string();
        let group = group.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.xack(&key, &group, &entry_ids).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, group, identifier="$", mkstream=false, entries_read=None))]
    fn xgroup_create(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        identifier: &str,
        mkstream: bool,
        entries_read: Option<i64>,
    ) -> PyResult<bool> {
        let r: Result<(), _> = adapter_sync!(
            slf,
            conn,
            conn.xgroup_create(key, group, identifier, mkstream, entries_read).await
        );
        r.map_err(crate::client::to_py_err)?;
        Ok(true)
    }

    #[pyo3(signature = (key, group, identifier="$", mkstream=false, entries_read=None))]
    fn axgroup_create(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        identifier: &str,
        mkstream: bool,
        entries_read: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let group = group.to_string();
        let identifier = identifier.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.xgroup_create(&key, &group, &identifier, mkstream, entries_read)
                    .await
                    .into_raw_result()
            };
            crate::async_bridge::AwaitTransform::TrueAfter
        )
    }

    fn xgroup_destroy(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
    ) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.xgroup_destroy(key, group).await)
            .map_err(crate::client::to_py_err)
    }

    fn axgroup_destroy(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let group = group.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.xgroup_destroy(&key, &group).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, group, identifier, entries_read=None))]
    fn xgroup_setid(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        identifier: &str,
        entries_read: Option<i64>,
    ) -> PyResult<bool> {
        let r: Result<(), _> = adapter_sync!(
            slf,
            conn,
            conn.xgroup_setid(key, group, identifier, entries_read).await
        );
        r.map_err(crate::client::to_py_err)?;
        Ok(true)
    }

    #[pyo3(signature = (key, group, identifier, entries_read=None))]
    fn axgroup_setid(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        identifier: &str,
        entries_read: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let group = group.to_string();
        let identifier = identifier.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.xgroup_setid(&key, &group, &identifier, entries_read)
                    .await
                    .into_raw_result()
            };
            crate::async_bridge::AwaitTransform::TrueAfter
        )
    }

    fn xgroup_delconsumer(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> PyResult<i64> {
        adapter_sync!(slf, conn, conn.xgroup_delconsumer(key, group, consumer).await)
            .map_err(crate::client::to_py_err)
    }

    fn axgroup_delconsumer(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.xgroup_delconsumer(&key, &group, &consumer)
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (keys, timeout=0.0))]
    fn blpop(slf: &Bound<'_, Self>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Option<(String, Vec<u8>)>, _> =
            adapter_sync!(slf, conn, conn.blpop(&keys, timeout).await);
        let opt = r.map_err(crate::client::to_py_err)?;
        Ok(match opt {
            Some((k, v)) => pyo3::types::PyTuple::new(
                py,
                [
                    pyo3::types::PyString::new(py, &k).into_any().unbind(),
                    pyo3::types::PyBytes::new(py, &v).into_any().unbind(),
                ],
            )?
            .into_any()
            .unbind(),
            None => py.None(),
        })
    }

    #[pyo3(signature = (keys, timeout=0.0))]
    fn brpop(slf: &Bound<'_, Self>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Option<(String, Vec<u8>)>, _> =
            adapter_sync!(slf, conn, conn.brpop(&keys, timeout).await);
        let opt = r.map_err(crate::client::to_py_err)?;
        Ok(match opt {
            Some((k, v)) => pyo3::types::PyTuple::new(
                py,
                [
                    pyo3::types::PyString::new(py, &k).into_any().unbind(),
                    pyo3::types::PyBytes::new(py, &v).into_any().unbind(),
                ],
            )?
            .into_any()
            .unbind(),
            None => py.None(),
        })
    }

    #[pyo3(signature = (keys, timeout=0.0))]
    fn ablpop(slf: &Bound<'_, Self>, keys: Vec<String>, timeout: f64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.blpop(&keys, timeout).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::OptTupleUnpack
        )
    }

    #[pyo3(signature = (keys, timeout=0.0))]
    fn abrpop(slf: &Bound<'_, Self>, keys: Vec<String>, timeout: f64) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.brpop(&keys, timeout).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::OptTupleUnpack
        )
    }

    #[pyo3(signature = (src, dst, timeout, wherefrom="LEFT", whereto="RIGHT"))]
    fn blmove(
        slf: &Bound<'_, Self>,
        src: &str,
        dst: &str,
        timeout: f64,
        wherefrom: &str,
        whereto: &str,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<Option<Vec<u8>>, _> =
            adapter_sync!(slf, conn, conn.blmove(src, dst, wherefrom, whereto, timeout).await);
        let bytes = r.map_err(crate::client::to_py_err)?;
        Ok(match bytes {
            Some(b) => pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
            None => py.None(),
        })
    }

    #[pyo3(signature = (src, dst, timeout, wherefrom="LEFT", whereto="RIGHT"))]
    fn ablmove(
        slf: &Bound<'_, Self>,
        src: &str,
        dst: &str,
        timeout: f64,
        wherefrom: &str,
        whereto: &str,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let src = src.to_string();
        let dst = dst.to_string();
        let wherefrom = wherefrom.to_string();
        let whereto = whereto.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.blmove(&src, &dst, &wherefrom, &whereto, timeout).await.into_raw_result()
        })
    }

    // =====================================================================
    // Phase 4f: admin/decoder methods + lock + eval
    //
    // Bulk-delete, FLUSHDB, KEYS, INFO, type, RENAME(NX), eval, lock(),
    // and the post-await decoders for ``ahgetall`` / ``ahkeys`` / ``azpopmin``
    // / ``azpopmax`` / ``azrange*``.
    // =====================================================================

    fn delete_many(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        adapter_sync!(slf, conn, conn.del_many(&keys).await).map_err(crate::client::to_py_err)
    }

    fn adelete_many(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if keys.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.del_many(&keys).await.into_raw_result()
        })
    }

    fn clear(slf: &Bound<'_, Self>) -> PyResult<bool> {
        let r: Result<(), _> = adapter_sync!(slf, conn, conn.flushdb().await);
        r.map_err(crate::client::to_py_err)?;
        Ok(true)
    }

    fn aclear(slf: &Bound<'_, Self>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.flushdb().await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::TrueAfter
        )
    }

    fn keys(slf: &Bound<'_, Self>, pattern: &str) -> PyResult<Vec<String>> {
        adapter_sync!(slf, conn, conn.keys(pattern).await).map_err(crate::client::to_py_err)
    }

    fn akeys(slf: &Bound<'_, Self>, pattern: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let pattern = pattern.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.keys(&pattern).await.into_raw_result()
        })
    }

    fn r#type(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let raw: String = adapter_sync!(slf, conn, conn.key_type(key).await)
            .map_err(crate::client::to_py_err)?;
        if raw == "none" {
            return Ok(py.None());
        }
        // Wrap in the ``KeyType`` enum so cachex callers get the typed
        // value the protocol promises.
        Ok(py
            .import("django_cachex.types")?
            .getattr("KeyType")?
            .call1((raw,))?
            .unbind())
    }

    fn atype(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.key_type(&key).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::DecodeKeytype
        )
    }

    #[pyo3(signature = (section=None))]
    fn info(
        slf: &Bound<'_, Self>,
        section: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.info().await);
        let mut raw = match r.map_err(crate::client::to_py_err)? {
            redis::Value::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
            redis::Value::SimpleString(s) => s,
            // RESP3 returns INFO as VerbatimString with a format hint.
            redis::Value::VerbatimString { text, .. } => text,
            other => format!("{other:?}"),
        };
        if let Some(sec) = section {
            raw = select_info_section(&raw, sec);
        }
        Ok(parse_info(py, &raw)?.into_any().unbind())
    }

    #[pyo3(signature = (**_kwargs))]
    fn close(&self, _kwargs: Option<&Bound<'_, PyDict>>) {
        // No-op. Django fires ``cache.close()`` on every ``request_finished``
        // signal as a request-cycle cleanup hook, not a shutdown call —
        // dropping the multiplexed connection here would force a reconnect
        // per request and saturate the server with TCP handshakes.
    }

    #[pyo3(signature = (**_kwargs))]
    fn aclose(&self, py: Python<'_>, _kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        await_constant(py, py.None())
    }

    fn rename(
        slf: &Bound<'_, Self>,
        src: &str,
        dst: &str,
    ) -> PyResult<bool> {
        let r: Result<(), _> = adapter_sync!(slf, conn, conn.rename(src, dst).await);
        r.map_err(|e| translate_rename_error_str(crate::client::to_py_err(e), src))?;
        Ok(true)
    }

    fn renamenx(
        slf: &Bound<'_, Self>,
        src: &str,
        dst: &str,
    ) -> PyResult<bool> {
        let r: Result<bool, _> = adapter_sync!(slf, conn, conn.renamenx(src, dst).await);
        r.map_err(|e| translate_rename_error_str(crate::client::to_py_err(e), src))
    }

    // ``arename`` / ``arenamenx`` wrap the inner awaitable in a Python
    // coroutine (``rename_after``) that re-raises ``ERR no such key`` as
    // ``ValueError``. The wrapped value is a coroutine — return type stays
    // ``Py<PyAny>``.
    fn arename(
        slf: &Bound<'_, Self>,
        src: &str,
        dst: &str,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let src_owned = src.to_string();
        let dst_owned = dst.to_string();
        let aw_inner = adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.rename(&src_owned, &dst_owned).await.into_raw_result()
        })?;
        let helpers = py.import("django_cachex.adapters._async_helpers")?;
        let src_py = pyo3::types::PyString::new(py, src).into_any();
        Ok(helpers
            .getattr("rename_after")?
            .call1((aw_inner, src_py))?
            .unbind())
    }

    fn arenamenx(
        slf: &Bound<'_, Self>,
        src: &str,
        dst: &str,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let src_owned = src.to_string();
        let dst_owned = dst.to_string();
        let aw_inner = adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.renamenx(&src_owned, &dst_owned).await.into_raw_result()
        })?;
        let helpers = py.import("django_cachex.adapters._async_helpers")?;
        let src_py = pyo3::types::PyString::new(py, src).into_any();
        Ok(helpers
            .getattr("renamenx_after")?
            .call1((aw_inner, src_py))?
            .unbind())
    }

    #[pyo3(signature = (script, numkeys, *keys_and_args))]
    fn eval(
        slf: &Bound<'_, Self>,
        script: &str,
        numkeys: usize,
        keys_and_args: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (keys, args) = split_eval_args(&keys_and_args, numkeys)?;
        let py = slf.py();
        let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.eval(script, &keys, &args).await);
        crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)
    }

    #[pyo3(signature = (script, numkeys, *keys_and_args))]
    fn aeval(
        slf: &Bound<'_, Self>,
        script: &str,
        numkeys: usize,
        keys_and_args: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let (keys, args) = split_eval_args(&keys_and_args, numkeys)?;
        let script = script.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.eval(&script, &keys, &args).await.into_raw_result()
        })
    }

    fn ahgetall(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.hgetall(&key).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::DecodeHgetall
        )
    }

    fn ahkeys(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.hkeys(&key).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, count=None))]
    fn azpopmin(
        slf: &Bound<'_, Self>,
        key: &str,
        count: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let n = count.unwrap_or(1);
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zpopmin(&key, n).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, count=None))]
    fn azpopmax(
        slf: &Bound<'_, Self>,
        key: &str,
        count: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let n = count.unwrap_or(1);
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zpopmax(&key, n).await.into_raw_result()
        })
    }

    #[pyo3(signature = (key, start, end, *, withscores=false))]
    fn azrange(
        slf: &Bound<'_, Self>,
        key: &str,
        start: i64,
        end: i64,
        withscores: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let transform = if withscores {
            crate::async_bridge::AwaitTransform::DecodeZrangeWithScores
        } else {
            crate::async_bridge::AwaitTransform::None
        };
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.zrange(&key, start, end, withscores).await.into_raw_result()
            };
            transform
        )
    }

    #[pyo3(signature = (key, start, end, *, withscores=false))]
    fn azrevrange(
        slf: &Bound<'_, Self>,
        key: &str,
        start: i64,
        end: i64,
        withscores: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let transform = if withscores {
            crate::async_bridge::AwaitTransform::DecodeZrangeWithScores
        } else {
            crate::async_bridge::AwaitTransform::None
        };
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.zrevrange(&key, start, end, withscores).await.into_raw_result()
            };
            transform
        )
    }

    #[pyo3(signature = (key, min_score, max_score, *, start=None, num=None, withscores=false))]
    fn azrangebyscore(
        slf: &Bound<'_, Self>,
        key: &str,
        min_score: &Bound<'_, PyAny>,
        max_score: &Bound<'_, PyAny>,
        start: Option<i64>,
        num: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let mn: String = min_score.str()?.extract()?;
        let mx: String = max_score.str()?.extract()?;
        let transform = if withscores {
            crate::async_bridge::AwaitTransform::DecodeZrangeWithScores
        } else {
            crate::async_bridge::AwaitTransform::None
        };
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.zrangebyscore(&key, &mn, &mx, withscores, start, num)
                    .await
                    .into_raw_result()
            };
            transform
        )
    }

    #[pyo3(signature = (key, timeout=None, sleep=0.1, *, blocking=true, blocking_timeout=None, thread_local=true))]
    fn lock(
        slf: &Bound<'_, Self>,
        key: &str,
        timeout: Option<f64>,
        sleep: f64,
        blocking: bool,
        blocking_timeout: Option<f64>,
        thread_local: bool,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let kwargs = PyDict::new(py);
        kwargs.set_item("timeout", timeout)?;
        kwargs.set_item("sleep", sleep)?;
        kwargs.set_item("blocking", blocking)?;
        kwargs.set_item("blocking_timeout", blocking_timeout)?;
        kwargs.set_item("thread_local", thread_local)?;
        Ok(py
            .import("django_cachex.lock")?
            .getattr("ValkeyLock")?
            .call((slf.clone(), key), Some(&kwargs))?
            .unbind())
    }

    // ``alock`` returns a Python ``AsyncValkeyLock`` instance (not an
    // awaitable) — return type is genuinely ``Py<PyAny>``.
    #[pyo3(signature = (key, timeout=None, sleep=0.1, *, blocking=true, blocking_timeout=None, thread_local=true))]
    fn alock(
        slf: &Bound<'_, Self>,
        key: &str,
        timeout: Option<f64>,
        sleep: f64,
        blocking: bool,
        blocking_timeout: Option<f64>,
        thread_local: bool,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let kwargs = PyDict::new(py);
        kwargs.set_item("timeout", timeout)?;
        kwargs.set_item("sleep", sleep)?;
        kwargs.set_item("blocking", blocking)?;
        kwargs.set_item("blocking_timeout", blocking_timeout)?;
        kwargs.set_item("thread_local", thread_local)?;
        Ok(py
            .import("django_cachex.lock")?
            .getattr("AsyncValkeyLock")?
            .call((slf.clone(), key), Some(&kwargs))?
            .unbind())
    }

    // =====================================================================
    // Phase 4g-3: stream passthroughs + iterators + scan/delete_pattern
    // =====================================================================

    /// Default ``itersize`` for ``iter_keys`` / ``aiter_keys`` /
    /// ``delete_pattern`` when the caller doesn't specify one. Mirrors
    /// the Python adapter's class-level default.
    const DEFAULT_SCAN_ITERSIZE: i64 = 100;

    fn _default_scan_itersize(&self) -> i64 {
        Self::DEFAULT_SCAN_ITERSIZE
    }

    // ---- streams: thin passthroughs to typed driver methods ----

    #[pyo3(signature = (key, fields, entry_id="*", maxlen=None, approximate=true, nomkstream=false, minid=None, limit=None))]
    #[allow(clippy::too_many_arguments)]
    fn xadd(
        slf: &Bound<'_, Self>,
        key: &str,
        fields: &Bound<'_, PyAny>,
        entry_id: &str,
        maxlen: Option<i64>,
        approximate: bool,
        nomkstream: bool,
        minid: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<String> {
        let pairs = encode_str_field_pairs(fields)?;
        if maxlen.is_none() && minid.is_none() && !nomkstream && limit.is_none() {
            return adapter_sync!(slf, conn, conn.xadd(key, entry_id, &pairs).await)
                .map_err(crate::client::to_py_err);
        }
        let r: Result<Option<String>, _> = adapter_sync!(
            slf,
            conn,
            conn.xadd_with_options(
                key, entry_id, &pairs, maxlen, approximate, nomkstream, minid, limit,
            )
            .await
        );
        // ``XADD ... NOMKSTREAM`` returns nil when the stream doesn't exist;
        // the cachex contract is ``str``, so map nil → "".
        Ok(r.map_err(crate::client::to_py_err)?.unwrap_or_default())
    }

    #[pyo3(signature = (key, fields, entry_id="*", maxlen=None, approximate=true, nomkstream=false, minid=None, limit=None))]
    #[allow(clippy::too_many_arguments)]
    fn axadd(
        slf: &Bound<'_, Self>,
        key: &str,
        fields: &Bound<'_, PyAny>,
        entry_id: &str,
        maxlen: Option<i64>,
        approximate: bool,
        nomkstream: bool,
        minid: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let entry_id = entry_id.to_string();
        let pairs = encode_str_field_pairs(fields)?;
        let minid = minid.map(str::to_string);
        if maxlen.is_none() && minid.is_none() && !nomkstream && limit.is_none() {
            return adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.xadd(&key, &entry_id, &pairs).await.into_raw_result()
            });
        }
        adapter_async!(slf, conn, {
            // ``XADD ... NOMKSTREAM`` returns nil when the stream doesn't exist;
            // the cachex contract is ``str``, so map nil → "".
            match conn
                .xadd_with_options(
                    &key, &entry_id, &pairs, maxlen, approximate, nomkstream,
                    minid.as_deref(), limit,
                )
                .await
            {
                Ok(opt) => crate::async_bridge::RawResult::from(opt.unwrap_or_default()),
                Err(e) => crate::client::classify(e),
            }
        })
    }

    #[pyo3(signature = (key, start="-", end="+", count=None))]
    fn xrange(
        slf: &Bound<'_, Self>,
        key: &str,
        start: &str,
        end: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> =
            adapter_sync!(slf, conn, conn.xrange(key, start, end, count).await);
        crate::stream_decode::entries_to_py(slf.py(), r.map_err(crate::client::to_py_err)?)
    }

    #[pyo3(signature = (key, start="-", end="+", count=None))]
    fn axrange(
        slf: &Bound<'_, Self>,
        key: &str,
        start: &str,
        end: &str,
        count: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let start = start.to_string();
        let end = end.to_string();
        adapter_async!(slf, conn, {
            match conn.xrange(&key, &start, &end, count).await {
                Ok(v) => crate::async_bridge::RawResult::StreamEntries(v),
                Err(e) => crate::client::classify(e),
            }
        })
    }

    #[pyo3(signature = (key, end="+", start="-", count=None))]
    fn xrevrange(
        slf: &Bound<'_, Self>,
        key: &str,
        end: &str,
        start: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> =
            adapter_sync!(slf, conn, conn.xrevrange(key, end, start, count).await);
        crate::stream_decode::entries_to_py(slf.py(), r.map_err(crate::client::to_py_err)?)
    }

    #[pyo3(signature = (key, end="+", start="-", count=None))]
    fn axrevrange(
        slf: &Bound<'_, Self>,
        key: &str,
        end: &str,
        start: &str,
        count: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let start = start.to_string();
        let end = end.to_string();
        adapter_async!(slf, conn, {
            match conn.xrevrange(&key, &end, &start, count).await {
                Ok(v) => crate::async_bridge::RawResult::StreamEntries(v),
                Err(e) => crate::client::classify(e),
            }
        })
    }

    #[pyo3(signature = (streams, count=None, block=None))]
    fn xread(
        slf: &Bound<'_, Self>,
        streams: &Bound<'_, PyAny>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let (keys, ids) = split_streams_dict(streams)?;
        let r: Result<redis::Value, _> =
            adapter_sync!(slf, conn, conn.xread(&keys, &ids, count, block).await);
        crate::stream_decode::read_to_py(slf.py(), r.map_err(crate::client::to_py_err)?)
    }

    #[pyo3(signature = (streams, count=None, block=None))]
    fn axread(
        slf: &Bound<'_, Self>,
        streams: &Bound<'_, PyAny>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let (keys, ids) = split_streams_dict(streams)?;
        adapter_async!(slf, conn, {
            match conn.xread(&keys, &ids, count, block).await {
                Ok(v) => crate::async_bridge::RawResult::StreamRead(v),
                Err(e) => crate::client::classify(e),
            }
        })
    }

    #[pyo3(signature = (group, consumer, streams, count=None, block=None, noack=false))]
    fn xreadgroup(
        slf: &Bound<'_, Self>,
        group: &str,
        consumer: &str,
        streams: &Bound<'_, PyAny>,
        count: Option<i64>,
        block: Option<i64>,
        noack: bool,
    ) -> PyResult<Py<PyAny>> {
        let (keys, ids) = split_streams_dict(streams)?;
        let r: Result<redis::Value, _> = adapter_sync!(
            slf,
            conn,
            conn.xreadgroup(group, consumer, &keys, &ids, count, block, noack).await
        );
        crate::stream_decode::read_to_py(slf.py(), r.map_err(crate::client::to_py_err)?)
    }

    #[pyo3(signature = (group, consumer, streams, count=None, block=None, noack=false))]
    fn axreadgroup(
        slf: &Bound<'_, Self>,
        group: &str,
        consumer: &str,
        streams: &Bound<'_, PyAny>,
        count: Option<i64>,
        block: Option<i64>,
        noack: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let group = group.to_string();
        let consumer = consumer.to_string();
        let (keys, ids) = split_streams_dict(streams)?;
        adapter_async!(slf, conn, {
            match conn
                .xreadgroup(&group, &consumer, &keys, &ids, count, block, noack)
                .await
            {
                Ok(v) => crate::async_bridge::RawResult::StreamRead(v),
                Err(e) => crate::client::classify(e),
            }
        })
    }

    #[pyo3(signature = (key, maxlen=None, approximate=true, minid=None, limit=None))]
    fn xtrim(
        slf: &Bound<'_, Self>,
        key: &str,
        maxlen: Option<i64>,
        approximate: bool,
        minid: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<i64> {
        if maxlen.is_none() && minid.is_none() {
            return Ok(0);
        }
        adapter_sync!(
            slf,
            conn,
            conn.xtrim_with_options(key, maxlen, approximate, minid, limit).await
        )
        .map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, maxlen=None, approximate=true, minid=None, limit=None))]
    fn axtrim(
        slf: &Bound<'_, Self>,
        key: &str,
        maxlen: Option<i64>,
        approximate: bool,
        minid: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        if maxlen.is_none() && minid.is_none() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        let key = key.to_string();
        let minid = minid.map(str::to_string);
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.xtrim_with_options(&key, maxlen, approximate, minid.as_deref(), limit)
                .await
                .into_raw_result()
        })
    }

    // ---- KEYS/SCAN iteration ----

    #[pyo3(signature = (pattern, itersize=None))]
    fn iter_keys(
        slf: &Bound<'_, Self>,
        pattern: &str,
        itersize: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let count = itersize.unwrap_or(Self::DEFAULT_SCAN_ITERSIZE);
        Ok(py
            .import("django_cachex.adapters._async_helpers")?
            .getattr("scan_iter_loop")?
            .call1((slf.clone(), pattern, count))?
            .unbind())
    }

    // ``aiter_keys`` returns a Python async generator (``ascan_iter_loop``)
    // — not an awaitable — so the return type is genuinely ``Py<PyAny>``.
    #[pyo3(signature = (pattern, itersize=None))]
    fn aiter_keys(
        slf: &Bound<'_, Self>,
        pattern: &str,
        itersize: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let count = itersize.unwrap_or(Self::DEFAULT_SCAN_ITERSIZE);
        Ok(py
            .import("django_cachex.adapters._async_helpers")?
            .getattr("ascan_iter_loop")?
            .call1((slf.clone(), pattern, count))?
            .unbind())
    }

    #[pyo3(signature = (cursor=0, r#match=None, count=None, _type=None))]
    #[pyo3(name = "scan")]
    fn scan_one(
        slf: &Bound<'_, Self>,
        cursor: u64,
        #[pyo3(from_py_with = py_optional_str)] r#match: Option<String>,
        count: Option<i64>,
        _type: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let count = count.unwrap_or(Self::DEFAULT_SCAN_ITERSIZE);
        let pattern = r#match.unwrap_or_else(|| "*".to_string());
        let r: Result<(u64, Vec<String>), _> = adapter_sync!(
            slf, conn,
            conn.scan_one(cursor, &pattern, count, _type).await
        );
        let (next_cursor, keys) = r.map_err(crate::client::to_py_err)?;
        crate::client::py_cursor_strings(slf.py(), next_cursor, keys)
    }

    #[pyo3(signature = (cursor=0, r#match=None, count=None, _type=None))]
    #[pyo3(name = "ascan")]
    fn ascan_one(
        slf: &Bound<'_, Self>,
        cursor: u64,
        #[pyo3(from_py_with = py_optional_str)] r#match: Option<String>,
        count: Option<i64>,
        _type: Option<&str>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let count = count.unwrap_or(Self::DEFAULT_SCAN_ITERSIZE);
        let pattern = r#match.unwrap_or_else(|| "*".to_string());
        let type_filter = _type.map(str::to_string);
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.scan_one(cursor, &pattern, count, type_filter.as_deref())
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (pattern, itersize=None))]
    fn delete_pattern(
        slf: &Bound<'_, Self>,
        pattern: &str,
        itersize: Option<i64>,
    ) -> PyResult<i64> {
        let count = itersize.unwrap_or(Self::DEFAULT_SCAN_ITERSIZE);
        let batch_size = count.max(1) as usize;
        let mut total: i64 = 0;
        let mut cursor: u64 = 0;
        let mut batch: Vec<String> = Vec::with_capacity(batch_size);
        loop {
            let r: Result<(u64, Vec<String>), _> =
                adapter_sync!(slf, conn, conn.scan_one(cursor, pattern, count, None).await);
            let (next_cursor, keys) = r.map_err(crate::client::to_py_err)?;
            cursor = next_cursor;
            for key in keys {
                batch.push(key);
                if batch.len() >= batch_size {
                    let to_delete = std::mem::take(&mut batch);
                    let n: i64 = adapter_sync!(slf, conn, conn.del_many(&to_delete).await)
                        .map_err(crate::client::to_py_err)?;
                    total += n;
                }
            }
            if cursor == 0 {
                break;
            }
        }
        if !batch.is_empty() {
            let n: i64 = adapter_sync!(slf, conn, conn.del_many(&batch).await)
                .map_err(crate::client::to_py_err)?;
            total += n;
        }
        Ok(total)
    }

    // ``adelete_pattern`` returns a Python coroutine — not an awaitable —
    // so the return type is genuinely ``Py<PyAny>``.
    #[pyo3(signature = (pattern, itersize=None))]
    fn adelete_pattern(
        slf: &Bound<'_, Self>,
        pattern: &str,
        itersize: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let count = itersize.unwrap_or(Self::DEFAULT_SCAN_ITERSIZE);
        Ok(py
            .import("django_cachex.adapters._async_helpers")?
            .getattr("adelete_pattern_loop")?
            .call1((slf.clone(), pattern, count))?
            .unbind())
    }

    // ---- SSCAN ----

    #[pyo3(signature = (key, cursor=0, r#match=None, count=None))]
    fn sscan(
        slf: &Bound<'_, Self>,
        key: &str,
        cursor: u64,
        #[pyo3(from_py_with = py_optional_str)] r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<(u64, Vec<Vec<u8>>), _> = adapter_sync!(
            slf, conn,
            conn.sscan_step(key, cursor, r#match.as_deref(), count).await
        );
        let (next_cursor, members) = r.map_err(crate::client::to_py_err)?;
        // Cachex contract: ``tuple[int, set[bytes]]``.
        let py_items: Vec<Py<PyAny>> = members
            .into_iter()
            .map(|b| pyo3::types::PyBytes::new(py, &b).into_any().unbind())
            .collect();
        let set_cls = py.import("builtins")?.getattr("set")?;
        let py_set = set_cls.call1((pyo3::types::PyList::new(py, py_items)?,))?;
        let cursor_py = next_cursor.into_pyobject(py)?.into_any().unbind();
        Ok(pyo3::types::PyTuple::new(py, [cursor_py, py_set.unbind()])?
            .into_any()
            .unbind())
    }

    #[pyo3(signature = (key, cursor=0, r#match=None, count=None))]
    fn asscan(
        slf: &Bound<'_, Self>,
        key: &str,
        cursor: u64,
        #[pyo3(from_py_with = py_optional_str)] r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let m = r#match;
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.sscan_step(&key, cursor, m.as_deref(), count)
                    .await
                    .into_raw_result()
            };
            crate::async_bridge::AwaitTransform::SscanCursorToSet
        )
    }

    #[pyo3(signature = (key, r#match=None, count=None))]
    fn sscan_iter(
        slf: &Bound<'_, Self>,
        key: &str,
        #[pyo3(from_py_with = py_optional_str)] r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Sync cursor loops in Rust need a stateful pyclass iterator; let
        // the Python helper drive the loop instead.
        let py = slf.py();
        Ok(py
            .import("django_cachex.adapters._async_helpers")?
            .getattr("sscan_iter_loop")?
            .call1((slf.clone(), key, r#match.as_deref(), count))?
            .unbind())
    }

    #[pyo3(signature = (key, r#match=None, count=None))]
    fn asscan_iter(
        slf: &Bound<'_, Self>,
        key: &str,
        #[pyo3(from_py_with = py_optional_str)] r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        Ok(py
            .import("django_cachex.adapters._async_helpers")?
            .getattr("asscan_iter_loop")?
            .call1((slf.clone(), key, r#match.as_deref(), count))?
            .unbind())
    }

    // =====================================================================
    // Phase 4g-4: ahset, zadd flag wrappers, azrevrangebyscore, xinfo_*,
    // xpending, xclaim, xautoclaim
    // =====================================================================

    #[pyo3(signature = (key, field=None, value=None, mapping=None, items=None))]
    fn ahset(
        slf: &Bound<'_, Self>,
        key: &str,
        field: Option<String>,
        value: Option<EncodableT>,
        mapping: Option<&Bound<'_, PyDict>>,
        items: Option<Vec<Bound<'_, PyAny>>>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        let key = key.to_string();
        let mut pairs: Vec<(String, Vec<u8>)> = Vec::new();
        if let Some(f) = field {
            let v = value.ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("ahset: value required when field provided")
            })?;
            pairs.push((f, v.into_bytes()));
        }
        if let Some(m) = mapping {
            for (k, v) in m.iter() {
                pairs.push((decode_str(&k)?, value_to_bytes(&v)?));
            }
        }
        if let Some(its) = items {
            let mut it = its.into_iter();
            while let (Some(f), Some(v)) = (it.next(), it.next()) {
                pairs.push((decode_str(&f)?, value_to_bytes(&v)?));
            }
        }
        if pairs.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        if pairs.len() == 1 {
            let (f, v) = pairs.into_iter().next().unwrap();
            return adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.hset(&key, &f, &v).await.into_raw_result()
            });
        }
        // Multi-field: HLEN before, HMSET, HLEN after — return diff. The
        // cachex contract for multi-field hset is "count of *new* fields",
        // not the OK return that HMSET produces.
        adapter_async!(slf, conn, {
            let before: i64 = match conn.hlen(&key).await {
                Ok(n) => n,
                Err(e) => return crate::client::classify(e),
            };
            if let Err(e) = conn.hmset(&key, &pairs).await {
                return crate::client::classify(e);
            }
            let after: i64 = match conn.hlen(&key).await {
                Ok(n) => n,
                Err(e) => return crate::client::classify(e),
            };
            crate::async_bridge::RawResult::Int(after - before)
        })
    }

    #[pyo3(signature = (key, mapping, *, nx=false, xx=false, gt=false, lt=false, ch=false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn zadd(
        slf: &Bound<'_, Self>,
        key: &str,
        mapping: &Bound<'_, PyDict>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> PyResult<i64> {
        let mut pairs: Vec<(Vec<u8>, f64)> = Vec::with_capacity(mapping.len());
        for (m, s) in mapping.iter() {
            let member = value_to_bytes(&m)?;
            let score: f64 = s.extract()?;
            pairs.push((member, score));
        }
        if pairs.is_empty() {
            return Ok(0);
        }
        let r: Result<i64, _> = if !(nx || xx || gt || lt || ch) {
            adapter_sync!(slf, conn, conn.zadd(key, pairs).await)
        } else {
            adapter_sync!(
                slf,
                conn,
                conn.zadd_with_flags(key, pairs, nx, xx, gt, lt, ch).await
            )
        };
        r.map_err(crate::client::to_py_err)
    }

    #[pyo3(signature = (key, mapping, *, nx=false, xx=false, gt=false, lt=false, ch=false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn azadd(
        slf: &Bound<'_, Self>,
        key: &str,
        mapping: &Bound<'_, PyDict>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let py = slf.py();
        let key = key.to_string();
        let mut pairs: Vec<(Vec<u8>, f64)> = Vec::with_capacity(mapping.len());
        for (m, s) in mapping.iter() {
            let member = value_to_bytes(&m)?;
            let score: f64 = s.extract()?;
            pairs.push((member, score));
        }
        if pairs.is_empty() {
            return await_constant(py, 0i64.into_pyobject(py)?.into_any().unbind());
        }
        if !(nx || xx || gt || lt || ch) {
            return adapter_async!(slf, conn, {
                use crate::client::IntoRawResult;
                conn.zadd(&key, pairs).await.into_raw_result()
            });
        }
        adapter_async!(slf, conn, {
            use crate::client::IntoRawResult;
            conn.zadd_with_flags(&key, pairs, nx, xx, gt, lt, ch)
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (key, max_score, min_score, *, start=None, num=None, withscores=false))]
    fn azrevrangebyscore(
        slf: &Bound<'_, Self>,
        key: &str,
        max_score: &Bound<'_, PyAny>,
        min_score: &Bound<'_, PyAny>,
        start: Option<i64>,
        num: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let mx: String = max_score.str()?.extract()?;
        let mn: String = min_score.str()?.extract()?;
        let offset_count = match (start, num) {
            (Some(s), Some(n)) => Some((s, n)),
            _ => None,
        };
        let transform = if withscores {
            crate::async_bridge::AwaitTransform::DecodeZrevrangebyscoreWithScores
        } else {
            crate::async_bridge::AwaitTransform::None
        };
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.zrevrangebyscore(&key, &mx, &mn, withscores, offset_count)
                    .await
                    .into_raw_result()
            };
            transform
        )
    }

    // ---- XINFO ----

    #[pyo3(signature = (key, full=false))]
    fn xinfo_stream(
        slf: &Bound<'_, Self>,
        key: &str,
        full: bool,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.xinfo_stream(key, full).await);
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        parse_xinfo_pairs(py, raw.bind(py))
    }

    #[pyo3(signature = (key, full=false))]
    fn axinfo_stream(
        slf: &Bound<'_, Self>,
        key: &str,
        full: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.xinfo_stream(&key, full).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::DecodeXinfoStream
        )
    }

    fn xinfo_groups(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.xinfo_groups(key).await);
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        parse_xinfo_pairs_list(py, raw.bind(py))
    }

    fn axinfo_groups(slf: &Bound<'_, Self>, key: &str) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.xinfo_groups(&key).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::DecodeXinfoPairsList
        )
    }

    fn xinfo_consumers(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.xinfo_consumers(key, group).await);
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        parse_xinfo_pairs_list(py, raw.bind(py))
    }

    fn axinfo_consumers(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let group = group.to_string();
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.xinfo_consumers(&key, &group).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::DecodeXinfoPairsList
        )
    }

    // ---- XPENDING ----

    #[pyo3(signature = (key, group, start=None, end=None, count=None, consumer=None, idle=None))]
    #[allow(clippy::too_many_arguments)]
    fn xpending(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        start: Option<&str>,
        end: Option<&str>,
        count: Option<i64>,
        consumer: Option<String>,
        idle: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        if let (Some(s), Some(e), Some(c)) = (start, end, count) {
            let r: Result<redis::Value, _> = adapter_sync!(
                slf,
                conn,
                conn.xpending_range(key, group, s, e, c, consumer.as_deref(), idle).await
            );
            let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
            return decode_xpending_range(py, raw.bind(py));
        }
        let r: Result<redis::Value, _> = adapter_sync!(slf, conn, conn.xpending(key, group).await);
        let raw = crate::client::py_redis_value(py, r.map_err(crate::client::to_py_err)?)?;
        decode_xpending_summary(py, raw.bind(py))
    }

    #[pyo3(signature = (key, group, start=None, end=None, count=None, consumer=None, idle=None))]
    #[allow(clippy::too_many_arguments)]
    fn axpending(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        start: Option<&str>,
        end: Option<&str>,
        count: Option<i64>,
        consumer: Option<String>,
        idle: Option<u64>,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let group = group.to_string();
        if let (Some(s), Some(e), Some(c)) = (start, end, count) {
            let s = s.to_string();
            let e = e.to_string();
            return adapter_async!(
                slf, conn,
                {
                    use crate::client::IntoRawResult;
                    conn.xpending_range(&key, &group, &s, &e, c, consumer.as_deref(), idle)
                        .await
                        .into_raw_result()
                };
                crate::async_bridge::AwaitTransform::DecodeXpendingRange
            );
        }
        adapter_async!(
            slf, conn,
            {
                use crate::client::IntoRawResult;
                conn.xpending(&key, &group).await.into_raw_result()
            };
            crate::async_bridge::AwaitTransform::DecodeXpendingSummary
        )
    }

    // ---- XCLAIM / XAUTOCLAIM (driver returns the right shape) ----

    #[pyo3(signature = (key, group, consumer, min_idle_time, entry_ids, idle=None, time=None, retrycount=None, force=false, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn xclaim(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        entry_ids: Vec<String>,
        idle: Option<i64>,
        time: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = adapter_sync!(
            slf,
            conn,
            conn.xclaim(
                key, group, consumer, min_idle_time, &entry_ids,
                idle, time, retrycount, force, justid,
            )
            .await
        );
        let v = r.map_err(crate::client::to_py_err)?;
        if justid {
            crate::stream_decode::xclaim_justid_to_py(slf.py(), v)
        } else {
            crate::stream_decode::entries_to_py(slf.py(), v)
        }
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, entry_ids, idle=None, time=None, retrycount=None, force=false, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn axclaim(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        entry_ids: Vec<String>,
        idle: Option<i64>,
        time: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        adapter_async!(slf, conn, {
            match conn
                .xclaim(
                    &key, &group, &consumer, min_idle_time, &entry_ids,
                    idle, time, retrycount, force, justid,
                )
                .await
            {
                Ok(v) => {
                    if justid {
                        crate::async_bridge::RawResult::XClaimJustId(v)
                    } else {
                        crate::async_bridge::RawResult::StreamEntries(v)
                    }
                }
                Err(e) => crate::client::classify(e),
            }
        })
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, start_id="0-0", count=None, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn xautoclaim(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        start_id: &str,
        count: Option<i64>,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = adapter_sync!(
            slf,
            conn,
            conn.xautoclaim(key, group, consumer, min_idle_time, start_id, count, justid).await
        );
        crate::stream_decode::xautoclaim_to_py(slf.py(), r.map_err(crate::client::to_py_err)?, justid)
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, start_id="0-0", count=None, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn axautoclaim(
        slf: &Bound<'_, Self>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        start_id: &str,
        count: Option<i64>,
        justid: bool,
    ) -> PyResult<Py<crate::async_bridge::RedisRsAwaitable>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        let start_id = start_id.to_string();
        adapter_async!(slf, conn, {
            match conn
                .xautoclaim(&key, &group, &consumer, min_idle_time, &start_id, count, justid)
                .await
            {
                Ok(v) => crate::async_bridge::RawResult::Xautoclaim(v, justid),
                Err(e) => crate::client::classify(e),
            }
        })
    }
}

// =========================================================================
// Topology subclasses (Phase 5)
//
// Both subclasses extend ``RedisRsAdapter`` and only differ in which
// ``connect_*`` they invoke from their ``__new__``.
// =========================================================================

#[pyclass(extends = RedisRsAdapter, subclass, module = "django_cachex.adapters._redis_rs")]
pub struct RedisRsClusterAdapter;

#[pymethods]
impl RedisRsClusterAdapter {
    #[new]
    #[pyo3(signature = (servers, **options))]
    fn new(
        py: Python<'_>,
        servers: Py<PyAny>,
        options: Option<Bound<'_, PyDict>>,
    ) -> PyResult<(Self, RedisRsAdapter)> {
        let options = options.unwrap_or_else(|| PyDict::new(py));
        let servers_bound = servers.bind(py);
        let config = cluster_config_from_options(py, servers_bound, &options)?;
        Ok((Self, RedisRsAdapter::build(py, servers, options, config)?))
    }
}

#[pyclass(extends = RedisRsAdapter, subclass, module = "django_cachex.adapters._redis_rs")]
pub struct RedisRsSentinelAdapter;

#[pymethods]
impl RedisRsSentinelAdapter {
    #[new]
    #[pyo3(signature = (servers, **options))]
    fn new(
        py: Python<'_>,
        servers: Py<PyAny>,
        options: Option<Bound<'_, PyDict>>,
    ) -> PyResult<(Self, RedisRsAdapter)> {
        let options = options.unwrap_or_else(|| PyDict::new(py));
        let servers_bound = servers.bind(py);
        let config = sentinel_config_from_options(py, servers_bound, &options)?;
        Ok((Self, RedisRsAdapter::build(py, servers, options, config)?))
    }
}
