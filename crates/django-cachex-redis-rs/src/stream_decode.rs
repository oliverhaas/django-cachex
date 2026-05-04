// Stream-reply → Python converters. Parses ``redis::Value`` via the
// redis crate's ``FromRedisValue`` impls for ``StreamRangeReply`` /
// ``StreamReadReply`` / ``StreamClaimReply``, then walks the typed
// reply struct to build the final adapter shape:
//
//   * XRANGE / XREVRANGE / XCLAIM (no JUSTID):
//       list[tuple[str, dict[str, bytes]]]
//   * XREAD / XREADGROUP:
//       dict[str, list[tuple[str, dict[str, bytes]]]] | None
//   * XCLAIM with JUSTID:
//       list[str]
//   * XAUTOCLAIM:
//       (str, list[tuple[str, dict[str, bytes]]] | list[str], list[str])

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use redis::FromRedisValue;
use redis::streams::{StreamId, StreamRangeReply, StreamReadReply};

fn parse_err<E: std::fmt::Display>(ctx: &str, e: E) -> PyErr {
    PyRuntimeError::new_err(format!("failed to parse {ctx}: {e}"))
}

fn shape_err(ctx: &str) -> PyErr {
    PyRuntimeError::new_err(format!("unexpected stream value shape: {ctx}"))
}

/// Coerce a ``redis::Value`` (typically ``BulkString`` from a stream field)
/// to ``Vec<u8>``. ``Int`` is decimal-encoded for parity with the Python
/// adapter's pre-Phase-4 behavior.
fn value_to_bytes(v: &redis::Value) -> PyResult<Vec<u8>> {
    match v {
        redis::Value::BulkString(b) => Ok(b.clone()),
        redis::Value::SimpleString(s) => Ok(s.as_bytes().to_vec()),
        redis::Value::Int(i) => Ok(i.to_string().into_bytes()),
        redis::Value::VerbatimString { text, .. } => Ok(text.as_bytes().to_vec()),
        redis::Value::Nil => Ok(Vec::new()),
        _ => Err(shape_err("expected bytes for stream field value")),
    }
}

/// Convert a parsed ``StreamId`` into a ``(id_str, dict[str, bytes])`` tuple.
fn stream_id_to_pytuple(py: Python<'_>, sid: StreamId) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (field, value) in &sid.map {
        let bytes = value_to_bytes(value)?;
        dict.set_item(PyString::new(py, field), PyBytes::new(py, &bytes))?;
    }
    let py_id = PyString::new(py, &sid.id).into_any().unbind();
    let py_dict = dict.into_any().unbind();
    Ok(PyTuple::new(py, [py_id, py_dict])?.into_any().unbind())
}

fn entries_list_to_py(py: Python<'_>, ids: Vec<StreamId>) -> PyResult<Py<PyAny>> {
    let mut out: Vec<Py<PyAny>> = Vec::with_capacity(ids.len());
    for sid in ids {
        out.push(stream_id_to_pytuple(py, sid)?);
    }
    Ok(PyList::new(py, out)?.into_any().unbind())
}

/// Build the ``list[tuple[str, dict[str, bytes]]]`` shape for XRANGE /
/// XREVRANGE / XCLAIM (without ``JUSTID``).
pub(crate) fn entries_to_py(py: Python<'_>, v: redis::Value) -> PyResult<Py<PyAny>> {
    if matches!(v, redis::Value::Nil) {
        return Ok(PyList::empty(py).into_any().unbind());
    }
    let reply =
        StreamRangeReply::from_redis_value(v).map_err(|e| parse_err("StreamRangeReply", e))?;
    entries_list_to_py(py, reply.ids)
}

/// Build the ``dict[str, list[...]] | None`` shape for XREAD / XREADGROUP.
pub(crate) fn read_to_py(py: Python<'_>, v: redis::Value) -> PyResult<Py<PyAny>> {
    if matches!(v, redis::Value::Nil) {
        return Ok(py.None());
    }
    let reply =
        StreamReadReply::from_redis_value(v).map_err(|e| parse_err("StreamReadReply", e))?;
    let dict = PyDict::new(py);
    for stream_key in reply.keys {
        let py_entries = entries_list_to_py(py, stream_key.ids)?;
        dict.set_item(PyString::new(py, &stream_key.key), py_entries)?;
    }
    Ok(dict.into_any().unbind())
}

/// Build ``list[str]`` from XCLAIM ``JUSTID`` (array of bulk-string ids).
pub(crate) fn xclaim_justid_to_py(py: Python<'_>, v: redis::Value) -> PyResult<Py<PyAny>> {
    if matches!(v, redis::Value::Nil) {
        return Ok(PyList::empty(py).into_any().unbind());
    }
    let ids: Vec<String> =
        Vec::<String>::from_redis_value(v).map_err(|e| parse_err("Vec<String>", e))?;
    let py_items: Vec<Py<PyAny>> = ids
        .iter()
        .map(|s| PyString::new(py, s).into_any().unbind())
        .collect();
    Ok(PyList::new(py, py_items)?.into_any().unbind())
}

/// Build ``(next_id, entries_or_ids, deleted_ids)`` for XAUTOCLAIM. The
/// reply layout is ``[next_id, entries, deleted_ids]``; we parse ``next_id``
/// + ``deleted_ids`` as strings and let ``entries`` flow through the
/// standard XRANGE-shape decoder (or ``xclaim_justid_to_py`` when
/// ``justid=true``). The redis crate has a typed ``StreamAutoClaimReply``
/// but exposing it would force a justid/no-justid split at this layer; the
/// adapter's contract returns either branch, so the value-level decode is
/// the simpler match.
pub(crate) fn xautoclaim_to_py(
    py: Python<'_>,
    v: redis::Value,
    justid: bool,
) -> PyResult<Py<PyAny>> {
    let items = match v {
        redis::Value::Nil => {
            let py_id = PyString::new(py, "0-0").into_any().unbind();
            let py_entries = PyList::empty(py).into_any().unbind();
            let py_deleted = PyList::empty(py).into_any().unbind();
            return Ok(PyTuple::new(py, [py_id, py_entries, py_deleted])?
                .into_any()
                .unbind());
        }
        redis::Value::Array(items) => items,
        _ => return Err(shape_err("expected array for xautoclaim response")),
    };
    let mut iter = items.into_iter();
    let next_id_v = iter.next().ok_or_else(|| shape_err("missing next_id"))?;
    let entries_v = iter.next().unwrap_or(redis::Value::Array(Vec::new()));
    let deleted_v = iter.next().unwrap_or(redis::Value::Array(Vec::new()));
    let next_id = String::from_redis_value(next_id_v)
        .map_err(|e| parse_err("xautoclaim next_id", e))?;
    let py_next_id = PyString::new(py, &next_id).into_any().unbind();
    let py_entries = if justid {
        xclaim_justid_to_py(py, entries_v)?
    } else {
        entries_to_py(py, entries_v)?
    };
    let py_deleted = xclaim_justid_to_py(py, deleted_v)?;
    Ok(PyTuple::new(py, [py_next_id, py_entries, py_deleted])?
        .into_any()
        .unbind())
}
