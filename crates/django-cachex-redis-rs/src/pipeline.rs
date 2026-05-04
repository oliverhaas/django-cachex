// Pipeline pyclass: buffer commands, dispatch via the driver's
// ``pipeline_exec`` / ``apipeline_exec`` in one round trip.
//
// The shape of each command on the wire is ``(name, args)``. Per-command
// parsers (Python callables in ``django_cachex.adapters._pipeline_parsers``)
// normalize the response shape — RESP2 flat lists → dicts, bytes → float,
// stream entries → ``(id, fields)`` tuples, etc.
//
// Sync ``execute()`` resolves to ``list[Any]`` directly. Async
// ``aexecute()`` returns a coroutine that awaits the driver awaitable then
// applies parsers (via ``apipeline_execute`` in ``_pipeline_parsers``).

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::adapter::{AbsExpiryT, ExpiryT};

const PARSERS_MOD: &str = "django_cachex.adapters._pipeline_parsers";

/// Resolve a parser by name from the Python ``_pipeline_parsers`` module.
fn parser<'py>(py: Python<'py>, name: &str) -> PyResult<Py<PyAny>> {
    Ok(py.import(PARSERS_MOD)?.getattr(name)?.unbind())
}

/// Coerce a Python value to ``bytes`` for the wire. Mirrors the redis-py
/// behavior: bool → b"0"/b"1", int → ASCII decimal, float → ``repr()``,
/// str → utf-8.
fn to_redis_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = value.extract::<Vec<u8>>() {
        return Ok(b);
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(if b { b"1".to_vec() } else { b"0".to_vec() });
    }
    if let Ok(n) = value.extract::<i64>() {
        return Ok(n.to_string().into_bytes());
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(format!("{f}").into_bytes());
    }
    let s: String = value.str()?.extract()?;
    Ok(s.into_bytes())
}

/// Coerce an iterable to ``Vec<Vec<u8>>`` via ``to_redis_bytes`` per item.
fn collect_args(values: &[Bound<'_, PyAny>]) -> PyResult<Vec<Vec<u8>>> {
    values.iter().map(to_redis_bytes).collect()
}

/// Convert ``int | timedelta`` to seconds bytes via ``ExpiryT``.
fn seconds_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    Ok(value.extract::<ExpiryT>()?.to_seconds().to_string().into_bytes())
}

fn milliseconds_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    Ok(value.extract::<ExpiryT>()?.to_milliseconds().to_string().into_bytes())
}

fn epoch_seconds_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    Ok(value.extract::<AbsExpiryT>()?.to_unix()?.to_string().into_bytes())
}

fn epoch_milliseconds_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    Ok(value.extract::<AbsExpiryT>()?.to_unix_milliseconds()?.to_string().into_bytes())
}

#[pyclass(subclass, module = "django_cachex.adapters._redis_rs")]
pub struct RedisRsPipelineAdapter {
    adapter: Py<crate::adapter::RedisRsAdapter>,
    transaction: bool,
    commands: Vec<(String, Vec<Vec<u8>>)>,
    parsers: Vec<Option<Py<PyAny>>>,
}

#[pymethods]
impl RedisRsPipelineAdapter {
    #[new]
    #[pyo3(signature = (adapter, *, transaction = true))]
    fn new(adapter: Py<crate::adapter::RedisRsAdapter>, transaction: bool) -> Self {
        Self {
            adapter,
            transaction,
            commands: Vec::new(),
            parsers: Vec::new(),
        }
    }

    fn reset(&mut self) {
        self.commands.clear();
        self.parsers.clear();
    }

    fn execute(slf: PyRefMut<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let mut this = slf;
        if this.commands.is_empty() {
            this.parsers.clear();
            return Ok(pyo3::types::PyList::empty(py).into_any().unbind());
        }
        let commands = std::mem::take(&mut this.commands);
        let parsers = std::mem::take(&mut this.parsers);
        let transaction = this.transaction;
        let adapter = this.adapter.clone_ref(py);
        drop(this);
        let raw = adapter
            .bind(py)
            .call_method1("pipeline_exec", (commands, transaction))?;
        apply_parsers(py, &raw, &parsers)
    }

    #[pyo3(signature = (*args))]
    fn execute_command<'py>(
        mut slf: PyRefMut<'py, Self>,
        args: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        if args.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "execute_command requires at least the command name",
            ));
        }
        let cmd: String = if let Ok(s) = args[0].extract::<String>() {
            s
        } else {
            let b: Vec<u8> = args[0].extract()?;
            String::from_utf8_lossy(&b).into_owned()
        };
        let rest = collect_args(&args[1..])?;
        slf.commands.push((cmd, rest));
        slf.parsers.push(None);
        Ok(slf)
    }

    // ============================================================== strings

    #[pyo3(signature = (key, value, *, ex=None, px=None, nx=false, xx=false, exat=None, pxat=None, keepttl=false, get=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn set<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        value: &Bound<'py, PyAny>,
        ex: Option<&Bound<'py, PyAny>>,
        px: Option<&Bound<'py, PyAny>>,
        nx: bool,
        xx: bool,
        exat: Option<&Bound<'py, PyAny>>,
        pxat: Option<&Bound<'py, PyAny>>,
        keepttl: bool,
        get: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args: Vec<Vec<u8>> = vec![to_redis_bytes(key)?, to_redis_bytes(value)?];
        if let Some(v) = ex {
            args.push(b"EX".to_vec());
            args.push(seconds_bytes(v)?);
        }
        if let Some(v) = px {
            args.push(b"PX".to_vec());
            args.push(milliseconds_bytes(v)?);
        }
        if let Some(v) = exat {
            args.push(b"EXAT".to_vec());
            args.push(epoch_seconds_bytes(v)?);
        }
        if let Some(v) = pxat {
            args.push(b"PXAT".to_vec());
            args.push(epoch_milliseconds_bytes(v)?);
        }
        if keepttl {
            args.push(b"KEEPTTL".to_vec());
        }
        if nx {
            args.push(b"NX".to_vec());
        }
        if xx {
            args.push(b"XX".to_vec());
        }
        if get {
            args.push(b"GET".to_vec());
        }
        slf.commands.push(("SET".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn get<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("GET".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    fn delete<'py>(
        mut slf: PyRefMut<'py, Self>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("DEL".to_string(), collect_args(&keys)?));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    fn exists<'py>(
        mut slf: PyRefMut<'py, Self>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("EXISTS".to_string(), collect_args(&keys)?));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, seconds, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn expire<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        seconds: &Bound<'py, PyAny>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?, seconds_bytes(seconds)?];
        push_ttl_flags(&mut args, nx, xx, gt, lt);
        let py = slf.py();
        slf.commands.push(("EXPIRE".to_string(), args));
        slf.parsers.push(Some(py.import("builtins")?.getattr("bool")?.unbind()));
        Ok(slf)
    }

    #[pyo3(signature = (key, when, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn expireat<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        when: &Bound<'py, PyAny>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?, epoch_seconds_bytes(when)?];
        push_ttl_flags(&mut args, nx, xx, gt, lt);
        let py = slf.py();
        slf.commands.push(("EXPIREAT".to_string(), args));
        slf.parsers.push(Some(py.import("builtins")?.getattr("bool")?.unbind()));
        Ok(slf)
    }

    #[pyo3(signature = (key, milliseconds, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn pexpire<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        milliseconds: &Bound<'py, PyAny>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?, milliseconds_bytes(milliseconds)?];
        push_ttl_flags(&mut args, nx, xx, gt, lt);
        let py = slf.py();
        slf.commands.push(("PEXPIRE".to_string(), args));
        slf.parsers.push(Some(py.import("builtins")?.getattr("bool")?.unbind()));
        Ok(slf)
    }

    #[pyo3(signature = (key, when, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn pexpireat<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        when: &Bound<'py, PyAny>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?, epoch_milliseconds_bytes(when)?];
        push_ttl_flags(&mut args, nx, xx, gt, lt);
        let py = slf.py();
        slf.commands.push(("PEXPIREAT".to_string(), args));
        slf.parsers.push(Some(py.import("builtins")?.getattr("bool")?.unbind()));
        Ok(slf)
    }

    fn persist<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.commands.push(("PERSIST".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(Some(py.import("builtins")?.getattr("bool")?.unbind()));
        Ok(slf)
    }

    fn ttl<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("TTL".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn pttl<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("PTTL".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn expiretime<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("EXPIRETIME".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(name = "type")]
    fn type_<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("TYPE".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn rename<'py>(
        mut slf: PyRefMut<'py, Self>,
        src: &Bound<'py, PyAny>,
        dst: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("RENAME".to_string(), vec![to_redis_bytes(src)?, to_redis_bytes(dst)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn renamenx<'py>(
        mut slf: PyRefMut<'py, Self>,
        src: &Bound<'py, PyAny>,
        dst: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("RENAMENX".to_string(), vec![to_redis_bytes(src)?, to_redis_bytes(dst)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, amount=1))]
    fn incrby<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        amount: i64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "INCRBY".to_string(),
            vec![to_redis_bytes(key)?, amount.to_string().into_bytes()],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, amount=1))]
    fn decrby<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        amount: i64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "DECRBY".to_string(),
            vec![to_redis_bytes(key)?, amount.to_string().into_bytes()],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    // ================================================================ lists

    #[pyo3(signature = (key, *values))]
    fn lpush<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        values: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        args.extend(collect_args(&values)?);
        slf.commands.push(("LPUSH".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, *values))]
    fn rpush<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        values: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        args.extend(collect_args(&values)?);
        slf.commands.push(("RPUSH".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, count=None))]
    fn lpop<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        count: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if let Some(n) = count {
            args.push(n.to_string().into_bytes());
        }
        slf.commands.push(("LPOP".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, count=None))]
    fn rpop<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        count: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if let Some(n) = count {
            args.push(n.to_string().into_bytes());
        }
        slf.commands.push(("RPOP".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn lrange<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        start: i64,
        end: i64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "LRANGE".to_string(),
            vec![
                to_redis_bytes(key)?,
                start.to_string().into_bytes(),
                end.to_string().into_bytes(),
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn lindex<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        index: i64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "LINDEX".to_string(),
            vec![to_redis_bytes(key)?, index.to_string().into_bytes()],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn llen<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("LLEN".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn lrem<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        count: i64,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "LREM".to_string(),
            vec![
                to_redis_bytes(key)?,
                count.to_string().into_bytes(),
                to_redis_bytes(value)?,
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn ltrim<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        start: i64,
        end: i64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "LTRIM".to_string(),
            vec![
                to_redis_bytes(key)?,
                start.to_string().into_bytes(),
                end.to_string().into_bytes(),
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn lset<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        index: i64,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "LSET".to_string(),
            vec![
                to_redis_bytes(key)?,
                index.to_string().into_bytes(),
                to_redis_bytes(value)?,
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn linsert<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        whence: &str,
        pivot: &Bound<'py, PyAny>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "LINSERT".to_string(),
            vec![
                to_redis_bytes(key)?,
                whence.to_uppercase().into_bytes(),
                to_redis_bytes(pivot)?,
                to_redis_bytes(value)?,
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, value, rank=None, count=None, maxlen=None))]
    fn lpos<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        value: &Bound<'py, PyAny>,
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?, to_redis_bytes(value)?];
        if let Some(n) = rank {
            args.push(b"RANK".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if let Some(n) = count {
            args.push(b"COUNT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if let Some(n) = maxlen {
            args.push(b"MAXLEN".to_vec());
            args.push(n.to_string().into_bytes());
        }
        slf.commands.push(("LPOS".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (source, destination, src="LEFT", dest="RIGHT"))]
    fn lmove<'py>(
        mut slf: PyRefMut<'py, Self>,
        source: &Bound<'py, PyAny>,
        destination: &Bound<'py, PyAny>,
        src: &str,
        dest: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "LMOVE".to_string(),
            vec![
                to_redis_bytes(source)?,
                to_redis_bytes(destination)?,
                src.to_uppercase().into_bytes(),
                dest.to_uppercase().into_bytes(),
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    // ================================================================= sets

    #[pyo3(signature = (key, *members))]
    fn sadd<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        members: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        args.extend(collect_args(&members)?);
        slf.commands.push(("SADD".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, *members))]
    fn srem<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        members: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        args.extend(collect_args(&members)?);
        slf.commands.push(("SREM".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn smembers<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("SMEMBERS".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn sismember<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        member: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "SISMEMBER".to_string(),
            vec![to_redis_bytes(key)?, to_redis_bytes(member)?],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, *members))]
    fn smismember<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        members: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        args.extend(collect_args(&members)?);
        slf.commands.push(("SMISMEMBER".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn scard<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("SCARD".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    fn sdiff<'py>(
        mut slf: PyRefMut<'py, Self>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("SDIFF".to_string(), collect_args(&keys)?));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (dest, *keys))]
    fn sdiffstore<'py>(
        mut slf: PyRefMut<'py, Self>,
        dest: &Bound<'py, PyAny>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(dest)?];
        args.extend(collect_args(&keys)?);
        slf.commands.push(("SDIFFSTORE".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    fn sinter<'py>(
        mut slf: PyRefMut<'py, Self>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("SINTER".to_string(), collect_args(&keys)?));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (dest, *keys))]
    fn sinterstore<'py>(
        mut slf: PyRefMut<'py, Self>,
        dest: &Bound<'py, PyAny>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(dest)?];
        args.extend(collect_args(&keys)?);
        slf.commands.push(("SINTERSTORE".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    fn sunion<'py>(
        mut slf: PyRefMut<'py, Self>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("SUNION".to_string(), collect_args(&keys)?));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (dest, *keys))]
    fn sunionstore<'py>(
        mut slf: PyRefMut<'py, Self>,
        dest: &Bound<'py, PyAny>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(dest)?];
        args.extend(collect_args(&keys)?);
        slf.commands.push(("SUNIONSTORE".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn smove<'py>(
        mut slf: PyRefMut<'py, Self>,
        source: &Bound<'py, PyAny>,
        destination: &Bound<'py, PyAny>,
        member: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "SMOVE".to_string(),
            vec![
                to_redis_bytes(source)?,
                to_redis_bytes(destination)?,
                to_redis_bytes(member)?,
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, count=None))]
    fn spop<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        count: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if let Some(n) = count {
            args.push(n.to_string().into_bytes());
        }
        slf.commands.push(("SPOP".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, count=None))]
    fn srandmember<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        count: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if let Some(n) = count {
            args.push(n.to_string().into_bytes());
        }
        slf.commands.push(("SRANDMEMBER".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    // =============================================================== hashes

    #[pyo3(signature = (key, field=None, value=None, mapping=None, items=None))]
    fn hset<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        field: Option<&Bound<'py, PyAny>>,
        value: Option<&Bound<'py, PyAny>>,
        mapping: Option<&Bound<'py, PyDict>>,
        items: Option<Vec<Bound<'py, PyAny>>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if let Some(f) = field {
            let v = value.ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("hset: value required when field provided")
            })?;
            args.push(to_redis_bytes(f)?);
            args.push(to_redis_bytes(v)?);
        }
        if let Some(m) = mapping {
            for (k, v) in m.iter() {
                args.push(to_redis_bytes(&k)?);
                args.push(to_redis_bytes(&v)?);
            }
        }
        if let Some(its) = items {
            let mut it = its.into_iter();
            while let (Some(f), Some(v)) = (it.next(), it.next()) {
                args.push(to_redis_bytes(&f)?);
                args.push(to_redis_bytes(&v)?);
            }
        }
        slf.commands.push(("HSET".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn hsetnx<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        field: &Bound<'py, PyAny>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "HSETNX".to_string(),
            vec![
                to_redis_bytes(key)?,
                to_redis_bytes(field)?,
                to_redis_bytes(value)?,
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    fn hdel<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        fields: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        args.extend(collect_args(&fields)?);
        slf.commands.push(("HDEL".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn hexists<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        field: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "HEXISTS".to_string(),
            vec![to_redis_bytes(key)?, to_redis_bytes(field)?],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn hget<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        field: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "HGET".to_string(),
            vec![to_redis_bytes(key)?, to_redis_bytes(field)?],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn hgetall<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.commands.push(("HGETALL".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(Some(parser(py, "hgetall")?));
        Ok(slf)
    }

    fn hkeys<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("HKEYS".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn hvals<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("HVALS".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn hlen<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("HLEN".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn hmget<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        fields: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if fields.extract::<&str>().is_ok() || fields.extract::<Vec<u8>>().is_ok() {
            args.push(to_redis_bytes(fields)?);
        } else {
            for item in fields.try_iter()? {
                args.push(to_redis_bytes(&item?)?);
            }
        }
        slf.commands.push(("HMGET".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, field, amount=1))]
    fn hincrby<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        field: &Bound<'py, PyAny>,
        amount: i64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "HINCRBY".to_string(),
            vec![
                to_redis_bytes(key)?,
                to_redis_bytes(field)?,
                amount.to_string().into_bytes(),
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, field, amount=1.0))]
    fn hincrbyfloat<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        field: &Bound<'py, PyAny>,
        amount: f64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.commands.push((
            "HINCRBYFLOAT".to_string(),
            vec![
                to_redis_bytes(key)?,
                to_redis_bytes(field)?,
                format!("{amount}").into_bytes(),
            ],
        ));
        slf.parsers.push(Some(parser(py, "to_float_or_none")?));
        Ok(slf)
    }

    // ========================================================== sorted sets

    #[pyo3(signature = (key, mapping, *, nx=false, xx=false, ch=false, incr=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn zadd<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        mapping: &Bound<'py, PyDict>,
        nx: bool,
        xx: bool,
        ch: bool,
        incr: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if nx {
            args.push(b"NX".to_vec());
        }
        if xx {
            args.push(b"XX".to_vec());
        }
        if gt {
            args.push(b"GT".to_vec());
        }
        if lt {
            args.push(b"LT".to_vec());
        }
        if ch {
            args.push(b"CH".to_vec());
        }
        if incr {
            args.push(b"INCR".to_vec());
        }
        for (member, score) in mapping.iter() {
            // ZADD argument order is ``score member``.
            let s: f64 = score.extract()?;
            args.push(format!("{s}").into_bytes());
            args.push(to_redis_bytes(&member)?);
        }
        let py = slf.py();
        slf.commands.push(("ZADD".to_string(), args));
        slf.parsers
            .push(if incr { Some(parser(py, "to_float_or_none")?) } else { None });
        Ok(slf)
    }

    fn zcard<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("ZCARD".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn zcount<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        min: &Bound<'py, PyAny>,
        max: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "ZCOUNT".to_string(),
            vec![
                to_redis_bytes(key)?,
                to_redis_bytes(min)?,
                to_redis_bytes(max)?,
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn zincrby<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        amount: f64,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.commands.push((
            "ZINCRBY".to_string(),
            vec![
                to_redis_bytes(key)?,
                format!("{amount}").into_bytes(),
                to_redis_bytes(value)?,
            ],
        ));
        slf.parsers.push(Some(parser(py, "to_float_or_none")?));
        Ok(slf)
    }

    #[pyo3(signature = (key, count=None))]
    fn zpopmax<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        count: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let n = count.unwrap_or(1);
        let py = slf.py();
        slf.commands.push((
            "ZPOPMAX".to_string(),
            vec![to_redis_bytes(key)?, n.to_string().into_bytes()],
        ));
        slf.parsers.push(Some(parser(py, "zset_with_scores")?));
        Ok(slf)
    }

    #[pyo3(signature = (key, count=None))]
    fn zpopmin<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        count: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let n = count.unwrap_or(1);
        let py = slf.py();
        slf.commands.push((
            "ZPOPMIN".to_string(),
            vec![to_redis_bytes(key)?, n.to_string().into_bytes()],
        ));
        slf.parsers.push(Some(parser(py, "zset_with_scores")?));
        Ok(slf)
    }

    #[pyo3(signature = (key, start, end, *, desc=false, withscores=false, score_cast_func=None))]
    fn zrange<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        start: i64,
        end: i64,
        desc: bool,
        withscores: bool,
        score_cast_func: Option<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let _ = score_cast_func;
        let mut args = vec![
            to_redis_bytes(key)?,
            start.to_string().into_bytes(),
            end.to_string().into_bytes(),
        ];
        if desc {
            args.push(b"REV".to_vec());
        }
        if withscores {
            args.push(b"WITHSCORES".to_vec());
        }
        let py = slf.py();
        slf.commands.push(("ZRANGE".to_string(), args));
        slf.parsers.push(if withscores {
            Some(parser(py, "zset_with_scores")?)
        } else {
            None
        });
        Ok(slf)
    }

    #[pyo3(signature = (key, start, end, *, withscores=false, score_cast_func=None))]
    fn zrevrange<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        start: i64,
        end: i64,
        withscores: bool,
        score_cast_func: Option<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let _ = score_cast_func;
        let mut args = vec![
            to_redis_bytes(key)?,
            start.to_string().into_bytes(),
            end.to_string().into_bytes(),
        ];
        if withscores {
            args.push(b"WITHSCORES".to_vec());
        }
        let py = slf.py();
        slf.commands.push(("ZREVRANGE".to_string(), args));
        slf.parsers.push(if withscores {
            Some(parser(py, "zset_with_scores")?)
        } else {
            None
        });
        Ok(slf)
    }

    #[pyo3(signature = (key, min, max, start=None, num=None, *, withscores=false, score_cast_func=None))]
    #[allow(clippy::too_many_arguments)]
    fn zrangebyscore<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        min: &Bound<'py, PyAny>,
        max: &Bound<'py, PyAny>,
        start: Option<i64>,
        num: Option<i64>,
        withscores: bool,
        score_cast_func: Option<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let _ = score_cast_func;
        let mut args = vec![
            to_redis_bytes(key)?,
            to_redis_bytes(min)?,
            to_redis_bytes(max)?,
        ];
        if withscores {
            args.push(b"WITHSCORES".to_vec());
        }
        if let (Some(s), Some(n)) = (start, num) {
            args.push(b"LIMIT".to_vec());
            args.push(s.to_string().into_bytes());
            args.push(n.to_string().into_bytes());
        }
        let py = slf.py();
        slf.commands.push(("ZRANGEBYSCORE".to_string(), args));
        slf.parsers.push(if withscores {
            Some(parser(py, "zset_with_scores")?)
        } else {
            None
        });
        Ok(slf)
    }

    #[pyo3(signature = (key, max, min, start=None, num=None, *, withscores=false, score_cast_func=None))]
    #[allow(clippy::too_many_arguments)]
    fn zrevrangebyscore<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        max: &Bound<'py, PyAny>,
        min: &Bound<'py, PyAny>,
        start: Option<i64>,
        num: Option<i64>,
        withscores: bool,
        score_cast_func: Option<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let _ = score_cast_func;
        let mut args = vec![
            to_redis_bytes(key)?,
            to_redis_bytes(max)?,
            to_redis_bytes(min)?,
        ];
        if withscores {
            args.push(b"WITHSCORES".to_vec());
        }
        if let (Some(s), Some(n)) = (start, num) {
            args.push(b"LIMIT".to_vec());
            args.push(s.to_string().into_bytes());
            args.push(n.to_string().into_bytes());
        }
        let py = slf.py();
        slf.commands.push(("ZREVRANGEBYSCORE".to_string(), args));
        slf.parsers.push(if withscores {
            Some(parser(py, "zset_with_scores")?)
        } else {
            None
        });
        Ok(slf)
    }

    fn zrank<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "ZRANK".to_string(),
            vec![to_redis_bytes(key)?, to_redis_bytes(value)?],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn zrevrank<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "ZREVRANK".to_string(),
            vec![to_redis_bytes(key)?, to_redis_bytes(value)?],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn zscore<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.commands.push((
            "ZSCORE".to_string(),
            vec![to_redis_bytes(key)?, to_redis_bytes(value)?],
        ));
        slf.parsers.push(Some(parser(py, "to_float_or_none")?));
        Ok(slf)
    }

    fn zmscore<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        members: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        for item in members.try_iter()? {
            args.push(to_redis_bytes(&item?)?);
        }
        let py = slf.py();
        slf.commands.push(("ZMSCORE".to_string(), args));
        slf.parsers.push(Some(parser(py, "list_to_float_or_none")?));
        Ok(slf)
    }

    #[pyo3(signature = (key, *values))]
    fn zrem<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        values: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        args.extend(collect_args(&values)?);
        slf.commands.push(("ZREM".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn zremrangebyscore<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        min: &Bound<'py, PyAny>,
        max: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "ZREMRANGEBYSCORE".to_string(),
            vec![
                to_redis_bytes(key)?,
                to_redis_bytes(min)?,
                to_redis_bytes(max)?,
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn zremrangebyrank<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        start: i64,
        end: i64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "ZREMRANGEBYRANK".to_string(),
            vec![
                to_redis_bytes(key)?,
                start.to_string().into_bytes(),
                end.to_string().into_bytes(),
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    // ============================================================== streams

    #[pyo3(signature = (key, fields, *, id="*", maxlen=None, approximate=true, nomkstream=false, minid=None, limit=None))]
    #[allow(clippy::too_many_arguments)]
    fn xadd<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        fields: &Bound<'py, PyDict>,
        id: &str,
        maxlen: Option<i64>,
        approximate: bool,
        nomkstream: bool,
        minid: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if nomkstream {
            args.push(b"NOMKSTREAM".to_vec());
        }
        if let Some(m) = maxlen {
            args.push(b"MAXLEN".to_vec());
            args.push(if approximate { b"~".to_vec() } else { b"=".to_vec() });
            args.push(m.to_string().into_bytes());
        } else if let Some(s) = minid {
            args.push(b"MINID".to_vec());
            args.push(if approximate { b"~".to_vec() } else { b"=".to_vec() });
            args.push(s.as_bytes().to_vec());
        }
        if let Some(n) = limit {
            args.push(b"LIMIT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        args.push(id.as_bytes().to_vec());
        for (k, v) in fields.iter() {
            args.push(to_redis_bytes(&k)?);
            args.push(to_redis_bytes(&v)?);
        }
        let py = slf.py();
        slf.commands.push(("XADD".to_string(), args));
        slf.parsers.push(Some(parser(py, "bytes_or_none_to_str")?));
        Ok(slf)
    }

    fn xlen<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push(("XLEN".to_string(), vec![to_redis_bytes(key)?]));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, min="-", max="+", count=None))]
    fn xrange<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        min: &str,
        max: &str,
        count: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![
            to_redis_bytes(key)?,
            min.as_bytes().to_vec(),
            max.as_bytes().to_vec(),
        ];
        if let Some(n) = count {
            args.push(b"COUNT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        let py = slf.py();
        slf.commands.push(("XRANGE".to_string(), args));
        slf.parsers.push(Some(parser(py, "stream_entries")?));
        Ok(slf)
    }

    #[pyo3(signature = (key, max="+", min="-", count=None))]
    fn xrevrange<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        max: &str,
        min: &str,
        count: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![
            to_redis_bytes(key)?,
            max.as_bytes().to_vec(),
            min.as_bytes().to_vec(),
        ];
        if let Some(n) = count {
            args.push(b"COUNT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        let py = slf.py();
        slf.commands.push(("XREVRANGE".to_string(), args));
        slf.parsers.push(Some(parser(py, "stream_entries")?));
        Ok(slf)
    }

    #[pyo3(signature = (streams, count=None, block=None))]
    fn xread<'py>(
        mut slf: PyRefMut<'py, Self>,
        streams: &Bound<'py, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args: Vec<Vec<u8>> = Vec::new();
        if let Some(n) = count {
            args.push(b"COUNT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if let Some(n) = block {
            args.push(b"BLOCK".to_vec());
            args.push(n.to_string().into_bytes());
        }
        args.push(b"STREAMS".to_vec());
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut ids: Vec<Vec<u8>> = Vec::new();
        for (k, v) in streams.iter() {
            keys.push(to_redis_bytes(&k)?);
            ids.push(to_redis_bytes(&v)?);
        }
        args.extend(keys);
        args.extend(ids);
        let py = slf.py();
        slf.commands.push(("XREAD".to_string(), args));
        slf.parsers.push(Some(parser(py, "stream_read")?));
        Ok(slf)
    }

    #[pyo3(signature = (groupname, consumername, streams, count=None, block=None, noack=false))]
    #[allow(clippy::too_many_arguments)]
    fn xreadgroup<'py>(
        mut slf: PyRefMut<'py, Self>,
        groupname: &str,
        consumername: &str,
        streams: &Bound<'py, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
        noack: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args: Vec<Vec<u8>> = vec![
            b"GROUP".to_vec(),
            groupname.as_bytes().to_vec(),
            consumername.as_bytes().to_vec(),
        ];
        if let Some(n) = count {
            args.push(b"COUNT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if let Some(n) = block {
            args.push(b"BLOCK".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if noack {
            args.push(b"NOACK".to_vec());
        }
        args.push(b"STREAMS".to_vec());
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut ids: Vec<Vec<u8>> = Vec::new();
        for (k, v) in streams.iter() {
            keys.push(to_redis_bytes(&k)?);
            ids.push(to_redis_bytes(&v)?);
        }
        args.extend(keys);
        args.extend(ids);
        let py = slf.py();
        slf.commands.push(("XREADGROUP".to_string(), args));
        slf.parsers.push(Some(parser(py, "stream_read")?));
        Ok(slf)
    }

    #[pyo3(signature = (key, maxlen=None, approximate=true, minid=None, limit=None))]
    fn xtrim<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        maxlen: Option<i64>,
        approximate: bool,
        minid: Option<&str>,
        limit: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        if let Some(m) = maxlen {
            args.push(b"MAXLEN".to_vec());
            args.push(if approximate { b"~".to_vec() } else { b"=".to_vec() });
            args.push(m.to_string().into_bytes());
        } else if let Some(s) = minid {
            args.push(b"MINID".to_vec());
            args.push(if approximate { b"~".to_vec() } else { b"=".to_vec() });
            args.push(s.as_bytes().to_vec());
        }
        if let Some(n) = limit {
            args.push(b"LIMIT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        slf.commands.push(("XTRIM".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, *entry_ids))]
    fn xdel<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        entry_ids: Vec<String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?];
        for id in entry_ids {
            args.push(id.into_bytes());
        }
        slf.commands.push(("XDEL".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, full=false))]
    fn xinfo_stream<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        full: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![b"STREAM".to_vec(), to_redis_bytes(key)?];
        if full {
            args.push(b"FULL".to_vec());
        }
        let py = slf.py();
        slf.commands.push(("XINFO".to_string(), args));
        slf.parsers.push(Some(parser(py, "xinfo_dict")?));
        Ok(slf)
    }

    fn xinfo_groups<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.commands.push((
            "XINFO".to_string(),
            vec![b"GROUPS".to_vec(), to_redis_bytes(key)?],
        ));
        slf.parsers.push(Some(parser(py, "xinfo_dict_list")?));
        Ok(slf)
    }

    fn xinfo_consumers<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.commands.push((
            "XINFO".to_string(),
            vec![
                b"CONSUMERS".to_vec(),
                to_redis_bytes(key)?,
                group.as_bytes().to_vec(),
            ],
        ));
        slf.parsers.push(Some(parser(py, "xinfo_dict_list")?));
        Ok(slf)
    }

    #[pyo3(signature = (key, group, id="$", *, mkstream=false, entries_read=None))]
    fn xgroup_create<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
        id: &str,
        mkstream: bool,
        entries_read: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args: Vec<Vec<u8>> = vec![
            b"CREATE".to_vec(),
            to_redis_bytes(key)?,
            group.as_bytes().to_vec(),
            id.as_bytes().to_vec(),
        ];
        if mkstream {
            args.push(b"MKSTREAM".to_vec());
        }
        if let Some(n) = entries_read {
            args.push(b"ENTRIESREAD".to_vec());
            args.push(n.to_string().into_bytes());
        }
        slf.commands.push(("XGROUP".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn xgroup_destroy<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "XGROUP".to_string(),
            vec![
                b"DESTROY".to_vec(),
                to_redis_bytes(key)?,
                group.as_bytes().to_vec(),
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, group, id, *, entries_read=None))]
    fn xgroup_setid<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
        id: &str,
        entries_read: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args: Vec<Vec<u8>> = vec![
            b"SETID".to_vec(),
            to_redis_bytes(key)?,
            group.as_bytes().to_vec(),
            id.as_bytes().to_vec(),
        ];
        if let Some(n) = entries_read {
            args.push(b"ENTRIESREAD".to_vec());
            args.push(n.to_string().into_bytes());
        }
        slf.commands.push(("XGROUP".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn xgroup_delconsumer<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
        consumer: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "XGROUP".to_string(),
            vec![
                b"DELCONSUMER".to_vec(),
                to_redis_bytes(key)?,
                group.as_bytes().to_vec(),
                consumer.as_bytes().to_vec(),
            ],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, group, *ids))]
    fn xack<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
        ids: Vec<String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?, group.as_bytes().to_vec()];
        for id in ids {
            args.push(id.into_bytes());
        }
        slf.commands.push(("XACK".to_string(), args));
        slf.parsers.push(None);
        Ok(slf)
    }

    fn xpending<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.commands.push((
            "XPENDING".to_string(),
            vec![to_redis_bytes(key)?, group.as_bytes().to_vec()],
        ));
        slf.parsers.push(None);
        Ok(slf)
    }

    #[pyo3(signature = (key, group, *, min, max, count, consumername=None, idle=None))]
    #[allow(clippy::too_many_arguments)]
    fn xpending_range<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
        min: &str,
        max: &str,
        count: i64,
        consumername: Option<&str>,
        idle: Option<i64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![to_redis_bytes(key)?, group.as_bytes().to_vec()];
        if let Some(n) = idle {
            args.push(b"IDLE".to_vec());
            args.push(n.to_string().into_bytes());
        }
        args.push(min.as_bytes().to_vec());
        args.push(max.as_bytes().to_vec());
        args.push(count.to_string().into_bytes());
        if let Some(c) = consumername {
            args.push(c.as_bytes().to_vec());
        }
        let py = slf.py();
        slf.commands.push(("XPENDING".to_string(), args));
        slf.parsers.push(Some(parser(py, "xpending_range")?));
        Ok(slf)
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, message_ids, idle=None, time=None, retrycount=None, force=false, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn xclaim<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        message_ids: Vec<String>,
        idle: Option<i64>,
        time: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![
            to_redis_bytes(key)?,
            group.as_bytes().to_vec(),
            consumer.as_bytes().to_vec(),
            min_idle_time.to_string().into_bytes(),
        ];
        for id in message_ids {
            args.push(id.into_bytes());
        }
        if let Some(n) = idle {
            args.push(b"IDLE".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if let Some(n) = time {
            args.push(b"TIME".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if let Some(n) = retrycount {
            args.push(b"RETRYCOUNT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if force {
            args.push(b"FORCE".to_vec());
        }
        if justid {
            args.push(b"JUSTID".to_vec());
        }
        let py = slf.py();
        slf.commands.push(("XCLAIM".to_string(), args));
        slf.parsers.push(if justid {
            None
        } else {
            Some(parser(py, "stream_entries")?)
        });
        Ok(slf)
    }

    #[pyo3(signature = (key, group, consumer, min_idle_time, start_id="0-0", count=None, justid=false))]
    #[allow(clippy::too_many_arguments)]
    fn xautoclaim<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: &Bound<'py, PyAny>,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        start_id: &str,
        count: Option<i64>,
        justid: bool,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut args = vec![
            to_redis_bytes(key)?,
            group.as_bytes().to_vec(),
            consumer.as_bytes().to_vec(),
            min_idle_time.to_string().into_bytes(),
            start_id.as_bytes().to_vec(),
        ];
        if let Some(n) = count {
            args.push(b"COUNT".to_vec());
            args.push(n.to_string().into_bytes());
        }
        if justid {
            args.push(b"JUSTID".to_vec());
        }
        let py = slf.py();
        slf.commands.push(("XAUTOCLAIM".to_string(), args));
        slf.parsers.push(if justid {
            None
        } else {
            Some(parser(py, "xautoclaim")?)
        });
        Ok(slf)
    }
}

/// Apply per-command parsers to a raw response list. Used by both the
/// sync ``execute`` path and the async ``apipeline_execute`` helper.
fn apply_parsers<'py>(
    py: Python<'py>,
    raw: &Bound<'py, PyAny>,
    parsers: &[Option<Py<PyAny>>],
) -> PyResult<Py<PyAny>> {
    let out = pyo3::types::PyList::empty(py);
    let mut iter = raw.try_iter()?;
    for parser in parsers {
        let item = match iter.next() {
            Some(v) => v?,
            None => break,
        };
        match parser {
            Some(p) => out.append(p.bind(py).call1((item,))?)?,
            None => out.append(item)?,
        }
    }
    Ok(out.into_any().unbind())
}

fn push_ttl_flags(args: &mut Vec<Vec<u8>>, nx: bool, xx: bool, gt: bool, lt: bool) {
    if nx {
        args.push(b"NX".to_vec());
    }
    if xx {
        args.push(b"XX".to_vec());
    }
    if gt {
        args.push(b"GT".to_vec());
    }
    if lt {
        args.push(b"LT".to_vec());
    }
}

#[pyclass(extends = RedisRsPipelineAdapter, subclass, module = "django_cachex.adapters._redis_rs")]
pub struct RedisRsAsyncPipelineAdapter;

#[pymethods]
impl RedisRsAsyncPipelineAdapter {
    #[new]
    #[pyo3(signature = (adapter, *, transaction = true))]
    fn new(
        adapter: Py<crate::adapter::RedisRsAdapter>,
        transaction: bool,
    ) -> (Self, RedisRsPipelineAdapter) {
        (Self, RedisRsPipelineAdapter::new(adapter, transaction))
    }

    /// Async ``execute`` returns a coroutine that awaits the adapter's
    /// ``apipeline_exec`` and applies parsers post-await.
    fn execute(slf: PyRefMut<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let mut parent: PyRefMut<'_, RedisRsPipelineAdapter> = slf.into_super();
        if parent.commands.is_empty() {
            parent.parsers.clear();
            return Ok(py
                .import("django_cachex.adapters._async_helpers")?
                .getattr("constant")?
                .call1((pyo3::types::PyList::empty(py),))?
                .unbind());
        }
        let commands = std::mem::take(&mut parent.commands);
        let parsers = std::mem::take(&mut parent.parsers);
        let transaction = parent.transaction;
        let adapter = parent.adapter.clone_ref(py);
        drop(parent);
        let kwargs = PyDict::new(py);
        kwargs.set_item("transaction", transaction)?;
        let aw = adapter
            .bind(py)
            .call_method("apipeline_exec", (commands,), Some(&kwargs))?;
        // Build a Python list of parsers as ``Py<PyAny>`` for the helper.
        let parser_list = pyo3::types::PyList::empty(py);
        for p in parsers.iter() {
            match p {
                Some(p) => parser_list.append(p.clone_ref(py))?,
                None => parser_list.append(py.None())?,
            }
        }
        Ok(py
            .import("django_cachex.adapters._pipeline_parsers")?
            .getattr("apipeline_execute")?
            .call1((aw, parser_list))?
            .unbind())
    }

    /// ``RespAsyncPipelineProtocol.reset`` is awaitable; clear synchronously
    /// and return a no-op coroutine.
    fn reset(slf: PyRefMut<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let mut parent: PyRefMut<'_, RedisRsPipelineAdapter> = slf.into_super();
        parent.reset();
        drop(parent);
        Ok(py
            .import("django_cachex.adapters._async_helpers")?
            .getattr("constant")?
            .call1((py.None(),))?
            .unbind())
    }
}

// Silence unused-import warnings — PyTuple is used via cast paths in extension.
const _: fn() = || {
    let _ = std::marker::PhantomData::<PyTuple>;
};
