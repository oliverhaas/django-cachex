# django-cachex-redis-rs

Rust I/O driver for [django-cachex] — built on PyO3 + tokio + [redis-rs].

This is a binary-only companion package that ships the compiled `_driver`
extension module into the `django_cachex` namespace. Install via the
`redis-rs` extra on the main package:

```console
pip install django-cachex[redis-rs]
```

Prebuilt wheels are published for Linux x86_64 (cp314, cp314t). On other
platforms there's no wheel — pip will try to build from source (requires
the Rust toolchain) or fail clearly.

The pure-Python `django-cachex` package is fully usable on its own; this
binary just unlocks the `RedisRsCache` family of
backends.

[django-cachex]: https://github.com/oliverhaas/django-cachex
[redis-rs]: https://github.com/redis-rs/redis-rs
