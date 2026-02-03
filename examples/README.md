# Example Projects

Example Django projects demonstrating django-cachex admin panel.

## Examples

| Example | Description |
|---------|-------------|
| [simple](simple/) | Minimal setup with single Valkey instance + locmem |
| [unfold](unfold/) | Same as simple with django-unfold admin theme |
| [full](full/) | **All backends**: standalone, cluster, sentinel, Django builtins |

## Quick Start

Each example has its own `run.sh` script:

```bash
cd examples/simple  # or full, unfold
./run.sh setup      # Start containers, run migrations, create admin
./run.sh server     # Start Django server
./run.sh test-data  # Add sample cache entries
```

Then visit: http://127.0.0.1:8000/admin/django_cachex/cache/

Login: `admin` / `password`

## Full Example Details

The `full` example demonstrates all supported cache backends:

**Standalone:**
- Valkey (port 6379)
- Redis (port 6380)

**Cluster:**
- Redis Cluster with 6 nodes (ports 7001-7006)

**Sentinel:**
- Redis Sentinel with 3 sentinels + master/replica setup

**Django Builtins:**
- LocMemCache, DatabaseCache, FileBasedCache, DummyCache

Note: The full example runs 14 Docker containers. Use `./run.sh status` to check their state.

## Requirements

- Docker (for Valkey/Redis containers)
- Python 3.12+ with venv at `../../.venv`
- django-cachex installed (editable install from repo root)
