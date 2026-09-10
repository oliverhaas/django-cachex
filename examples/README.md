# Example Projects

Example Django projects demonstrating django-cachex cache admin.

## Examples

| Example | Description |
|---------|-------------|
| [simple](simple/) | Minimal setup with a single Valkey instance plus locmem |
| [full](full/) | Standalone, cluster, sentinel, stream-synced, the cachex local drop-ins, and two stock Django backends |

## Quick Start

Each example has its own `run.sh` script:

```bash
cd examples/simple  # or full
./run.sh setup      # Start containers, run migrations, create admin
./run.sh server     # Start Django server
./run.sh test-data  # Add sample cache entries
```

Then visit: http://127.0.0.1:8000/admin/django_cachex/cache/

Login: `admin` / `password`

## Full Example Details

The `full` example wires up eleven cache aliases:

**Standalone:**
- Valkey (port 6381)
- Redis (port 6380; db 0 = cache, db 1 = Celery, db 2 = StreamCache transport)

**Cluster:**
- Redis Cluster with 6 nodes (ports 7001-7006)

**Sentinel:**
- Redis Sentinel with 3 sentinels (ports 26379-26381) in front of a master (6390) and two replicas (6391, 6392)

**Stream-synced:**
- `StreamCache`, a local cache kept in sync over a Redis Stream

**Local drop-ins (cachex):**
- `LocMemCache`, `DatabaseCache`

**Stock Django backends:**
- `FileBasedCache`, `DummyCache`

`TrackingCache` and `ValkeyGlideCache` are not part of this example.

The compose file defines 15 services; the cluster initializer exits once the
cluster is formed, so 14 keep running. Run `./run.sh status` to check them.

## Requirements

- Docker (for Valkey/Redis containers)
- Python 3.14+ with venv at `../../.venv`
- django-cachex installed (editable install from repo root)
