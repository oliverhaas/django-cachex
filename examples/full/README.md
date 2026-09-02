# Full Example

Example demonstrating django-cachex cache admin with multiple cache backends.

## Cache Backends

| Alias | Backend | Description |
|-------|---------|-------------|
| `default` | `ValkeyCache` | Valkey standalone (port 6381) |
| `redis` | `RedisCache` | Redis standalone (port 6380, db 0) |
| `celery` | `RedisCache` | Celery broker and results (Redis db 1), raw keys via pass-through key functions |
| `cluster` | `RedisClusterCache` | Redis Cluster, 6 nodes (ports 7001-7006) |
| `sentinel` | `RedisSentinelCache` | Redis Sentinel, 3 sentinels (ports 26379-26381) |
| `sync` | `StreamCache` | Local cache synchronized over a Redis Stream |
| `stream_transport` | `RedisCache` | Stream transport for the `sync` cache (Redis db 2) |
| `locmem` | `LocMemCache` | Local memory cache (cachex drop-in) |
| `database` | `DatabaseCache` | Database-backed cache (cachex drop-in) |
| `file` | Django `FileBasedCache` | File-based cache |
| `dummy` | Django `DummyCache` | No-op cache |

The cluster and sentinel aliases use the redis-py backends because the
valkey-py equivalents hit an upstream bug. See `full/settings.py`.

## Quick Start

```bash
# Setup (starts Docker containers, runs migrations, creates admin user)
./run.sh setup

# Start the server
./run.sh server

# Add sample data to all caches
./run.sh test-data
```

Then visit: http://127.0.0.1:8000/admin/django_cachex/cache/

Login: `admin` / `password`

## Commands

- `./run.sh setup` - Start containers, install Celery, run migrations, create admin
- `./run.sh server` - Start Django development server
- `./run.sh test-data` - Add sample cache entries to all backends
- `./run.sh worker` - Start Celery worker (processes tasks from the queue)
- `./run.sh send-tasks` - Send sample Celery tasks
- `./run.sh shell` - Open Django shell
- `./run.sh stop` - Stop Docker containers
- `./run.sh clean` - Stop containers and remove all data
- `./run.sh logs` - Show container logs (optionally for one container)
- `./run.sh status` - Show container, cluster and sentinel status

## Feature Demos

Runnable HTTP endpoints that exercise specific cachex features. Hit them with
curl after `./run.sh server`:

```bash
curl http://127.0.0.1:8000/demo/pipeline/    # sync pipeline batch
curl http://127.0.0.1:8000/demo/async/       # async aget/aset
curl http://127.0.0.1:8000/demo/apipeline/   # async pipeline batch
curl 'http://127.0.0.1:8000/demo/lua/?by=5'  # eval_script atomic increment
```

Source: [`full/demo.py`](full/demo.py).

## Celery Integration

This example includes [Celery](https://docs.celeryq.dev/) with Redis as broker
and result backend, demonstrating how Celery's Redis keys appear in the admin.

### Celery Keys in the Admin

Select the `celery` cache in the admin to see:

| Key Pattern | Redis Type | Description |
|---|---|---|
| `celery` | list | Default task queue (pending messages) |
| `_kombu.binding.*` | set | Queue/exchange binding metadata |
| `celery-task-meta-*` | string | Task results (after worker processes them) |

### Usage

```bash
# Terminal 1: start Django
./run.sh server

# Terminal 2: send tasks (they pile up in the queue)
./run.sh send-tasks

# Visit admin -> "celery" cache to see the queue list

# Terminal 2: start worker (drains queue, creates result keys)
./run.sh worker
```

## Docker Services

- **valkey** (port 6381): Valkey 8 server
- **redis** (port 6380): Redis 7 server (db 0 = cache, db 1 = Celery, db 2 = StreamCache transport)
- **redis-cluster-1 to redis-cluster-6** (ports 7001-7006): Redis 7 cluster nodes, 3 masters and 3 replicas, formed by the one-shot `redis-cluster-init` service
- **redis-master** (port 6390), **redis-replica-1** (6391), **redis-replica-2** (6392): Redis 7 replication group
- **sentinel-1 to sentinel-3** (ports 26379-26381): Redis Sentinel quorum monitoring `mymaster`
