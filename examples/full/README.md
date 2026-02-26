# Full Example

Comprehensive example demonstrating django-cachex cache admin with multiple cache backends.

## Cache Backends

| Name | Backend | Description |
|------|---------|-------------|
| `default` | ValkeyCache | Primary cache using Valkey |
| `redis` | RedisCache | Secondary cache using Redis |
| `celery` | RedisCache | Celery broker & results (Redis db 1) |
| `locmem` | LocMemCache | Local memory cache |
| `database` | DatabaseCache | Database-backed cache |
| `file` | FileBasedCache | File-based cache |
| `dummy` | DummyCache | No-op cache |

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
- **redis** (port 6380): Redis 7 server (db 0 = cache, db 1 = Celery)
