# Full Example

Comprehensive example demonstrating django-cachex admin panel with multiple cache backends.

## Cache Backends

| Name | Backend | Description |
|------|---------|-------------|
| `default` | ValkeyCache | Primary cache using Valkey |
| `redis` | RedisCache | Secondary cache using Redis |
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

- `./run.sh setup` - Start containers, run migrations, create admin
- `./run.sh server` - Start Django development server
- `./run.sh test-data` - Add sample cache entries to all backends
- `./run.sh shell` - Open Django shell
- `./run.sh stop` - Stop Docker containers
- `./run.sh clean` - Stop containers and remove all data

## Docker Services

- **valkey** (port 6379): Valkey 8 server
- **redis** (port 6380): Redis 7 server
