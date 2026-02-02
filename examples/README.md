# Example Projects

Example Django projects demonstrating django-cachex admin panel.

## Examples

| Example | Description |
|---------|-------------|
| [simple](simple/) | Minimal setup with single Valkey instance |
| [full](full/) | Multiple cache backends (Valkey, Redis, LocMem, Database, File) |
| [unfold](unfold/) | django-unfold admin theme integration |

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

## Requirements

- Docker (for Valkey/Redis containers)
- Python 3.12+ with venv at `../../.venv`
- django-cachex installed (editable install from repo root)
