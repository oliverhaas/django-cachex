# Example Django Project

Minimal Django project for testing django-cachex and the cache panel.

## Requirements

- Docker (for Valkey)
- Python virtual environment (from repo root)

## Quick Start

```bash
cd example

./run.sh setup    # Start Valkey, migrate, create admin user
./run.sh server   # Start the dev server
```

Open http://127.0.0.1:8000/admin/ and click "Caches" in the sidebar

Login: `admin` / `admin`

## Commands

```bash
./run.sh setup      # Start Valkey, run migrations, create admin user
./run.sh server     # Start Django dev server
./run.sh test-data  # Add sample cache entries
./run.sh shell      # Open Django shell
./run.sh stop       # Stop Valkey container
./run.sh clean      # Stop Valkey and remove database
```

## Manual Setup

```bash
# Start Valkey
docker compose up -d

# Activate venv and run Django
source ../.venv/bin/activate
python manage.py migrate
python manage.py createsuperuser
python manage.py runserver
```
