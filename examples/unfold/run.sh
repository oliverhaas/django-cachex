#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"
VENV="../../.venv/bin/activate"

case "${1:-}" in
    setup)
        echo "Starting Valkey..."
        docker compose up -d 2>/dev/null || echo "Valkey may already be running"
        sleep 2

        echo "Checking for django-unfold..."
        source "$VENV" && pip show django-unfold >/dev/null 2>&1 || {
            echo "Installing django-unfold..."
            source "$VENV" && pip install django-unfold
        }

        echo "Running migrations..."
        source "$VENV" && python manage.py migrate --verbosity=0

        echo "Creating admin user (admin/password)..."
        source "$VENV" && DJANGO_SUPERUSER_PASSWORD=password python manage.py createsuperuser \
            --username=admin --email=admin@example.com --noinput 2>/dev/null || true

        echo ""
        echo "Setup complete! Run './run.sh server' to start."
        ;;

    server|run)
        echo "Starting Django with Unfold theme at http://127.0.0.1:8000/admin/django_cachex/cache/"
        echo "Login: admin / password"
        echo ""
        source "$VENV" && python manage.py runserver
        ;;

    test-data)
        source "$VENV" && python manage.py shell -c "
from django.core.cache import caches
cache = caches['default']
cache.set('greeting', 'Hello, World!', timeout=3600)
cache.set('user:1', {'name': 'Alice', 'email': 'alice@example.com'}, timeout=7200)
cache.set('user:2', {'name': 'Bob', 'email': 'bob@example.com'}, timeout=7200)
cache.set('counter', 42)
cache.set('tags', ['python', 'django', 'valkey'])
print('Added 5 test keys to cache')
"
        ;;

    shell)
        source "$VENV" && python manage.py shell
        ;;

    stop)
        docker compose down
        ;;

    clean)
        docker compose down -v 2>/dev/null || true
        rm -f db.sqlite3
        echo "Cleaned up."
        ;;

    *)
        echo "Usage: ./run.sh <command>"
        echo ""
        echo "Commands:"
        echo "  setup      Start Valkey, install unfold, run migrations, create admin user"
        echo "  server     Start the Django development server with Unfold theme"
        echo "  test-data  Add sample cache entries"
        echo "  shell      Open Django shell"
        echo "  stop       Stop Valkey container"
        echo "  clean      Stop Valkey and remove database"
        ;;
esac
