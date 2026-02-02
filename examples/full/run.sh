#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"
VENV="../../.venv/bin/activate"

case "${1:-}" in
    setup)
        echo "Starting Valkey and Redis..."
        docker compose up -d 2>/dev/null || echo "Containers may already be running"
        sleep 2

        echo "Running migrations..."
        source "$VENV" && python manage.py migrate --verbosity=0

        echo "Creating cache table for database backend..."
        source "$VENV" && python manage.py createcachetable 2>/dev/null || true

        echo "Creating admin user (admin/password)..."
        source "$VENV" && DJANGO_SUPERUSER_PASSWORD=password python manage.py createsuperuser \
            --username=admin --email=admin@example.com --noinput 2>/dev/null || true

        echo ""
        echo "Setup complete! Run './run.sh server' to start."
        ;;

    server|run)
        echo "Starting Django at http://127.0.0.1:8000/admin/django_cachex/cache/"
        echo "Login: admin / password"
        echo ""
        echo "Configured caches: default (Valkey), redis, locmem, database, file, dummy"
        echo ""
        source "$VENV" && python manage.py runserver
        ;;

    test-data)
        source "$VENV" && python manage.py shell -c "
from django.core.cache import caches

# Add data to Valkey (default)
valkey = caches['default']
valkey.set('greeting', 'Hello from Valkey!', timeout=3600)
valkey.set('user:1', {'name': 'Alice', 'email': 'alice@example.com'}, timeout=7200)
valkey.rpush('mylist', 'a', 'b', 'c')
valkey.sadd('myset', 'x', 'y', 'z')
valkey.hset('myhash', 'field1', 'value1')
valkey.zadd('myzset', {'one': 1.0, 'two': 2.0, 'three': 3.0})
print('Added test data to Valkey (default)')

# Add data to Redis
redis = caches['redis']
redis.set('greeting', 'Hello from Redis!', timeout=3600)
redis.set('counter', 100)
print('Added test data to Redis')

# Add data to locmem
locmem = caches['locmem']
locmem.set('session:123', {'user_id': 1, 'active': True}, timeout=1800)
print('Added test data to LocMem')

# Add data to database cache
db = caches['database']
db.set('config:version', '1.0.0', timeout=None)
print('Added test data to Database cache')

# Add data to file cache
file = caches['file']
file.set('report:daily', {'generated': '2024-01-01'}, timeout=86400)
print('Added test data to File cache')

print('')
print('Done! Visit the admin to see all caches.')
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
        rm -rf cache_files
        echo "Cleaned up."
        ;;

    *)
        echo "Usage: ./run.sh <command>"
        echo ""
        echo "Commands:"
        echo "  setup      Start containers, run migrations, create admin user"
        echo "  server     Start the Django development server"
        echo "  test-data  Add sample cache entries to all backends"
        echo "  shell      Open Django shell"
        echo "  stop       Stop containers"
        echo "  clean      Stop containers and remove all data"
        ;;
esac
