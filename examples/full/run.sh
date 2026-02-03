#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"
VENV="../../.venv/bin/activate"

case "${1:-}" in
    setup)
        echo "Starting all containers (this may take a moment)..."
        docker compose up -d 2>/dev/null || echo "Containers may already be running"

        echo "Waiting for cluster initialization (10s)..."
        sleep 10

        echo "Running migrations..."
        source "$VENV" && python manage.py migrate --verbosity=0

        echo "Creating cache table for database backend..."
        source "$VENV" && python manage.py createcachetable 2>/dev/null || true

        echo "Creating admin user (admin/password)..."
        source "$VENV" && DJANGO_SUPERUSER_PASSWORD=password python manage.py createsuperuser \
            --username=admin --email=admin@example.com --noinput 2>/dev/null || true

        echo ""
        echo "Setup complete! Run './run.sh server' to start."
        echo ""
        echo "Containers running:"
        echo "  - Valkey standalone    (6381)"
        echo "  - Redis standalone     (6380)"
        echo "  - Redis Cluster        (7001-7006)"
        echo "  - Redis Sentinel       (26379-26381, master: 6390)"
        ;;

    server|run)
        echo "Starting Django at http://127.0.0.1:8000/admin/django_cachex/cache/"
        echo "Login: admin / password"
        echo ""
        echo "Configured caches:"
        echo "  - default   : Valkey standalone"
        echo "  - redis     : Redis standalone"
        echo "  - cluster   : Redis Cluster (6 nodes)"
        echo "  - sentinel  : Redis Sentinel (3 sentinels)"
        echo "  - locmem    : Django LocMemCache"
        echo "  - database  : Django DatabaseCache"
        echo "  - file      : Django FileBasedCache"
        echo "  - dummy     : Django DummyCache"
        echo ""
        source "$VENV" && python manage.py runserver
        ;;

    test-data)
        source "$VENV" && python manage.py shell -c "
from django.core.cache import caches

# Add data to Valkey (default)
print('Adding data to Valkey standalone...')
valkey = caches['default']
valkey.set('greeting', 'Hello from Valkey!', timeout=3600)
valkey.set('user:1', {'name': 'Alice', 'email': 'alice@example.com'}, timeout=7200)
valkey.rpush('mylist', 'a', 'b', 'c')
valkey.sadd('myset', 'x', 'y', 'z')
valkey.hset('myhash', 'field1', 'value1')
valkey.zadd('myzset', {'one': 1.0, 'two': 2.0, 'three': 3.0})

# Add data to Redis standalone
print('Adding data to Redis standalone...')
redis = caches['redis']
redis.set('greeting', 'Hello from Redis!', timeout=3600)
redis.set('counter', 100)
redis.rpush('queue', 'job1', 'job2', 'job3')

# Add data to Redis Cluster
print('Adding data to Redis Cluster...')
try:
    cluster = caches['cluster']
    cluster.set('cluster:key1', 'Clustered value 1', timeout=3600)
    cluster.set('cluster:key2', 'Clustered value 2', timeout=3600)
    cluster.set('cluster:key3', {'type': 'cluster', 'node': 'auto'}, timeout=3600)
except Exception as e:
    print(f'  Cluster error (may not be initialized): {e}')

# Add data to Redis Sentinel
print('Adding data to Redis Sentinel...')
try:
    sentinel = caches['sentinel']
    sentinel.set('sentinel:key1', 'HA value 1', timeout=3600)
    sentinel.set('sentinel:key2', 'HA value 2', timeout=3600)
    sentinel.set('sentinel:config', {'failover': 'automatic'}, timeout=3600)
except Exception as e:
    print(f'  Sentinel error: {e}')

# Add data to locmem
print('Adding data to LocMem...')
locmem = caches['locmem']
locmem.set('session:123', {'user_id': 1, 'active': True}, timeout=1800)

# Add data to database cache
print('Adding data to Database cache...')
db = caches['database']
db.set('config:version', '1.0.0', timeout=None)

# Add data to file cache
print('Adding data to File cache...')
file = caches['file']
file.set('report:daily', {'generated': '2024-01-01'}, timeout=86400)

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

    logs)
        docker compose logs -f "${2:-}"
        ;;

    status)
        echo "Container status:"
        docker compose ps
        echo ""
        echo "Cluster info (if running):"
        docker compose exec -T redis-cluster-1 redis-cli cluster info 2>/dev/null || echo "  Cluster not ready"
        echo ""
        echo "Sentinel info (if running):"
        docker compose exec -T sentinel-1 redis-cli -p 26379 sentinel master mymaster 2>/dev/null | head -10 || echo "  Sentinel not ready"
        ;;

    *)
        echo "Usage: ./run.sh <command>"
        echo ""
        echo "Commands:"
        echo "  setup      Start all containers, run migrations, create admin user"
        echo "  server     Start the Django development server"
        echo "  test-data  Add sample cache entries to all backends"
        echo "  shell      Open Django shell"
        echo "  stop       Stop all containers"
        echo "  clean      Stop containers and remove all data"
        echo "  logs       Show container logs (optional: container name)"
        echo "  status     Show container and cluster/sentinel status"
        ;;
esac
