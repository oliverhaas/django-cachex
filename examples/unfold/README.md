# Unfold Example

Example demonstrating django-cachex cache admin with [django-unfold](https://unfoldadmin.com/) theme.

## About django-unfold

django-unfold is a modern, responsive Django admin theme with:
- Tailwind CSS styling
- Dark mode support
- Improved navigation and UI components

## Quick Start

```bash
# Setup (installs unfold, starts Valkey, runs migrations)
./run.sh setup

# Start the server
./run.sh server

# Add sample data
./run.sh test-data
```

Then visit: http://127.0.0.1:8000/admin/django_cachex/cache/

Login: `admin` / `password`

## Status

**Work in Progress**: This example demonstrates the current state of django-unfold compatibility.
Full integration with unfold's template system and styling is still being developed.

See `TODO.md` Phase 8 for the full django-unfold compatibility roadmap.

## Commands

- `./run.sh setup` - Start Valkey, install unfold, run migrations, create admin
- `./run.sh server` - Start Django development server
- `./run.sh test-data` - Add sample cache entries
- `./run.sh shell` - Open Django shell
- `./run.sh stop` - Stop Valkey container
- `./run.sh clean` - Stop container and remove database
