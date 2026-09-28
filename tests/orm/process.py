"""Run an ORM operation in a fresh interpreter: ``python -m tests.orm.process create <name>``."""

import os
import sys

import django


def main(argv: list[str]) -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.orm.settings")
    from django.conf import settings

    # The test database the parent process created, not the one the settings name.
    settings.DATABASES["default"]["NAME"] = os.environ["CACHEX_ORM_TEST_DB_NAME"]
    django.setup()

    from tests.orm.app.models import Test

    command, *args = argv
    if command == "create":
        (name,) = args
        sys.stdout.write(f"{Test.objects.create(name=name).pk}\n")
    elif command == "first":
        first = Test.objects.first()
        sys.stdout.write(f"{first.pk if first is not None else ''}\n")
    else:
        msg = f"unknown command {command!r}"
        raise SystemExit(msg)


if __name__ == "__main__":
    main(sys.argv[1:])
