"""Management command invalidating the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from typing import TYPE_CHECKING, Any

from django.apps import apps
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError, CommandParser

from django_cachex.orm.api import invalidate

if TYPE_CHECKING:
    from django.db.models import Model


def _models(label: str) -> list[type[Model]]:
    # A label with a dot names a model; app labels are identifiers. An app
    # includes its auto-created many-to-many tables.
    if "." in label:
        return [apps.get_model(label)]
    return list(apps.get_app_config(label).get_models(include_auto_created=True))


class Command(BaseCommand):
    help = "Invalidates the queries cached by the ORM cache."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("app_label[.model_name]", nargs="*")
        parser.add_argument(
            "-c",
            "--cache",
            action="store",
            dest="cache_alias",
            choices=list(settings.CACHES.keys()),
            help="Cache alias from the CACHES setting.",
        )
        parser.add_argument(
            "-d",
            "--db",
            action="store",
            dest="db_alias",
            choices=list(settings.DATABASES.keys()),
            help="Database alias from the DATABASES setting.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        cache_alias = options["cache_alias"]
        db_alias = options["db_alias"]
        verbosity = int(options["verbosity"])
        labels = options["app_label[.model_name]"]

        models: list[type[Model]] = []
        for label in labels:
            try:
                models.extend(_models(label))
            except (LookupError, ValueError) as e:
                raise CommandError(str(e)) from e
        models = list(dict.fromkeys(models))
        if labels and not models:
            # invalidate() without tables would invalidate every table.
            if verbosity > 0:
                self.stdout.write("No models to invalidate.")
            return

        target = f"{len(models)} model{'' if len(models) == 1 else 's'}" if labels else "all tables"
        cache_str = "" if cache_alias is None else f"on cache '{cache_alias}'"
        db_str = "" if db_alias is None else f"for database '{db_alias}'"
        if verbosity > 0:
            self.stdout.write(" ".join(filter(bool, ["Invalidating", target, cache_str, db_str])) + "...")

        invalidate(*models, cache_alias=cache_alias, db_alias=db_alias)
        if verbosity > 0:
            self.stdout.write("ORM cache invalidated.")
