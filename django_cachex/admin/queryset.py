"""CacheQuerySet and admin support for the cache list view.

Provides a fake queryset-like object that satisfies Django's ChangeList
and Paginator interfaces, backed by settings.CACHES instead of a database.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from django.contrib import admin, messages
from django.urls import reverse
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _

from django_cachex.admin.helpers import get_cache
from django_cachex.admin.models import Cache

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django.http import HttpRequest, HttpResponse


class _FakeQuery:
    """Minimal query-like object for ChangeList compatibility.

    ChangeList accesses queryset.query.select_related and queryset.query.order_by
    during filter and ordering resolution.
    """

    select_related = False
    order_by: tuple[str, ...] = ()


class CacheQuerySet:
    """In-memory queryset-like object backed by settings.CACHES.

    Implements the minimum interface required by Django's ChangeList and
    Paginator: iteration, slicing, counting, cloning, filtering, and ordering.
    """

    model = Cache
    ordered = True
    db = "default"

    def __init__(self, data: list[Cache] | None = None):
        if data is None:
            self._data = Cache.get_all()
        else:
            self._data = list(data)
        self.query = _FakeQuery()

    def _clone(self) -> CacheQuerySet:
        return CacheQuerySet(self._data)

    def count(self) -> int:
        return len(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator[Cache]:
        return iter(self._data)

    def __bool__(self) -> bool:
        return bool(self._data)

    def __getitem__(self, key: int | slice) -> CacheQuerySet | Cache:
        if isinstance(key, slice):
            return CacheQuerySet(self._data[key])
        return self._data[key]

    def filter(self, *args: Any, **kwargs: Any) -> CacheQuerySet:
        """Filter caches. Supports pk__in for admin action selection."""
        clone = self._clone()
        if "pk__in" in kwargs:
            names = set(kwargs["pk__in"])
            clone._data = [c for c in clone._data if c.pk in names]
        return clone

    def order_by(self, *fields: str) -> CacheQuerySet:
        """Sort by name/pk. Unknown fields are ignored."""
        clone = self._clone()
        for field in fields:
            bare = field.lstrip("-")
            if bare in ("name", "pk"):
                clone._data.sort(key=lambda c: c.name, reverse=field.startswith("-"))
                break
        return clone

    def select_related(self, *args: Any) -> CacheQuerySet:
        return self._clone()

    def distinct(self) -> CacheQuerySet:
        return self._clone()

    def alias(self, **kwargs: Any) -> CacheQuerySet:
        return self._clone()


class SupportLevelFilter(admin.SimpleListFilter):
    """Filter caches by support level (cachex / wrapped / limited)."""

    title = _("support level")
    parameter_name = "support"

    def lookups(
        self,
        request: HttpRequest,
        model_admin: admin.ModelAdmin,
    ) -> list[tuple[str, str]]:
        return [
            ("cachex", "cachex"),
            ("wrapped", "wrapped"),
            ("limited", "limited"),
        ]

    def queryset(self, request: HttpRequest, queryset: CacheQuerySet) -> CacheQuerySet:  # type: ignore[override]
        value = self.value()
        if value:
            return CacheQuerySet([c for c in queryset if c.support_level == value])
        return queryset

    def get_facet_queryset(self, changelist: Any) -> dict[str, int]:
        """Compute facet counts in-memory (CacheQuerySet has no .aggregate)."""
        filtered_qs = changelist.get_queryset(
            self.request,
            exclude_parameters=self.expected_parameters(),
        )
        return {
            f"{i}__c": sum(1 for c in filtered_qs if c.support_level == value)
            for i, (value, _) in enumerate(self.lookup_choices)
        }


class CacheAdminMixin:
    """Shared cache list admin behaviour for default and unfold themes.

    Provides list_display, filtering, search, the flush action, and help
    button handling.  Designed to be used as a mixin before the concrete
    ModelAdmin base class.
    """

    list_display: ClassVar[Any] = [
        "name",
        "backend_display",
        "location_display",
        "support_display",
        "keys_link",
    ]
    list_display_links: ClassVar[Any] = ["name"]
    list_filter: ClassVar[Any] = [SupportLevelFilter]
    search_fields: ClassVar[Any] = ["name"]
    ordering: ClassVar[Any] = ["name"]
    actions: ClassVar[Any] = ["flush_selected"]
    list_per_page: ClassVar[int] = 100

    # ------------------------------------------------------------------
    # QuerySet / search
    # ------------------------------------------------------------------

    def get_queryset(self, request: HttpRequest) -> CacheQuerySet:
        return CacheQuerySet()

    def get_search_results(
        self,
        request: HttpRequest,
        queryset: CacheQuerySet,
        search_term: str,
    ) -> tuple[CacheQuerySet, bool]:
        if not search_term:
            return queryset, False
        term = search_term.lower()
        filtered = [
            c for c in queryset if term in c.name.lower() or term in c.backend.lower() or term in c.location.lower()
        ]
        return CacheQuerySet(filtered), False

    # ------------------------------------------------------------------
    # changelist_view - help-button handling
    # ------------------------------------------------------------------

    def changelist_view(
        self,
        request: HttpRequest,
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        extra_context = extra_context or {}
        if request.GET.get("help"):
            help_messages = getattr(self, "_cachex_help_messages", {})
            help_text = help_messages.get("cache_list", "")
            if help_text:
                messages.info(request, help_text)
            # Strip 'help' so ChangeList doesn't treat it as a field lookup.
            request.GET = request.GET.copy()  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
            del request.GET["help"]  # type: ignore[misc]
            extra_context["help_active"] = True
        return super().changelist_view(request, extra_context)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    # ------------------------------------------------------------------
    # Admin action
    # ------------------------------------------------------------------

    @admin.action(description=_("Flush selected caches"), permissions=["change"])
    def flush_selected(self, request: HttpRequest, queryset: CacheQuerySet) -> None:
        flushed = 0
        for cache_obj in queryset:
            try:
                cache = get_cache(cache_obj.name)
                cache.clear()
                flushed += 1
            except Exception as exc:  # noqa: BLE001
                messages.error(request, f"Error flushing '{cache_obj.name}': {exc}")
        if flushed:
            messages.success(
                request,
                f"Successfully flushed {flushed} cache(s).",
            )

    # ------------------------------------------------------------------
    # Display columns
    # ------------------------------------------------------------------

    @admin.display(description=_("Backend"))
    def backend_display(self, obj: Cache) -> str:
        try:
            get_cache(obj.name)
            return format_html("<code>{}</code>", obj.backend)
        except Exception as exc:  # noqa: BLE001
            return format_html(
                '<code>{}</code><br><span style="color:#dc2626;font-size:.75rem">{}</span>',
                obj.backend,
                str(exc),
            )

    @admin.display(description=_("Location"))
    def location_display(self, obj: Cache) -> str:
        loc = obj.location
        if loc:
            return format_html("<code>{}</code>", loc)
        return "-"

    @admin.display(description=_("Support"))
    def support_display(self, obj: Cache) -> str:
        level = obj.support_level
        styles: dict[str, tuple[str, str]] = {
            "cachex": (
                "background:#dcfce7;color:#15803d;",
                "Full support \u2014 django-cachex backend",
            ),
            "wrapped": (
                "background:#dbeafe;color:#1d4ed8;",
                "Wrapped support \u2014 Django builtin backend",
            ),
            "limited": (
                "background:#f3f4f6;color:#374151;",
                "Limited support \u2014 custom backend",
            ),
        }
        style, title = styles.get(level, styles["limited"])
        return format_html(
            '<span style="{}padding:2px 8px;border-radius:4px;'
            'font-size:11px;font-weight:600;text-transform:uppercase" '
            'title="{}">{}</span>',
            style,
            title,
            level,
        )

    @admin.display(description=_("Actions"))
    def keys_link(self, obj: Cache) -> str:
        url = reverse("admin:django_cachex_key_changelist") + f"?cache={obj.name}"
        return format_html('<a href="{}">{}</a>', url, _("List Keys"))
