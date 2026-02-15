"""QuerySet-like objects and admin mixins for cache and key list views.

Provides fake queryset objects that satisfy Django's ChangeList and Paginator
interfaces, backed by settings.CACHES (for caches) and Redis SCAN (for keys).
"""

from __future__ import annotations

import contextlib
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin import ShowFacets
from django.contrib.admin.utils import unquote
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html
from django.utils.timesince import timeuntil
from django.utils.translation import gettext_lazy as _

from django_cachex.admin.helpers import get_cache, get_size
from django_cachex.admin.models import Cache, Key
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django.http import HttpRequest, HttpResponse

logger = logging.getLogger(__name__)


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


# ======================================================================
# Key list: KeyQuerySet, filters, and KeyAdminMixin
# ======================================================================

KEY_TYPES = tuple(KeyType)

# Inline styles for type badges (theme-agnostic)
_TYPE_STYLES: dict[str, str] = {
    "string": "background:#dbeafe;color:#1d4ed8;",
    "list": "background:#dbeafe;color:#1d4ed8;",
    "set": "background:#dcfce7;color:#15803d;",
    "hash": "background:#ffedd5;color:#c2410c;",
    "zset": "background:#dcfce7;color:#15803d;",
    "stream": "background:#ede9fe;color:#7c3aed;",
}


class KeyQuerySet:
    """In-memory queryset-like object backed by a Redis SCAN batch.

    Pre-populated with Key instances and cursor pagination metadata.
    ChangeList sees a single page (list_per_page >> batch size); cursor-based
    Start/Next navigation is rendered in the template.
    """

    model = Key
    ordered = True
    db = "default"

    def __init__(
        self,
        data: list[Key],
        cache_name: str,
        *,
        next_cursor: int = 0,
        cursor: int = 0,
        scan_count: int = 100,
    ):
        self._data = list(data)
        self.cache_name = cache_name
        self.next_cursor = next_cursor
        self.cursor = cursor
        self.scan_count = scan_count
        self.query = _FakeQuery()

    def _clone(self) -> KeyQuerySet:
        return KeyQuerySet(
            self._data,
            self.cache_name,
            next_cursor=self.next_cursor,
            cursor=self.cursor,
            scan_count=self.scan_count,
        )

    @property
    def has_next(self) -> bool:
        return self.next_cursor != 0

    def count(self) -> int:
        return len(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator[Key]:
        return iter(self._data)

    def __bool__(self) -> bool:
        return bool(self._data)

    def __getitem__(self, key: int | slice) -> KeyQuerySet | Key:
        if isinstance(key, slice):
            clone = self._clone()
            clone._data = self._data[key]
            return clone
        return self._data[key]

    def filter(self, *args: Any, **kwargs: Any) -> KeyQuerySet:
        """Filter keys. pk__in builds Key objects from composite PKs directly."""
        clone = self._clone()
        if "pk__in" in kwargs:
            data = []
            for pk in kwargs["pk__in"]:
                cache_name, key_name = Key.parse_pk(unquote(str(pk)))
                if cache_name:
                    data.append(Key.from_cache_key(cache_name, key_name))
            clone._data = data
        return clone

    def order_by(self, *fields: str) -> KeyQuerySet:
        return self._clone()

    def select_related(self, *args: Any) -> KeyQuerySet:
        return self._clone()

    def distinct(self) -> KeyQuerySet:
        return self._clone()

    def alias(self, **kwargs: Any) -> KeyQuerySet:
        return self._clone()


class CacheFilter(admin.SimpleListFilter):
    """Filter keys by cache backend."""

    title = _("cache")
    parameter_name = "cache"

    def lookups(
        self,
        request: HttpRequest,
        model_admin: admin.ModelAdmin,
    ) -> list[tuple[str, str]]:
        return [(name, name) for name in settings.CACHES]

    def queryset(self, request: HttpRequest, queryset: KeyQuerySet) -> KeyQuerySet:  # type: ignore[override]
        return queryset  # No-op: cache selection handled in get_queryset

    def has_output(self) -> bool:
        return len(settings.CACHES) > 1

    def value(self) -> str:
        """Default to first cache when not specified."""
        val = super().value()
        if val is None:
            return next(iter(settings.CACHES))
        return val

    def choices(self, changelist: Any) -> Iterator[dict[str, Any]]:  # type: ignore[override]
        """Skip 'All' option - always viewing one cache at a time."""
        for lookup, title in self.lookup_choices:
            yield {
                "selected": self.value() == str(lookup),
                "query_string": changelist.get_query_string({self.parameter_name: lookup}),
                "display": title,
            }


class TypeFilter(admin.SimpleListFilter):
    """Filter keys by Redis data type."""

    title = _("type")
    parameter_name = "type"

    def lookups(
        self,
        request: HttpRequest,
        model_admin: admin.ModelAdmin,
    ) -> list[tuple[str, str]]:
        return [(t, t) for t in KeyType]

    def queryset(self, request: HttpRequest, queryset: KeyQuerySet) -> KeyQuerySet:  # type: ignore[override]
        return queryset  # No-op: type filtering handled via SCAN in get_queryset

    def get_facet_queryset(self, changelist: Any) -> dict[str, int]:
        """Compute type counts in-memory from current batch."""
        data = list(changelist.queryset)
        return {
            f"{i}__c": sum(1 for k in data if getattr(k, "key_type", None) == value)
            for i, (value, _) in enumerate(self.lookup_choices)
        }


class KeyAdminMixin:
    """Shared key list admin behaviour for default and unfold themes.

    Provides list_display, filtering, search, the delete action, and cursor-based
    pagination.  Designed to be used as a mixin before the concrete ModelAdmin base.
    """

    list_display: ClassVar[Any] = [
        "key_name",
        "type_display",
        "ttl_display",
        "size_display",
    ]
    list_display_links: ClassVar[Any] = ["key_name"]
    list_filter: ClassVar[Any] = [CacheFilter, TypeFilter]
    search_fields: ClassVar[Any] = ["key_name"]
    actions: ClassVar[Any] = ["delete_selected_keys"]
    list_per_page: ClassVar[int] = 10000
    show_facets = ShowFacets.NEVER
    show_full_result_count: ClassVar[bool] = False

    def get_actions(self, request: HttpRequest) -> dict:
        """Remove Django's built-in delete_selected (KeyQuerySet doesn't support it)."""
        actions = super().get_actions(request)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        actions.pop("delete_selected", None)
        return actions

    # ------------------------------------------------------------------
    # QuerySet / search
    # ------------------------------------------------------------------

    def get_queryset(self, request: HttpRequest) -> KeyQuerySet:
        cache_name = request.GET.get("cache") or next(iter(settings.CACHES))
        search_query = request.GET.get("q", "").strip()
        type_filter = request.GET.get("type", "").strip().lower()
        cursor = getattr(request, "_cachex_cursor", 0)
        count = getattr(request, "_cachex_count", 100)

        if type_filter not in KEY_TYPES:
            type_filter = ""

        # Auto-wrap in wildcards for Django-style contains search
        if search_query:
            if "*" not in search_query and "?" not in search_query:
                pattern = f"*{search_query}*"
            else:
                pattern = search_query
        else:
            pattern = "*"

        cache = get_cache(cache_name)
        try:
            # SCAN's COUNT is a hint — Redis may return fewer matching keys
            # per call.  Loop up to 5 times, but stop early once we have at
            # least half the requested count (avoids showing ~2x items when a
            # late batch pushes us way over).
            max_scans = 5
            keys: list[str] = []
            next_cursor = cursor
            half = count // 2
            scan_kw = {"pattern": pattern, "count": count, "key_type": type_filter or None}
            for _ in range(max_scans):
                next_cursor, batch = cache.scan(cursor=next_cursor, **scan_kw)
                keys.extend(batch)
                if next_cursor == 0 or len(keys) >= half:
                    break

            data: list[Key] = []
            for key_item in keys:
                user_key = key_item["key"] if isinstance(key_item, dict) else key_item
                key_obj = Key.from_cache_key(cache_name, user_key)

                with contextlib.suppress(Exception):
                    ttl = cache.ttl(user_key)
                    key_obj.ttl = ttl  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                    if ttl >= 0:
                        key_obj.ttl_expires_at = timezone.now() + timedelta(seconds=ttl)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                with contextlib.suppress(Exception):
                    key_type = cache.type(user_key)
                    key_obj.key_type = key_type  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                with contextlib.suppress(Exception):
                    key_obj.key_size = get_size(cache, user_key, getattr(key_obj, "key_type", None))  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]

                data.append(key_obj)

            return KeyQuerySet(data, cache_name, next_cursor=next_cursor, cursor=cursor, scan_count=count)

        except Exception:
            logger.exception("Error querying cache '%s'", cache_name)
            return KeyQuerySet([], cache_name)

    def get_search_results(
        self,
        request: HttpRequest,
        queryset: KeyQuerySet,
        search_term: str,
    ) -> tuple[KeyQuerySet, bool]:
        # No-op: search is already applied via SCAN pattern in get_queryset
        return queryset, False

    # ------------------------------------------------------------------
    # changelist_view - cursor/help/cache handling
    # ------------------------------------------------------------------

    def changelist_view(
        self,
        request: HttpRequest,
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        extra_context = extra_context or {}

        # Cache validation
        cache_name = request.GET.get("cache") or next(iter(settings.CACHES))
        if Cache.get_by_name(cache_name) is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(reverse("admin:django_cachex_cache_changelist"))
        extra_context["cache_name"] = cache_name
        extra_context["title"] = f"Keys in '{cache_name}'"

        # Help handling
        if request.GET.get("help"):
            help_messages = getattr(self, "_cachex_help_messages", {})
            help_text = help_messages.get("key_list", "")
            if help_text:
                messages.info(request, help_text)
            extra_context["help_active"] = True

        # Extract cursor/count before ChangeList sees them, store on request
        cursor = int(request.GET.get("cursor", 0))
        count = int(request.GET.get("count", 100))
        request._cachex_cursor = cursor  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
        request._cachex_count = count  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]

        # Strip params ChangeList doesn't understand
        mutable = request.GET.copy()
        for key in ("cursor", "count", "help"):
            mutable.pop(key, None)
        request.GET = mutable  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

        return super().changelist_view(request, extra_context)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    # ------------------------------------------------------------------
    # Admin action
    # ------------------------------------------------------------------

    @admin.action(description=_("Delete selected keys"), permissions=["delete"])
    def delete_selected_keys(self, request: HttpRequest, queryset: KeyQuerySet) -> None:
        deleted = 0
        for key_obj in queryset:
            with contextlib.suppress(Exception):
                cache = get_cache(key_obj.cache_name)
                cache.delete(key_obj.key_name)
                deleted += 1
        if deleted:
            messages.success(request, f"Successfully deleted {deleted} key(s).")

    # ------------------------------------------------------------------
    # Display columns
    # ------------------------------------------------------------------

    @admin.display(description=_("Type"))
    def type_display(self, obj: Key) -> str:
        key_type = getattr(obj, "key_type", None)
        if not key_type:
            return "-"
        style = _TYPE_STYLES.get(str(key_type), "background:#f3f4f6;color:#374151;")
        return format_html(
            '<span style="{}padding:2px 8px;border-radius:4px;'
            'font-size:11px;font-weight:600;text-transform:uppercase">{}</span>',
            style,
            key_type,
        )

    @admin.display(description=_("TTL"))
    def ttl_display(self, obj: Key) -> str:
        expires_at = getattr(obj, "ttl_expires_at", None)
        if expires_at:
            ttl = getattr(obj, "ttl", "")
            return format_html('<code title="{}s">{}</code>', ttl, timeuntil(expires_at))
        return "-"

    @admin.display(description=_("Size"))
    def size_display(self, obj: Key) -> str:
        size = getattr(obj, "key_size", None)
        if size is not None:
            return format_html("<code>{}</code>", size)
        return "-"
