"""Template tags shared by the django-cachex admin pages."""

import django
from django import template
from django.utils.html import format_html, format_html_join
from django.utils.safestring import SafeString, mark_safe

register = template.Library()

# Django 6.1 styles ``ol.breadcrumbs`` and hoists the ``object-tools`` block out
# of ``content``; 6.0 styles ``div.breadcrumbs`` and nests the tools in ``content``.
_ADMIN_61_CHROME = django.VERSION >= (6, 1)


@register.simple_tag
def cachex_breadcrumbs(*crumbs: str) -> SafeString:
    """Render an admin breadcrumb trail as label, url, label, url, ..., label.

    The final argument is the current page and is rendered without a link.
    """
    if len(crumbs) % 2 == 0:
        msg = "cachex_breadcrumbs takes label/url pairs followed by a final label."
        raise template.TemplateSyntaxError(msg)
    links = list(zip(crumbs[:-1:2], crumbs[1::2], strict=True))
    current = crumbs[-1]

    if _ADMIN_61_CHROME:
        items = format_html_join("\n", '<li><a href="{}">{}</a></li>', ((url, label) for label, url in links))
        return format_html(
            '<ol class="breadcrumbs">\n{}\n<li aria-current="page">{}</li>\n</ol>',
            items,
            current,
        )

    items = format_html_join(
        mark_safe("\n&rsaquo;\n"),
        '<a href="{}">{}</a>',
        ((url, label) for label, url in links),
    )
    return format_html('<div class="breadcrumbs">\n{}\n&rsaquo;\n{}\n</div>', items, current)


@register.simple_tag
def cachex_tools_in_header() -> bool:
    """Report whether the admin renders ``object-tools`` outside ``content``."""
    return _ADMIN_61_CHROME
