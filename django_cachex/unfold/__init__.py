"""
django_cachex.unfold - Unfold theme integration for cache admin.

Add 'django_cachex.unfold' to INSTALLED_APPS (after 'unfold') to enable
the unfold-styled cache admin interface.

This app provides the same functionality as django_cachex.admin but with
templates optimized for the django-unfold theme.
"""

default_app_config = "django_cachex.unfold.apps.UnfoldCacheAdminConfig"
