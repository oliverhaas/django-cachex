"""Signals sent by the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from django.dispatch import Signal

# Sent with the table name as sender and db_alias once the table's cached queries are
# invalidated: after an autocommit write, a commit that wrote to it, or invalidate().
post_invalidation = Signal()
