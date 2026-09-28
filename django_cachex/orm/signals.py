"""Signals sent by the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from django.dispatch import Signal

# Sent with the table name as sender and its database's db_alias once the table's
# cached queries are invalidated: after a write under autocommit, when a
# transaction that wrote to the table commits, and by invalidate().
post_invalidation = Signal()
