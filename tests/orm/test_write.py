"""Queries that write data are not cached, and they invalidate the cached data they affect."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from pathlib import Path

import pytest
from django.contrib.auth.models import Group, Permission, User
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import MultipleObjectsReturned
from django.core.management import call_command
from django.core.management.sql import emit_post_migrate_signal
from django.db import DEFAULT_DB_ALIAS, OperationalError, ProgrammingError, connection, transaction
from django.db.migrations import Migration
from django.db.models import Count
from django.db.models.expressions import RawSQL

from tests.orm.app.models import Test, TestChild, TestParent
from tests.orm.utils import assert_num_queries

pytestmark = pytest.mark.django_db(transaction=True)


def test_create():
    with assert_num_queries(1):
        data1 = list(Test.objects.all())
    assert data1 == []

    with assert_num_queries(1):
        t1 = Test.objects.create(name="test1")
    with assert_num_queries(1):
        t2 = Test.objects.create(name="test2")

    with assert_num_queries(1):
        data2 = list(Test.objects.all())
    with assert_num_queries(1):
        t3 = Test.objects.create(name="test3")
    with assert_num_queries(1):
        data3 = list(Test.objects.all())
    assert data2 == [t1, t2]
    assert data3 == [t1, t2, t3]

    with assert_num_queries(1):
        t3_copy = Test.objects.create(name="test3")
    assert t3_copy != t3
    with assert_num_queries(1):
        data4 = list(Test.objects.all())
    assert data4 == [t1, t2, t3, t3_copy]


def test_get_or_create():
    """The ``SELECT`` query of ``QuerySet.get_or_create`` is cached, but not the ``INSERT`` one."""
    with assert_num_queries(1):
        data1 = list(Test.objects.all())
    assert data1 == []

    with assert_num_queries(2):
        t, created = Test.objects.get_or_create(name="test")
    assert created

    with assert_num_queries(1):
        t_clone, created = Test.objects.get_or_create(name="test")
    assert not created
    assert t_clone == t

    with assert_num_queries(0):
        t_clone, created = Test.objects.get_or_create(name="test")
    assert not created
    assert t_clone == t

    with assert_num_queries(1):
        data2 = list(Test.objects.all())
    assert data2 == [t]


def test_update_or_create():
    with assert_num_queries(1):
        assert list(Test.objects.all()) == []

    with assert_num_queries(4):
        t, created = Test.objects.update_or_create(name="test", defaults={"public": True})
        assert created
        assert t.name == "test"
        assert t.public is True

    with assert_num_queries(2):
        t, created = Test.objects.update_or_create(name="test", defaults={"public": False})
        assert not created
        assert t.name == "test"
        assert t.public is False

    # update_or_create() runs the UPDATE even when nothing changed.
    with assert_num_queries(2):
        t, created = Test.objects.update_or_create(name="test", defaults={"public": False})
        assert not created
        assert t.name == "test"
        assert t.public is False

    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t]


def test_bulk_create():
    with assert_num_queries(1):
        data1 = list(Test.objects.all())
    assert data1 == []

    with assert_num_queries(1):
        unsaved_tests = [Test(name=f"test{i:02d}") for i in range(1, 11)]
        Test.objects.bulk_create(unsaved_tests)
    assert Test.objects.count() == 10

    with assert_num_queries(1):
        unsaved_tests = [Test(name=f"test{i:02d}") for i in range(1, 11)]
        Test.objects.bulk_create(unsaved_tests)
    assert Test.objects.count() == 20

    with assert_num_queries(1):
        data2 = list(Test.objects.all())
    assert len(data2) == 20
    assert [t.name for t in data2] == [f"test{i // 2:02d}" for i in range(2, 22)]


def test_update():
    with assert_num_queries(1):
        t = Test.objects.create(name="test1")

    with assert_num_queries(1):
        t1 = Test.objects.get()
    with assert_num_queries(1):
        t.name = "test2"
        t.save()
    with assert_num_queries(1):
        t2 = Test.objects.get()
    assert t1.name == "test1"
    assert t2.name == "test2"

    with assert_num_queries(1):
        Test.objects.update(name="test3")
    with assert_num_queries(1):
        t3 = Test.objects.get()
    assert t3.name == "test3"


def test_delete():
    with assert_num_queries(1):
        t1 = Test.objects.create(name="test1")
    with assert_num_queries(1):
        t2 = Test.objects.create(name="test2")

    with assert_num_queries(1):
        data1 = list(Test.objects.values_list("name", flat=True))
    with assert_num_queries(1):
        t2.delete()
    with assert_num_queries(1):
        data2 = list(Test.objects.values_list("name", flat=True))
    assert data1 == [t1.name, t2.name]
    assert data2 == [t1.name]

    with assert_num_queries(1):
        Test.objects.bulk_create([Test(name=f"test{i}") for i in range(2, 11)])
    with assert_num_queries(1):
        assert Test.objects.count() == 10
    with assert_num_queries(1):
        Test.objects.all().delete()
    with assert_num_queries(1):
        assert Test.objects.count() == 0


def test_invalidate_exists():
    with assert_num_queries(1):
        assert not Test.objects.exists()

    Test.objects.create(name="test")

    with assert_num_queries(1):
        assert Test.objects.exists()


def test_invalidate_count():
    with assert_num_queries(1):
        assert Test.objects.count() == 0

    Test.objects.create(name="test1")

    with assert_num_queries(1):
        assert Test.objects.count() == 1

    Test.objects.create(name="test2")

    with assert_num_queries(1):
        assert Test.objects.count() == 2


def test_invalidate_get():
    with assert_num_queries(1), pytest.raises(Test.DoesNotExist):
        Test.objects.get(name="test")

    Test.objects.create(name="test")

    with assert_num_queries(1):
        Test.objects.get(name="test")

    Test.objects.create(name="test")

    with assert_num_queries(1), pytest.raises(MultipleObjectsReturned):
        Test.objects.get(name="test")


def test_invalidate_values():
    with assert_num_queries(1):
        data1 = list(Test.objects.values("name", "public"))
    assert data1 == []

    Test.objects.bulk_create([Test(name="test1"), Test(name="test2", public=True)])

    with assert_num_queries(1):
        data2 = list(Test.objects.values("name", "public"))
    assert len(data2) == 2
    assert data2[0] == {"name": "test1", "public": False}
    assert data2[1] == {"name": "test2", "public": True}

    Test.objects.all()[0].delete()

    with assert_num_queries(1):
        data3 = list(Test.objects.values("name", "public"))
    assert len(data3) == 1
    assert data3[0] == {"name": "test2", "public": True}


def test_invalidate_foreign_key():
    with assert_num_queries(1):
        data1 = [t.owner.username for t in Test.objects.all() if t.owner]
    assert data1 == []

    u1 = User.objects.create_user("user1")
    Test.objects.bulk_create([Test(name="test1", owner=u1), Test(name="test2")])

    with assert_num_queries(2):
        data2 = [t.owner.username for t in Test.objects.all() if t.owner]
    assert data2 == ["user1"]

    Test.objects.create(name="test3")

    with assert_num_queries(1):
        data3 = [t.owner.username for t in Test.objects.all() if t.owner]
    assert data3 == ["user1"]

    t2 = Test.objects.get(name="test2")
    t2.owner = u1
    t2.save()

    with assert_num_queries(1):
        data4 = [t.owner.username for t in Test.objects.all() if t.owner]
    assert data4 == ["user1", "user1"]

    u2 = User.objects.create_user("user2")
    Test.objects.filter(name="test3").update(owner=u2)

    with assert_num_queries(3):
        data5 = [t.owner.username for t in Test.objects.all() if t.owner]
    assert data5 == ["user1", "user1", "user2"]

    User.objects.filter(username="user2").update(username="user3")

    with assert_num_queries(2):
        data6 = [t.owner.username for t in Test.objects.all() if t.owner]
    assert data6 == ["user1", "user1", "user3"]

    u2 = User.objects.create_user("user2")
    Test.objects.filter(name="test2").update(owner=u2)

    with assert_num_queries(4):
        data7 = [t.owner.username for t in Test.objects.all() if t.owner]
    assert data7 == ["user1", "user2", "user3"]

    with assert_num_queries(0):
        data8 = [t.owner.username for t in Test.objects.all() if t.owner]
    assert data8 == ["user1", "user2", "user3"]


def test_invalidate_many_to_many():
    u = User.objects.create_user("test_user")
    ct = ContentType.objects.get_for_model(User)
    discuss = Permission.objects.create(name="Can discuss", content_type=ct, codename="discuss")
    touch = Permission.objects.create(name="Can touch", content_type=ct, codename="touch")
    cuddle = Permission.objects.create(name="Can cuddle", content_type=ct, codename="cuddle")
    u.user_permissions.add(discuss, touch, cuddle)
    with assert_num_queries(1):
        data1 = [p.codename for p in u.user_permissions.all()]
    assert data1 == ["cuddle", "discuss", "touch"]

    touch.name = "Can lick"
    touch.codename = "lick"
    touch.save()

    with assert_num_queries(1):
        data2 = [p.codename for p in u.user_permissions.all()]
    assert data2 == ["cuddle", "discuss", "lick"]

    Permission.objects.filter(pk=discuss.pk).update(name="Can finger", codename="finger")

    with assert_num_queries(1):
        data3 = [p.codename for p in u.user_permissions.all()]
    assert data3 == ["cuddle", "finger", "lick"]


def test_invalidate_aggregate():
    with assert_num_queries(1):
        assert User.objects.aggregate(n=Count("test"))["n"] == 0

    with assert_num_queries(1):
        u = User.objects.create_user("test")
    with assert_num_queries(1):
        assert User.objects.aggregate(n=Count("test"))["n"] == 0

    with assert_num_queries(1):
        Test.objects.create(name="test1")
    with assert_num_queries(1):
        assert User.objects.aggregate(n=Count("test"))["n"] == 0

    with assert_num_queries(1):
        Test.objects.create(name="test2", owner=u)
    with assert_num_queries(1):
        assert User.objects.aggregate(n=Count("test"))["n"] == 1

    with assert_num_queries(1):
        Test.objects.create(name="test3")
    with assert_num_queries(1):
        assert User.objects.aggregate(n=Count("test"))["n"] == 1


def test_invalidate_annotate():
    with assert_num_queries(1):
        data1 = list(User.objects.annotate(n=Count("test")).order_by("pk"))
    assert data1 == []

    with assert_num_queries(1):
        Test.objects.create(name="test1")
    with assert_num_queries(1):
        data2 = list(User.objects.annotate(n=Count("test")).order_by("pk"))
    assert data2 == []

    with assert_num_queries(2):
        user1 = User.objects.create_user("user1")
        user2 = User.objects.create_user("user2")
    with assert_num_queries(1):
        data3 = list(User.objects.annotate(n=Count("test")).order_by("pk"))
    assert data3 == [user1, user2]
    assert [u.n for u in data3] == [0, 0]

    with assert_num_queries(1):
        Test.objects.create(name="test2", owner=user1)
    with assert_num_queries(1):
        data4 = list(User.objects.annotate(n=Count("test")).order_by("pk"))
    assert data4 == [user1, user2]
    assert [u.n for u in data4] == [1, 0]

    with assert_num_queries(1):
        Test.objects.bulk_create(
            [
                Test(name="test3", owner=user1),
                Test(name="test4", owner=user2),
                Test(name="test5", owner=user1),
                Test(name="test6", owner=user2),
            ],
        )
    with assert_num_queries(1):
        data5 = list(User.objects.annotate(n=Count("test")).order_by("pk"))
    assert data5 == [user1, user2]
    assert [u.n for u in data5] == [3, 2]


def test_invalidate_subquery():
    with assert_num_queries(1):
        data1 = list(Test.objects.filter(owner__in=User.objects.all()))
    assert data1 == []

    u = User.objects.create_user("test")

    with assert_num_queries(1):
        data2 = list(Test.objects.filter(owner__in=User.objects.all()))
    assert data2 == []

    t = Test.objects.create(name="test", owner=u)

    with assert_num_queries(1):
        data3 = list(Test.objects.filter(owner__in=User.objects.all()))
    assert data3 == [t]

    with assert_num_queries(1):
        data4 = list(Test.objects.filter(owner__groups__permissions__in=Permission.objects.all()).distinct())
    assert data4 == []

    g = Group.objects.create(name="test_group")

    with assert_num_queries(1):
        data5 = list(Test.objects.filter(owner__groups__permissions__in=Permission.objects.all()).distinct())
    assert data5 == []

    p = Permission.objects.first()
    g.permissions.add(p)

    with assert_num_queries(1):
        data6 = list(Test.objects.filter(owner__groups__permissions__in=Permission.objects.all()).distinct())
    assert data6 == []

    u.groups.add(g)

    with assert_num_queries(1):
        data7 = list(Test.objects.filter(owner__groups__permissions__in=Permission.objects.all()).distinct())
    assert data7 == [t]

    with assert_num_queries(1):
        data8 = list(User.objects.filter(user_permissions__in=g.permissions.all()))
    assert data8 == []

    u.user_permissions.add(p)

    with assert_num_queries(1):
        data9 = list(User.objects.filter(user_permissions__in=g.permissions.all()))
    assert data9 == [u]

    g.permissions.remove(p)

    with assert_num_queries(1):
        data10 = list(User.objects.filter(user_permissions__in=g.permissions.all()))
    assert data10 == []

    with assert_num_queries(1):
        data11 = list(User.objects.exclude(user_permissions=None))
    assert data11 == [u]

    u.user_permissions.clear()

    with assert_num_queries(1):
        data12 = list(User.objects.exclude(user_permissions=None))
    assert data12 == []


def test_invalidate_nested_subqueries():
    with assert_num_queries(1):
        data1 = list(User.objects.filter(pk__in=User.objects.filter(user_permissions__in=Permission.objects.all())))
    assert data1 == []

    u = User.objects.create_user("test")

    with assert_num_queries(1):
        data2 = list(User.objects.filter(pk__in=User.objects.filter(user_permissions__in=Permission.objects.all())))
    assert data2 == []

    p = Permission.objects.first()
    u.user_permissions.add(p)

    with assert_num_queries(1):
        data3 = list(User.objects.filter(pk__in=User.objects.filter(user_permissions__in=Permission.objects.all())))
    assert data3 == [u]

    with assert_num_queries(1):
        data4 = list(
            User.objects.filter(
                pk__in=User.objects.filter(
                    pk__in=User.objects.filter(user_permissions__in=Permission.objects.all()),
                ),
            ),
        )
    assert data4 == [u]

    u.user_permissions.remove(p)

    with assert_num_queries(1):
        data5 = list(
            User.objects.filter(
                pk__in=User.objects.filter(
                    pk__in=User.objects.filter(user_permissions__in=Permission.objects.all()),
                ),
            ),
        )
    assert data5 == []


def test_invalidate_raw_subquery():
    permission = Permission.objects.first()
    with assert_num_queries(0):
        raw_sql = RawSQL("SELECT id FROM auth_permission WHERE id = %s", (permission.pk,))
    with assert_num_queries(1):
        data1 = list(Test.objects.filter(permission=raw_sql))
    assert data1 == []

    test = Test.objects.create(name="test", permission=permission)

    with assert_num_queries(1):
        data2 = list(Test.objects.filter(permission=raw_sql))
    assert data2 == [test]

    permission.save()

    with assert_num_queries(1):
        data3 = list(Test.objects.filter(permission=raw_sql))
    assert data3 == [test]

    test.delete()

    with assert_num_queries(1):
        data4 = list(Test.objects.filter(permission=raw_sql))
    assert data4 == []


def test_invalidate_nested_raw_subquery():
    permission = Permission.objects.first()
    with assert_num_queries(0):
        raw_sql = RawSQL("SELECT id FROM auth_permission WHERE id = %s", (permission.pk,))
    with assert_num_queries(1):
        data1 = list(Test.objects.filter(pk__in=Test.objects.filter(permission=raw_sql)))
    assert data1 == []

    test = Test.objects.create(name="test", permission=permission)

    with assert_num_queries(1):
        data2 = list(Test.objects.filter(pk__in=Test.objects.filter(permission=raw_sql)))
    assert data2 == [test]

    permission.save()

    with assert_num_queries(1):
        data3 = list(Test.objects.filter(pk__in=Test.objects.filter(permission=raw_sql)))
    assert data3 == [test]

    test.delete()

    with assert_num_queries(1):
        data4 = list(Test.objects.filter(pk__in=Test.objects.filter(permission=raw_sql)))
    assert data4 == []


def test_invalidate_select_related():
    with assert_num_queries(1):
        data1 = list(Test.objects.select_related("owner"))
    assert data1 == []

    with assert_num_queries(2):
        u1 = User.objects.create_user("test1")
        u2 = User.objects.create_user("test2")
    with assert_num_queries(1):
        data2 = list(Test.objects.select_related("owner"))
    assert data2 == []

    with assert_num_queries(1):
        Test.objects.bulk_create(
            [
                Test(name="test1", owner=u1),
                Test(name="test2", owner=u2),
                Test(name="test3", owner=u2),
                Test(name="test4", owner=u1),
            ],
        )
    with assert_num_queries(1):
        data3 = list(Test.objects.select_related("owner"))
        assert data3[0].owner == u1
        assert data3[1].owner == u2
        assert data3[2].owner == u2
        assert data3[3].owner == u1

    with assert_num_queries(1):
        Test.objects.filter(name__in=["test1", "test2"]).delete()
    with assert_num_queries(1):
        data4 = list(Test.objects.select_related("owner"))
        assert data4[0].owner == u2
        assert data4[1].owner == u1


def test_invalidate_prefetch_related():  # noqa: PLR0915
    with assert_num_queries(1):
        data1 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
        assert data1 == []

    with assert_num_queries(1):
        t1 = Test.objects.create(name="test1")
    with assert_num_queries(1):
        data2 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
        assert data2 == [t1]
        assert data2[0].owner is None

    with assert_num_queries(2):
        u = User.objects.create_user("user")
        t1.owner = u
        t1.save()
    with assert_num_queries(2):
        data3 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
        assert data3 == [t1]
        assert data3[0].owner == u
        assert list(data3[0].owner.groups.all()) == []

    with assert_num_queries(4):
        group = Group.objects.create(name="test_group")
        permissions = list(Permission.objects.all()[:5])
        group.permissions.add(*permissions)
        u.groups.add(group)
    with assert_num_queries(2):
        data4 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
        assert data4 == [t1]
        owner = data4[0].owner
        assert owner == u
        groups = list(owner.groups.all())
        assert groups == [group]
        assert list(groups[0].permissions.all()) == permissions

    with assert_num_queries(1):
        t2 = Test.objects.create(name="test2")
    with assert_num_queries(1):
        data5 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
        assert data5 == [t1, t2]
        owners = [t.owner for t in data5 if t.owner is not None]
        assert owners == [u]
        groups = [g for o in owners for g in o.groups.all()]
        assert groups == [group]
        data5_permissions = [p for g in groups for p in g.permissions.all()]
        assert data5_permissions == permissions

    with assert_num_queries(1):
        permissions[0].save()
    with assert_num_queries(1):
        list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))

    with assert_num_queries(1):
        group.name = "modified_test_group"
        group.save()
    with assert_num_queries(2):
        data6 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
        g = list(data6[0].owner.groups.all())[0]
        assert g.name == "modified_test_group"

    with assert_num_queries(1):
        User.objects.update(username="modified_user")

    with assert_num_queries(2):
        data7 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
        assert data7[0].owner.username == "modified_user"


@pytest.mark.skipif(
    not connection.features.has_select_for_update,
    reason="Database doesn't support feature(s): has_select_for_update",
)
def test_invalidate_select_for_update():
    with assert_num_queries(1):
        Test.objects.bulk_create([Test(name="test1"), Test(name="test2")])

    with assert_num_queries(1), transaction.atomic():
        data1 = list(Test.objects.select_for_update())
        assert [t.name for t in data1] == ["test1", "test2"]

    with assert_num_queries(1), transaction.atomic():
        qs = Test.objects.select_for_update()
        qs.update(name="test3")

    with assert_num_queries(1), transaction.atomic():
        data2 = list(Test.objects.select_for_update())
        assert [t.name for t in data2] == ["test3"] * 2


def test_invalidate_extra_select():
    user = User.objects.create_user("user")
    t1 = Test.objects.create(name="test1", owner=user, public=True)

    user_table = User._meta.db_table
    test_table = Test._meta.db_table
    username_length_sql = f"""
        SELECT LENGTH({user_table}.username)
        FROM {user_table}
        WHERE {user_table}.id = {test_table}.owner_id
        """

    with assert_num_queries(1):
        data1 = list(Test.objects.extra(select={"username_length": username_length_sql}))
        assert data1 == [t1]
        assert [o.username_length for o in data1] == [4]

    Test.objects.update(public=False)

    with assert_num_queries(1):
        data2 = list(Test.objects.extra(select={"username_length": username_length_sql}))
        assert data2 == [t1]
        assert [o.username_length for o in data2] == [4]

    admin = User.objects.create_superuser("admin", "admin@test.me", "password")

    with assert_num_queries(1):
        data3 = list(Test.objects.extra(select={"username_length": username_length_sql}))
        assert data3 == [t1]
        assert [o.username_length for o in data3] == [4]

    t2 = Test.objects.create(name="test2", owner=admin)

    with assert_num_queries(1):
        data4 = list(Test.objects.extra(select={"username_length": username_length_sql}))
        assert data4 == [t1, t2]
        assert [o.username_length for o in data4] == [4, 5]


def test_invalidate_having():
    def _query():
        return User.objects.annotate(n=Count("user_permissions")).filter(n__gte=1)

    with assert_num_queries(1):
        data1 = list(_query())
        assert data1 == []

    u = User.objects.create_user("user")
    with assert_num_queries(1):
        data2 = list(_query())
        assert data2 == []

    p = Permission.objects.first()
    p.save()
    with assert_num_queries(1):
        data3 = list(_query())
        assert data3 == []

    u.user_permissions.add(p)
    with assert_num_queries(1):
        data3 = list(_query())
        assert data3 == [u]

    with assert_num_queries(1):
        assert _query().count() == 1

    u.user_permissions.clear()
    with assert_num_queries(1):
        assert _query().count() == 0


def test_invalidate_extra_where():
    sql_condition = "owner_id IN (SELECT id FROM auth_user WHERE username = 'admin')"
    with assert_num_queries(1):
        data1 = list(Test.objects.extra(where=[sql_condition]))
        assert data1 == []

    admin = User.objects.create_superuser("admin", "admin@test.me", "password")
    with assert_num_queries(1):
        data2 = list(Test.objects.extra(where=[sql_condition]))
        assert data2 == []

    t = Test.objects.create(name="test", owner=admin)
    with assert_num_queries(1):
        data3 = list(Test.objects.extra(where=[sql_condition]))
        assert data3 == [t]

    admin.username = "modified"
    admin.save()
    with assert_num_queries(1):
        data4 = list(Test.objects.extra(where=[sql_condition]))
        assert data4 == []


def test_invalidate_extra_tables():
    with assert_num_queries(1):
        User.objects.create_user("user1")

    with assert_num_queries(1):
        data1 = list(Test.objects.all().extra(tables=["auth_user"]))
    assert data1 == []

    with assert_num_queries(1):
        t1 = Test.objects.create(name="test1")
    with assert_num_queries(1):
        data2 = list(Test.objects.all().extra(tables=["auth_user"]))
    assert data2 == [t1]

    with assert_num_queries(1):
        t2 = Test.objects.create(name="test2")
    with assert_num_queries(1):
        data3 = list(Test.objects.all().extra(tables=["auth_user"]))
    assert data3 == [t1, t2]

    with assert_num_queries(1):
        User.objects.create_user("user2")
    with assert_num_queries(1):
        data4 = list(Test.objects.all().extra(tables=["auth_user"]))
    assert data4 == [t1, t1, t2, t2]


def test_invalidate_extra_order_by():
    with assert_num_queries(1):
        data1 = list(Test.objects.extra(order_by=["-ormtest_test.name"]))
        assert data1 == []
    t1 = Test.objects.create(name="test1")
    with assert_num_queries(1):
        data2 = list(Test.objects.extra(order_by=["-ormtest_test.name"]))
        assert data2 == [t1]
    t2 = Test.objects.create(name="test2")
    with assert_num_queries(1):
        data2 = list(Test.objects.extra(order_by=["-ormtest_test.name"]))
        assert data2 == [t2, t1]


def test_invalidate_table_inheritance():
    with assert_num_queries(1), pytest.raises(TestChild.DoesNotExist):
        TestChild.objects.get()

    with assert_num_queries(2):
        t_child = TestChild.objects.create(name="test_child")

    with assert_num_queries(1):
        assert TestChild.objects.get() == t_child

    with assert_num_queries(1):
        TestParent.objects.filter(pk=t_child.pk).update(name="modified")

    with assert_num_queries(1):
        modified_t_child = TestChild.objects.get()
        assert modified_t_child.pk == t_child.pk
        assert modified_t_child.name == "modified"

    with assert_num_queries(2):
        TestChild.objects.filter(pk=t_child.pk).update(name="modified2")

    with assert_num_queries(1):
        modified2_t_child = TestChild.objects.get()
        assert modified2_t_child.pk == t_child.pk
        assert modified2_t_child.name == "modified2"


def test_raw_insert():
    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == []

    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute("INSERT INTO ormtest_test (name, public) VALUES ('test1', %s)", [True])

    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == ["test1"]

    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute("INSERT INTO ormtest_test (name, public) VALUES ('test2', %s)", [True])

    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == ["test1", "test2"]

    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.executemany("INSERT INTO ormtest_test (name, public) VALUES ('test3', %s)", [[True]])

    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == ["test1", "test2", "test3"]


def test_raw_update():
    with assert_num_queries(1):
        Test.objects.create(name="test")
    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == ["test"]

    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute("UPDATE ormtest_test SET name = 'new name';")

    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == ["new name"]


def test_raw_delete():
    with assert_num_queries(1):
        Test.objects.create(name="test")
    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == ["test"]

    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute("DELETE FROM ormtest_test;")

    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == []


def test_raw_create():
    with assert_num_queries(1):
        assert list(Test.objects.all()) == []

    try:
        with assert_num_queries(1), connection.cursor() as cursor:
            cursor.execute("CREATE INDEX tmp_index ON ormtest_test(name);")

        with assert_num_queries(1):
            assert list(Test.objects.all()) == []
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP INDEX tmp_index;")


def test_raw_alter():
    with assert_num_queries(1):
        assert list(Test.objects.all()) == []

    try:
        with assert_num_queries(1), connection.cursor() as cursor:
            cursor.execute("ALTER TABLE ormtest_test ADD COLUMN tmp INTEGER;")

        with assert_num_queries(1):
            assert list(Test.objects.all()) == []
    finally:
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE ormtest_test DROP COLUMN tmp;")


@pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="SQLite does not revert schema changes in a transaction, making it hard to test this.",
)
@transaction.atomic
def test_raw_drop():
    with assert_num_queries(1):
        assert list(Test.objects.all()) == []

    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute("DROP TABLE ormtest_test;")

    with pytest.raises((ProgrammingError, OperationalError)):
        list(Test.objects.all())


@pytest.fixture
def row():
    return Test.objects.create(name="test1")


def test_flush(row):
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [row]

    call_command("flush", verbosity=0, interactive=False)

    with assert_num_queries(1):
        assert list(Test.objects.all()) == []


def test_migrate():
    # Only a migrate that applies a migration invalidates, many-to-many tables included.
    TestChild.objects.create(name="child").permissions.add(Permission.objects.first())
    permissions = TestChild.permissions.through.objects.all()
    with assert_num_queries(1):
        assert len(permissions.all()) == 1

    call_command("migrate", verbosity=0)
    with assert_num_queries(0):
        assert len(permissions.all()) == 1

    emit_post_migrate_signal(0, False, DEFAULT_DB_ALIAS, plan=[(Migration("0002_test", "ormtest"), False)])
    with assert_num_queries(1):
        assert len(permissions.all()) == 1


def test_loaddata(row):
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [row]

    call_command("loaddata", Path(__file__).with_name("loaddata_fixture.json"), verbosity=0)

    with assert_num_queries(1):
        assert [t.name for t in Test.objects.all()] == ["test1", "test2"]
