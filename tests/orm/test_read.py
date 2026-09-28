"""Every query that only reads data is cached.

The exceptions bypass the ORM: ``Model.objects.raw`` and ``cursor.execute``.
"""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

import datetime
import logging
import re
from types import SimpleNamespace

import pytest
from django.contrib.auth.models import Group, Permission, User
from django.contrib.contenttypes.models import ContentType
from django.db import OperationalError, ProgrammingError, connection, transaction
from django.db.models import Case, Count, F, FilteredRelation, Q, Value, When
from django.db.models.expressions import Exists, OuterRef, RawSQL, Subquery
from django.db.models.functions import Coalesce, Now
from django.db.transaction import TransactionManagementError
from django.test import override_settings

from django_cachex.orm.utils import UncachableQuery
from tests.orm.app.models import SomeChoices, Test, TestChild, TestParent, UnmanagedModel
from tests.orm.utils import (
    assert_num_queries,
    assert_query_cached,
    assert_tables,
    corrupt_entry,
    evict_generation,
    override_orm_settings,
)

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def rows():
    """Create the rows every test reads."""
    rows = SimpleNamespace()
    rows.group = Group.objects.create(name="test_group")
    rows.group__permissions = list(Permission.objects.all()[:3])
    rows.group.permissions.add(*rows.group__permissions)
    rows.user = User.objects.create_user("user")
    rows.user__permissions = list(Permission.objects.filter(content_type__app_label="auth")[3:6])
    rows.user.groups.add(rows.group)
    rows.user.user_permissions.add(*rows.user__permissions)
    rows.admin = User.objects.create_superuser("admin", "admin@test.me", "password")
    rows.t1__permission = Permission.objects.order_by("?").select_related("content_type")[0]
    rows.t1 = Test.objects.create(
        name="test1",
        owner=rows.user,
        date="1789-07-14",
        datetime="1789-07-14T16:43:27",
        permission=rows.t1__permission,
    )
    rows.t2 = Test.objects.create(
        name="test2",
        owner=rows.admin,
        public=True,
        date="1944-06-06",
        datetime="1944-06-06T06:35:00",
    )
    return rows


def test_empty():
    with assert_num_queries(0):
        data1 = list(Test.objects.none())
    with assert_num_queries(0):
        data2 = list(Test.objects.none())
    assert data2 == data1
    assert data2 == []


def test_exists():
    with assert_num_queries(1):
        n1 = Test.objects.exists()
    with assert_num_queries(0):
        n2 = Test.objects.exists()
    assert n2 == n1
    assert n2


def test_count():
    with assert_num_queries(1):
        n1 = Test.objects.count()
    with assert_num_queries(0):
        n2 = Test.objects.count()
    assert n2 == n1
    assert n2 == 2


def test_get(rows):
    with assert_num_queries(1):
        data1 = Test.objects.get(name="test1")
    with assert_num_queries(0):
        data2 = Test.objects.get(name="test1")
    assert data2 == data1
    assert data2 == rows.t1


def test_first(rows):
    with assert_num_queries(1):
        assert Test.objects.filter(name="bad").first() is None
    with assert_num_queries(0):
        assert Test.objects.filter(name="bad").first() is None

    with assert_num_queries(1):
        data1 = Test.objects.first()
    with assert_num_queries(0):
        data2 = Test.objects.first()
    assert data2 == data1
    assert data2 == rows.t1


def test_last(rows):
    with assert_num_queries(1):
        data1 = Test.objects.last()
    with assert_num_queries(0):
        data2 = Test.objects.last()
    assert data2 == data1
    assert data2 == rows.t2


def test_all(rows):
    with assert_num_queries(1):
        data1 = list(Test.objects.all())
    with assert_num_queries(0):
        data2 = list(Test.objects.all())
    assert data2 == data1
    assert data2 == [rows.t1, rows.t2]


@pytest.mark.usefixtures("final_sql_check")
def test_filter(rows):
    qs = Test.objects.filter(public=True)
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t2])

    qs = Test.objects.filter(name__in=["test2", "test72"])
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t2])

    qs = Test.objects.filter(date__gt=datetime.date(1900, 1, 1))
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t2])

    qs = Test.objects.filter(datetime__lt=datetime.datetime(1900, 1, 1))
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t1])


@pytest.mark.usefixtures("final_sql_check")
def test_filter_empty():
    qs = Test.objects.filter(public=True, name="user")
    assert_tables(qs, Test)
    assert_query_cached(qs, [])


@pytest.mark.usefixtures("final_sql_check")
def test_exclude(rows):
    qs = Test.objects.exclude(public=True)
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t1])

    qs = Test.objects.exclude(name__in=["test2", "test72"])
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t1])


@pytest.mark.usefixtures("final_sql_check")
def test_slicing(rows):
    qs = Test.objects.all()[:1]
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t1])


@pytest.mark.usefixtures("final_sql_check")
def test_order_by(rows):
    qs = Test.objects.order_by("pk")
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t1, rows.t2])

    qs = Test.objects.order_by("-name")
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t2, rows.t1])


@pytest.mark.usefixtures("final_sql_check")
def test_random_order_by():
    qs = Test.objects.order_by("?")
    with pytest.raises(UncachableQuery):
        assert_tables(qs, Test)
    assert_query_cached(qs, after=1, compare_results=False)


@pytest.mark.usefixtures("final_sql_check")
def test_order_by_field_of_another_table(rows):
    qs = Test.objects.order_by("owner__username")
    assert_tables(qs, Test, User)
    assert_query_cached(qs, [rows.t2, rows.t1])


@pytest.mark.usefixtures("final_sql_check")
def test_order_by_field_of_another_table_with_expression(rows):
    qs = Test.objects.order_by(Coalesce("name", "owner__username"))
    assert_tables(qs, Test, User)
    assert_query_cached(qs, [rows.t1, rows.t2])


@pytest.mark.usefixtures("final_sql_check")
def test_random_order_by_subquery():
    qs = Test.objects.filter(pk__in=Test.objects.order_by("?")[:10])
    with pytest.raises(UncachableQuery):
        assert_tables(qs, Test)
    assert_query_cached(qs, after=1, compare_results=False)


@pytest.mark.usefixtures("final_sql_check")
def test_reverse(rows):
    qs = Test.objects.reverse()
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t2, rows.t1])


@pytest.mark.usefixtures("final_sql_check")
def test_distinct(rows):
    # Across many-to-many relations, the query returns duplicates without distinct().
    qs = Test.objects.filter(owner__user_permissions__content_type__app_label="auth")
    assert_tables(qs, Test, User, User.user_permissions.through, Permission, ContentType)
    assert_query_cached(qs, [rows.t1, rows.t1, rows.t1])

    qs = qs.distinct()
    assert_tables(qs, Test, User, User.user_permissions.through, Permission, ContentType)
    assert_query_cached(qs, [rows.t1])


def test_django_enums():
    t = Test.objects.create(name="test1", a_choice=SomeChoices.foo)
    qs = Test.objects.filter(a_choice=SomeChoices.foo)
    assert_query_cached(qs, [t])


def test_iterator_not_cached(rows):
    with assert_num_queries(2):
        assert list(Test.objects.iterator()) == [rows.t1, rows.t2]
        assert list(Test.objects.iterator()) == [rows.t1, rows.t2]


def test_in_bulk(rows):
    with assert_num_queries(1):
        data1 = Test.objects.in_bulk((5432, rows.t2.pk, 9200))
    with assert_num_queries(0):
        data2 = Test.objects.in_bulk((5432, rows.t2.pk, 9200))
    assert data2 == data1
    assert data2 == {rows.t2.pk: rows.t2}


@pytest.mark.usefixtures("final_sql_check")
def test_values():
    qs = Test.objects.values("name", "public")
    assert_tables(qs, Test)
    assert_query_cached(qs, [{"name": "test1", "public": False}, {"name": "test2", "public": True}])


@pytest.mark.usefixtures("final_sql_check")
def test_values_list():
    qs = Test.objects.values_list("name", flat=True)
    assert_tables(qs, Test)
    assert_query_cached(qs, ["test1", "test2"])


def test_earliest(rows):
    with assert_num_queries(1):
        data1 = Test.objects.earliest("date")
    with assert_num_queries(0):
        data2 = Test.objects.earliest("date")
    assert data2 == data1
    assert data2 == rows.t1


def test_latest(rows):
    with assert_num_queries(1):
        data1 = Test.objects.latest("date")
    with assert_num_queries(0):
        data2 = Test.objects.latest("date")
    assert data2 == data1
    assert data2 == rows.t2


@pytest.mark.usefixtures("final_sql_check")
def test_dates():
    qs = Test.objects.dates("date", "year")
    assert_tables(qs, Test)
    assert_query_cached(qs, [datetime.date(1789, 1, 1), datetime.date(1944, 1, 1)])


@pytest.mark.usefixtures("final_sql_check")
def test_datetimes():
    qs = Test.objects.datetimes("datetime", "hour")
    assert_tables(qs, Test)
    assert_query_cached(qs, [datetime.datetime(1789, 7, 14, 16), datetime.datetime(1944, 6, 6, 6)])


@pytest.mark.usefixtures("final_sql_check")
@override_settings(USE_TZ=True)
def test_datetimes_with_time_zones():
    qs = Test.objects.datetimes("datetime", "hour")
    assert_tables(qs, Test)
    assert_query_cached(
        qs,
        [
            datetime.datetime(1789, 7, 14, 16, tzinfo=datetime.UTC),
            datetime.datetime(1944, 6, 6, 6, tzinfo=datetime.UTC),
        ],
    )


@pytest.mark.usefixtures("final_sql_check")
def test_foreign_key(rows):
    with assert_num_queries(3):
        data1 = [t.owner for t in Test.objects.all()]
    with assert_num_queries(0):
        data2 = [t.owner for t in Test.objects.all()]
    assert data2 == data1
    assert data2 == [rows.user, rows.admin]

    qs = Test.objects.values_list("owner", flat=True)
    assert_tables(qs, Test, User)
    assert_query_cached(qs, [rows.user.pk, rows.admin.pk])


@pytest.mark.usefixtures("final_sql_check")
def test_many_to_many():
    u = User.objects.create_user("test_user")
    ct = ContentType.objects.get_for_model(User)
    u.user_permissions.add(
        Permission.objects.create(name="Can discuss", content_type=ct, codename="discuss"),
        Permission.objects.create(name="Can touch", content_type=ct, codename="touch"),
        Permission.objects.create(name="Can cuddle", content_type=ct, codename="cuddle"),
    )
    qs = u.user_permissions.values_list("codename", flat=True)
    assert_tables(qs, User, User.user_permissions.through, Permission, ContentType)
    assert_query_cached(qs, ["cuddle", "discuss", "touch"])


@pytest.mark.usefixtures("final_sql_check")
def test_subquery(rows):
    qs = Test.objects.filter(owner__in=User.objects.all())
    assert_tables(qs, Test, User)
    assert_query_cached(qs, [rows.t1, rows.t2])

    qs = Test.objects.filter(owner__groups__permissions__in=Permission.objects.all())
    assert_tables(
        qs,
        Test,
        User,
        User.groups.through,
        Group,
        Group.permissions.through,
        Permission,
    )
    assert_query_cached(qs, [rows.t1, rows.t1, rows.t1])

    qs = Test.objects.filter(owner__groups__permissions__in=Permission.objects.all()).distinct()
    assert_tables(
        qs,
        Test,
        User,
        User.groups.through,
        Group,
        Group.permissions.through,
        Permission,
    )
    assert_query_cached(qs, [rows.t1])

    qs = TestChild.objects.exclude(permissions__isnull=True)
    assert_tables(qs, TestParent, TestChild, TestChild.permissions.through, Permission)
    assert_query_cached(qs, [])

    qs = TestChild.objects.exclude(permissions__name="")
    assert_tables(qs, TestParent, TestChild, TestChild.permissions.through, Permission)
    assert_query_cached(qs, [])


@pytest.mark.usefixtures("final_sql_check")
def test_custom_subquery():
    tests = Test.objects.filter(permission=OuterRef("pk")).values("name")
    qs = Permission.objects.annotate(first_permission=Subquery(tests[:1]))
    assert_tables(qs, Permission, Test, ContentType)
    assert_query_cached(qs, list(Permission.objects.all()))


@pytest.mark.usefixtures("final_sql_check")
def test_custom_subquery_exists():
    tests = Test.objects.filter(permission=OuterRef("pk"))
    qs = Permission.objects.annotate(has_tests=Exists(tests))
    assert_tables(qs, Permission, Test, ContentType)
    assert_query_cached(qs, list(Permission.objects.all()))


@pytest.mark.usefixtures("final_sql_check")
def test_raw_subquery(rows):
    with assert_num_queries(0):
        raw_sql = RawSQL("SELECT id FROM auth_permission WHERE id = %s", (rows.t1__permission.pk,))
    qs = Test.objects.filter(permission=raw_sql)
    assert_tables(qs, Test, Permission)
    assert_query_cached(qs, [rows.t1])

    qs = Test.objects.filter(pk__in=Test.objects.filter(permission=raw_sql))
    assert_tables(qs, Test, Permission)
    assert_query_cached(qs, [rows.t1])


@override_orm_settings(FINAL_SQL_CHECK=False)
def test_subquery_in_expression(rows):
    group_name = Subquery(Group.objects.order_by("pk").values("name")[:1])
    qs = Test.objects.filter(name=Coalesce(group_name, Value("")))
    assert_tables(qs, Test, Group)
    assert_query_cached(qs, [])

    rows.group.name = "test1"
    rows.group.save()
    with assert_num_queries(1):
        assert list(qs) == [rows.t1]


@pytest.mark.parametrize("alias", [False, True], ids=["expression", "alias"])
@override_orm_settings(FINAL_SQL_CHECK=False)
def test_subquery_in_order_by(rows, alias):
    group_pk = Subquery(Group.objects.filter(name=OuterRef("name")).values("pk")[:1])
    qs = Test.objects.alias(group_pk=group_pk).order_by("group_pk") if alias else Test.objects.order_by(group_pk)
    group2 = Group.objects.create(name="test2")
    Group.objects.create(name="test1")
    assert_tables(qs, Test, Group)
    assert_query_cached(qs, [rows.t2, rows.t1])

    # The group of t2 now has the highest pk.
    group2.delete()
    Group.objects.create(name="test2")
    with assert_num_queries(1):
        assert list(qs) == [rows.t1, rows.t2]


@override_orm_settings(FINAL_SQL_CHECK=False)
def test_subquery_in_filtered_relation(rows):
    qs = User.objects.annotate(
        grouped_tests=FilteredRelation("test", condition=Q(test__name__in=Subquery(Group.objects.values("name")))),
    ).filter(grouped_tests__isnull=False)
    assert_tables(qs, User, Test, Group)
    assert_query_cached(qs, [])

    rows.group.name = "test1"
    rows.group.save()
    with assert_num_queries(1):
        assert list(qs) == [rows.user]


@pytest.mark.usefixtures("final_sql_check")
def test_aggregate(rows):
    Test.objects.create(name="test3", owner=rows.user)
    with assert_num_queries(1):
        n1 = User.objects.aggregate(n=Count("test"))["n"]
    with assert_num_queries(0):
        n2 = User.objects.aggregate(n=Count("test"))["n"]
    assert n2 == n1
    assert n2 == 3


@pytest.mark.usefixtures("final_sql_check")
def test_annotate(rows):
    Test.objects.create(name="test3", owner=rows.user)
    qs = User.objects.annotate(n=Count("test")).order_by("pk").values_list("n", flat=True)
    assert_tables(qs, User, Test)
    assert_query_cached(qs, [2, 1])


@pytest.mark.usefixtures("final_sql_check")
def test_annotate_subquery(rows):
    tests = Test.objects.filter(owner=OuterRef("pk")).values("name")
    qs = User.objects.annotate(first_test=Subquery(tests[:1]))
    assert_tables(qs, User, Test)
    assert_query_cached(qs, [rows.user, rows.admin])


@pytest.mark.usefixtures("final_sql_check")
def test_annotate_case_with_when_and_query_in_default(rows):
    tests = Test.objects.filter(owner=OuterRef("pk")).values("name")
    qs = User.objects.annotate(first_test=Case(When(Q(pk=1), then=Value("noname")), default=Subquery(tests[:1])))
    assert_tables(qs, User, Test)
    assert_query_cached(qs, [rows.user, rows.admin])


@pytest.mark.usefixtures("final_sql_check")
def test_annotate_case_with_when(rows):
    tests = Test.objects.filter(owner=OuterRef("pk")).values("name")
    qs = User.objects.annotate(first_test=Case(When(Q(pk=1), then=Subquery(tests[:1])), default=Value("noname")))
    assert_tables(qs, User, Test)
    assert_query_cached(qs, [rows.user, rows.admin])


@pytest.mark.usefixtures("final_sql_check")
def test_annotate_coalesce(rows):
    tests = Test.objects.filter(owner=OuterRef("pk")).values("name")
    qs = User.objects.annotate(name=Coalesce(Subquery(tests[:1]), Value("notest")))
    assert_tables(qs, User, Test)
    assert_query_cached(qs, [rows.user, rows.admin])


@pytest.mark.usefixtures("final_sql_check")
def test_annotate_raw(rows):
    qs = User.objects.annotate(
        perm_id=RawSQL("SELECT id FROM auth_permission WHERE id = %s", (rows.t1__permission.pk,)),
    )
    assert_tables(qs, User, Permission)
    assert_query_cached(qs, [rows.user, rows.admin])


@pytest.mark.usefixtures("final_sql_check")
def test_only():
    with assert_num_queries(1):
        t1 = Test.objects.only("name").first()
        t1.name
    with assert_num_queries(0):
        t2 = Test.objects.only("name").first()
        t2.name
    with assert_num_queries(1):
        t1.public
    with assert_num_queries(0):
        t2.public
    assert t2 == t1
    assert t2.name == t1.name
    assert t2.public == t1.public


@pytest.mark.usefixtures("final_sql_check")
def test_defer():
    with assert_num_queries(1):
        t1 = Test.objects.defer("name").first()
        t1.public
    with assert_num_queries(0):
        t2 = Test.objects.defer("name").first()
        t2.public
    with assert_num_queries(1):
        t1.name
    with assert_num_queries(0):
        t2.name
    assert t2 == t1
    assert t2.name == t1.name
    assert t2.public == t1.public


@pytest.mark.usefixtures("final_sql_check")
def test_select_related(rows):
    with assert_num_queries(1):
        t1 = Test.objects.select_related("owner").get(name="test1")
        assert t1.owner == rows.user
    with assert_num_queries(0):
        t2 = Test.objects.select_related("owner").get(name="test1")
        assert t2.owner == rows.user
    assert t2 == t1
    assert t2 == rows.t1

    with assert_num_queries(1):
        t3 = Test.objects.select_related("permission__content_type")[0]
        assert t3.permission == rows.t1.permission
        assert t3.permission.content_type == rows.t1__permission.content_type
    with assert_num_queries(0):
        t4 = Test.objects.select_related("permission__content_type")[0]
        assert t4.permission == rows.t1.permission
        assert t4.permission.content_type == rows.t1__permission.content_type
    assert t4 == t3
    assert t4 == rows.t1


@pytest.mark.usefixtures("final_sql_check")
def test_prefetch_related(rows):
    with assert_num_queries(2):
        data1 = list(User.objects.prefetch_related("user_permissions"))
    with assert_num_queries(0):
        permissions1 = [p for u in data1 for p in u.user_permissions.all()]
    with assert_num_queries(0):
        data2 = list(User.objects.prefetch_related("user_permissions"))
        permissions2 = [p for u in data2 for p in u.user_permissions.all()]
    assert permissions2 == permissions1
    assert permissions2 == rows.user__permissions

    # The prefetch query ran before, so only the main query runs.
    with assert_num_queries(1):
        data3 = list(Test.objects.select_related("owner").prefetch_related("owner__user_permissions"))
    with assert_num_queries(0):
        permissions3 = [p for t in data3 for p in t.owner.user_permissions.all()]
    with assert_num_queries(0):
        data4 = list(Test.objects.select_related("owner").prefetch_related("owner__user_permissions"))
        permissions4 = [p for t in data4 for p in t.owner.user_permissions.all()]
    assert permissions4 == permissions3
    assert permissions4 == rows.user__permissions

    # The prefetch query, for one owner only, did not run before.
    with assert_num_queries(2):
        data5 = list(Test.objects.select_related("owner").prefetch_related("owner__user_permissions")[:1])
    with assert_num_queries(0):
        permissions5 = [p for t in data5 for p in t.owner.user_permissions.all()]
    with assert_num_queries(0):
        data6 = list(Test.objects.select_related("owner").prefetch_related("owner__user_permissions")[:1])
        permissions6 = [p for t in data6 for p in t.owner.user_permissions.all()]
    assert permissions6 == permissions5
    assert permissions6 == rows.user__permissions

    with assert_num_queries(2):
        data7 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
    with assert_num_queries(0):
        permissions7 = [p for t in data7 for g in t.owner.groups.all() for p in g.permissions.all()]
    with assert_num_queries(0):
        data8 = list(Test.objects.select_related("owner").prefetch_related("owner__groups__permissions"))
        permissions8 = [p for t in data8 for g in t.owner.groups.all() for p in g.permissions.all()]
    assert permissions8 == permissions7
    assert permissions8 == rows.group__permissions


@pytest.mark.usefixtures("final_sql_check")
def test_test_parent():
    TestChild.objects.create(name="child")
    qs = TestChild.objects.filter(name="child")
    assert_query_cached(qs)

    parent = TestParent.objects.all().first()
    parent.name = "another name"
    parent.save()

    child = TestChild.objects.all().first()
    assert child.name == "another name"


@pytest.mark.usefixtures("final_sql_check")
def test_filtered_relation():
    qs = TestChild.objects.annotate(
        filtered_permissions=FilteredRelation("permissions", condition=Q(permissions__pk__gt=1)),
    )
    assert_tables(qs, TestParent, TestChild)
    assert_query_cached(qs)

    values_qs = qs.values("filtered_permissions")
    assert_tables(values_qs, TestParent, TestChild, TestChild.permissions.through, Permission)
    assert_query_cached(values_qs)

    filtered_qs = qs.filter(filtered_permissions__pk__gt=2)
    assert_tables(filtered_qs, TestParent, TestChild, TestChild.permissions.through, Permission)
    assert_query_cached(filtered_qs)


@pytest.mark.skipif(
    not connection.features.supports_select_union,
    reason="Database doesn't support feature(s): supports_select_union",
)
@pytest.mark.usefixtures("final_sql_check")
def test_union():
    sqlite = connection.vendor == "sqlite"
    qs = Test.objects.filter(pk__lt=5) | Test.objects.filter(permission__name__contains="a")
    assert_tables(qs, Test, Permission)
    assert_query_cached(qs)

    with pytest.raises(TypeError, match=re.escape("Cannot combine queries on two different base models.")):
        Test.objects.all() | Permission.objects.all()

    qs = Test.objects.filter(pk__lt=5)
    sub_qs = Test.objects.filter(permission__name__contains="a")
    if sqlite:
        qs = qs.order_by()
        sub_qs = sub_qs.order_by()
    qs = qs.union(sub_qs)
    assert_tables(qs, Test, Permission)
    assert_query_cached(qs)

    qs = Test.objects.all()
    sub_qs = Permission.objects.all()
    if sqlite:
        qs = qs.order_by()
        sub_qs = sub_qs.order_by()
    qs = qs.union(sub_qs)
    tables = {Test, Permission}
    # Permission orders by its content type, but not on SQLite, where the ordering is cleared.
    if not sqlite:
        tables.add(ContentType)
    assert_tables(qs, *tables)
    with pytest.raises((ProgrammingError, OperationalError)):
        assert_query_cached(qs)


@pytest.mark.skipif(
    not connection.features.supports_select_intersection,
    reason="Database doesn't support feature(s): supports_select_intersection",
)
@pytest.mark.usefixtures("final_sql_check")
def test_intersection():
    sqlite = connection.vendor == "sqlite"
    qs = Test.objects.filter(pk__lt=5) & Test.objects.filter(permission__name__contains="a")
    assert_tables(qs, Test, Permission)
    assert_query_cached(qs)

    with pytest.raises(TypeError, match=re.escape("Cannot combine queries on two different base models.")):
        Test.objects.all() & Permission.objects.all()

    qs = Test.objects.filter(pk__lt=5)
    sub_qs = Test.objects.filter(permission__name__contains="a")
    if sqlite:
        qs = qs.order_by()
        sub_qs = sub_qs.order_by()
    qs = qs.intersection(sub_qs)
    assert_tables(qs, Test, Permission)
    assert_query_cached(qs)

    qs = Test.objects.all()
    sub_qs = Permission.objects.all()
    if sqlite:
        qs = qs.order_by()
        sub_qs = sub_qs.order_by()
    qs = qs.intersection(sub_qs)
    tables = {Test, Permission}
    if not sqlite:
        tables.add(ContentType)
    assert_tables(qs, *tables)
    with pytest.raises((ProgrammingError, OperationalError)):
        assert_query_cached(qs)


@pytest.mark.skipif(
    not connection.features.supports_select_difference,
    reason="Database doesn't support feature(s): supports_select_difference",
)
@pytest.mark.usefixtures("final_sql_check")
def test_difference():
    sqlite = connection.vendor == "sqlite"
    qs = Test.objects.filter(pk__lt=5)
    sub_qs = Test.objects.filter(permission__name__contains="a")
    if sqlite:
        qs = qs.order_by()
        sub_qs = sub_qs.order_by()
    qs = qs.difference(sub_qs)
    assert_tables(qs, Test, Permission)
    assert_query_cached(qs)

    qs = Test.objects.all()
    sub_qs = Permission.objects.all()
    if sqlite:
        qs = qs.order_by()
        sub_qs = sub_qs.order_by()
    qs = qs.difference(sub_qs)
    tables = {Test, Permission}
    if not sqlite:
        tables.add(ContentType)
    assert_tables(qs, *tables)
    with pytest.raises((ProgrammingError, OperationalError)):
        assert_query_cached(qs)


@pytest.mark.skipif(
    not connection.features.has_select_for_update,
    reason="Database doesn't support feature(s): has_select_for_update",
)
def test_select_for_update(rows):
    """Tests if ``select_for_update`` queries are not cached."""
    with pytest.raises(TransactionManagementError):
        list(Test.objects.select_for_update())

    with assert_num_queries(1), transaction.atomic():
        data1 = list(Test.objects.select_for_update())
        assert data1 == [rows.t1, rows.t2]
        assert [t.name for t in data1] == ["test1", "test2"]

    with assert_num_queries(1), transaction.atomic():
        data2 = list(Test.objects.select_for_update())
        assert data2 == [rows.t1, rows.t2]
        assert [t.name for t in data2] == ["test1", "test2"]

    with assert_num_queries(2), transaction.atomic():
        data3 = list(Test.objects.select_for_update())
        data4 = list(Test.objects.select_for_update())
        assert data3 == [rows.t1, rows.t2]
        assert data4 == [rows.t1, rows.t2]
        assert [t.name for t in data3] == ["test1", "test2"]
        assert [t.name for t in data4] == ["test1", "test2"]


@pytest.mark.usefixtures("final_sql_check")
def test_having(rows):
    qs = User.objects.annotate(n=Count("user_permissions")).filter(n__gte=1)
    assert_tables(qs, User, User.user_permissions.through, Permission)
    assert_query_cached(qs, [rows.user])

    with assert_num_queries(1):
        assert User.objects.annotate(n=Count("user_permissions")).filter(n__gte=1).count() == 1

    with assert_num_queries(0):
        assert User.objects.annotate(n=Count("user_permissions")).filter(n__gte=1).count() == 1


def test_extra_select(rows):
    user_table = User._meta.db_table
    test_table = Test._meta.db_table
    username_length_sql = f"""
    SELECT LENGTH({user_table}.username)
    FROM {user_table}
    WHERE {user_table}.id = {test_table}.owner_id
    """

    with assert_num_queries(1):
        data1 = list(Test.objects.extra(select={"username_length": username_length_sql}))
        assert data1 == [rows.t1, rows.t2]
        assert [o.username_length for o in data1] == [4, 5]
    with assert_num_queries(0):
        data2 = list(Test.objects.extra(select={"username_length": username_length_sql}))
        assert data2 == [rows.t1, rows.t2]
        assert [o.username_length for o in data2] == [4, 5]


@pytest.mark.usefixtures("final_sql_check")
def test_extra_where(rows):
    sql_condition = "owner_id IN (SELECT id FROM auth_user WHERE username = 'admin')"
    qs = Test.objects.extra(where=[sql_condition])
    assert_tables(qs, Test, User)
    assert_query_cached(qs, [rows.t2])


@pytest.mark.usefixtures("final_sql_check")
def test_extra_tables():
    qs = Test.objects.extra(tables=["auth_user"], select={"extra_id": "auth_user.id"})
    assert_tables(qs, Test, User)
    assert_query_cached(qs)


@pytest.mark.usefixtures("final_sql_check")
def test_extra_order_by(rows):
    qs = Test.objects.extra(order_by=["-ormtest_test.name"])
    assert_tables(qs, Test)
    assert_query_cached(qs, [rows.t2, rows.t1])


def test_table_inheritance():
    with assert_num_queries(2):
        t_child = TestChild.objects.create(name="test_child")

    with assert_num_queries(1):
        assert TestChild.objects.get() == t_child

    with assert_num_queries(0):
        assert TestChild.objects.get() == t_child


def test_explain():
    explain_kwargs = {}
    if connection.vendor == "sqlite":
        # Recent SQLite versions fill the third column with a row estimate.
        expected = (
            r"\d+ \d+ \d+ SCAN ormtest_test\n"
            r"\d+ \d+ \d+ USE TEMP B-TREE FOR ORDER BY"
        )
    else:
        explain_kwargs.update(
            analyze=True,
            costs=False,
        )
        operation_detail = (
            r"\(actual time=[\d\.]+..[\d\.]+\ "
            r"rows=[\d\.]+ loops=\d+\)"
        )
        expected = (
            rf"^Sort {operation_detail}\n"
            r"  Sort Key: name\n"
            r"  Sort Method: quicksort  Memory: \d+kB\n"
            r"  Buffers: shared hit=\d+\n"
            rf"  ->  Seq Scan on ormtest_test {operation_detail}\n"
            r"        Buffers: shared hit=\d+\n"
            # A warm catalog leaves the planner nothing to read.
            r"(Planning:\n  Buffers: shared hit=\d+\n)?"
            r"Planning Time: [\d\.]+ ms\n"
            r"Execution Time: [\d\.]+ ms$"
        )
    # EXPLAIN describes the plan, not the rows, so it is never cached.
    for _ in range(2):
        with assert_num_queries(1):
            explanation = Test.objects.explain(**explain_kwargs)
        assert re.search(expected, explanation)


def test_raw(rows):
    """Tests if ``Model.objects.raw`` queries are not cached."""
    sql = f"SELECT * FROM {Test._meta.db_table};"

    with assert_num_queries(1):
        data1 = list(Test.objects.raw(sql))
    with assert_num_queries(1):
        data2 = list(Test.objects.raw(sql))
    assert data2 == data1
    assert data2 == [rows.t1, rows.t2]


def test_raw_no_table():
    sql = "SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2) AS t;"

    with assert_num_queries(1):
        data1 = list(Test.objects.raw(sql))
    with assert_num_queries(1):
        data2 = list(Test.objects.raw(sql))
    assert data2 == data1
    assert data2 == [Test(pk=1), Test(pk=2)]


def test_cursor_execute_unicode():
    """Tests if queries executed from a DB cursor are not cached."""
    attname_column_list = [f.get_attname_column() for f in Test._meta.fields]
    attnames = [t[0] for t in attname_column_list]
    columns = [t[1] for t in attname_column_list]
    sql = f"SELECT CAST('é' AS CHAR), {', '.join(columns)} FROM {Test._meta.db_table};"

    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute(sql)
        data1 = list(cursor.fetchall())
    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute(sql)
        data2 = list(cursor.fetchall())
    assert data2 == data1
    assert data2 == [("é", *values) for values in Test.objects.values_list(*attnames)]


@pytest.mark.skipif(connection.vendor == "sqlite", reason="SQLite doesn't accept bytes as raw query.")
def test_cursor_execute_bytes():
    attname_column_list = [f.get_attname_column() for f in Test._meta.fields]
    attnames = [t[0] for t in attname_column_list]
    columns = [t[1] for t in attname_column_list]
    sql = f"SELECT CAST('é' AS CHAR), {', '.join(columns)} FROM {Test._meta.db_table};"
    sql = sql.encode("utf-8")

    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute(sql)
        data1 = list(cursor.fetchall())
    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute(sql)
        data2 = list(cursor.fetchall())
    assert data2 == data1
    assert data2 == [("é", *values) for values in Test.objects.values_list(*attnames)]


def test_cursor_execute_no_table():
    sql = "SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2) AS t;"
    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute(sql)
        data1 = list(cursor.fetchall())
    with assert_num_queries(1), connection.cursor() as cursor:
        cursor.execute(sql)
        data2 = list(cursor.fetchall())
    assert data2 == data1
    assert data2 == [(1,), (2,)]


@pytest.mark.usefixtures("final_sql_check")
def test_evicted_generation():
    qs = Test.objects.all()
    assert_tables(qs, Test)
    assert_query_cached(qs)

    # The generation comes back new, so the old result is not served.
    evict_generation(connection.alias, Test._meta.db_table)

    assert_query_cached(qs)


@pytest.mark.usefixtures("final_sql_check")
def test_undecodable_cached_result(caplog):
    qs = Test.objects.all()
    assert_tables(qs, Test)
    assert_query_cached(qs)

    corrupt_entry(qs)

    with caplog.at_level(logging.WARNING, logger="django_cachex.orm"):
        assert_query_cached(qs)
    assert {record.name for record in caplog.records} == {"django_cachex.orm.store"}


def test_unicode_get():
    with assert_num_queries(1), pytest.raises(Test.DoesNotExist):
        Test.objects.get(name="Clémentine")
    with assert_num_queries(0), pytest.raises(Test.DoesNotExist):
        Test.objects.get(name="Clémentine")


@pytest.mark.usefixtures("final_sql_check")
def test_unicode_table_name():
    """Tests if using unicode in table names does not break caching."""
    table_name = "Clémentine"
    if connection.vendor == "postgresql":
        table_name = f'"{table_name}"'
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (taste VARCHAR(20));")
    try:
        qs = Test.objects.extra(tables=["Clémentine"], select={"taste": f"{table_name}.taste"})
        # Unknown to Django, but named by extra(tables=...).
        assert_tables(qs, Test, "Clémentine")
        assert_query_cached(qs)
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE {table_name};")


@pytest.mark.usefixtures("final_sql_check")
def test_unmanaged_model():
    qs = UnmanagedModel.objects.all()
    assert_tables(qs, UnmanagedModel)
    assert_query_cached(qs)


def test_now_annotate():
    """Check that queries with a Now() annotation are not cached #193"""
    qs = Test.objects.annotate(now=Now())
    assert_query_cached(qs, after=1)


@pytest.mark.parametrize(
    "make_queryset",
    [
        pytest.param(lambda: Test.objects.filter(datetime__gte=Now() - datetime.timedelta(days=1)), id="filter"),
        pytest.param(
            lambda: Test.objects.filter(datetime__range=(Now() - datetime.timedelta(days=1), Now())),
            id="range",
        ),
        pytest.param(lambda: Test.objects.annotate(age=Now() - F("datetime")), id="annotate"),
        pytest.param(lambda: Test.objects.order_by(Now() - F("datetime")), id="order_by"),
        pytest.param(
            lambda: Test.objects.filter(pk__in=Test.objects.filter(datetime__lte=Now()).values("pk")),
            id="subquery",
        ),
        pytest.param(
            lambda: Test.objects.filter(Exists(Test.objects.filter(datetime__lte=Now() - datetime.timedelta(days=1)))),
            id="exists",
        ),
        pytest.param(
            lambda: User.objects.annotate(
                past_tests=FilteredRelation("test", condition=Q(test__datetime__lte=Now())),
            ).filter(past_tests__isnull=False),
            id="filtered_relation",
        ),
    ],
)
def test_now_nested(make_queryset):
    """Now() keeps a query from being cached wherever it is."""
    assert_query_cached(make_queryset(), after=1)
