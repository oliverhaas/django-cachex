from django.conf import settings
from django.contrib.postgres.fields import (
    ArrayField,
    DateRangeField,
    DateTimeRangeField,
    DecimalRangeField,
    HStoreField,
    IntegerRangeField,
)
from django.db.models import (
    PROTECT,
    SET_NULL,
    BinaryField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    DurationField,
    FloatField,
    ForeignKey,
    GenericIPAddressField,
    IntegerField,
    JSONField,
    ManyToManyField,
    Model,
    TextChoices,
    UUIDField,
)


class SomeChoices(TextChoices):
    foo = "foo"
    bar = "bar"


class Test(Model):
    __test__ = False  # Not a pytest test class.

    name = CharField(max_length=20)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=SET_NULL)
    public = BooleanField(default=False)
    date = DateField(null=True, blank=True)
    datetime = DateTimeField(null=True, blank=True)
    permission = ForeignKey("auth.Permission", null=True, blank=True, on_delete=PROTECT)

    a_float = FloatField(null=True, blank=True)
    a_decimal = DecimalField(null=True, blank=True, max_digits=5, decimal_places=2)
    a_choice = CharField(max_length=3, choices=SomeChoices.choices, null=True)  # noqa: DJ001
    bin = BinaryField(null=True, blank=True)
    ip = GenericIPAddressField(null=True, blank=True)
    duration = DurationField(null=True, blank=True)
    uuid = UUIDField(null=True, blank=True)
    json = JSONField(null=True, blank=True)

    class Meta:
        ordering = ("name",)

    def __str__(self) -> str:
        return self.name


class TestParent(Model):
    __test__ = False  # Not a pytest test class.

    name = CharField(max_length=20)

    def __str__(self) -> str:
        return self.name


class TestChild(TestParent):
    """Multi-table inheritance: a OneToOneField to TestParent is added automatically."""

    public = BooleanField(default=False)
    permissions = ManyToManyField("auth.Permission", blank=True)


class PostgresModel(Model):
    int_array = ArrayField(IntegerField(null=True, blank=True), size=3, null=True, blank=True)
    hstore = HStoreField(null=True, blank=True)
    int_range = IntegerRangeField(null=True, blank=True)
    decimal_range = DecimalRangeField(null=True, blank=True)
    date_range = DateRangeField(null=True, blank=True)
    datetime_range = DateTimeRangeField(null=True, blank=True)

    class Meta:
        # Tests a schema name in the table name.
        db_table = '"public"."ormtest_postgresmodel"'

    def __str__(self) -> str:
        return f"PostgresModel {self.pk}"


class UnmanagedModel(Model):
    name = CharField(max_length=50)

    class Meta:
        managed = False

    def __str__(self) -> str:
        return self.name
