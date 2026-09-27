from django.conf import settings
from django.db.models import SET_NULL, CharField, ForeignKey, Model, Q, UniqueConstraint


class TestModel(Model):
    __test__ = False  # Not a pytest test class.

    name = CharField(max_length=20)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=SET_NULL)

    class Meta:
        ordering = ("name",)
        constraints = [
            UniqueConstraint(
                fields=["name"],
                condition=Q(owner=None),
                name="unique_name",
            ),
        ]

    def __str__(self) -> str:
        return self.name
