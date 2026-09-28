# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

import pytest
from django.contrib.auth.models import User

from tests.orm.admin_tests.models import TestModel


@pytest.mark.django_db
def test_save_test_model(client):
    """Model 'TestModel' has UniqueConstraint which caused problems when saving TestModelAdmin in Django >= 4.1."""
    user = User.objects.create(username="admin", is_staff=True, is_superuser=True)
    client.force_login(user)
    response = client.post("/admin/admin_tests/testmodel/add/", {"name": "test", "public": True})
    assert response.status_code == 302
    assert TestModel.objects.count() == 1
