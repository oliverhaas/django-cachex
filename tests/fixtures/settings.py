"""Django settings fixture."""

import pytest

from tests.settings_wrapper import SettingsWrapper


@pytest.fixture
def settings():
    """A Django settings object which restores changes after the testrun."""
    wrapper = SettingsWrapper()
    yield wrapper
    wrapper.finalize()
