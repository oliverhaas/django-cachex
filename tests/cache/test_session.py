from collections import Counter
from typing import TYPE_CHECKING

import pytest
from django.contrib.sessions.backends.base import UpdateError
from django.contrib.sessions.backends.cache import SessionStore

if TYPE_CHECKING:
    from collections.abc import Iterable


@pytest.fixture
def session(cache) -> Iterable[SessionStore]:
    s = SessionStore()

    yield s

    s.delete()


def test_save(session):
    session.save()
    assert session.exists(session.session_key) is True


def test_delete(session):
    session.save()
    session.delete(session.session_key)
    assert session.exists(session.session_key) is False


def test_flush(session):
    session["foo"] = "bar"
    session.save()
    prev_key = session.session_key
    session.flush()
    assert session.exists(prev_key) is False
    assert session.session_key != prev_key
    assert session.session_key is None
    assert session.modified is True
    assert session.accessed is True


def test_cycle(session):
    session["a"], session["b"] = "c", "d"
    session.save()
    prev_key = session.session_key
    prev_data = list(session.items())
    session.cycle_key()
    assert session.exists(prev_key) is False
    assert session.session_key != prev_key
    assert list(session.items()) == prev_data


def test_cycle_with_no_session_cache(session):
    session["a"], session["b"] = "c", "d"
    session.save()
    prev_data = session.items()
    session = SessionStore(session.session_key)
    assert hasattr(session, "_session_cache") is False
    session.cycle_key()
    assert Counter(session.items()) == Counter(prev_data)


def test_save_doesnt_clear_data(session):
    session["a"] = "b"
    session.save()
    assert session["a"] == "b"


def test_unknown_key_is_replaced_on_save(session):
    session = SessionStore("unknownkey")
    session["cat"] = "dog"
    session.save()
    assert session.session_key not in (None, "unknownkey")
    assert session.exists("unknownkey") is False


def test_session_load_does_not_create_record(session):
    """Loading an unknown session key does not create a session record.
    Creating session records on load is a DOS vulnerability.
    """
    session = SessionStore("someunknownkey")
    session.load()

    assert session.session_key is None
    assert session.exists("someunknownkey") is False
    # provided unknown key was cycled, not reused
    assert session.session_key != "someunknownkey"


def test_session_save_does_not_resurrect_session_logged_out_in_other_context(session):
    """Sessions shouldn't be resurrected by a concurrent request."""
    # Create new session.
    s1 = SessionStore()
    s1["test_data"] = "value1"
    s1.save(must_create=True)

    # Logout in another context.
    s2 = SessionStore(s1.session_key)
    s2.delete()

    # Modify session in first context.
    s1["test_data"] = "value2"
    with pytest.raises(UpdateError):
        # This should throw an exception as the session is deleted, not
        # resurrect the session.
        s1.save()

    assert s1.load() == {}
