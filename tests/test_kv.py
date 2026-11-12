import pytest

from raftlite.kv import KVStore


def test_put_then_get():
    kv = KVStore()
    assert kv.apply(("PUT", "a", 1)) is None
    assert kv.apply(("GET", "a")) == 1


def test_put_overwrites():
    kv = KVStore()
    kv.apply(("PUT", "a", 1))
    kv.apply(("PUT", "a", 2))
    assert kv.apply(("GET", "a")) == 2


def test_get_missing_returns_none():
    assert KVStore().apply(("GET", "nope")) is None


def test_delete_existing_and_missing():
    kv = KVStore()
    kv.apply(("PUT", "a", 1))
    assert kv.apply(("DELETE", "a")) is True
    assert kv.apply(("GET", "a")) is None
    assert kv.apply(("DELETE", "a")) is False


def test_delete_key_with_none_value():
    kv = KVStore()
    kv.apply(("PUT", "a", None))
    assert kv.apply(("DELETE", "a")) is True


def test_unknown_command_raises():
    with pytest.raises(ValueError):
        KVStore().apply(("INCR", "a"))


def test_wrong_arity_raises():
    with pytest.raises(ValueError):
        KVStore().apply(("PUT", "a"))


def test_same_log_gives_same_state():
    log = [("PUT", "a", 1), ("PUT", "b", 2), ("DELETE", "a"), ("GET", "b"), ("PUT", "a", 3)]
    one, two = KVStore(), KVStore()
    for cmd in log:
        one.apply(cmd)
        two.apply(cmd)
    assert one.data == two.data == {"b": 2, "a": 3}
