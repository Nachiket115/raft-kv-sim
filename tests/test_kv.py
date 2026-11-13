import pytest

from raftlite.kv import STALE_SEQ, KVStore


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


def session(client_id, seq, op):
    return ("SESSION", client_id, seq, op)


def test_execute_plain_command_returns_ok_and_result():
    kv = KVStore()
    assert kv.execute(("PUT", "a", 1)) == (True, None)
    assert kv.execute(("GET", "a")) == (True, 1)
    assert kv.sessions == {}


def test_execute_turns_an_invalid_command_into_a_failed_reply():
    ok, result = KVStore().execute(("INCR", "a"))
    assert ok is False and "unknown command" in result


def test_session_command_applies_and_records_the_reply():
    kv = KVStore()
    assert kv.execute(session(7, 1, ("PUT", "a", 1))) == (True, None)
    assert kv.data == {"a": 1}
    assert kv.sessions == {7: (1, (True, None))}


def test_duplicate_seq_returns_cached_reply_without_applying_again():
    kv = KVStore()
    kv.execute(session(7, 1, ("PUT", "a", 1)))
    assert kv.execute(session(7, 2, ("DELETE", "a"))) == (True, True)
    kv.execute(("PUT", "a", 2))
    assert kv.execute(session(7, 2, ("DELETE", "a"))) == (True, True)
    assert kv.data == {"a": 2}


def test_duplicate_get_returns_the_value_read_the_first_time():
    kv = KVStore()
    kv.execute(("PUT", "a", 1))
    assert kv.execute(session(7, 1, ("GET", "a"))) == (True, 1)
    kv.execute(("PUT", "a", 2))
    assert kv.execute(session(7, 1, ("GET", "a"))) == (True, 1)


def test_older_seq_is_rejected_and_not_applied():
    kv = KVStore()
    kv.execute(session(7, 1, ("PUT", "a", 1)))
    kv.execute(session(7, 2, ("PUT", "a", 2)))
    assert kv.execute(session(7, 1, ("PUT", "a", 1))) == (False, STALE_SEQ)
    assert kv.data == {"a": 2}
    assert kv.sessions[7] == (2, (True, None))


def test_failed_session_command_is_cached_too():
    kv = KVStore()
    first = kv.execute(session(7, 1, ("INCR", "a")))
    assert first[0] is False
    assert kv.execute(session(7, 1, ("INCR", "a"))) == first
    assert kv.sessions[7] == (1, first)


def test_seq_may_skip_ahead():
    kv = KVStore()
    kv.execute(session(7, 1, ("PUT", "a", 1)))
    assert kv.execute(session(7, 5, ("PUT", "a", 5))) == (True, None)
    assert kv.sessions[7][0] == 5


def test_first_seq_of_a_client_may_be_zero():
    kv = KVStore()
    assert kv.execute(session(7, 0, ("PUT", "a", 1))) == (True, None)
    assert kv.data == {"a": 1}


def test_clients_have_separate_sessions():
    kv = KVStore()
    kv.execute(session(1, 1, ("PUT", "a", 1)))
    kv.execute(session(2, 1, ("PUT", "a", 2)))
    assert kv.data == {"a": 2}
    assert set(kv.sessions) == {1, 2}


def test_same_session_log_gives_same_table():
    log = [
        session(1, 1, ("PUT", "a", 1)),
        session(2, 1, ("DELETE", "a")),
        session(1, 1, ("PUT", "a", 1)),
        session(2, 2, ("GET", "a")),
        session(2, 1, ("DELETE", "a")),
    ]
    one, two = KVStore(), KVStore()
    for cmd in log:
        one.execute(cmd)
        two.execute(cmd)
    assert one.data == two.data == {}
    assert one.sessions == two.sessions == {1: (1, (True, None)), 2: (2, (True, None))}
