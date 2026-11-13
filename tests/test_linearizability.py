import random
from itertools import combinations, permutations

import pytest

from raftlite.kv import KVStore
from raftlite.linearizability import ABSENT, Op, by_key, failing_key, linearizable, step


def put(value, invoke, ret=None, key="a"):
    return Op(("PUT", key, value), invoke, ret)


def get(result, invoke, ret=None, key="a"):
    return Op(("GET", key), invoke, ret, result)


def delete(result, invoke, ret=None, key="a"):
    return Op(("DELETE", key), invoke, ret, result)


def test_empty_history_is_linearizable():
    assert linearizable([])
    assert failing_key([]) is None


def test_sequential_history_is_linearizable():
    assert linearizable(
        [get(None, 0, 5), put(1, 10, 15), get(1, 20, 25), delete(True, 30, 35), get(None, 40, 45)]
    )


def test_order_of_the_list_does_not_matter():
    assert linearizable([get(1, 20, 25), put(1, 10, 15)])


def test_read_that_misses_a_finished_write_is_not_linearizable():
    assert not linearizable([put(1, 0, 10), get(None, 20, 30)])


def test_stale_read_after_a_newer_write_is_not_linearizable():
    assert not linearizable([put(1, 0, 10), put(2, 20, 30), get(1, 40, 50)])


def test_read_concurrent_with_a_write_may_see_either_value():
    assert linearizable([put(1, 0, 10), put(2, 20, 40), get(1, 25, 35)])
    assert linearizable([put(1, 0, 10), put(2, 20, 40), get(2, 25, 35)])
    assert not linearizable([put(1, 0, 10), put(2, 20, 40), get(3, 25, 35)])


def test_two_reads_cannot_see_a_write_and_then_unsee_it():
    assert not linearizable([put(1, 0, 10), put(2, 20, 60), get(2, 25, 35), get(1, 40, 50)])


def test_concurrent_writes_may_land_in_either_order():
    assert linearizable([put(1, 0, 10), put(2, 0, 10), get(1, 20, 30)])
    assert linearizable([put(1, 0, 10), put(2, 0, 10), get(2, 20, 30)])


def test_delete_reports_whether_the_key_was_there():
    assert linearizable([put(1, 0, 10), delete(True, 20, 30), delete(False, 40, 50)])
    assert not linearizable([put(1, 0, 10), delete(False, 20, 30)])
    assert not linearizable([delete(True, 0, 10)])


def test_write_applied_twice_is_not_linearizable():
    assert not linearizable([put(1, 0, 10), put(2, 20, 30), get(2, 40, 50), get(1, 60, 70)])
    assert not linearizable([put(1, 0, 10), delete(True, 20, 30), delete(True, 40, 50)])


def test_put_of_none_is_a_present_key():
    assert linearizable([put(None, 0, 10), get(None, 20, 30), delete(True, 40, 50)])
    assert not linearizable([put(None, 0, 10), delete(False, 20, 30)])


def test_operations_that_touch_at_the_same_tick_count_as_concurrent():
    assert linearizable([put(1, 0, 10), get(None, 10, 20)])
    assert not linearizable([put(1, 0, 10), get(None, 11, 20)])


def test_pending_write_may_have_taken_effect():
    assert linearizable([put(1, 0), get(1, 10, 20)])


def test_pending_write_may_never_have_taken_effect():
    assert linearizable([put(1, 0), get(None, 10, 20)])
    assert linearizable([put(1, 0), get(None, 10, 20), get(None, 30, 40)])


def test_pending_write_may_take_effect_late():
    assert linearizable([put(1, 0), get(None, 10, 20), get(1, 30, 40)])


def test_pending_write_cannot_take_effect_and_then_vanish():
    assert not linearizable([put(1, 0), get(1, 10, 20), get(None, 30, 40)])


def test_pending_write_cannot_take_effect_before_it_was_invoked():
    assert not linearizable([get(1, 0, 10), put(1, 20)])


def test_pending_write_takes_effect_at_most_once():
    assert not linearizable(
        [put(1, 0), get(1, 10, 20), put(2, 30, 40), get(2, 50, 60), get(1, 70, 80)]
    )


def test_pending_delete_and_read_need_no_result():
    assert linearizable([put(1, 0, 10), delete(None, 20), get(None, 30, 40)])
    assert linearizable([put(1, 0, 10), delete(None, 20), get(1, 30, 40)])
    assert linearizable([put(1, 0, 10), get("anything", 20)])


def test_history_of_only_pending_operations_is_linearizable():
    assert linearizable([put(1, 0), delete(None, 5), get(None, 7)])


def test_keys_are_checked_independently():
    history = [
        put(1, 0, 10, key="a"),
        put(2, 0, 10, key="b"),
        get(2, 20, 30, key="b"),
        get(None, 20, 30, key="a"),
    ]
    assert set(by_key(history)) == {"a", "b"}
    assert failing_key(history) == "a"
    assert failing_key(history[:3]) is None


def test_long_concurrent_history_is_decided_quickly():
    history = []
    for i in range(300):
        start = i * 10
        history += [put(i, start, start + 25), get(i, start + 3, start + 28)]
    assert linearizable(history)
    history.append(get(0, 5000, 5010))
    assert not linearizable(history)


def test_unknown_command_is_rejected_by_the_model():
    with pytest.raises(ValueError):
        step(ABSENT, ("INCR", "a"))


def random_op(rng):
    key = rng.choice("ab")
    kind = rng.choice(["PUT", "GET", "DELETE"])
    if kind == "PUT":
        return ("PUT", key, rng.choice([None, 0, 1, 2, "x"]))
    return (kind, key)


def test_model_agrees_with_the_kv_store_on_random_sequential_histories():
    for seed in range(200):
        rng = random.Random(seed)
        kv = KVStore()
        state = {}
        history = []
        for tick in range(rng.randint(1, 40)):
            command = random_op(rng)
            key = command[1]
            expected = kv.apply(command)
            result, state[key] = step(state.get(key, ABSENT), command)
            assert result == expected and type(result) is type(expected), (seed, tick, command)
            history.append(Op(command, 2 * tick, 2 * tick + 1, expected))
        present = {key: value for key, value in state.items() if value is not ABSENT}
        assert present == kv.data, seed
        assert failing_key(history) is None, seed


def brute_force(ops):
    returned = [op for op in ops if op.ret is not None]
    pending = [op for op in ops if op.ret is None]
    for count in range(len(pending) + 1):
        for chosen in combinations(pending, count):
            for order in permutations(returned + list(chosen)):
                if any(
                    later.ret is not None and later.ret < earlier.invoke
                    for i, earlier in enumerate(order)
                    for later in order[i + 1 :]
                ):
                    continue
                state = ABSENT
                for op in order:
                    result, state = step(state, op.command)
                    if op.ret is not None and result != op.result:
                        break
                else:
                    return True
    return False


def random_history(rng):
    ops = []
    for _ in range(rng.randint(1, 6)):
        kind = rng.choice(["PUT", "GET", "DELETE"])
        invoke = rng.randint(0, 12)
        ret = None if rng.random() < 0.25 else invoke + rng.randint(0, 6)
        if kind == "PUT":
            ops.append(Op(("PUT", "a", rng.choice([1, 2])), invoke, ret))
        elif kind == "GET":
            ops.append(Op(("GET", "a"), invoke, ret, rng.choice([None, 1, 2])))
        else:
            ops.append(Op(("DELETE", "a"), invoke, ret, rng.choice([True, False])))
    return ops


def test_search_agrees_with_brute_force_on_small_random_histories():
    verdicts = set()
    for seed in range(1500):
        ops = random_history(random.Random(seed))
        expected = brute_force(ops)
        assert linearizable(ops) == expected, (seed, ops)
        verdicts.add(expected)
    assert verdicts == {True, False}
