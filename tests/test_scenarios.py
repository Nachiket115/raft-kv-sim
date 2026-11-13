import pytest

from raftlite.checker import InvariantViolation
from raftlite.linearizability import Op
from raftlite.scenarios import (
    SCENARIOS,
    ScenarioFailed,
    check_linearizable,
    expect,
    fuzz,
    leaders,
    run,
)
from raftlite.sim import Simulator


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_scenario_passes_on_every_seed(name):
    for seed in range(50):
        run(name, seed)


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_scenario_traces_are_byte_identical_for_a_seed(name):
    assert run(name, 7).trace_text() == run(name, 7).trace_text()


def test_scenario_failure_reports_seed_and_trace_tail():
    sim = Simulator(seed=99)
    sim.run_until(500)
    with pytest.raises(ScenarioFailed) as failure:
        expect(False, "boom", sim)
    assert failure.value.seed == 99
    assert "boom (seed=99" in str(failure.value)
    assert sim.trace[-1] in str(failure.value)


def test_split_vote_leaves_terms_without_a_leader():
    sim = SCENARIOS["split_vote"](3)
    (leader,) = leaders(sim)
    final = sim.persistent[leader].current_term
    assert len(sim.checker.leaders_by_term) < final


def stale_read(seed):
    sim = Simulator(seed=seed)
    sim.run_until(100)
    sim.history.update(
        {
            "w1": Op(("PUT", "a", 1), 10, 20, None, "w1"),
            "w2": Op(("PUT", "a", 2), 30, 40, None, "w2"),
            "r": Op(("GET", "a"), 50, 60, 1, "r"),
            "lost": Op(("DELETE", "a"), 70, request_id="lost"),
            "other": Op(("PUT", "b", 9), 10, 20, None, "other"),
        }
    )
    return sim


def test_history_that_is_not_linearizable_fails_the_run(monkeypatch):
    monkeypatch.setitem(SCENARIOS, "stale_read", stale_read)
    with pytest.raises(InvariantViolation) as failure:
        run("stale_read", 12)
    message = str(failure.value)
    assert failure.value.invariant == "linearizability"
    assert failure.value.seed == 12 and "seed=12" in message
    assert "no valid order for the operations on key 'a'" in message
    assert "  000010 PUT a 1  000020 -> None  (request 'w1')" in message
    assert "  000050 GET a  000060 -> 1  (request 'r')" in message
    assert "  000070 DELETE a  never returned  (request 'lost')" in message
    assert "PUT b 9" not in message
    assert message.endswith("repro: python -c \"from raftlite.scenarios import *; run('stale_read', 12)\"")


def test_linearizable_history_passes_the_check():
    sim = stale_read(1)
    sim.history["r"].result = 2
    check_linearizable(sim)


def test_scenarios_and_fuzz_leave_a_history_that_was_checked():
    assert [op.command for op in run("leader_partitioned", 3).history.values()] == [
        ("PUT", "stale", 1),
        ("PUT", "a", 1),
    ]
    history = fuzz(3).history
    assert history and all(op.command[0] == "PUT" for op in history.values())
    assert any(op.ret is not None for op in history.values())
