import pytest

from raftlite.scenarios import SCENARIOS, ScenarioFailed, expect, leaders, run
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
