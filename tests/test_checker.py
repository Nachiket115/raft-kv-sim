import pytest

from raftlite.checker import Checker, InvariantViolation
from raftlite.messages import ClientCommand, LogEntry
from raftlite.node import RaftNode, Role
from raftlite.sim import SimConfig, Simulator


class CountingChecker(Checker):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def check(self, sim):
        self.calls += 1
        super().check(sim)


class DoubleVoter(RaftNode):
    def _can_grant_vote(self, msg):
        return msg.term == self.persistent.current_term


def replicated(seed=41):
    sim = Simulator(seed=seed)
    sim.run_until(3000)
    (leader,) = [i for i, node in sim.nodes.items() if node.role is Role.LEADER]
    for i in range(3):
        sim.submit(leader, ClientCommand(i, ("PUT", f"k{i}", i)))
    sim.run_until(sim.now + 500)
    follower = next(i for i in sim.nodes if i != leader)
    assert sim.nodes[follower].applied == sim.nodes[leader].applied
    assert len(sim.nodes[leader].applied) == 3
    return sim, leader, follower


def violation(sim):
    with pytest.raises(InvariantViolation) as caught:
        sim.checker.check(sim)
    return caught.value


def test_healthy_run_passes_and_fills_the_checker_history():
    sim, leader, follower = replicated()
    sim.crash(follower)
    sim.submit(leader, ClientCommand(3, ("DELETE", "k0")))
    sim.run_until(sim.now + 100)
    sim.restart(follower)
    sim.run_until(sim.now + 500)
    term = sim.persistent[leader].current_term
    assert sim.checker.leaders_by_term == {term: leader}
    assert sim.checker.applied == dict(sim.nodes[follower].applied)
    assert sorted(sim.checker.committed) == [1, 2, 3, 4]


def test_checker_runs_once_after_every_step():
    checker = CountingChecker()
    sim = Simulator(seed=42, checker=checker)
    assert checker.calls == 1
    steps = 0
    while sim.now < 3000 and sim.step():
        steps += 1
    assert checker.calls == 1 + steps
    sim.submit(0, ClientCommand(1, ("GET", "k")))
    sim.crash(0)
    sim.restart(0)
    assert checker.calls == 4 + steps


def test_two_leaders_in_one_term_break_election_safety():
    sim, leader, follower = replicated()
    sim.nodes[follower].role = Role.LEADER
    error = violation(sim)
    assert error.invariant == "election safety"
    assert f"n{leader}" in str(error) and f"n{follower}" in str(error)


def test_leader_dropping_its_own_entry_breaks_leader_append_only():
    sim, leader, _ = replicated()
    del sim.persistent[leader].log[-1]
    assert violation(sim).invariant == "leader append-only"


def test_same_index_and_term_with_different_entries_breaks_log_matching():
    sim, _, follower = replicated()
    log = sim.persistent[follower].log
    log[0] = LogEntry(log[0].term, ("PUT", "other", 0))
    assert violation(sim).invariant == "log matching"


def test_later_leader_missing_a_committed_entry_breaks_leader_completeness():
    sim, _, follower = replicated()
    sim.persistent[follower].log.clear()
    sim.persistent[follower].current_term += 1
    sim.nodes[follower].role = Role.LEADER
    assert violation(sim).invariant == "leader completeness"


def test_different_entries_committed_at_one_index_break_state_machine_safety():
    sim, _, follower = replicated()
    log = sim.persistent[follower].log
    log[:] = [LogEntry(entry.term + 7, ("PUT", "other", 0)) for entry in log]
    assert violation(sim).invariant == "state machine safety"


def test_different_commands_applied_at_one_index_break_state_machine_safety():
    sim, _, follower = replicated()
    sim.nodes[follower].applied[0] = (1, ("PUT", "other", 0))
    assert violation(sim).invariant == "state machine safety"


def test_gap_in_a_nodes_applied_sequence_breaks_state_machine_safety():
    sim, _, follower = replicated()
    del sim.nodes[follower].applied[0]
    assert violation(sim).invariant == "state machine safety"


def test_violation_carries_seed_time_and_trace():
    sim, _, follower = replicated(seed=43)
    sim.nodes[follower].role = Role.LEADER
    error = violation(sim)
    assert error.seed == 43
    assert error.trace == sim.trace
    assert f"seed=43, t={sim.now}" in str(error)
    assert sim.trace[-1] in str(error)


def test_double_voting_node_is_caught_by_election_safety_on_some_seed():
    config = SimConfig(election_timeout_min=150, election_timeout_max=155)
    caught = []
    for seed in range(20):
        try:
            Simulator(seed=seed, config=config, node_cls=DoubleVoter).run_until(3000)
        except InvariantViolation as error:
            caught.append(error.invariant)
    assert "election safety" in caught
