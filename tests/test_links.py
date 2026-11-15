import pytest

from raftlite.sim import Simulator
from tests.test_observer import Spy
from tests.test_sim import IdleNode, PingNode


def senders(sim):
    return {node_id: {msg[1] for msg in node.received} for node_id, node in sim.nodes.items()}


def clear(sim):
    for node in sim.nodes.values():
        node.received.clear()


def test_cut_link_blocks_only_that_pair_in_both_directions():
    spy = Spy()
    sim = Simulator(seed=33, node_cls=PingNode, observer=spy)
    sim.run_until(500)
    sim.cut_link(0, 1)
    clear(sim)
    sim.run_until(1500)
    heard = senders(sim)
    assert heard[0] == {2, 3, 4}
    assert heard[1] == {2, 3, 4}
    assert heard[2] == {0, 1, 3, 4}
    assert any(" n0 -> n1 dropped (cut) " in line for line in sim.trace)
    assert any(" n1 -> n0 dropped (cut) " in line for line in sim.trace)
    assert spy.count("drop", reason="cut") > 0 == spy.count("drop", reason="partition")


def test_restored_link_delivers_again():
    sim = Simulator(seed=33, node_cls=PingNode)
    sim.cut_link(3, 4)
    sim.run_until(1000)
    sim.restore_link(4, 3)
    clear(sim)
    sim.run_until(2000)
    assert senders(sim) == {i: set(range(5)) - {i} for i in range(5)}


def test_heal_leaves_cut_links_in_place():
    sim = Simulator(seed=33, node_cls=PingNode)
    sim.cut_link(0, 1)
    sim.partition([0, 2])
    sim.heal()
    clear(sim)
    sim.run_until(1500)
    assert 1 not in senders(sim)[0]
    assert senders(sim)[0] == {2, 3, 4}


def test_cut_and_restore_are_traced_and_observed_with_the_pair_in_order():
    spy = Spy()
    sim = Simulator(seed=1, node_cls=IdleNode, observer=spy)
    before = spy.snapshots
    sim.cut_link(4, 2)
    sim.restore_link(2, 4)
    assert [line.split(" ", 1)[1] for line in sim.trace[-2:]] == ["cut n2-n4", "restore n2-n4"]
    assert spy.events[-2:] == [("cut", {"a": 2, "b": 4}), ("restore", {"a": 2, "b": 4})]
    assert spy.snapshots == before + 2


def test_links_reject_unknown_or_identical_nodes():
    sim = Simulator(seed=1, node_cls=IdleNode)
    for a, b in ((0, 9), (2, 2)):
        with pytest.raises(ValueError):
            sim.cut_link(a, b)
        with pytest.raises(ValueError):
            sim.restore_link(a, b)


def test_unused_link_cuts_leave_the_trace_unchanged():
    sim = Simulator(seed=5)
    sim.run_until(2000)
    assert not any(" cut " in line or " restore " in line for line in sim.trace)
    assert not sim._cut
