import hashlib
import re
from functools import partial

import pytest

from raftlite import scenarios
from raftlite.sim import Simulator
from tests.test_golden import GOLDEN

TRACE_LINES = {
    "deliver": r" deliver ",
    "duplicate": r" duplicate ",
    "delay": r" delay \+",
    "timer": r" timer for t=",
    "crash": r"^\d+ n\d+ crash$",
    "restart": r"^\d+ n\d+ restart$",
    "partition": r"^\d+ partition ",
    "heal": r"^\d+ heal$",
    "invoke": r" client -> n\d+ (?!unreachable)",
    "return": r" -> client ",
}


class Spy:
    def __init__(self):
        self.events = []
        self.snapshots = 0

    def event(self, kind, **fields):
        self.events.append((kind, fields))

    def snapshot(self, sim):
        self.snapshots += 1

    def count(self, kind, **match):
        return sum(
            1
            for seen, fields in self.events
            if seen == kind and all(fields[key] == value for key, value in match.items())
        )


def observed(monkeypatch):
    spy = Spy()
    monkeypatch.setattr(scenarios, "Simulator", partial(Simulator, observer=spy))
    return spy


def digest(sim):
    return hashlib.sha256(sim.trace_text().encode()).hexdigest()


def lines(sim, pattern):
    return sum(1 for line in sim.trace if re.search(pattern, line))


@pytest.mark.parametrize("name, seed", GOLDEN, ids=lambda value: str(value))
def test_observed_run_matches_the_pinned_trace(name, seed, monkeypatch):
    spy = observed(monkeypatch)
    sim = scenarios.fuzz(seed) if name == "fuzz" else scenarios.run(name, seed)
    assert digest(sim) == GOLDEN[name, seed]
    assert spy.events and spy.snapshots


@pytest.mark.parametrize("seed", range(3))
def test_observed_client_fuzz_matches_the_unobserved_trace(seed, monkeypatch):
    plain = scenarios.fuzz(seed, clients=3)
    observed(monkeypatch)
    assert scenarios.fuzz(seed, clients=3).trace == plain.trace


def test_events_agree_with_the_trace(monkeypatch):
    spy = observed(monkeypatch)
    sim = scenarios.fuzz(0, clients=3)
    for kind, pattern in TRACE_LINES.items():
        assert spy.count(kind) == lines(sim, pattern) > 0, kind
    fault = lines(sim, r" dropped \(fault\) ")
    cut = lines(sim, r" dropped \(partition\) ")
    down = lines(sim, r" dropped ") - fault - cut
    assert spy.count("drop", reason="fault") == fault > 0
    assert spy.count("drop", reason="partition") == cut > 0
    assert spy.count("drop", reason="down") == down > 0
    assert spy.count("send") == lines(sim, r" send ") - fault + spy.count("duplicate")


def test_send_carries_the_message_and_its_arrival_tick():
    spy = Spy()
    sim = Simulator(seed=1, observer=spy)
    sim.run_until(1000)
    sends = [fields for kind, fields in spy.events if kind == "send"]
    delivered = [fields for kind, fields in spy.events if kind == "deliver"]
    assert sends and len(delivered) <= len(sends)
    for fields in sends:
        assert type(fields["msg"]).__name__ in sim.trace_text()
        assert 1 <= fields["arrive"] <= sim.now + sim.config.message_delay_max


def test_snapshot_runs_before_the_check():
    order = []

    class Recording(Spy):
        def snapshot(self, sim):
            order.append("snapshot")

    class Checking:
        def check(self, sim):
            order.append("check")

    Simulator(seed=1, observer=Recording(), checker=Checking())
    assert order == ["snapshot", "check"]


def test_no_observer_is_the_default():
    assert Simulator(seed=1)._observer is None
