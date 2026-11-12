import io

from raftlite import cli
from raftlite.scenarios import SCENARIOS


def call(*argv):
    out = io.StringIO()
    code = cli.main(list(argv), out)
    return code, out.getvalue()


def test_passing_scenario_prints_trace_and_pass_line():
    code, text = call("leader_partitioned", "--seed", "3")
    assert code == 0
    assert text.splitlines()[-1].startswith("PASS leader_partitioned seed=3 ticks=")
    assert len(text.splitlines()) > 10


def test_same_seed_prints_identical_output():
    assert call("split_vote", "--seed", "5") == call("split_vote", "--seed", "5")


def test_list_prints_every_scenario():
    code, text = call("--list")
    assert code == 0
    assert text.split() == sorted(SCENARIOS)


def test_unknown_scenario_exits_2():
    code, text = call("nope")
    assert code == 2
    assert "liveness" in text


def test_failure_prints_trace_seed_and_repro(monkeypatch):
    def broken(seed):
        from raftlite.scenarios import expect
        from raftlite.sim import Simulator

        sim = Simulator(seed=seed)
        sim.run_until(300)
        expect(False, "boom", sim)

    monkeypatch.setitem(SCENARIOS, "broken", broken)
    code, text = call("broken", "--seed", "9")
    assert code == 1
    assert "FAIL broken boom (seed=9" in text
    assert "repro: python -c" in text
