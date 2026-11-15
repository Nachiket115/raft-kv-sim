import io
import json

from raftlite import cli, record
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


def add_broken(monkeypatch):
    def broken(seed):
        from raftlite import scenarios

        sim = scenarios.Simulator(seed=seed)
        sim.run_until(300)
        scenarios.expect(False, "boom", sim)

    monkeypatch.setitem(SCENARIOS, "broken", broken)


def test_record_writes_valid_json_for_passing_run(tmp_path):
    path = tmp_path / "run.json"
    code, text = call("leader_partitioned", "--seed", "3", "--record", str(path))
    recording = json.loads(path.read_text())
    assert code == 0
    assert text.startswith("PASS leader_partitioned seed=3 ticks=")
    assert text.strip().endswith(f"recorded {path}")
    assert recording["meta"]["result"] == "pass"
    assert recording["meta"]["seed"] == 3
    record.validate(recording)


def test_record_failing_run_writes_file_and_exits_1(tmp_path, monkeypatch):
    add_broken(monkeypatch)
    path = tmp_path / "run.json"
    code, text = call("broken", "--seed", "9", "--record", str(path))
    recording = json.loads(path.read_text())
    assert code == 1
    assert text.startswith("FAIL broken seed=9 liveness: boom")
    assert recording["meta"]["result"] == "fail"
    assert recording["meta"]["violation"]["invariant"] == "liveness"


def test_record_unknown_scenario_exits_2_and_writes_nothing(tmp_path):
    path = tmp_path / "run.json"
    code, _ = call("nope", "--record", str(path))
    assert code == 2
    assert not path.exists()


def test_record_invalid_recording_exits_2_and_writes_nothing(tmp_path, monkeypatch):
    def reject(recording):
        raise ValueError("bad frame")

    monkeypatch.setattr(cli, "validate", reject)
    path = tmp_path / "run.json"
    code, text = call("split_vote", "--record", str(path))
    assert code == 2
    assert "bad frame" in text
    assert not path.exists()


def test_record_unwritable_path_exits_2(tmp_path):
    code, text = call("split_vote", "--record", str(tmp_path / "missing" / "run.json"))
    assert code == 2
    assert "missing" in text


def test_record_same_seed_writes_identical_bytes(tmp_path):
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    call("split_vote", "--seed", "5", "--record", str(first))
    call("split_vote", "--seed", "5", "--record", str(second))
    assert first.read_bytes() == second.read_bytes()
