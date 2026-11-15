import hashlib
import json
from functools import partial

import pytest

from raftlite import record, scenarios
from raftlite.record import Recorder, dumps, label, record_fuzz, record_scenario, validate
from raftlite.sim import Simulator
from tests import mutants
from tests.test_golden import GOLDEN

BUDGET = 1_500_000


def replay(frames):
    state = {}
    for frame in frames:
        for node in frame.get("nodes", ()):
            node = dict(node)
            node_id = node.pop("id")
            if frame.get("key"):
                state[node_id] = node
                continue
            log = node.pop("log", None)
            current = state.setdefault(node_id, {})
            current.update(node)
            if log is not None:
                current["log"] = current["log"][: log["keep"]] + log["add"]
        yield frame["t"], {node_id: dict(fields) for node_id, fields in state.items()}


def final_state(frames):
    return list(replay(frames))[-1][1]


def expected_state(sim):
    roles = {"follower": "F", "candidate": "C", "leader": "L"}
    state = {}
    for node_id, persistent in sim.persistent.items():
        node = sim.nodes.get(node_id)
        state[node_id] = {
            "alive": node is not None,
            "role": roles[node.role.value] if node else None,
            "term": persistent.current_term,
            "voted": persistent.voted_for,
            "commit": node.commit_index if node else 0,
            "applied": node.last_applied if node else 0,
            "log": [[entry.term, label(entry.command)] for entry in persistent.log],
        }
    return state


def recorded_fuzz(seed, clients=0):
    recorder = Recorder()
    original = scenarios.Simulator
    scenarios.Simulator = partial(Simulator, observer=recorder)
    try:
        sim = scenarios.fuzz(seed, clients=clients)
    finally:
        scenarios.Simulator = original
    recorder.finish()
    return sim, recorder


def kinds(recording):
    return {event["k"] for frame in recording["frames"] for event in frame["events"]}


def test_scenario_recording_has_meta_frames_and_expected_events():
    recording = validate(record_scenario("leader_partitioned", 3))
    assert recording["version"] == 1
    assert recording["meta"] == {
        "name": "leader_partitioned",
        "seed": 3,
        "source": "scenario",
        "mutant": None,
        "nodes": [0, 1, 2, 3, 4],
        "ticks": scenarios.run("leader_partitioned", 3).now,
        "result": "pass",
        "violation": None,
    }
    first = recording["frames"][0]
    assert first["t"] == 0 and first["key"] is True and len(first["nodes"]) == 5
    assert {"send", "deliver", "timer", "partition", "invoke", "return", "drop"} <= kinds(recording)
    json.loads(dumps(recording))


def test_first_events_follow_simulator_order():
    recording = record_scenario("leader_partitioned", 3)
    first = recording["frames"][0]["events"]
    assert [event["k"] for event in first] == ["timer"] * 5
    assert [event["node"] for event in first] == [0, 1, 2, 3, 4]
    sends = [
        (frame["t"], event)
        for frame in recording["frames"]
        for event in frame["events"]
        if event["k"] == "send"
    ]
    assert sends[0][1]["type"] == "RequestVote" and sends[0][1]["entries"] == 0
    assert all(t < event["arrive"] for t, event in sends)


def test_recording_does_not_change_the_trace(monkeypatch):
    for name, seed in (("leader_partitioned", 0), ("figure8", 1), ("fuzz", 2)):
        monkeypatch.setattr(scenarios, "Simulator", partial(Simulator, observer=Recorder()))
        sim = scenarios.fuzz(seed) if name == "fuzz" else scenarios.run(name, seed)
        assert hashlib.sha256(sim.trace_text().encode()).hexdigest() == GOLDEN[name, seed]


@pytest.mark.parametrize("clients", [0, 3])
def test_replaying_the_deltas_rebuilds_the_final_node_state(clients):
    sim, recorder = recorded_fuzz(4, clients)
    assert final_state(recorder.frames) == expected_state(sim)


def test_keyframes_agree_with_the_deltas(monkeypatch):
    _, keyed = recorded_fuzz(2, clients=3)
    monkeypatch.setattr(record, "KEYFRAME_TICKS", 10**9)
    _, plain = recorded_fuzz(2, clients=3)
    assert sum(1 for frame in plain.frames if frame.get("key")) == 1
    plain_states = list(replay(plain.frames))
    for t, state in replay(keyed.frames):
        assert state == [s for at, s in plain_states if at <= t][-1], t
    assert set(t for t, _ in plain_states) <= set(frame["t"] for frame in keyed.frames)


def test_keyframes_are_at_most_the_interval_apart_in_ticks():
    _, recorder = recorded_fuzz(1)
    keys = [frame["t"] for frame in recorder.frames if frame.get("key")]
    times = [frame["t"] for frame in recorder.frames]
    assert len(keys) > 10
    for before, after in zip(keys, keys[1:]):
        gap_end = min(t for t in times if t >= before + record.KEYFRAME_TICKS)
        assert after == gap_end


@pytest.mark.parametrize(
    "call, invariant",
    [
        (partial(record_fuzz, 0, mutants.NoStepDown), "election safety"),
        (partial(record_fuzz, 1, mutants.StaleRead, clients=3), "linearizability"),
        (partial(record_scenario, "figure8", 0, mutants.CommitOldTermNoNoop), "leader completeness"),
    ],
    ids=["NoStepDown", "StaleRead", "CommitOldTermNoNoop"],
)
def test_mutant_run_records_the_violation_and_stops_there(call, invariant):
    recording = validate(call())
    meta = recording["meta"]
    assert meta["result"] == "fail"
    assert meta["violation"]["invariant"] == invariant
    assert meta["violation"]["detail"] and "seed=" not in meta["violation"]["detail"]
    last = recording["frames"][-1]
    assert last["t"] == meta["violation"]["tick"] == meta["ticks"]
    detail = meta["violation"]["detail"]
    assert last["events"][-1] == {"k": "violation", "invariant": invariant, "detail": detail}
    assert scenarios.Simulator is Simulator


def test_failed_expectation_is_recorded_as_liveness():
    for seed in range(40):
        recording = record_fuzz(seed, mutants.NoNoop)
        if recording["meta"]["result"] == "fail":
            break
    validate(recording)
    assert recording["meta"]["violation"]["invariant"] == "liveness"
    assert recording["meta"]["mutant"] == "NoNoop"


def test_unexpected_error_is_recorded_with_the_state_at_the_raise():
    def call():
        sim = scenarios.Simulator(seed=1)
        sim.run_until(500)
        sim.persistent[0].current_term = 99
        raise RuntimeError("boom")

    meta = {"name": "boom", "seed": 1, "source": "scenario", "mutant": None}
    recording = validate(record.record(call, meta))
    assert meta["violation"] == {"invariant": "error", "detail": "RuntimeError: boom", "tick": 500}
    assert recording["frames"][-1]["t"] == 500
    assert final_state(recording["frames"])[0]["term"] == 99
    assert scenarios.Simulator is Simulator


def test_violation_detail_keeps_multiline_text_but_not_the_trace():
    recording = record_fuzz(1, mutants.StaleRead, clients=3)
    text = recording["meta"]["violation"]["detail"]
    assert text.startswith("no valid order for the operations on key 'x':\n")
    assert "repro:" not in text and " send " not in text


def test_bad_arguments_are_rejected_and_the_simulator_is_restored():
    with pytest.raises(ValueError):
        record_scenario("nope", 1)
    with pytest.raises(ValueError):
        record_scenario("split_vote", 1, mutants.NoNoop)
    assert scenarios.Simulator is Simulator


def test_labels_are_short_and_readable():
    assert label(("NOOP",)) == "NOOP"
    assert label(("SESSION", 2, 7, ("PUT", "x", "c2s7"))) == "c2#7 PUT x c2s7"
    assert label(("SESSION", 2, 7, None)) == "c2#7 None"
    assert label(("PUT", "k" * 40, 1)) == "PUT " + "k" * 15 + "…"
    assert len(label("y" * 100)) == record.LABEL_MAX


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_long_client_fuzz_recording_fits_the_budget(seed):
    size = len(dumps(record_fuzz(seed, clients=3)).encode())
    assert size < BUDGET


def test_validate_rejects_broken_recordings():
    good = record_scenario("split_vote", 1)
    validate(json.loads(dumps(good)))
    broken = [
        {},
        {**good, "version": 2},
        {**good, "frames": []},
        {**good, "frames": [{**good["frames"][0], "key": False}] + good["frames"][1:]},
        {**good, "frames": good["frames"][:1] + good["frames"][:1]},
        {**good, "frames": [{**good["frames"][0], "events": [{"k": "boom"}]}]},
        {**good, "meta": {**good["meta"], "result": "fail"}},
    ]
    for recording in broken:
        with pytest.raises(ValueError):
            validate(recording)
