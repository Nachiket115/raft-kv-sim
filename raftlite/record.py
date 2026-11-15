import json
from functools import partial

from raftlite import scenarios
from raftlite.checker import InvariantViolation
from raftlite.messages import AppendEntriesResponse, RequestVoteResponse
from raftlite.node import RaftNode, Role
from raftlite.scenarios import FUZZ_TICKS, SCENARIOS, ScenarioFailed
from raftlite.sim import Simulator

VERSION = 1
KEYFRAME_TICKS = 200
LABEL_MAX = 20
ROLES = {Role.FOLLOWER: "F", Role.CANDIDATE: "C", Role.LEADER: "L"}
KINDS = {
    "send", "deliver", "drop", "duplicate", "delay", "timer", "crash", "restart",
    "partition", "heal", "cut", "restore", "invoke", "return", "violation",
}
NODE_FIELDS = ("alive", "role", "term", "voted", "commit", "applied")


def label(value):
    match value:
        case ("SESSION", client_id, seq, op):
            text = f"c{client_id}#{seq} " + " ".join(map(str, op))
        case tuple():
            text = " ".join(map(str, value))
        case _:
            text = str(value)
    return text if len(text) <= LABEL_MAX else text[: LABEL_MAX - 1] + "…"


def message(msg):
    fields = {"type": type(msg).__name__}
    if isinstance(msg, RequestVoteResponse):
        fields["granted"] = msg.vote_granted
    elif isinstance(msg, AppendEntriesResponse):
        fields["ok"] = msg.success
    return fields


class Recorder:
    def __init__(self):
        self.frames = []
        self.sim = None
        self._events = []
        self._pending = None
        self._shown = {}
        self._key_at = None

    def event(self, kind, **fields):
        match kind:
            case "send":
                msg = fields["msg"]
                entry = {"src": fields["src"], "dst": fields["dst"], **message(msg)}
                entry["term"] = msg.term
                entry["entries"] = len(getattr(msg, "entries", ()))
                entry["arrive"] = fields["arrive"]
            case "deliver" | "drop" | "duplicate" | "delay":
                entry = {"src": fields["src"], "dst": fields["dst"], "type": type(fields["msg"]).__name__}
                if kind == "drop":
                    entry["reason"] = fields["reason"]
                if kind == "delay":
                    entry["extra"] = fields["extra"]
            case "timer":
                entry = {"node": fields["node"], "kind": fields["timer"].value, "fires": fields["fires"]}
            case "invoke":
                cmd = fields["cmd"]
                entry = {"node": fields["node"], "req": cmd.request_id, "op": label(cmd.command)}
            case "return":
                reply = fields["reply"]
                result = None if reply.result is None else label(reply.result)
                entry = {"node": fields["node"], "req": reply.request_id, "ok": reply.ok, "result": result}
            case "partition":
                entry = {"groups": [list(group) for group in fields["groups"]]}
            case _:
                entry = dict(fields)
        self._events.append({"k": kind, **entry})

    def snapshot(self, sim):
        self.sim = sim
        previous = self._pending["nodes"] if self._pending else {}
        nodes = {}
        for node_id, persistent in sim.persistent.items():
            node = sim.nodes.get(node_id)
            log = persistent.log
            old = previous.get(node_id)
            nodes[node_id] = {
                "alive": node is not None,
                "role": ROLES[node.role] if node else None,
                "term": persistent.current_term,
                "voted": persistent.voted_for,
                "commit": node.commit_index if node else 0,
                "applied": node.last_applied if node else 0,
                "log": old["log"] if old and old["log"] == log else list(log),
            }
        if self._pending and self._pending["t"] == sim.now:
            self._pending["events"] += self._events
            self._pending["nodes"] = nodes
        else:
            self._flush()
            self._pending = {"t": sim.now, "events": self._events, "nodes": nodes}
        self._events = []

    def finish(self, now, violation=None):
        if violation is not None:
            if self._pending is not None and self._pending["t"] != now:
                self._flush()
            if self._pending is None:
                self._pending = {"t": now, "events": [], "nodes": self._shown}
            self._events.append({"k": "violation", **violation})
        if self._pending is not None:
            self._pending["events"] += self._events
        self._events = []
        self._flush()

    def _flush(self):
        pending, self._pending = self._pending, None
        if pending is None:
            return
        t, nodes = pending["t"], pending["nodes"]
        frame = {"t": t, "events": pending["events"]}
        if self._key_at is None or t - self._key_at >= KEYFRAME_TICKS:
            self._key_at = t
            frame["key"] = True
            frame["nodes"] = [self._full(node_id, state) for node_id, state in nodes.items()]
        else:
            changes = [
                change
                for node_id, state in nodes.items()
                if (change := self._change(node_id, state))
            ]
            if changes:
                frame["nodes"] = changes
        self._shown = nodes
        if frame["events"] or "nodes" in frame:
            self.frames.append(frame)

    @staticmethod
    def _full(node_id, state):
        fields = {key: state[key] for key in NODE_FIELDS}
        return {"id": node_id, **fields, "log": [[e.term, label(e.command)] for e in state["log"]]}

    def _change(self, node_id, state):
        old = self._shown.get(node_id)
        if old is None:
            return self._full(node_id, state)
        change = {key: state[key] for key in NODE_FIELDS if state[key] != old[key]}
        new_log, old_log = state["log"], old["log"]
        if new_log is not old_log and new_log != old_log:
            keep = next(
                (i for i, (a, b) in enumerate(zip(new_log, old_log)) if a != b),
                min(len(new_log), len(old_log)),
            )
            change["log"] = {"keep": keep, "add": [[e.term, label(e.command)] for e in new_log[keep:]]}
        return {"id": node_id, **change} if change else None


def detail(error):
    text = str(error).split(" (seed=", 1)[0]
    prefix = f"{getattr(error, 'invariant', '')}: "
    return text.removeprefix(prefix)


def record(call, meta):
    recorder = Recorder()
    original = scenarios.Simulator
    scenarios.Simulator = partial(Simulator, observer=recorder)
    try:
        call()
        violation = None
    except (InvariantViolation, ScenarioFailed) as error:
        invariant = getattr(error, "invariant", "liveness")
        violation = {"invariant": invariant, "detail": detail(error), "tick": recorder.sim.now}
    finally:
        scenarios.Simulator = original
    now = recorder.sim.now
    recorder.finish(now, violation and {key: violation[key] for key in ("invariant", "detail")})
    nodes = sorted({n["id"] for f in recorder.frames if f.get("key") for n in f["nodes"]})
    meta |= {
        "nodes": nodes,
        "ticks": now,
        "result": "fail" if violation else "pass",
        "violation": violation,
    }
    return {"version": VERSION, "meta": meta, "frames": recorder.frames}


def mutant_name(node_cls):
    return None if node_cls in (None, RaftNode) else node_cls.__name__


def record_scenario(name, seed, node_cls=None):
    if name not in SCENARIOS:
        raise ValueError(f"unknown scenario {name!r}")
    if node_cls is None:
        call = partial(scenarios.run, name, seed)
    elif name == "figure8":
        call = partial(scenarios.figure8, seed, node_cls)
    else:
        raise ValueError(f"only figure8 takes a node class, not {name!r}")
    meta = {"name": name, "seed": seed, "source": "scenario", "mutant": mutant_name(node_cls)}
    return record(call, meta)


def record_fuzz(seed, node_cls=None, ticks=FUZZ_TICKS, clients=0):
    call = partial(scenarios.fuzz, seed, node_cls or RaftNode, ticks, clients)
    meta = {"name": f"fuzz {seed}", "seed": seed, "source": "fuzz", "mutant": mutant_name(node_cls)}
    meta["clients"] = clients
    return record(call, meta)


def validate(recording):
    problems = []

    def need(condition, text):
        if not condition:
            problems.append(text)

    need(isinstance(recording, dict), "recording is not an object")
    if problems:
        raise ValueError("; ".join(problems))
    meta, frames = recording.get("meta"), recording.get("frames")
    need(recording.get("version") == VERSION, f"version is not {VERSION}")
    need(isinstance(meta, dict), "meta is not an object")
    need(isinstance(frames, list) and frames, "frames is not a non-empty list")
    if problems:
        raise ValueError("; ".join(problems))
    for key in ("name", "seed", "source", "mutant", "nodes", "ticks", "result", "violation"):
        need(key in meta, f"meta lacks {key!r}")
    need(meta.get("result") in ("pass", "fail"), "meta.result is not pass or fail")
    failed = meta.get("result") == "fail"
    need(failed == (meta.get("violation") is not None), "result and violation disagree")
    need(frames[0].get("key") is True, "first frame is not a keyframe")
    last = -1
    for index, frame in enumerate(frames):
        where = f"frame {index}"
        t = frame.get("t")
        need(isinstance(t, int) and t > last, f"{where}: t is not an increasing integer")
        last = t if isinstance(t, int) else last
        for event in frame.get("events", ()):
            need(event.get("k") in KINDS, f"{where}: unknown event kind {event.get('k')!r}")
        for node in frame.get("nodes", ()):
            need(node.get("id") in meta.get("nodes", ()), f"{where}: unknown node {node.get('id')!r}")
            if frame.get("key"):
                for key in NODE_FIELDS + ("log",):
                    need(key in node, f"{where}: keyframe node lacks {key!r}")
    if meta.get("violation"):
        last = frames[-1].get("events") or [{}]
        need(last[-1].get("k") == "violation", "violation is not the last event")
    if problems:
        raise ValueError("; ".join(problems[:10]))
    return recording


def dumps(recording):
    return json.dumps(recording, separators=(",", ":"), ensure_ascii=False)
