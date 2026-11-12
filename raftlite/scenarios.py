from contextlib import contextmanager
from dataclasses import replace

from raftlite.checker import TRACE_TAIL
from raftlite.messages import ClientCommand
from raftlite.node import RaftNode, Role
from raftlite.sim import SimConfig, Simulator

LIVENESS_BOUND = 10 * SimConfig().election_timeout_max
FUZZ_CONFIG = SimConfig(drop_prob=0.05, duplicate_prob=0.05, delay_prob=0.05)
FUZZ_TICKS = 6000
MAX_DOWN = 2


class ScenarioFailed(AssertionError):
    def __init__(self, detail, sim):
        self.seed = sim.seed
        self.trace = list(sim.trace)
        tail = "\n".join(sim.trace[-TRACE_TAIL:])
        super().__init__(f"{detail} (seed={sim.seed}, t={sim.now})\n{tail}")


def expect(condition, detail, sim):
    if not condition:
        raise ScenarioFailed(detail, sim)


def repro_command(call, node_cls=RaftNode):
    imports = "from raftlite.scenarios import *"
    if node_cls is not RaftNode:
        imports += f"; from {node_cls.__module__} import {node_cls.__qualname__}"
    return f'python -c "{imports}; {call}"'


@contextmanager
def repro(call, node_cls=RaftNode):
    try:
        yield
    except AssertionError as error:
        error.args = (f"{error}\nrepro: {repro_command(call, node_cls)}",)
        raise


def run(name, seed):
    with repro(f"run({name!r}, {seed})"):
        return SCENARIOS[name](seed)


def leaders(sim, among=None):
    return [
        i
        for i, node in sim.nodes.items()
        if node.role is Role.LEADER and (among is None or i in among)
    ]


def term(sim, node_id):
    return sim.persistent[node_id].current_term


def elect(seed, config=None):
    sim = Simulator(seed=seed, config=config)
    while len(leaders(sim)) != 1 and sim.now < 3000:
        sim.run_until(sim.now + 50)
    sim.run_until(sim.now + 200)
    found = leaders(sim)
    expect(len(found) == 1, f"expected one leader, found {found}", sim)
    expect(logs_and_state_agree(sim), "leader's first entry did not settle", sim)
    return sim, found[0]


def request(sim, request_id, command, among=None, deadline=None):
    deadline = sim.now + 3000 if deadline is None else deadline
    seen = len(sim.client_replies)
    while sim.now < deadline:
        found = leaders(sim, among)
        if found:
            sim.submit(max(found, key=lambda i: term(sim, i)), ClientCommand(request_id, command))
        sim.run_until(min(sim.now + 200, deadline))
        for _, reply in sim.client_replies[seen:]:
            if reply.request_id == request_id and reply.ok:
                return reply
    raise ScenarioFailed(f"request {request_id} {command} never committed", sim)


def logs_and_state_agree(sim):
    first = next(iter(sim.nodes.values()))
    for node in sim.nodes.values():
        if node.persistent.log != first.persistent.log:
            return False
        if node.commit_index != first.commit_index or node.kv.data != first.kv.data:
            return False
    return True


def leader_partitioned(seed):
    sim, old = elect(seed)
    old_term = term(sim, old)
    sim.partition([old])
    commit_before = sim.nodes[old].commit_index
    sim.submit(old, ClientCommand(0, ("PUT", "stale", 1)))
    majority = set(sim.nodes) - {old}
    request(sim, 1, ("PUT", "a", 1), among=majority)
    (new,) = leaders(sim, majority)
    expect(term(sim, new) > old_term, f"n{new} did not move past term {old_term}", sim)
    expect(sim.nodes[old].role is Role.LEADER, f"isolated n{old} stopped leading", sim)
    expect(sim.nodes[old].commit_index == commit_before, f"isolated n{old} committed", sim)
    for node_id in majority:
        expect(sim.nodes[node_id].kv.data == {"a": 1}, f"n{node_id} missing the write", sim)
    return sim


def minority_partition(seed):
    sim, old = elect(seed)
    minority = [old, next(i for i in sim.nodes if i != old)]
    majority = set(sim.nodes) - set(minority)
    sim.partition(minority)
    before = {i: (sim.nodes[i].commit_index, len(sim.nodes[i].applied)) for i in minority}
    sim.submit(old, ClientCommand(0, ("PUT", "stale", 1)))
    request(sim, 1, ("PUT", "a", 1), among=majority)
    sim.run_until(sim.now + 500)
    for node_id in minority:
        node = sim.nodes[node_id]
        after = (node.commit_index, len(node.applied))
        expect(after == before[node_id], f"minority n{node_id} committed or applied", sim)
    for node_id in majority:
        expect(sim.nodes[node_id].kv.data == {"a": 1}, f"n{node_id} missing the write", sim)
    return sim


def follower_catches_up(seed):
    sim, leader = elect(seed)
    follower = next(i for i in sim.nodes if i != leader)
    sim.crash(follower)
    for i in range(3):
        request(sim, i, ("PUT", f"k{i}", i))
    sim.restart(follower)
    sim.run_until(sim.now + 1000)
    expected = {f"k{i}": i for i in range(3)}
    expect(sim.nodes[follower].kv.data == expected, f"n{follower} did not catch up", sim)
    expect(logs_and_state_agree(sim), "nodes disagree after catch-up", sim)
    return sim


def split_vote(seed):
    sim, old = elect(seed)
    old_term = term(sim, old)
    sim.crash(old)
    rest = sorted(sim.nodes)
    sim.partition(rest[:2], rest[2:])
    sim.run_until(sim.now + 2000)
    top = max(term(sim, i) for i in sim.nodes)
    expect(top > old_term, "no election started during the split", sim)
    led = [t for t in sim.checker.leaders_by_term if old_term < t <= top]
    expect(not led and not leaders(sim), f"a 2-node side won terms {led}", sim)
    sim.heal()
    sim.run_until(sim.now + 3000)
    found = leaders(sim)
    expect(len(found) == 1, f"expected one leader after heal, found {found}", sim)
    expect(term(sim, found[0]) >= top, "new leader is behind the split terms", sim)
    return sim


def stale_leader_steps_down(seed):
    sim, old = elect(seed)
    sim.partition([old])
    stale = ("PUT", "stale", 1)
    sim.submit(old, ClientCommand(0, stale))
    majority = set(sim.nodes) - {old}
    request(sim, 1, ("PUT", "a", 1), among=majority)
    request(sim, 2, ("PUT", "b", 2), among=majority)
    sim.heal()
    sim.run_until(sim.now + 1000)
    expect(sim.nodes[old].role is Role.FOLLOWER, f"n{old} did not step down", sim)
    expect(len(leaders(sim)) == 1, f"expected one leader, found {leaders(sim)}", sim)
    expect(logs_and_state_agree(sim), "nodes disagree after heal", sim)
    commands = [entry.command for entry in sim.persistent[old].log]
    expect(stale not in commands, f"n{old} kept its uncommitted entry", sim)
    expect(sim.nodes[old].kv.data == {"a": 1, "b": 2}, f"n{old} has wrong state", sim)
    return sim


def liveness(seed):
    config = SimConfig(drop_prob=0.1, duplicate_prob=0.1, delay_prob=0.1)
    sim = Simulator(seed=seed, config=config)
    sim.run_until(1500)
    sim.partition([0, 1])
    sim.crash(4)
    sim.run_until(3000)
    sim.heal()
    sim.restart(4)
    expect_live(sim, 0)
    return sim


def expect_live(sim, request_id):
    sim.config = replace(sim.config, drop_prob=0.0, duplicate_prob=0.0, delay_prob=0.0)
    deadline = sim.now + LIVENESS_BOUND
    request(sim, request_id, ("PUT", "live", 1), deadline=deadline)
    while sim.now < deadline and not all("live" in n.kv.data for n in sim.nodes.values()):
        sim.run_until(sim.now + 50)
    expect(
        all("live" in n.kv.data for n in sim.nodes.values()),
        f"command not applied everywhere within {LIVENESS_BOUND} ticks",
        sim,
    )


def fuzz(seed, node_cls=RaftNode, ticks=FUZZ_TICKS):
    call = f"fuzz({seed})" if node_cls is RaftNode else f"fuzz({seed}, {node_cls.__qualname__})"
    with repro(call, node_cls):
        sim = Simulator(seed=seed, config=FUZZ_CONFIG, node_cls=node_cls)
        rng = sim.rng
        down = []
        request_id = 0
        while sim.now < ticks:
            sim.run_until(sim.now + rng.randint(20, 300))
            action = rng.choice(["partition", "heal", "crash", "restart", "submit", "submit"])
            if action == "partition":
                ids = sorted(sim.persistent)
                rng.shuffle(ids)
                cut = rng.randint(1, len(ids) - 1)
                sim.partition(ids[:cut], ids[cut:])
            elif action == "heal":
                sim.heal()
            elif action == "crash" and len(down) < MAX_DOWN:
                node_id = rng.choice(sorted(sim.nodes))
                sim.crash(node_id)
                down.append(node_id)
            elif action == "restart" and down:
                sim.restart(down.pop(rng.randrange(len(down))))
            elif action == "submit" and leaders(sim):
                newest = max(leaders(sim), key=lambda i: term(sim, i))
                sim.submit(newest, ClientCommand(request_id, ("PUT", f"k{request_id}", request_id)))
                request_id += 1
        sim.heal()
        for node_id in sorted(down):
            sim.restart(node_id)
        expect_live(sim, request_id)
        return sim


SCENARIOS = {
    "leader_partitioned": leader_partitioned,
    "minority_partition": minority_partition,
    "follower_catches_up": follower_catches_up,
    "split_vote": split_vote,
    "stale_leader_steps_down": stale_leader_steps_down,
    "liveness": liveness,
}
