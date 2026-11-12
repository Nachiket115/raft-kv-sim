from raftlite.messages import ClientCommand, ClientReply, Send, SetTimer, TimerKind
from raftlite.node import RaftNode, Role
from raftlite.sim import SimConfig, Simulator


class IdleNode(RaftNode):
    def handle_timeout(self, kind):
        return [SetTimer(TimerKind.ELECTION)]


class PingNode(RaftNode):
    def __init__(self, node_id, node_ids, persistent=None):
        super().__init__(node_id, node_ids, persistent)
        self.received = []
        self.rounds = 0

    def handle_timeout(self, kind):
        self.rounds += 1
        sends = [Send(peer, ("ping", self.node_id, self.rounds)) for peer in self.peers]
        return sends + [SetTimer(TimerKind.ELECTION)]

    def handle_message(self, msg):
        self.received.append(msg)
        return []


class DoubleTimerNode(RaftNode):
    def __init__(self, node_id, node_ids, persistent=None):
        super().__init__(node_id, node_ids, persistent)
        self.timeouts = 0

    def start(self):
        return [SetTimer(TimerKind.ELECTION), SetTimer(TimerKind.ELECTION)]

    def handle_timeout(self, kind):
        self.timeouts += 1
        return []


def run(seed, until=5000, **kwargs):
    sim = Simulator(seed=seed, **kwargs)
    sim.run_until(until)
    return sim


def test_idle_nodes_with_same_seed_give_byte_identical_traces():
    first = run(42, node_cls=IdleNode).trace_text().encode()
    second = run(42, node_cls=IdleNode).trace_text().encode()
    assert first
    assert first == second


def test_different_seeds_give_different_traces():
    assert run(1).trace_text() != run(2).trace_text()


def test_idle_nodes_keep_timing_out_and_never_send():
    sim = run(3, node_cls=IdleNode)
    assert sim.now == 5000
    for node_id in sim.nodes:
        timeouts = [line for line in sim.trace if f"n{node_id} election timeout" in line]
        assert len(timeouts) >= 5000 // SimConfig().election_timeout_max
    assert not any("send" in line for line in sim.trace)


def test_trace_times_never_go_backwards():
    times = [int(line.split()[0]) for line in run(4, node_cls=PingNode).trace]
    assert times == sorted(times)


def test_every_sent_message_is_delivered_once_within_delay_bounds():
    config = SimConfig(message_delay_min=2, message_delay_max=6)
    sim = Simulator(seed=5, config=config, node_cls=PingNode)
    sim.run_until(1000)
    while any(event[0] == "deliver" for _, _, event in sim._queue):
        sim.step()

    sent = {}
    delivered = {}
    for line in sim.trace:
        time, rest = line.split(" ", 1)
        if " send " in rest:
            assert rest.replace(" send ", " ") not in sent
            sent[rest.replace(" send ", " ")] = int(time)
        elif " deliver " in rest:
            assert rest.replace(" deliver ", " ") not in delivered
            delivered[rest.replace(" deliver ", " ")] = int(time)

    assert sent
    assert sent.keys() == delivered.keys()
    for key, sent_at in sent.items():
        assert 2 <= delivered[key] - sent_at <= 6
    assert sum(len(node.received) for node in sim.nodes.values()) == len(sent)


def test_message_runs_with_same_seed_are_byte_identical():
    first = run(6, node_cls=PingNode).trace_text().encode()
    second = run(6, node_cls=PingNode).trace_text().encode()
    assert first == second


def test_new_timer_replaces_pending_timer_of_same_kind():
    sim = run(7, node_cls=DoubleTimerNode)
    assert [node.timeouts for node in sim.nodes.values()] == [1] * 5


def test_submit_to_non_leader_records_rejection():
    sim = Simulator(seed=8)
    sim.submit(2, ClientCommand(1, ("GET", "k")))
    assert sim.client_replies == [(2, ClientReply(1, ok=False, leader_hint=None))]


def leaders(sim):
    return [i for i, node in sim.nodes.items() if node.role is Role.LEADER]


def run_recording_leaders(sim, until):
    by_term = {}
    while sim._queue and sim._queue[0][0] <= until:
        sim.step()
        for node_id in leaders(sim):
            by_term.setdefault(sim.nodes[node_id].persistent.current_term, set()).add(node_id)
    return by_term


def test_five_nodes_elect_exactly_one_leader():
    for seed in range(50):
        sim = Simulator(seed=seed)
        by_term = run_recording_leaders(sim, 3000)
        assert all(len(ids) == 1 for ids in by_term.values()), (seed, by_term)
        assert len(leaders(sim)) == 1, seed
        leader = sim.nodes[leaders(sim)[0]]
        for node in sim.nodes.values():
            assert node.persistent.current_term == leader.persistent.current_term, seed
            assert node.leader_id == leader.node_id, seed


def test_stable_leader_is_not_challenged():
    sim = Simulator(seed=11)
    sim.run_until(3000)
    term = sim.nodes[leaders(sim)[0]].persistent.current_term
    sim.run_until(20000)
    assert sim.nodes[leaders(sim)[0]].persistent.current_term == term


def test_election_runs_with_same_seed_are_byte_identical():
    first = run(12).trace_text().encode()
    second = run(12).trace_text().encode()
    assert first == second


def elected(seed):
    sim = Simulator(seed=seed)
    sim.run_until(3000)
    (leader,) = leaders(sim)
    return sim, leader


def test_cluster_elects_new_leader_after_leader_is_killed():
    for seed in range(50):
        sim, old = elected(seed)
        old_term = sim.nodes[old].persistent.current_term
        sim.crash(old)
        by_term = run_recording_leaders(sim, 6000)
        assert all(len(ids) == 1 for ids in by_term.values()), (seed, by_term)
        (new,) = leaders(sim)
        assert new != old, seed
        assert sim.nodes[new].persistent.current_term > old_term, seed
        assert old not in sim.nodes


def test_restarted_leader_rejoins_as_follower_with_persistent_state():
    sim, old = elected(21)
    persistent = sim.nodes[old].persistent
    sim.crash(old)
    assert persistent.voted_for == old
    sim.run_until(6000)
    (new,) = leaders(sim)
    sim.restart(old)
    assert sim.nodes[old].persistent is persistent
    assert sim.nodes[old].role is Role.FOLLOWER
    assert list(sim.nodes) == sorted(sim.nodes)
    sim.run_until(9000)
    assert leaders(sim) == [new]
    assert sim.nodes[old].leader_id == new
    assert persistent.current_term == sim.nodes[new].persistent.current_term


def test_crashed_node_receives_nothing_and_its_timers_never_fire():
    sim, old = elected(22)
    sim.crash(old)
    start = len(sim.trace)
    sim.run_until(6000)
    after = sim.trace[start:]
    assert not any(f"n{old} election timeout" in line for line in after)
    assert not any(f"n{old} heartbeat timeout" in line for line in after)
    assert not any(f"-> n{old} deliver" in line for line in after)
    assert any(f"-> n{old} dropped" in line for line in after)


def test_timer_set_before_crash_does_not_fire_after_restart():
    sim, old = elected(23)
    sim.crash(old)
    sim.restart(old)
    start = len(sim.trace)
    sim.run_until(sim.now + SimConfig().heartbeat_interval + 1)
    assert not any(f"n{old} heartbeat timeout" in line for line in sim.trace[start:])


def test_crash_and_restart_runs_are_byte_identical():
    def scripted():
        sim, old = elected(24)
        sim.crash(old)
        sim.run_until(6000)
        sim.restart(old)
        sim.run_until(9000)
        return sim.trace_text().encode()

    assert scripted() == scripted()


def test_commands_sent_to_leader_reach_every_log_in_order():
    for seed in range(20):
        sim, leader = elected(seed)
        commands = [("PUT", f"k{i}", i) for i in range(5)]
        for i, command in enumerate(commands):
            sim.submit(leader, ClientCommand(i, command))
            sim.run_until(sim.now + 7)
        sim.run_until(sim.now + 500)
        assert leaders(sim) == [leader], seed
        for node_id, persistent in sim.persistent.items():
            assert [e.command for e in persistent.log] == commands, (seed, node_id)


def test_restarted_follower_catches_up_on_missed_entries():
    sim, leader = elected(31)
    follower = next(i for i in sim.nodes if i != leader)
    sim.submit(leader, ClientCommand(1, ("PUT", "a", 1)))
    sim.run_until(sim.now + 100)
    sim.crash(follower)
    sim.submit(leader, ClientCommand(2, ("PUT", "b", 2)))
    sim.submit(leader, ClientCommand(3, ("DELETE", "a")))
    sim.run_until(sim.now + 100)
    assert len(sim.persistent[follower].log) == 1
    sim.restart(follower)
    sim.run_until(sim.now + 500)
    assert leaders(sim) == [leader]
    assert sim.persistent[follower].log == sim.persistent[leader].log
    assert len(sim.persistent[leader].log) == 3
