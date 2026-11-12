from raftlite.messages import ClientCommand, ClientReply, Send, SetTimer, TimerKind
from raftlite.node import RaftNode
from raftlite.sim import SimConfig, Simulator


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
    first = run(42).trace_text().encode()
    second = run(42).trace_text().encode()
    assert first
    assert first == second


def test_different_seeds_give_different_traces():
    assert run(1).trace_text() != run(2).trace_text()


def test_idle_nodes_keep_timing_out_and_never_send():
    sim = run(3)
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
