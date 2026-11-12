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


def test_commands_sent_to_leader_are_committed_and_applied_on_all_nodes():
    for seed in range(20):
        sim, leader = elected(seed)
        for i in range(5):
            sim.submit(leader, ClientCommand(i, ("PUT", f"k{i}", i)))
            sim.run_until(sim.now + 7)
        sim.run_until(sim.now + 500)
        for node_id, node in sim.nodes.items():
            assert (node.commit_index, node.last_applied) == (5, 5), (seed, node_id)


def test_restarted_follower_commits_and_applies_what_it_missed():
    sim, leader = elected(32)
    follower = next(i for i in sim.nodes if i != leader)
    sim.crash(follower)
    for i in range(3):
        sim.submit(leader, ClientCommand(i, ("PUT", f"k{i}", i)))
    sim.run_until(sim.now + 100)
    assert sim.nodes[leader].commit_index == 3
    sim.restart(follower)
    assert sim.nodes[follower].commit_index == 0
    sim.run_until(sim.now + 500)
    node = sim.nodes[follower]
    assert (node.commit_index, node.last_applied) == (3, 3)
    assert node.persistent.log == sim.persistent[leader].log


def test_every_node_applies_the_whole_log_in_order_including_after_restart():
    sim, leader = elected(33)
    follower = next(i for i in sim.nodes if i != leader)
    sim.submit(leader, ClientCommand(0, ("PUT", "a", 0)))
    sim.run_until(sim.now + 100)
    sim.crash(follower)
    for i in range(1, 4):
        sim.submit(leader, ClientCommand(i, ("PUT", f"k{i}", i)))
    sim.run_until(sim.now + 100)
    sim.restart(follower)
    sim.run_until(sim.now + 500)
    expected = [(i, e.command) for i, e in enumerate(sim.persistent[leader].log, 1)]
    assert len(expected) == 4
    for node_id, node in sim.nodes.items():
        assert node.applied == expected, node_id


def test_scripted_workload_gives_identical_kv_state_on_all_live_nodes():
    for seed in range(20):
        sim, leader = elected(seed)
        workload = [
            ("PUT", "a", 1),
            ("PUT", "b", 2),
            ("GET", "a"),
            ("DELETE", "a"),
            ("PUT", "c", 3),
            ("GET", "a"),
        ]
        for i, command in enumerate(workload):
            sim.submit(leader, ClientCommand(i, command))
            sim.run_until(sim.now + 7)
        sim.run_until(sim.now + 500)
        for node_id, node in sim.nodes.items():
            assert node.kv.data == {"b": 2, "c": 3}, (seed, node_id)
        replies = {r.request_id: r for n, r in sim.client_replies if n == leader}
        assert [replies[i].result for i in range(6)] == [None, None, 1, True, None, None], seed
        assert all(r.ok for r in replies.values()), seed


def test_non_leader_in_running_cluster_rejects_with_leader_hint():
    sim, leader = elected(5)
    follower = next(i for i in sim.nodes if i != leader)
    sim.run_until(sim.now + 200)
    sim.submit(follower, ClientCommand(9, ("PUT", "a", 1)))
    assert sim.client_replies[-1] == (follower, ClientReply(9, ok=False, leader_hint=leader))


def drain(sim):
    while any(event[0] == "deliver" for _, _, event in sim._queue):
        sim.step()


def count_lines(sim, word):
    counts = {}
    for line in sim.trace:
        time, rest = line.split(" ", 1)
        if f" {word} " in rest:
            key = rest.replace(f" {word} ", " ", 1)
            counts[key] = counts.get(key, 0) + 1
    return counts


def test_drop_prob_one_delivers_nothing():
    sim = Simulator(seed=30, config=SimConfig(drop_prob=1.0), node_cls=PingNode)
    sim.run_until(1000)
    drain(sim)
    assert count_lines(sim, "send")
    assert count_lines(sim, "dropped (fault)") == count_lines(sim, "send")
    assert not any(" deliver " in line for line in sim.trace)
    assert all(not node.received for node in sim.nodes.values())


def test_duplicate_prob_one_delivers_every_message_twice():
    sim = Simulator(seed=31, config=SimConfig(duplicate_prob=1.0), node_cls=PingNode)
    sim.run_until(1000)
    drain(sim)
    sent = count_lines(sim, "send")
    assert sent
    assert count_lines(sim, "deliver") == {key: 2 for key in sent}


def test_delayed_messages_arrive_within_long_delay_bounds():
    config = SimConfig(
        message_delay_min=2, message_delay_max=6, delay_prob=1.0, long_delay_max=100
    )
    sim = Simulator(seed=32, config=config, node_cls=PingNode)
    sim.run_until(1000)
    drain(sim)
    sent_at = {}
    gaps = []
    for line in sim.trace:
        time, rest = line.split(" ", 1)
        if " send " in rest:
            sent_at[rest.replace(" send ", " ")] = int(time)
        elif " deliver " in rest:
            gaps.append(int(time) - sent_at[rest.replace(" deliver ", " ")])
    assert gaps
    assert all(3 <= gap <= 106 for gap in gaps)
    assert max(gaps) > 6


def test_partition_blocks_messages_between_groups_until_heal():
    sim = Simulator(seed=33, node_cls=PingNode)
    sim.run_until(500)
    sim.partition([0, 1])
    for node in sim.nodes.values():
        node.received.clear()
    sim.run_until(1500)
    for node_id, node in sim.nodes.items():
        senders = {msg[1] for msg in node.received}
        same_group = {0, 1} if node_id in (0, 1) else {2, 3, 4}
        assert senders and senders <= same_group, node_id
    assert any("dropped (partition)" in line for line in sim.trace)

    sim.heal()
    for node in sim.nodes.values():
        node.received.clear()
    sim.run_until(2500)
    for node_id, node in sim.nodes.items():
        assert {msg[1] for msg in node.received} == set(sim.nodes) - {node_id}


def test_partition_rejects_unknown_or_repeated_nodes():
    sim = Simulator(seed=34, node_cls=IdleNode)
    for groups in ([[0, 9]], [[0, 1], [1, 2]]):
        try:
            sim.partition(*groups)
        except ValueError:
            pass
        else:
            raise AssertionError(groups)


def test_isolated_leader_steps_down_after_heal():
    for seed in range(20):
        sim, old = elected(seed)
        old_term = sim.nodes[old].persistent.current_term
        sim.partition([old])
        sim.run_until(sim.now + 2000)
        new = [i for i in leaders(sim) if i != old]
        assert len(new) == 1, seed
        assert sim.nodes[new[0]].persistent.current_term > old_term, seed
        assert sim.nodes[old].role is Role.LEADER, seed
        assert sim.nodes[old].persistent.current_term == old_term, seed
        commit_before = sim.nodes[old].commit_index
        sim.submit(old, ClientCommand(1, ("PUT", "x", 1)))
        sim.run_until(sim.now + 500)
        assert sim.nodes[old].commit_index == commit_before, seed

        sim.heal()
        sim.run_until(sim.now + 1000)
        assert leaders(sim) == new, seed
        assert sim.nodes[old].role is Role.FOLLOWER, seed
        assert sim.nodes[old].leader_id == new[0], seed


def faulty(seed):
    config = SimConfig(drop_prob=0.1, duplicate_prob=0.1, delay_prob=0.1)
    return Simulator(seed=seed, config=config)


def test_fault_runs_with_same_seed_are_byte_identical():
    def scripted():
        sim = faulty(35)
        sim.run_until(1500)
        sim.partition([0, 1])
        sim.run_until(3000)
        sim.heal()
        sim.run_until(5000)
        return sim.trace_text().encode()

    first = scripted()
    assert first == scripted()
    assert b"dropped (fault)" in first and b"duplicate" in first and b"delay +" in first


def newest_leader(sim):
    found = leaders(sim)
    return max(found, key=lambda i: sim.nodes[i].persistent.current_term) if found else None


def test_cluster_with_message_faults_still_elects_and_applies_workload():
    for seed in range(20):
        sim = faulty(seed)
        sim.run_until(3000)
        workload = [("PUT", "a", 1), ("PUT", "b", 2), ("PUT", "a", 3)]
        for request_id, command in enumerate(workload):
            for _ in range(20):
                leader = newest_leader(sim)
                if leader is not None:
                    sim.submit(leader, ClientCommand(request_id, command))
                sim.run_until(sim.now + 300)
                if any(r.request_id == request_id and r.ok for _, r in sim.client_replies):
                    break
            else:
                raise AssertionError((seed, request_id))
        sim.run_until(sim.now + 2000)
        for node_id, node in sim.nodes.items():
            assert node.kv.data == {"a": 3, "b": 2}, (seed, node_id)
