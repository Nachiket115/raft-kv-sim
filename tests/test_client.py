import random

from raftlite.client import REDIRECT_DELAY, RETRY_TIMEOUT, Client
from raftlite.linearizability import failing_key
from raftlite.messages import ClientCommand, ClientReply
from raftlite.node import Role
from raftlite.sim import SimConfig, Simulator

FAULTY = SimConfig(drop_prob=0.05, duplicate_prob=0.05, delay_prob=0.05)


def leaders(sim):
    return [i for i, node in sim.nodes.items() if node.role is Role.LEADER]


def elected(seed):
    sim = Simulator(seed=seed)
    sim.run_until(3000)
    (leader,) = leaders(sim)
    return sim, leader


def with_clients(seed, until=3000, config=None):
    sim = Simulator(seed=seed, config=config)
    sim.add_clients(3)
    sim.run_until(until)
    return sim


def submits(sim, request_id):
    marker = f"ClientCommand(request_id={request_id!r},"
    return [
        (int(line[:6]), int(line.split(" -> n")[1].split()[0]))
        for line in sim.trace
        if " client -> n" in line and marker in line and "unreachable" not in line
    ]


def test_simulator_without_clients_schedules_no_client_events():
    sim = Simulator(seed=1)
    sim.run_until(2000)
    assert sim.clients == {}
    assert not any(event[0] == "client" for _, _, event in sim._queue)
    assert not any("client" in line for line in sim.trace)


def test_adding_no_clients_leaves_the_trace_untouched():
    plain = Simulator(seed=4)
    plain.run_until(2000)
    empty = Simulator(seed=4)
    empty.add_clients(0)
    empty.run_until(2000)
    assert plain.trace_text() == empty.trace_text()


def test_clients_complete_operations_of_every_kind():
    sim = with_clients(1)
    done = [op for op in sim.history.values() if op.ret is not None]
    assert len(done) > 50
    assert {op.command[0] for op in done} == {"PUT", "GET", "DELETE"}
    assert {op.command[1] for op in done} == {"x", "y"}
    assert {op.request_id[0] for op in done} == {0, 1, 2}
    assert any(op.command[0] == "GET" and op.result is not None for op in done)
    assert any(op.command[0] == "DELETE" and op.result is True for op in done)


def test_client_histories_are_linearizable_with_and_without_faults():
    for seed in range(5):
        for config in (None, FAULTY):
            sim = with_clients(seed, config=config)
            assert failing_key(sim.history.values()) is None, (seed, config)


def test_runs_with_clients_are_byte_identical_for_a_seed():
    first = with_clients(9, config=FAULTY).trace_text()
    assert first == with_clients(9, config=FAULTY).trace_text()
    assert first != with_clients(10, config=FAULTY).trace_text()


def test_each_client_uses_consecutive_seqs_one_operation_at_a_time():
    sim = with_clients(2, config=FAULTY)
    for client_id in sim.clients:
        ops = [op for op in sim.history.values() if op.request_id[0] == client_id]
        assert [op.request_id[1] for op in ops] == list(range(1, len(ops) + 1))
        for earlier, later in zip(ops, ops[1:]):
            assert earlier.ret is not None and earlier.ret < later.invoke


def test_put_values_are_unique():
    sim = with_clients(3)
    values = [op.command[2] for op in sim.history.values() if op.command[0] == "PUT"]
    assert len(values) == len(set(values)) > 10


def test_nodes_that_applied_the_same_prefix_have_the_same_session_table():
    for seed in range(5):
        sim = with_clients(seed, config=FAULTY)
        seen = {}
        for node in sim.nodes.values():
            state = (node.kv.sessions, node.kv.data)
            assert seen.setdefault(node.last_applied, state) == state, seed
        assert all(node.kv.sessions for node in sim.nodes.values())


def test_client_follows_the_leader_hint_after_a_redirect():
    sim, leader = elected(5)
    follower = next(i for i in sim.nodes if i != leader)
    sim.add_clients(1)
    client = sim.clients[0]
    client.target = follower
    started = sim.now
    sim.run_until(started + 100)
    first, second = submits(sim, (0, 1))
    assert first == (started, follower)
    assert second == (started + REDIRECT_DELAY, leader)
    assert sim.history[0, 1].ret is not None


def test_client_stays_with_the_node_that_answered():
    sim, leader = elected(6)
    sim.add_clients(1)
    sim.run_until(sim.now + 1500)
    done = [op for op in sim.history.values() if op.ret is not None]
    assert len(done) > 5
    for op in done[1:]:
        assert submits(sim, op.request_id) == [(op.invoke, leader)]


def test_client_retries_the_same_seq_elsewhere_after_a_timeout():
    sim, old = elected(7)
    sim.add_clients(1)
    client = sim.clients[0]
    client.target = old
    sim.partition([old])
    started = sim.now
    sim.run_until(started + 3000)
    tries = submits(sim, (0, 1))
    assert tries[0] == (started, old)
    assert tries[1][0] == started + RETRY_TIMEOUT
    op = sim.history[0, 1]
    assert op.invoke == started
    assert op.ret is not None and op.ret > started + RETRY_TIMEOUT
    assert [key for key in sim.history if key == (0, 1)] == [(0, 1)]
    copies = [
        entry.command
        for entry in sim.persistent[old].log
        if entry.command[:3] == ("SESSION", 0, 1)
    ]
    assert len(copies) == 1


def test_client_skips_a_crashed_node_and_tries_again_soon():
    sim, leader = elected(8)
    down = next(i for i in sim.nodes if i != leader)
    sim.crash(down)
    sim.add_clients(1)
    sim.clients[0].target = down
    started = sim.now
    sim.run_until(started + 500)
    assert f"{started:06d} client -> n{down} unreachable" in sim.trace_text()
    assert submits(sim, (0, 1))[0][0] == started + REDIRECT_DELAY
    assert sim.history[0, 1].ret is not None


def test_client_ignores_replies_for_other_requests():
    sim, leader = elected(9)
    client = Client(0, ["x"])
    client.seq, client.op = 3, ("GET", "x")
    client.on_reply(sim, leader, ClientReply((0, 2), ok=True))
    client.on_reply(sim, leader, ClientReply((1, 3), ok=True))
    client.on_reply(sim, leader, ClientReply(3, ok=True))
    assert client.op == ("GET", "x") and client.generation == 0
    client.on_reply(sim, leader, ClientReply((0, 3), ok=True))
    assert client.op is None and client.target == leader
    client.on_reply(sim, leader, ClientReply((0, 3), ok=True))
    assert client.generation == 1


def test_client_draws_only_from_the_rng_it_is_given():
    one, two = Client(0, ["x", "y"]), Client(0, ["x", "y"])
    for client in (one, two):
        client.seq = 1
    assert [one._next_op(random.Random(3)) for _ in range(5)] == [
        two._next_op(random.Random(3)) for _ in range(5)
    ]


def session(client_id, seq, op):
    return ClientCommand((client_id, seq), ("SESSION", client_id, seq, op))


def test_retry_after_the_leader_crashes_gets_the_cached_reply_and_applies_once():
    for seed in range(10):
        sim, old = elected(seed)
        sim.submit(old, session(7, 1, ("PUT", "a", 1)))
        sim.run_until(sim.now + 200)
        delete = session(7, 2, ("DELETE", "a"))
        sim.submit(old, delete)
        others = [i for i in sim.nodes if i != old]

        def holders():
            return [
                i
                for i in others
                if sim.persistent[i].log and sim.persistent[i].log[-1].command == delete.command
            ]

        while len(holders()) < 2:
            sim.step()
        assert sim.history[7, 2].ret is None, seed
        sim.crash(old)
        sim.run_until(sim.now + 2000)
        (new,) = leaders(sim)
        assert all(node.kv.data == {} for node in sim.nodes.values()), seed
        assert sim.history[7, 2].ret is None, seed

        sim.submit(new, delete)
        sim.run_until(sim.now + 200)
        assert sim.client_replies[-1] == (new, ClientReply((7, 2), ok=True, result=True)), seed
        op = sim.history[7, 2]
        assert op.result is True and op.ret is not None, seed
        copies = [e for e in sim.persistent[new].log if e.command == delete.command]
        assert len(copies) == 2, seed
        assert sim.nodes[new].kv.sessions[7] == (2, (True, True)), seed
        assert failing_key(sim.history.values()) is None, seed
