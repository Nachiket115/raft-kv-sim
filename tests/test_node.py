import ast
from pathlib import Path

import raftlite
from raftlite.messages import ClientCommand, ClientReply, LogEntry, SetTimer, TimerKind
from raftlite.node import PersistentState, RaftNode, Role


def make_node(node_id=0, persistent=None):
    return RaftNode(node_id, range(5), persistent)


def test_new_node_starts_as_follower_with_empty_state():
    node = make_node(2)
    assert node.role is Role.FOLLOWER
    assert node.peers == (0, 1, 3, 4)
    assert node.persistent == PersistentState(0, None, [])
    assert node.commit_index == 0
    assert node.last_applied == 0
    assert node.leader_id is None


def test_start_requests_election_timer():
    assert make_node().start() == [SetTimer(TimerKind.ELECTION)]


def test_follower_ignores_heartbeat_timeout():
    assert make_node().handle_timeout(TimerKind.HEARTBEAT) == []


def test_non_leader_rejects_client_command_with_leader_hint():
    node = make_node()
    cmd = ClientCommand(7, ("PUT", "k", "v"))
    assert node.handle_client_command(cmd) == [ClientReply(7, ok=False, leader_hint=None)]
    node.leader_id = 3
    assert node.handle_client_command(cmd) == [ClientReply(7, ok=False, leader_hint=3)]


def test_restart_keeps_persistent_state_and_resets_volatile_state():
    persistent = PersistentState(4, 1, [LogEntry(3, ("PUT", "k", "v"))])
    old = make_node(0, persistent)
    old.commit_index = 1
    old.last_applied = 1
    old.leader_id = 1

    new = make_node(0, old.persistent)

    assert new.persistent is persistent
    assert new.persistent.current_term == 4
    assert new.persistent.voted_for == 1
    assert new.persistent.log == [LogEntry(3, ("PUT", "k", "v"))]
    assert new.role is Role.FOLLOWER
    assert new.commit_index == 0
    assert new.last_applied == 0
    assert new.leader_id is None


def test_node_layer_does_not_import_simulator_clock_or_randomness():
    banned = {"random", "time", "datetime", "secrets", "raftlite.sim"}
    package = Path(raftlite.__file__).parent
    for name in ("node.py", "messages.py"):
        for stmt in ast.walk(ast.parse((package / name).read_text())):
            if isinstance(stmt, ast.Import):
                imported = {alias.name for alias in stmt.names}
            elif isinstance(stmt, ast.ImportFrom):
                imported = {stmt.module}
            else:
                continue
            assert not imported & banned, f"{name} imports {imported & banned}"
