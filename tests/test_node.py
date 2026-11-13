import ast
from pathlib import Path

import raftlite
from raftlite.messages import (
    AppendEntries,
    AppendEntriesResponse,
    ClientCommand,
    ClientReply,
    LogEntry,
    RequestVote,
    RequestVoteResponse,
    Send,
    SetTimer,
    TimerKind,
)
from raftlite.kv import STALE_SEQ
from raftlite.node import NOOP, PersistentState, RaftNode, Role


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


ELECTION = SetTimer(TimerKind.ELECTION)
HEARTBEAT = SetTimer(TimerKind.HEARTBEAT)


def make_leader(node_id=0, persistent=None):
    node = make_node(node_id, persistent)
    node.handle_timeout(TimerKind.ELECTION)
    term = node.persistent.current_term
    for voter in node.peers[:2]:
        node.handle_message(RequestVoteResponse(term, voter, True))
    assert node.role is Role.LEADER
    return node


def heartbeats(node):
    term = node.persistent.current_term
    log = tuple(node.persistent.log)
    return [Send(peer, AppendEntries(term, node.node_id, 0, 0, log, 0)) for peer in node.peers]


def test_election_timeout_starts_election():
    node = make_node(0)
    effects = node.handle_timeout(TimerKind.ELECTION)
    assert node.role is Role.CANDIDATE
    assert node.persistent.current_term == 1
    assert node.persistent.voted_for == 0
    assert effects == [Send(peer, RequestVote(1, 0, 0, 0)) for peer in (1, 2, 3, 4)] + [ELECTION]


def test_request_vote_carries_last_log_position():
    node = make_node(0, PersistentState(3, None, [LogEntry(1, ("a",)), LogEntry(3, ("b",))]))
    effects = node.handle_timeout(TimerKind.ELECTION)
    assert effects[0] == Send(1, RequestVote(4, 0, 2, 3))


def test_candidate_timeout_starts_new_term():
    node = make_node(0)
    node.handle_timeout(TimerKind.ELECTION)
    node.handle_message(RequestVoteResponse(1, 1, True))
    node.handle_timeout(TimerKind.ELECTION)
    assert node.persistent.current_term == 2
    assert node.role is Role.CANDIDATE
    assert node.handle_message(RequestVoteResponse(2, 2, True)) == []
    assert node.role is Role.CANDIDATE


def test_leader_ignores_election_timeout():
    node = make_leader()
    assert node.handle_timeout(TimerKind.ELECTION) == []
    assert node.role is Role.LEADER
    assert node.persistent.current_term == 1


def test_follower_grants_vote_and_resets_election_timer():
    node = make_node(0)
    effects = node.handle_message(RequestVote(1, 3, 0, 0))
    assert effects == [Send(3, RequestVoteResponse(1, 0, True)), ELECTION]
    assert node.persistent.current_term == 1
    assert node.persistent.voted_for == 3


def test_vote_is_granted_again_to_same_candidate():
    node = make_node(0)
    node.handle_message(RequestVote(1, 3, 0, 0))
    effects = node.handle_message(RequestVote(1, 3, 0, 0))
    assert effects == [Send(3, RequestVoteResponse(1, 0, True)), ELECTION]


def test_node_never_votes_for_two_candidates_in_one_term():
    node = make_node(0)
    node.handle_message(RequestVote(1, 3, 0, 0))
    effects = node.handle_message(RequestVote(1, 4, 0, 0))
    assert effects == [Send(4, RequestVoteResponse(1, 0, False))]
    assert node.persistent.voted_for == 3


def test_vote_survives_restart():
    node = make_node(0)
    node.handle_message(RequestVote(1, 3, 0, 0))
    restarted = make_node(0, node.persistent)
    assert restarted.handle_message(RequestVote(1, 4, 0, 0)) == [
        Send(4, RequestVoteResponse(1, 0, False))
    ]


def test_request_vote_from_stale_term_is_rejected_with_current_term():
    node = make_node(0, PersistentState(5, None, []))
    effects = node.handle_message(RequestVote(4, 3, 0, 0))
    assert effects == [Send(3, RequestVoteResponse(5, 0, False))]
    assert node.persistent.voted_for is None


def test_candidate_with_stale_log_is_denied():
    log = [LogEntry(1, ("a",)), LogEntry(2, ("b",))]
    for last_index, last_term in ((5, 1), (1, 2), (0, 0)):
        node = make_node(0, PersistentState(2, None, list(log)))
        effects = node.handle_message(RequestVote(3, 3, last_index, last_term))
        assert effects == [Send(3, RequestVoteResponse(3, 0, False))]
        assert node.persistent.current_term == 3
        assert node.persistent.voted_for is None


def test_candidate_with_equal_or_newer_log_gets_vote():
    log = [LogEntry(1, ("a",)), LogEntry(2, ("b",))]
    for last_index, last_term in ((2, 2), (3, 2), (1, 3)):
        node = make_node(0, PersistentState(2, None, list(log)))
        effects = node.handle_message(RequestVote(3, 3, last_index, last_term))
        assert effects == [Send(3, RequestVoteResponse(3, 0, True)), ELECTION]


def test_majority_of_votes_makes_leader_and_resets_leader_state():
    node = make_node(0, PersistentState(0, None, [LogEntry(0, ("a",))]))
    node.next_index = {1: 99}
    node.match_index = {1: 99}
    node.handle_timeout(TimerKind.ELECTION)
    assert node.handle_message(RequestVoteResponse(1, 1, True)) == []
    assert node.role is Role.CANDIDATE
    effects = node.handle_message(RequestVoteResponse(1, 2, True))
    assert node.role is Role.LEADER
    assert node.leader_id == 0
    assert node.next_index == {1: 2, 2: 2, 3: 2, 4: 2}
    assert node.match_index == {1: 0, 2: 0, 3: 0, 4: 0}
    assert node.persistent.log[-1] == LogEntry(1, NOOP)
    assert effects == [
        Send(peer, AppendEntries(1, 0, 1, 0, (LogEntry(1, NOOP),), 0)) for peer in (1, 2, 3, 4)
    ] + [HEARTBEAT]


def test_duplicate_vote_from_same_voter_counts_once():
    node = make_node(0)
    node.handle_timeout(TimerKind.ELECTION)
    node.handle_message(RequestVoteResponse(1, 1, True))
    assert node.handle_message(RequestVoteResponse(1, 1, True)) == []
    assert node.role is Role.CANDIDATE


def test_denied_votes_do_not_count():
    node = make_node(0)
    node.handle_timeout(TimerKind.ELECTION)
    for voter in node.peers:
        node.handle_message(RequestVoteResponse(1, voter, False))
    assert node.role is Role.CANDIDATE


def test_vote_response_from_earlier_election_is_ignored():
    node = make_node(0)
    node.handle_timeout(TimerKind.ELECTION)
    node.handle_timeout(TimerKind.ELECTION)
    assert node.handle_message(RequestVoteResponse(1, 1, True)) == []
    assert node.handle_message(RequestVoteResponse(1, 2, True)) == []
    assert node.role is Role.CANDIDATE


def test_vote_response_is_ignored_when_no_longer_candidate():
    node = make_node(0)
    node.handle_timeout(TimerKind.ELECTION)
    node.handle_message(RequestVoteResponse(1, 1, True))
    node.handle_message(AppendEntries(1, 4, 0, 0, (), 0))
    assert node.handle_message(RequestVoteResponse(1, 2, True)) == []
    assert node.role is Role.FOLLOWER


def test_extra_votes_after_winning_do_nothing():
    node = make_leader()
    assert node.handle_message(RequestVoteResponse(1, 3, True)) == []
    assert node.role is Role.LEADER


def test_higher_term_makes_candidate_step_down():
    node = make_node(0)
    node.handle_timeout(TimerKind.ELECTION)
    effects = node.handle_message(RequestVoteResponse(2, 1, False))
    assert effects == []
    assert node.role is Role.FOLLOWER
    assert node.persistent.current_term == 2
    assert node.persistent.voted_for is None


def test_higher_term_makes_leader_step_down_and_restart_election_timer():
    for msg in (
        RequestVoteResponse(2, 1, False),
        AppendEntriesResponse(2, 1, False, 0),
    ):
        node = make_leader()
        assert node.handle_message(msg) == [ELECTION]
        assert node.role is Role.FOLLOWER
        assert node.leader_id is None
        assert node.persistent.current_term == 2
        assert node.handle_timeout(TimerKind.HEARTBEAT) == []


def test_leader_stepping_down_for_vote_request_sets_one_election_timer():
    node = make_leader()
    effects = node.handle_message(RequestVote(2, 3, 1, 1))
    assert effects == [Send(3, RequestVoteResponse(2, 0, True)), ELECTION]
    assert node.role is Role.FOLLOWER


def test_leader_sends_heartbeats_on_heartbeat_timeout():
    node = make_leader()
    assert node.handle_timeout(TimerKind.HEARTBEAT) == heartbeats(node) + [HEARTBEAT]


def test_append_entries_from_stale_term_is_rejected():
    node = make_node(0, PersistentState(5, None, []))
    effects = node.handle_message(AppendEntries(4, 2, 0, 0, (), 0))
    assert effects == [Send(2, AppendEntriesResponse(5, 0, False, 0))]
    assert node.leader_id is None


def test_append_entries_records_leader_and_resets_election_timer():
    node = make_node(0)
    effects = node.handle_message(AppendEntries(1, 2, 0, 0, (), 0))
    assert effects == [ELECTION, Send(2, AppendEntriesResponse(1, 0, True, 0))]
    assert node.leader_id == 2
    assert node.persistent.current_term == 1


def test_candidate_steps_down_for_leader_of_same_term():
    node = make_node(0)
    node.handle_timeout(TimerKind.ELECTION)
    node.handle_message(AppendEntries(1, 2, 0, 0, (), 0))
    assert node.role is Role.FOLLOWER
    assert node.leader_id == 2
    assert node.persistent.voted_for == 0


def test_single_node_cluster_elects_itself():
    node = RaftNode(0, [0])
    assert node.handle_timeout(TimerKind.ELECTION) == [HEARTBEAT]
    assert node.role is Role.LEADER


def entries(*terms):
    return [LogEntry(term, ("PUT", f"k{i}", i)) for i, term in enumerate(terms, 1)]


def make_follower(*terms, term=None):
    log = entries(*terms)
    return make_node(0, PersistentState(term or max(terms, default=1), None, log))


def test_leader_appends_command_and_sends_it_to_every_peer():
    node = make_leader()
    effects = node.handle_client_command(ClientCommand(7, ("PUT", "a", 1)))
    noop, entry = LogEntry(1, NOOP), LogEntry(1, ("PUT", "a", 1))
    assert node.persistent.log == [noop, entry]
    sent = (noop, entry)
    assert effects == [Send(peer, AppendEntries(1, 0, 0, 0, sent, 0)) for peer in node.peers]


def test_new_leader_starts_after_its_existing_log():
    node = make_leader(0, PersistentState(2, None, entries(1, 2)))
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    sent = node.handle_timeout(TimerKind.HEARTBEAT)[0].msg
    assert (sent.prev_log_index, sent.prev_log_term) == (2, 2)
    assert sent.entries == (LogEntry(3, NOOP), LogEntry(3, ("PUT", "a", 1)))


def test_heartbeat_resends_entries_that_are_not_acknowledged():
    node = make_leader()
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    node.handle_client_command(ClientCommand(2, ("PUT", "b", 2)))
    node.handle_message(AppendEntriesResponse(1, 1, True, 2))
    effects = node.handle_timeout(TimerKind.HEARTBEAT)
    log = node.persistent.log
    assert effects[0] == Send(1, AppendEntries(1, 0, 2, 1, (log[2],), 0))
    assert effects[1] == Send(2, AppendEntries(1, 0, 0, 0, tuple(log), 0))


def test_follower_appends_entries_and_reports_match_index():
    node = make_follower(1)
    new = tuple(entries(1, 1, 1)[1:])
    effects = node.handle_message(AppendEntries(1, 2, 1, 1, new, 0))
    assert node.persistent.log == entries(1, 1, 1)
    assert effects == [ELECTION, Send(2, AppendEntriesResponse(1, 0, True, 3))]


def test_follower_rejects_when_previous_entry_is_missing():
    node = make_follower(1)
    effects = node.handle_message(AppendEntries(1, 2, 2, 1, (LogEntry(1, ("X",)),), 0))
    assert effects == [ELECTION, Send(2, AppendEntriesResponse(1, 0, False, 0))]
    assert node.persistent.log == entries(1)
    assert node.leader_id == 2


def test_follower_rejects_when_previous_term_differs():
    node = make_follower(1, 1, term=3)
    effects = node.handle_message(AppendEntries(3, 2, 2, 2, (LogEntry(3, ("X",)),), 0))
    assert effects == [ELECTION, Send(2, AppendEntriesResponse(3, 0, False, 0))]
    assert node.persistent.log == entries(1, 1)


def test_candidate_steps_down_even_when_consistency_check_fails():
    node = make_node(0)
    node.handle_timeout(TimerKind.ELECTION)
    node.handle_message(AppendEntries(1, 2, 3, 1, (), 0))
    assert node.role is Role.FOLLOWER
    assert node.leader_id == 2


def test_follower_replaces_conflicting_suffix():
    node = make_follower(1, 2, 2, term=3)
    new = (LogEntry(3, ("NEW", 2)), LogEntry(3, ("NEW", 3)))
    effects = node.handle_message(AppendEntries(3, 2, 1, 1, new, 0))
    assert node.persistent.log == entries(1) + list(new)
    assert effects[-1] == Send(2, AppendEntriesResponse(3, 0, True, 3))


def test_conflict_truncates_everything_after_it():
    node = make_follower(1, 2, 2, term=3)
    new = (LogEntry(3, ("NEW", 2)),)
    node.handle_message(AppendEntries(3, 2, 1, 1, new, 0))
    assert node.persistent.log == entries(1) + list(new)


def test_late_shorter_append_entries_does_not_truncate():
    node = make_follower(1, 1, 1)
    old = tuple(entries(1, 1)[1:])
    effects = node.handle_message(AppendEntries(1, 2, 1, 1, old, 0))
    assert node.persistent.log == entries(1, 1, 1)
    assert effects[-1] == Send(2, AppendEntriesResponse(1, 0, True, 2))


def test_late_heartbeat_does_not_truncate():
    node = make_follower(1, 1, 1)
    effects = node.handle_message(AppendEntries(1, 2, 0, 0, (), 0))
    assert node.persistent.log == entries(1, 1, 1)
    assert effects[-1] == Send(2, AppendEntriesResponse(1, 0, True, 0))


def test_duplicate_append_entries_changes_nothing():
    node = make_follower()
    msg = AppendEntries(1, 2, 0, 0, tuple(entries(1, 1)), 0)
    first = node.handle_message(msg)
    assert node.handle_message(msg) == first
    assert node.persistent.log == entries(1, 1)


def leader_with_two_entries():
    node = make_leader()
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    node.handle_client_command(ClientCommand(2, ("PUT", "b", 2)))
    return node


def test_success_response_advances_match_and_next_index():
    node = leader_with_two_entries()
    assert node.handle_message(AppendEntriesResponse(1, 3, True, 2)) == []
    assert (node.match_index[3], node.next_index[3]) == (2, 3)
    assert (node.match_index[1], node.next_index[1]) == (0, 1)


def test_stale_success_response_does_not_move_match_index_back():
    node = leader_with_two_entries()
    node.handle_message(AppendEntriesResponse(1, 3, True, 2))
    node.handle_message(AppendEntriesResponse(1, 3, True, 1))
    node.handle_message(AppendEntriesResponse(1, 3, True, 0))
    assert (node.match_index[3], node.next_index[3]) == (2, 3)


def test_failure_response_backs_up_and_retries_at_once():
    node = make_leader(0, PersistentState(2, None, entries(1, 2)))
    log = node.persistent.log
    effects = node.handle_message(AppendEntriesResponse(3, 1, False, 0))
    assert node.next_index[1] == 2
    assert effects == [Send(1, AppendEntries(3, 0, 1, 1, (log[1], log[2]), 0))]
    effects = node.handle_message(AppendEntriesResponse(3, 1, False, 0))
    assert node.next_index[1] == 1
    assert effects == [Send(1, AppendEntries(3, 0, 0, 0, tuple(log), 0))]


def test_stale_failure_response_never_drops_next_index_to_match_index():
    node = leader_with_two_entries()
    node.handle_message(AppendEntriesResponse(1, 3, True, 2))
    node.handle_message(AppendEntriesResponse(1, 3, False, 0))
    node.handle_message(AppendEntriesResponse(1, 3, False, 0))
    assert (node.match_index[3], node.next_index[3]) == (2, 3)


def test_append_response_from_another_term_is_ignored():
    node = make_leader(0, PersistentState(2, None, entries(1, 2)))
    assert node.handle_message(AppendEntriesResponse(2, 1, True, 2)) == []
    assert node.handle_message(AppendEntriesResponse(2, 1, False, 0)) == []
    assert (node.match_index[1], node.next_index[1]) == (0, 3)


def test_append_response_is_ignored_when_not_leader():
    node = make_follower(1)
    assert node.handle_message(AppendEntriesResponse(1, 1, False, 0)) == []
    assert node.handle_message(AppendEntriesResponse(1, 1, True, 1)) == []


def ack(node, peer, match_index):
    term = node.persistent.current_term
    return node.handle_message(AppendEntriesResponse(term, peer, True, match_index))


def test_leader_commits_only_once_a_majority_has_the_entry():
    node = leader_with_two_entries()
    ack(node, 1, 1)
    assert node.commit_index == 0
    ack(node, 2, 1)
    assert (node.commit_index, node.last_applied) == (1, 1)


def test_leader_commits_highest_index_a_majority_has():
    node = leader_with_two_entries()
    ack(node, 1, 2)
    ack(node, 2, 2)
    assert (node.commit_index, node.last_applied) == (2, 2)


def test_leader_commits_lower_index_when_higher_lacks_majority():
    node = leader_with_two_entries()
    ack(node, 1, 2)
    ack(node, 2, 1)
    assert node.commit_index == 1


def test_duplicate_acks_from_one_peer_do_not_make_a_majority():
    node = leader_with_two_entries()
    ack(node, 1, 2)
    ack(node, 1, 2)
    assert node.commit_index == 0


def test_old_term_entry_is_not_committed_by_counting_replicas():
    node = make_leader(0, PersistentState(2, None, entries(1, 2)))
    for peer in node.peers:
        ack(node, peer, 2)
    assert node.commit_index == 0
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    ack(node, 1, 3)
    assert node.commit_index == 0
    ack(node, 2, 3)
    assert (node.commit_index, node.last_applied) == (3, 3)


def test_leader_sends_its_commit_index():
    node = leader_with_two_entries()
    ack(node, 1, 2)
    ack(node, 2, 2)
    sent = node.handle_timeout(TimerKind.HEARTBEAT)[0].msg
    assert sent.leader_commit == 2


def test_single_node_cluster_commits_at_once():
    node = RaftNode(0, [0])
    node.handle_timeout(TimerKind.ELECTION)
    assert node.handle_client_command(ClientCommand(1, ("PUT", "a", 1))) == [
        ClientReply(1, ok=True)
    ]
    assert (node.commit_index, node.last_applied) == (2, 2)


def test_follower_commit_follows_leader_commit():
    node = make_follower()
    node.handle_message(AppendEntries(1, 2, 0, 0, tuple(entries(1, 1, 1)), 2))
    assert (node.commit_index, node.last_applied) == (2, 2)


def test_follower_commit_stops_at_last_new_entry():
    node = make_follower()
    node.handle_message(AppendEntries(1, 2, 0, 0, tuple(entries(1, 1)), 5))
    assert (node.commit_index, node.last_applied) == (2, 2)


def test_follower_commit_uses_last_entry_of_message_not_log_length():
    node = make_follower(1, 1, 1)
    node.handle_message(AppendEntries(1, 2, 0, 0, tuple(entries(1)), 3))
    assert node.commit_index == 1


def test_follower_does_not_commit_when_consistency_check_fails():
    node = make_follower(1)
    node.handle_message(AppendEntries(1, 2, 2, 1, (), 1))
    assert (node.commit_index, node.last_applied) == (0, 0)


def test_follower_commit_index_never_goes_backwards():
    node = make_follower()
    node.handle_message(AppendEntries(1, 2, 0, 0, tuple(entries(1, 1, 1)), 3))
    node.handle_message(AppendEntries(1, 2, 0, 0, tuple(entries(1, 1)), 1))
    node.handle_message(AppendEntries(1, 2, 0, 0, (), 0))
    node.handle_message(AppendEntries(1, 2, 3, 1, (), 2))
    assert (node.commit_index, node.last_applied) == (3, 3)


def test_leader_ignores_append_entries_from_its_own_term():
    node = leader_with_two_entries()
    log = list(node.persistent.log)
    other = (LogEntry(1, ("OTHER",)),)
    assert node.handle_message(AppendEntries(1, 2, 0, 0, other, 1)) == []
    assert node.role is Role.LEADER
    assert node.leader_id == 0
    assert node.persistent.log == log
    assert node.commit_index == 0


def applied_from(log):
    return [(index, entry.command) for index, entry in enumerate(log, 1)]


def test_new_node_has_applied_nothing():
    assert make_node().applied == []


def test_follower_applies_each_entry_in_order_when_commit_jumps():
    node = make_follower()
    log = entries(1, 1, 1)
    node.handle_message(AppendEntries(1, 2, 0, 0, tuple(log), 3))
    assert node.applied == applied_from(log)
    assert (node.commit_index, node.last_applied) == (3, 3)


def test_leader_applies_each_entry_in_order_when_commit_jumps():
    node = make_leader()
    for i in range(3):
        node.handle_client_command(ClientCommand(i, ("PUT", f"k{i}", i)))
    ack(node, 1, 4)
    assert node.applied == []
    ack(node, 2, 4)
    assert node.applied == applied_from(node.persistent.log)


def test_applying_continues_from_last_applied():
    node = make_follower()
    log = entries(1, 1, 1)
    node.handle_message(AppendEntries(1, 2, 0, 0, tuple(log), 1))
    assert node.applied == applied_from(log)[:1]
    node.handle_message(AppendEntries(1, 2, 3, 1, (), 3))
    assert node.applied == applied_from(log)


def test_duplicate_append_entries_applies_nothing_twice():
    node = make_follower()
    log = entries(1, 1)
    msg = AppendEntries(1, 2, 0, 0, tuple(log), 2)
    node.handle_message(msg)
    node.handle_message(msg)
    assert node.applied == applied_from(log)


def test_restarted_node_applies_again_from_the_first_entry():
    log = entries(1, 1)
    persistent = PersistentState(1, None, [])
    make_node(0, persistent).handle_message(AppendEntries(1, 2, 0, 0, tuple(log), 2))
    node = make_node(0, persistent)
    assert node.applied == []
    node.handle_message(AppendEntries(1, 2, 2, 1, (), 2))
    assert node.applied == applied_from(log)


def test_failure_response_that_cannot_move_next_index_sends_nothing():
    node = leader_with_two_entries()
    ack(node, 3, 2)
    assert node.handle_message(AppendEntriesResponse(1, 3, False, 0)) == []
    assert (node.match_index[3], node.next_index[3]) == (2, 3)
    assert node.handle_message(AppendEntriesResponse(1, 1, False, 0)) == []
    assert (node.match_index[1], node.next_index[1]) == (0, 1)


def test_leader_replies_with_result_once_entry_commits():
    node = make_leader()
    assert not any(
        isinstance(e, ClientReply)
        for e in node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    )
    node.handle_client_command(ClientCommand(2, ("GET", "a")))
    assert ack(node, 1, 3) == []
    assert ack(node, 2, 3) == [ClientReply(1, ok=True), ClientReply(2, ok=True, result=1)]
    assert node.kv.data == {"a": 1}


def test_follower_applies_to_kv_without_replying():
    node = make_follower()
    log = [LogEntry(1, ("PUT", "a", 1)), LogEntry(1, ("DELETE", "a"))]
    effects = node.handle_message(AppendEntries(1, 2, 0, 0, tuple(log), 2))
    assert not any(isinstance(e, ClientReply) for e in effects)
    assert node.kv.data == {}


def test_leader_that_steps_down_sends_no_reply_for_pending_request():
    node = make_leader()
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    node.handle_message(AppendEntries(2, 3, 0, 0, (), 0))
    assert node.pending == {}
    assert node.role is Role.FOLLOWER


def test_invalid_command_is_applied_as_error_reply():
    node = RaftNode(0, [0])
    node.handle_timeout(TimerKind.ELECTION)
    [reply] = node.handle_client_command(ClientCommand(1, ("INCR", "a")))
    assert reply.request_id == 1 and not reply.ok and "INCR" in reply.result


def test_restarted_node_rebuilds_kv_from_log():
    log = [LogEntry(1, ("PUT", "a", 1)), LogEntry(1, ("PUT", "b", 2))]
    persistent = PersistentState(1, None, list(log))
    node = make_node(0, persistent)
    assert node.kv.data == {}
    node.handle_message(AppendEntries(1, 2, 2, 1, (), 2))
    assert node.kv.data == {"a": 1, "b": 2}


def replies_of(effects):
    return [e for e in effects if isinstance(e, ClientReply)]


def test_leader_deposed_by_request_vote_clears_pending():
    node = make_leader()
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    node.handle_message(RequestVote(2, 3, 5, 2))
    assert node.role is Role.FOLLOWER
    assert node.pending == {}


def test_each_command_gets_its_own_reply_when_commit_jumps():
    node = make_leader()
    for i in range(3):
        node.handle_client_command(ClientCommand(10 + i, ("PUT", f"k{i}", i)))
    ack(node, 1, 4)
    replies = replies_of(ack(node, 2, 4))
    assert [r.request_id for r in replies] == [10, 11, 12]
    assert all(r.ok for r in replies)


def test_request_id_zero_still_gets_a_reply():
    node = RaftNode(0, [0])
    node.handle_timeout(TimerKind.ELECTION)
    assert node.handle_client_command(ClientCommand(0, ("GET", "a"))) == [ClientReply(0, ok=True)]


def test_reply_is_sent_only_once():
    node = make_leader()
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    ack(node, 1, 2)
    assert len(replies_of(ack(node, 2, 2))) == 1
    assert replies_of(ack(node, 2, 2)) == []
    assert replies_of(ack(node, 3, 2)) == []
    assert replies_of(node.handle_timeout(TimerKind.HEARTBEAT)) == []


def test_failure_response_emits_no_reply():
    node = make_leader()
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    effects = node.handle_message(AppendEntriesResponse(1, 1, False, 0))
    assert replies_of(effects) == []
    assert 1 in node.pending.values()


def test_old_term_entries_committed_under_new_leader_get_no_reply():
    node = make_leader(0, PersistentState(2, None, entries(1, 1)))
    assert node.pending == {}
    node.handle_client_command(ClientCommand(5, ("PUT", "a", 1)))
    ack(node, 1, 4)
    effects = ack(node, 2, 4)
    assert node.commit_index == 4
    assert replies_of(effects) == [ClientReply(5, ok=True)]
    assert len(node.applied) == 4


def test_invalid_command_does_not_stop_later_entries_applying():
    node = RaftNode(0, [0])
    node.handle_timeout(TimerKind.ELECTION)
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    bad = node.handle_client_command(ClientCommand(2, ("BOGUS",)))
    last = node.handle_client_command(ClientCommand(3, ("GET", "a")))
    assert [r.ok for r in bad] == [False]
    assert last == [ClientReply(3, ok=True, result=1)]
    assert (node.commit_index, node.last_applied) == (4, 4)
    assert node.kv.data == {"a": 1}


def test_follower_survives_invalid_command_in_log():
    node = make_follower()
    log = (LogEntry(1, ("NOPE",)), LogEntry(1, ("PUT", "a", 1)))
    effects = node.handle_message(AppendEntries(1, 2, 0, 0, log, 2))
    assert replies_of(effects) == []
    assert node.kv.data == {"a": 1}
    assert node.last_applied == 2


def test_restarted_node_has_no_pending_or_queued_replies():
    leader = make_leader()
    leader.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    fresh = make_node(0, leader.persistent)
    assert fresh.pending == {} and fresh.replies == []
    assert fresh.kv.data == {}


def test_no_reply_is_left_queued_after_any_handler_returns():
    node = make_leader()
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    ack(node, 1, 1)
    ack(node, 2, 1)
    node.handle_timeout(TimerKind.HEARTBEAT)
    node.handle_message(AppendEntries(3, 4, 0, 0, (), 0))
    assert node.replies == []


def test_new_leader_commits_old_term_entries_through_its_noop():
    node = make_leader(0, PersistentState(2, None, entries(1, 1)))
    assert node.persistent.log[-1] == LogEntry(3, NOOP)
    ack(node, 1, 2)
    ack(node, 2, 2)
    assert node.commit_index == 0
    ack(node, 1, 3)
    assert ack(node, 2, 3) == []
    assert node.commit_index == 3
    assert node.kv.data == {"k1": 1, "k2": 2}


def test_noop_is_applied_without_touching_kv_or_replying():
    node = make_follower()
    log = (LogEntry(1, NOOP), LogEntry(1, ("PUT", "a", 1)))
    effects = node.handle_message(AppendEntries(1, 2, 0, 0, log, 2))
    assert replies_of(effects) == []
    assert node.applied == [(1, NOOP), (2, ("PUT", "a", 1))]
    assert node.kv.data == {"a": 1}


def test_single_node_leader_commits_its_noop_at_once():
    node = RaftNode(0, [0])
    node.handle_timeout(TimerKind.ELECTION)
    assert (node.commit_index, node.applied) == (1, [(1, NOOP)])


def session(client_id, seq, op):
    return ("SESSION", client_id, seq, op)


def single_leader():
    node = RaftNode(0, [0])
    node.handle_timeout(TimerKind.ELECTION)
    return node


def test_session_command_is_applied_and_answered_like_a_plain_one():
    node = single_leader()
    put = node.handle_client_command(ClientCommand("p", session(7, 1, ("PUT", "a", 1))))
    get = node.handle_client_command(ClientCommand("g", session(7, 2, ("GET", "a"))))
    assert put == [ClientReply("p", ok=True)]
    assert get == [ClientReply("g", ok=True, result=1)]
    assert node.kv.sessions == {7: (2, (True, 1))}


def test_retried_session_command_is_logged_twice_but_applied_once():
    node = single_leader()
    node.handle_client_command(ClientCommand(1, session(7, 1, ("PUT", "a", 1))))
    delete = ClientCommand(2, session(7, 2, ("DELETE", "a")))
    assert node.handle_client_command(delete) == [ClientReply(2, ok=True, result=True)]
    node.handle_client_command(ClientCommand(3, ("PUT", "a", 9)))
    assert node.handle_client_command(delete) == [ClientReply(2, ok=True, result=True)]
    assert [entry.command for entry in node.persistent.log].count(delete.command) == 2
    assert node.kv.data == {"a": 9}


def test_retry_is_answered_from_the_table_by_a_leader_that_never_saw_the_first_copy():
    delete = session(7, 2, ("DELETE", "a"))
    log = [LogEntry(1, session(7, 1, ("PUT", "a", 1))), LogEntry(1, delete)]
    node = make_leader(0, PersistentState(1, None, log))
    node.handle_client_command(ClientCommand("retry", delete))
    ack(node, 1, 4)
    assert replies_of(ack(node, 2, 4)) == [ClientReply("retry", ok=True, result=True)]
    assert node.kv.data == {}


def test_older_seq_gets_a_stale_reply_and_changes_nothing():
    node = single_leader()
    node.handle_client_command(ClientCommand(1, session(7, 1, ("PUT", "a", 1))))
    node.handle_client_command(ClientCommand(2, session(7, 2, ("PUT", "a", 2))))
    late = node.handle_client_command(ClientCommand(3, session(7, 1, ("PUT", "a", 1))))
    assert late == [ClientReply(3, ok=False, result=STALE_SEQ)]
    assert node.kv.data == {"a": 2}


def test_follower_builds_the_same_session_table_as_the_leader():
    leader = single_leader()
    for seq, op in enumerate([("PUT", "a", 1), ("GET", "a"), ("GET", "a"), ("DELETE", "a")], 1):
        leader.handle_client_command(ClientCommand(seq, session(7 + seq % 2, seq, op)))
    leader.handle_client_command(ClientCommand(9, session(7, 4, ("DELETE", "a"))))
    log = tuple(leader.persistent.log)
    follower = make_follower()
    follower.handle_message(AppendEntries(1, 2, 0, 0, log, len(log)))
    assert follower.kv.sessions == leader.kv.sessions != {}
    assert follower.kv.data == leader.kv.data


def test_restarted_node_rebuilds_the_session_table_from_its_log():
    node = single_leader()
    delete = session(7, 2, ("DELETE", "a"))
    node.handle_client_command(ClientCommand(1, session(7, 1, ("PUT", "a", 1))))
    node.handle_client_command(ClientCommand(2, delete))
    node.handle_client_command(ClientCommand(3, session(8, 1, ("GET", "a"))))
    before = dict(node.kv.sessions)
    restarted = RaftNode(0, [0], node.persistent)
    assert restarted.kv.sessions == {}
    restarted.handle_timeout(TimerKind.ELECTION)
    assert restarted.kv.sessions == before
    retry = restarted.handle_client_command(ClientCommand("again", delete))
    assert retry == [ClientReply("again", ok=True, result=True)]
