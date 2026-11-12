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
    return [Send(peer, AppendEntries(term, node.node_id, 0, 0, (), 0)) for peer in node.peers]


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
    assert effects == [
        Send(peer, AppendEntries(1, 0, 1, 0, (), 0)) for peer in (1, 2, 3, 4)
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
    effects = node.handle_message(RequestVote(2, 3, 0, 0))
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
    entry = LogEntry(1, ("PUT", "a", 1))
    assert node.persistent.log == [entry]
    assert effects == [Send(peer, AppendEntries(1, 0, 0, 0, (entry,), 0)) for peer in node.peers]


def test_new_leader_starts_after_its_existing_log():
    node = make_leader(0, PersistentState(2, None, entries(1, 2)))
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    sent = node.handle_timeout(TimerKind.HEARTBEAT)[0].msg
    assert (sent.prev_log_index, sent.prev_log_term) == (2, 2)
    assert sent.entries == (LogEntry(3, ("PUT", "a", 1)),)


def test_heartbeat_resends_entries_that_are_not_acknowledged():
    node = make_leader()
    node.handle_client_command(ClientCommand(1, ("PUT", "a", 1)))
    node.handle_client_command(ClientCommand(2, ("PUT", "b", 2)))
    node.handle_message(AppendEntriesResponse(1, 1, True, 1))
    effects = node.handle_timeout(TimerKind.HEARTBEAT)
    log = node.persistent.log
    assert effects[0] == Send(1, AppendEntries(1, 0, 1, 1, (log[1],), 0))
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
    assert effects == [Send(1, AppendEntries(3, 0, 1, 1, (log[1],), 0))]
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
    assert node.handle_client_command(ClientCommand(1, ("PUT", "a", 1))) == []
    assert (node.commit_index, node.last_applied) == (1, 1)


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
    ack(node, 1, 3)
    assert node.applied == []
    ack(node, 2, 3)
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
