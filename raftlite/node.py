from dataclasses import dataclass, field
from enum import Enum

from raftlite.kv import KVStore
from raftlite.messages import (
    AppendEntries,
    AppendEntriesResponse,
    ClientCommand,
    ClientReply,
    Effect,
    LogEntry,
    Message,
    RequestVote,
    RequestVoteResponse,
    Send,
    SetTimer,
    TimerKind,
)


NOOP = ("NOOP",)


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: int | None = None
    log: list[LogEntry] = field(default_factory=list)


class RaftNode:
    def __init__(self, node_id, node_ids, persistent=None):
        self.node_id = node_id
        self.peers = tuple(sorted(n for n in node_ids if n != node_id))
        self.persistent = persistent if persistent is not None else PersistentState()
        self.role = Role.FOLLOWER
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self.applied = []
        self.kv = KVStore()
        self.pending = {}
        self.replies = []
        self.next_index = {}
        self.match_index = {}
        self.votes = set()

    def start(self) -> list[Effect]:
        return [SetTimer(TimerKind.ELECTION)]

    def handle_timeout(self, kind: TimerKind) -> list[Effect]:
        if kind is TimerKind.ELECTION and self.role is not Role.LEADER:
            return self._start_election()
        if kind is TimerKind.HEARTBEAT and self.role is Role.LEADER:
            return self._broadcast_append_entries() + [SetTimer(TimerKind.HEARTBEAT)]
        return []

    def handle_message(self, msg: Message) -> list[Effect]:
        was_leader = self.role is Role.LEADER
        if msg.term > self.persistent.current_term:
            self._become_follower(msg.term)
        match msg:
            case RequestVote():
                effects = self._on_request_vote(msg)
            case RequestVoteResponse():
                effects = self._on_request_vote_response(msg)
            case AppendEntries():
                effects = self._on_append_entries(msg)
            case AppendEntriesResponse():
                effects = self._on_append_entries_response(msg)
            case _:
                raise TypeError(f"unknown message: {msg!r}")
        effects += self._take_replies()
        election_timer = SetTimer(TimerKind.ELECTION)
        if was_leader and self.role is not Role.LEADER and election_timer not in effects:
            effects.append(election_timer)
        return effects

    def handle_client_command(self, cmd: ClientCommand) -> list[Effect]:
        if self.role is not Role.LEADER:
            return [ClientReply(cmd.request_id, ok=False, leader_hint=self.leader_id)]
        self.persistent.log.append(LogEntry(self.persistent.current_term, cmd.command))
        self.pending[self._last_log_index()] = cmd.request_id
        self._advance_commit_index()
        return self._broadcast_append_entries() + self._take_replies()

    def _last_log_index(self):
        return len(self.persistent.log)

    def _last_log_term(self):
        return self.persistent.log[-1].term if self.persistent.log else 0

    def _log_up_to_date(self, last_log_index, last_log_term):
        return (last_log_term, last_log_index) >= (self._last_log_term(), self._last_log_index())

    def _has_majority(self, count):
        return 2 * count > len(self.peers) + 1

    def _can_grant_vote(self, msg):
        return (
            msg.term == self.persistent.current_term
            and self.persistent.voted_for in (None, msg.candidate_id)
            and self._log_up_to_date(msg.last_log_index, msg.last_log_term)
        )

    def _become_follower(self, term):
        self.persistent.current_term = term
        self.persistent.voted_for = None
        self.role = Role.FOLLOWER
        self.leader_id = None
        self.pending.clear()

    def _start_election(self):
        self.persistent.current_term += 1
        self.persistent.voted_for = self.node_id
        self.role = Role.CANDIDATE
        self.leader_id = None
        self.votes = {self.node_id}
        if self._has_majority(len(self.votes)):
            return self._become_leader()
        request = RequestVote(
            self.persistent.current_term,
            self.node_id,
            self._last_log_index(),
            self._last_log_term(),
        )
        return [Send(peer, request) for peer in self.peers] + [SetTimer(TimerKind.ELECTION)]

    def _become_leader(self):
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self.next_index = {peer: self._last_log_index() + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self._append_noop()
        return self._broadcast_append_entries() + [SetTimer(TimerKind.HEARTBEAT)]

    def _append_noop(self):
        self.persistent.log.append(LogEntry(self.persistent.current_term, NOOP))
        self._advance_commit_index()

    def _broadcast_append_entries(self):
        return [Send(peer, self._append_entries_for(peer)) for peer in self.peers]

    def _append_entries_for(self, peer):
        prev_index = self.next_index[peer] - 1
        prev_term = self.persistent.log[prev_index - 1].term if prev_index else 0
        return AppendEntries(
            self.persistent.current_term,
            self.node_id,
            prev_index,
            prev_term,
            tuple(self.persistent.log[prev_index:]),
            self.commit_index,
        )

    def _on_request_vote(self, msg):
        term = self.persistent.current_term
        if not self._can_grant_vote(msg):
            return [Send(msg.candidate_id, RequestVoteResponse(term, self.node_id, False))]
        self.persistent.voted_for = msg.candidate_id
        return [
            Send(msg.candidate_id, RequestVoteResponse(term, self.node_id, True)),
            SetTimer(TimerKind.ELECTION),
        ]

    def _on_request_vote_response(self, msg):
        if (
            self.role is not Role.CANDIDATE
            or msg.term != self.persistent.current_term
            or not msg.vote_granted
        ):
            return []
        self.votes.add(msg.voter_id)
        if self._has_majority(len(self.votes)):
            return self._become_leader()
        return []

    def _on_append_entries(self, msg):
        term = self.persistent.current_term
        if msg.term < term:
            return [Send(msg.leader_id, AppendEntriesResponse(term, self.node_id, False, 0))]
        if self.role is Role.LEADER:
            return []
        self.role = Role.FOLLOWER
        self.leader_id = msg.leader_id
        success = self._log_matches(msg.prev_log_index, msg.prev_log_term)
        match_index = 0
        if success:
            self._store_entries(msg.prev_log_index, msg.entries)
            match_index = msg.prev_log_index + len(msg.entries)
            self._set_commit_index(min(msg.leader_commit, match_index))
        return [
            SetTimer(TimerKind.ELECTION),
            Send(msg.leader_id, AppendEntriesResponse(term, self.node_id, success, match_index)),
        ]

    def _log_matches(self, prev_log_index, prev_log_term):
        if prev_log_index == 0:
            return True
        log = self.persistent.log
        return prev_log_index <= len(log) and log[prev_log_index - 1].term == prev_log_term

    def _store_entries(self, prev_log_index, entries):
        log = self.persistent.log
        for index, entry in enumerate(entries, prev_log_index + 1):
            if index <= len(log) and log[index - 1].term != entry.term:
                del log[index - 1 :]
            if index > len(log):
                log.append(entry)

    def _on_append_entries_response(self, msg):
        if self.role is not Role.LEADER or msg.term != self.persistent.current_term:
            return []
        peer = msg.follower_id
        if msg.success:
            self.match_index[peer] = max(self.match_index[peer], msg.match_index)
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit_index()
            return []
        next_index = max(self.match_index[peer] + 1, self.next_index[peer] - 1)
        if next_index == self.next_index[peer]:
            return []
        self.next_index[peer] = next_index
        return [Send(peer, self._append_entries_for(peer))]

    def _can_commit(self, index):
        replicas = 1 + sum(1 for match in self.match_index.values() if match >= index)
        return (
            self.persistent.log[index - 1].term == self.persistent.current_term
            and self._has_majority(replicas)
        )

    def _advance_commit_index(self):
        for index in range(self._last_log_index(), self.commit_index, -1):
            if self._can_commit(index):
                self._set_commit_index(index)
                return

    def _set_commit_index(self, index):
        self.commit_index = max(self.commit_index, index)
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self._apply(self.last_applied, self.persistent.log[self.last_applied - 1])

    def _apply(self, index, entry):
        self.applied.append((index, entry.command))
        request_id = self.pending.pop(index, None)
        if entry.command == NOOP:
            return
        try:
            ok, result = True, self.kv.apply(entry.command)
        except ValueError as error:
            ok, result = False, str(error)
        if request_id is not None:
            self.replies.append(ClientReply(request_id, ok, result))

    def _take_replies(self):
        replies, self.replies = self.replies, []
        return replies
