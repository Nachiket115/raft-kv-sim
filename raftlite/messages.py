from dataclasses import dataclass
from enum import Enum


class TimerKind(Enum):
    ELECTION = "election"
    HEARTBEAT = "heartbeat"


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: tuple


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteResponse:
    term: int
    voter_id: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesResponse:
    term: int
    follower_id: int
    success: bool
    match_index: int


Message = RequestVote | RequestVoteResponse | AppendEntries | AppendEntriesResponse


@dataclass(frozen=True)
class ClientCommand:
    request_id: int
    command: tuple


@dataclass(frozen=True)
class Send:
    dst: int
    msg: Message


@dataclass(frozen=True)
class SetTimer:
    kind: TimerKind


@dataclass(frozen=True)
class ClientReply:
    request_id: int
    ok: bool
    result: object = None
    leader_hint: int | None = None


Effect = Send | SetTimer | ClientReply
