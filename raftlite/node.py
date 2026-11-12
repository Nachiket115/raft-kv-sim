from dataclasses import dataclass, field
from enum import Enum

from raftlite.messages import (
    ClientCommand,
    ClientReply,
    Effect,
    LogEntry,
    Message,
    SetTimer,
    TimerKind,
)


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
        self.next_index = {}
        self.match_index = {}

    def start(self) -> list[Effect]:
        return [SetTimer(TimerKind.ELECTION)]

    def handle_timeout(self, kind: TimerKind) -> list[Effect]:
        if kind is TimerKind.ELECTION and self.role is not Role.LEADER:
            return [SetTimer(TimerKind.ELECTION)]
        return []

    def handle_message(self, msg: Message) -> list[Effect]:
        raise NotImplementedError("RPC handling is not implemented yet")

    def handle_client_command(self, cmd: ClientCommand) -> list[Effect]:
        if self.role is not Role.LEADER:
            return [ClientReply(cmd.request_id, ok=False, leader_hint=self.leader_id)]
        raise NotImplementedError("leader command handling is not implemented yet")
