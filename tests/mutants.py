from copy import deepcopy

from raftlite.kv import KVStore
from raftlite.messages import ClientReply
from raftlite.node import RaftNode, Role


class DoubleVoter(RaftNode):
    def _can_grant_vote(self, msg):
        return msg.term == self.persistent.current_term


class PhantomVote(RaftNode):
    def _has_majority(self, count):
        return super()._has_majority(count + 1)


class NoStepDown(RaftNode):
    def _become_follower(self, term):
        was_leader = self.role is Role.LEADER
        super()._become_follower(term)
        if was_leader:
            self.role = Role.LEADER
            self.leader_id = self.node_id


class StaleLogVoter(RaftNode):
    def _log_up_to_date(self, last_log_index, last_log_term):
        return True


class TruncateOnStale(RaftNode):
    def _store_entries(self, prev_log_index, entries):
        del self.persistent.log[prev_log_index:]
        self.persistent.log.extend(entries)


class NoConsistencyCheck(RaftNode):
    def _log_matches(self, prev_log_index, prev_log_term):
        return True


class NoNoop(RaftNode):
    def _append_noop(self):
        pass


class CommitOldTerm(RaftNode):
    def _can_commit(self, index):
        replicas = 1 + sum(1 for match in self.match_index.values() if match >= index)
        return self._has_majority(replicas)


class CommitOldTermNoNoop(CommitOldTerm, NoNoop):
    pass


class StaleRead(RaftNode):
    def handle_client_command(self, cmd):
        match cmd.command:
            case ("SESSION", _, _, ("GET", key)) if self.role is Role.LEADER:
                return [ClientReply(cmd.request_id, True, self.kv.data.get(key))]
        return super().handle_client_command(cmd)


class ForgetfulKVStore(KVStore):
    def _is_duplicate(self, seq, last_seq):
        return False


class DedupDisabled(RaftNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kv = ForgetfulKVStore()


class ReplyBeforeCommit(RaftNode):
    def handle_client_command(self, cmd):
        effects = super().handle_client_command(cmd)
        if self._last_log_index() in self.pending:
            del self.pending[self._last_log_index()]
            effects.append(ClientReply(cmd.request_id, *deepcopy(self.kv).execute(cmd.command)))
        return effects
