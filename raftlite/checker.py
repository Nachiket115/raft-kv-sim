from itertools import combinations

from raftlite.node import Role

TRACE_TAIL = 30


class InvariantViolation(AssertionError):
    def __init__(self, invariant, detail, sim):
        self.invariant = invariant
        self.seed = sim.seed
        self.trace = list(sim.trace)
        tail = "\n".join(sim.trace[-TRACE_TAIL:])
        super().__init__(f"{invariant}: {detail} (seed={sim.seed}, t={sim.now})\n{tail}")


class Checker:
    def __init__(self):
        self.leaders_by_term = {}
        self.leader_logs = {}
        self.committed = {}
        self.ledger = []
        self.applied = {}
        self.applied_seen = {}

    def check(self, sim):
        leaders = [node for node in sim.nodes.values() if node.role is Role.LEADER]
        self._check_election_safety(sim, leaders)
        self._check_leader_append_only(sim, leaders)
        self._check_log_matching(sim)
        self._check_leader_completeness(sim, leaders)
        self._check_state_machine_safety(sim)

    def _check_election_safety(self, sim, leaders):
        for node in leaders:
            term = node.persistent.current_term
            first = self.leaders_by_term.setdefault(term, node.node_id)
            if first != node.node_id:
                raise InvariantViolation(
                    "election safety",
                    f"term {term} has leaders n{first} and n{node.node_id}",
                    sim,
                )

    def _check_leader_append_only(self, sim, leaders):
        for node in leaders:
            key = (node.persistent.current_term, node.node_id)
            log = node.persistent.log
            seen = self.leader_logs.get(key, [])
            if log[: len(seen)] != seen:
                raise InvariantViolation(
                    "leader append-only",
                    f"n{node.node_id} changed its own log in term {key[0]}",
                    sim,
                )
            self.leader_logs[key] = list(log)

    def _check_log_matching(self, sim):
        for (a_id, a), (b_id, b) in combinations(sim.persistent.items(), 2):
            for index in range(min(len(a.log), len(b.log)), 0, -1):
                if a.log[index - 1].term == b.log[index - 1].term:
                    if a.log[:index] != b.log[:index]:
                        raise InvariantViolation(
                            "log matching",
                            f"n{a_id} and n{b_id} agree on the term at index {index}"
                            " but their logs differ up to there",
                            sim,
                        )
                    break

    def _check_leader_completeness(self, sim, leaders):
        ledger = self.ledger
        for node in sim.nodes.values():
            term = node.persistent.current_term
            prefix = node.persistent.log[: node.commit_index]
            if prefix[: len(ledger)] != ledger[: len(prefix)]:
                index, entry, committed = next(
                    (index, entry, committed)
                    for index, (entry, committed) in enumerate(zip(prefix, ledger), 1)
                    if entry != committed
                )
                raise InvariantViolation(
                    "state machine safety",
                    f"n{node.node_id} committed {entry} at index {index},"
                    f" where {committed} was committed before",
                    sim,
                )
            for index in range(len(ledger) + 1, len(prefix) + 1):
                self.committed[index] = (prefix[index - 1], term)
                ledger.append(prefix[index - 1])
        for node in leaders:
            log = node.persistent.log
            if log[: len(ledger)] == ledger:
                continue
            for index, (entry, term) in self.committed.items():
                if node.persistent.current_term > term and log[index - 1 : index] != [entry]:
                    raise InvariantViolation(
                        "leader completeness",
                        f"leader n{node.node_id} of term {node.persistent.current_term}"
                        f" lacks {entry} committed at index {index} in term {term}",
                        sim,
                    )

    def _check_state_machine_safety(self, sim):
        for node in sim.nodes.values():
            seen = self.applied_seen.get(node.node_id, [])
            checked = len(seen) if node.applied[: len(seen)] == seen else 0
            self.applied_seen[node.node_id] = list(node.applied)
            for position, (index, command) in enumerate(node.applied[checked:], checked + 1):
                if index != position:
                    raise InvariantViolation(
                        "state machine safety",
                        f"n{node.node_id} applied index {index} at position {position}",
                        sim,
                    )
                first = self.applied.setdefault(index, command)
                if first != command:
                    raise InvariantViolation(
                        "state machine safety",
                        f"n{node.node_id} applied {command} at index {index},"
                        f" where {first} was applied before",
                        sim,
                    )
