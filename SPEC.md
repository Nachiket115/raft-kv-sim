# Raft-lite: Project Spec

## 1. Goal

Build a small replicated key-value store in Python that implements the core of
the Raft consensus algorithm (leader election, log replication, commit),
running inside a deterministic simulator that can inject failures. An invariant
checker runs after every simulation step and fails loudly if any safety
property is violated.

The point of the project is correctness under failure, not features. A reviewer
should be able to run one command, watch nodes elect a leader through
partitions and crashes, and see the checker confirm that nothing unsafe
happened.

## 2. Constraints

- Python 3.11+, standard library only for the core. `pytest` for tests. No web
  framework.
- Single process, no threads, no real networking, no wall-clock time.
  Everything runs on a virtual clock so every run is reproducible from a seed.
- Persistence is simulated in memory: a crashed node keeps its persistent state
  and loses its volatile state.

## 3. Architecture

Three layers, kept strictly separate:

1. **Node (pure state machine).** A `RaftNode` has no knowledge of the network
   or the clock. It exposes `handle_message(msg)`, `handle_timeout(kind)`, and
   `handle_client_command(cmd)`, and each returns a list of outgoing messages
   and timer requests. This makes it trivial to test and reason about.
2. **Simulator.** Owns the virtual clock, a priority queue of events (message
   deliveries, timer firings, scheduled faults), a seeded `random.Random`, and
   a simulated network that can drop, delay, duplicate, reorder, and partition
   messages. It also crashes and restarts nodes.
3. **Checker and tooling.** The invariant checker, a trace logger, a CLI, and
   an optional text timeline view.

Layout:

```
raftlite/
  node.py        RaftNode state machine
  messages.py    dataclasses for RPCs and effects
  sim.py         clock, event queue, network, fault injection
  kv.py          key-value state machine
  checker.py     invariant checker
  scenarios.py   named scenarios
  cli.py         run scenarios, print trace
tests/
  test_node.py
  test_sim.py
  test_scenarios.py
  test_fuzz.py
  mutants.py     deliberate bugs the checker must catch
```

## 4. Protocol scope

Implemented from the Raft paper:

- Persistent state per node: `current_term`, `voted_for`, `log` (entries of
  term and command). Volatile state: `commit_index`, `last_applied`, role, and
  leader-only `next_index` and `match_index`.
- RPCs: `RequestVote` and `AppendEntries`, each with a response.
- Roles: follower, candidate, leader. Randomized election timeouts (chosen from
  the seeded RNG) and a fixed heartbeat interval.
- Rules that must be exactly right:
  - Step down to follower on seeing a higher term in any message.
  - Grant a vote only if the candidate's log is at least as up to date as the
    voter's (compare last log term, then last log index), and only once per
    term.
  - A leader only advances `commit_index` by counting replicas for entries from
    its current term.
  - `AppendEntries` consistency check on `prev_log_index` and `prev_log_term`,
    with follower log truncation on conflict.
- Client interface: a command submitted to a non-leader is rejected with a
  leader hint. A command submitted to the leader is appended, replicated,
  committed, then applied to the KV state machine. Reads also go through the
  log for simplicity.

Out of scope: membership changes, log compaction and snapshots, real disk
persistence, real sockets, linearizable read optimizations, exactly-once client
semantics.

## 5. Key-value state machine

Commands: `PUT key value`, `GET key`, `DELETE key`. The state machine is a dict
applied in log order. Every node applies committed entries in order, so all
nodes must end with identical state once they have applied the same prefix.

## 6. Fault injection

The simulator supports, both by scripted scenario and by random schedule:

- Message drop, delay, duplication, and reordering, each with a configurable
  probability.
- Network partitions between arbitrary groups of nodes, and healing them.
- Node crash and restart (persistent state kept, volatile state reset).
- Repeated leader kills.

Every run is identified by a seed. A failing run must print its seed and the
exact command to reproduce it.

## 7. Invariants the checker enforces

Run after every simulation step, raising a clear error with the trace that led
to the violation:

1. **Election safety:** at most one leader per term.
2. **Leader append-only:** a leader never overwrites or deletes entries in its
   own log.
3. **Log matching:** if two logs contain an entry with the same index and term,
   the logs are identical up to that index.
4. **Leader completeness:** every entry committed in a term is present in the
   logs of all leaders of later terms.
5. **State machine safety:** no two nodes ever apply different commands at the
   same log index.

A liveness sanity check is tracked in tests: with a stable majority and no new
faults, a leader is eventually elected and new commands commit within a bounded
number of virtual ticks.

## 8. Milestones

Each milestone ends with passing tests.

- **M1: design and skeleton.** Message types, `RaftNode` interface, simulator
  event loop, virtual clock, seeded RNG, trace logging. No protocol logic yet.
  Done when a simulation of idle nodes runs deterministically and the same seed
  gives byte-identical traces.
- **M2: leader election.** Timeouts, `RequestVote`, term handling, election
  restriction. Done when a 5-node cluster elects exactly one leader, and
  re-elects after the leader is killed.
- **M3: log replication and commit.** `AppendEntries`, consistency check,
  truncation, commit index advancement with the current-term rule. Done when
  commands submitted to the leader are replicated and applied on all nodes.
- **M4: KV state machine and client path.** `PUT`, `GET`, `DELETE`, leader
  redirect. Done when a scripted client workload produces identical KV state on
  all live nodes.
- **M5: checker and failure injection.** The invariant checker, the full fault
  set, scripted scenarios, and a fuzz test that runs many seeds. Done when
  1,000 seeds with random faults pass, and deliberately breaking a rule (for
  example, removing the election restriction) makes the checker fail on some
  seed. That proves the checker works.
- **M6: tooling and docs.** CLI, optional text timeline of terms and leaders,
  README with design notes and a failure-found-and-fixed story.

## 9. Testing

- Unit tests for `RaftNode` using direct message calls, no simulator needed.
- Scenario tests for named situations: leader partitioned from the majority,
  minority partition cannot commit, crashed follower catches up, split vote
  resolves, stale leader steps down after healing.
- Fuzz test over many seeds with random faults, checker enabled.
- A mutation check: a small list of deliberate bugs that the checker must
  catch.

## 10. Done criteria

- All five invariants are checked on every step.
- 1,000 random-fault seeds pass, and the mutation check proves the checker
  catches deliberate bugs.
- One command runs a named scenario and prints a readable trace.
- The README explains the design, the bugs the checker found along the way, and
  how to reproduce any run from its seed.
