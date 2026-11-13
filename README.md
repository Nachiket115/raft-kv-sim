# raftlite

A Raft consensus core with a key-value state machine, run inside a
deterministic simulator that injects network faults, partitions and crashes.
An invariant checker runs after every simulated step. Simulated clients
retry their requests, the state machine applies each request once, and a
linearizability checker judges the recorded client history at the end of
every run. The same seed always produces the same trace, byte for byte, so
any failure can be replayed.

Python 3.11+, standard library only. Tests use pytest.

## Quick start

Commands are shown for a POSIX shell (bash, zsh) and have not been tried on
Windows.

```bash
pip install -e '.[dev]'
```

```bash
python -m raftlite.cli --list
```

```bash
python -m raftlite.cli leader_partitioned --seed 3
```

```bash
python -m raftlite.cli leader_partitioned --seed 3 --timeline
```

The CLI prints the full trace (or a timeline of terms and leaders with
`--timeline`) followed by `PASS` or `FAIL`. Exit code is 0 on pass, 1 on
failure, 2 for an unknown scenario name. `--seed` defaults to 1.

```
000183  term 1  n2 starts an election
000189  term 1  n2 becomes leader
000400  partition [2] | [0, 1, 3, 4]
000571  term 2  n4 starts an election
000578  term 2  n4 becomes leader

leaders by term:
  term 1: n2
  term 2: n4
PASS leader_partitioned seed=3 ticks=800
```

Run the tests (276 tests, about 37 seconds; the fuzz tests run 100 seeds
without clients and 100 with):

```bash
pytest
```

Full 1,000-seed fuzz run:

```bash
FUZZ_SEEDS=1000 pytest tests/test_fuzz.py
```

## Layout

```
raftlite/
  node.py        RaftNode, the pure protocol state machine
  messages.py    RPCs, log entries and effects
  sim.py         virtual clock, event queue, network, faults, crash/restart
  kv.py          key-value state machine and client session table
  client.py      simulated client that retries and follows leader hints
  checker.py     invariant checker
  linearizability.py  history checker (Wing and Gong search)
  scenarios.py   named scenarios and the fuzz run, shared by CLI and tests
  cli.py         command-line entry point
  timeline.py    terms-and-leaders view derived from the trace
tests/
  test_node.py, test_sim.py, test_kv.py, test_checker.py,
  test_scenarios.py, test_fuzz.py, test_cli.py, test_timeline.py,
  test_client.py, test_linearizability.py
  test_golden.py pinned trace hashes for runs without clients
  mutants.py     deliberate bugs, each a RaftNode subclass
```

## Design

### The node is a pure state machine

`RaftNode` never imports the simulator, a clock or randomness. It has three
inputs, `handle_message`, `handle_timeout` and `handle_client_command`, and
each returns a list of effects: `Send(dst, msg)`, `SetTimer(kind)` and
`ClientReply`. The simulator carries the effects out.

- The node only asks for an election timer. The simulator picks the
  randomized duration from its seeded RNG, and owns the fixed heartbeat
  interval. A new timer of the same kind replaces the old one; a timeout
  that does not fit the node's current role is ignored.
- Persistent state (`current_term`, `voted_for`, `log`) lives in its own
  `PersistentState` object. A crash throws the `RaftNode` away; a restart
  builds a new one around the same persistent object, so volatile state
  (`commit_index`, `last_applied`, the KV store, leader bookkeeping) starts
  over and the node re-applies its log from index 1 as entries commit again.
- The log is 1-indexed as in the paper. `commit_index` and `last_applied`
  start at 0.
- Every protocol rule is its own small method: `_log_up_to_date`,
  `_can_grant_vote`, `_has_majority`, `_log_matches`, `_store_entries`,
  `_can_commit`, `_append_noop` and so on. A mutant overrides exactly one of
  them, which is how the mutation check works.

Choices worth knowing about:

- **No-op on election.** A new leader appends a `("NOOP",)` entry in its own
  term. This lets it commit entries left over from earlier terms without
  waiting for a new client command (see the bug story below). No-ops are
  recorded as applied but never touch the KV store and never get a reply.
- **`match_index` comes from the follower.** A successful
  `AppendEntriesResponse` carries the follower's own match index rather
  than the leader working out what it sent. The leader only accepts
  responses from its current term and only moves `match_index` forward, so
  stale or duplicated responses are harmless.
- **Simple back-off.** A failed consistency check moves `next_index` back by
  one, never below `match_index + 1`, and retries at once. There is no
  conflict-term hint. If `next_index` cannot move, nothing is resent.
- **No truncation on stale messages.** A follower deletes entries only on a
  real term conflict, so a late, shorter `AppendEntries` cannot cut off
  entries that a newer one already delivered.
- **Leader commit rule.** A leader counts replicas only for entries from its
  current term (Figure 8 of the Raft paper).
- **Client path.** A command sent to a non-leader is rejected with a leader
  hint. The leader appends it, and replies once the entry is applied. A
  leader that steps down before then stays silent and the client retries.
  An invalid command fails deterministically on every node and does not
  stall later entries.

### Simulator

- Integer virtual ticks and a heap of `(time, seq, event)`; `seq` breaks
  ties, so the event order is fixed.
- One `random.Random(seed)` drives everything: timer durations, message
  delays and every fault decision. Nodes are always visited in sorted id
  order, and nothing that reaches the trace depends on set or hash order.
- The trace is a list of plain text lines with a zero-padded tick prefix.
- Faults, all off by default and set through `SimConfig`: drop, duplicate
  (each copy gets its own delay) and long delay. Reordering comes from the
  delay jitter plus long delays. A probability of 0 draws nothing from the
  RNG, so turning a fault off leaves the other traces unchanged.
- `sim.partition(*groups)` cuts the network between groups, checked at
  delivery time so in-flight messages across the cut are lost. `sim.heal()`
  removes it. `sim.crash(n)` and `sim.restart(n)` take a node down and bring
  it back; messages that arrive while it is down are dropped.

### Checker

The simulator calls `checker.check(sim)` from one place, after every step:
message delivery, timer, crash, restart, client submit and every fault step.
There is no way to turn it off. It keeps its own history across the whole
run, and crashed nodes' persistent logs still count.

1. **Election safety:** at most one leader per term. After each step every
   node that is leader is recorded as `(term, node)`; a second node in the
   same term is a violation.
2. **Leader append-only:** a leader never overwrites or deletes its own
   entries. For each `(term, leader)` the checker keeps the log it last saw
   and fails if any earlier entry changed.
3. **Log matching:** if two logs have an entry with the same index and
   term, they are identical up to that index.
4. **Leader completeness:** every committed entry is present in the log of
   every leader of a later term. The checker keeps a ledger of committed
   entries and the term in which each was first seen committed.
5. **State machine safety:** no two nodes apply different commands at the
   same index. This compares every application of an index across all nodes
   and across a node's own restarts, and checks each node applies a gapless
   sequence from 1.

A violation raises `InvariantViolation` with the invariant name, the seed,
the tick and the last 30 trace lines.

Logs and applied lists only grow in a healthy run, so the checker remembers
what it has already checked and looks at the new tail. If a list no longer
starts with what the checker saw before, it rechecks that list from the
start.

### Client sessions

A client command is the tuple `("SESSION", client_id, seq, op)`, where `op`
is a plain `PUT`, `GET` or `DELETE`. A client sends one operation at a time
and gives each one the next `seq`, starting at 1.

`KVStore` keeps a session table: for each client, the last `seq` it applied
and the reply it gave. The table is part of the state machine. It is changed
only by applying log entries, so every node that has applied the same prefix
has the same table, and a restarted node rebuilds it by re-applying its log.
Nothing about sessions is stored anywhere else.

When a session command is applied:

- `seq` higher than the last one, or a new client: run the operation, store
  `(seq, reply)`, reply. A failed operation is stored too, so its retry gets
  the same failure.
- `seq` equal to the last one: return the stored reply. The operation is not
  run again.
- `seq` lower than the last one: do nothing and reply `ok=False` with
  `"stale seq"`. The client has already moved on, so it has seen that reply
  and the stored one is gone. This cannot be told apart from a late copy of
  an old request, and refusing is the safe answer to both.

Choices worth knowing about:

- **Dedup happens at apply time, not at the leader.** A leader appends a
  retry like any other command, so the same `(client_id, seq)` can sit in
  the log twice. The second copy does nothing when applied except return the
  stored reply. This keeps the rule in one place and makes it hold on every
  node, including a leader that was elected after the first copy was logged.
- **No new message fields.** The session data is wrapped inside the command
  tuple. `LogEntry` and `ClientCommand` are unchanged, so traces of runs
  without clients did not change by a byte. `tests/test_golden.py` pins
  SHA-256 hashes of 24 such traces, taken before M7 started.
- **Commands without a session still work.** A bare `("PUT", ...)` is applied
  as before, with no dedup. The scripted scenarios use these.

### Client driver

`sim.add_clients(n)` adds clients that run until the simulation ends. Each
one picks a random operation on key `x` or `y`, sends it, and waits.

- On `ok=True` it stays with that node and sends its next operation after a
  random pause of 10 to 100 ticks.
- On a rejection it retries after 10 ticks at the hinted leader, or at a
  random node if there was no hint.
- After 300 ticks with no answer it retries at a random node.
- Every retry carries the same `(client_id, seq)`.

All choices come from the simulator's seeded RNG. A client is an event on the
same heap as messages and timers. With no clients there are no client events,
no extra RNG draws and no extra trace lines.

### Linearizability checker

The simulator records one entry per client operation:

- **Invoke tick:** the first time the operation is submitted to any node.
  Retries do not start a new operation.
- **Return tick and result:** the first reply produced by applying the
  command. That includes a stored reply returned to a retry, and results
  like `False` for a `DELETE` of a missing key. Redirects, rejections from
  non-leaders and stale-seq replies are not returns.
- An operation with no such reply is recorded as never returned.

At the end of every scenario and fuzz run, `check_linearizable` looks for an
order of the operations that a single correct key-value store could have
produced, where an operation that returned before another was invoked comes
first. The search follows Wing and Gong:

- Keys are checked one at a time, since operations on different keys do not
  affect each other.
- At each point the candidates for "next" are the operations invoked before
  the earliest return among those not yet placed.
- A state is the set of placed operations (a bitmask) plus the key's value.
  States already shown to fail are remembered and not tried again.
- An operation that never returned may be placed anywhere after its invoke,
  or left out. Its result is not checked.
- Two operations that touch at the same tick count as concurrent.

The check runs once per run, not per step, and takes about 0.3 ms for a
fuzz run of roughly 200 operations.

The sequential model inside the checker is separate from `KVStore` on
purpose, so a bug in one does not hide in the other. A test replays 200
random operation lists through both and requires the same results, and
another compares the search against a brute-force check on 1,500 small
random histories.

A failure names the key and prints every operation on it:

```
raftlite.checker.InvariantViolation: linearizability: no valid order for the operations on key 'x':
  000000 PUT x c1s1  000201 -> None  (request (1, 1))
  000258 GET x  000258 -> 'c1s1'  (request (0, 2))
  000260 PUT x c2s2  000272 -> None  (request (2, 2))
  ...
  006341 DELETE x  006349 -> True  (request (1, 75)) (seed=1, t=6390)
  ... last 30 trace lines ...
repro: python -c "from raftlite.scenarios import *; from tests.mutants import StaleRead; fuzz(1, StaleRead, clients=3)"
```

Each line is the invoke tick, the operation, the return tick and result (or
`never returned`), and the request id. The report does not point at the
operation that broke the order; for a busy key it can be over a hundred
lines.

## Testing

- **Unit tests** call `RaftNode` directly with messages, no simulator.
- **Named scenarios** (`raftlite/scenarios.py`, run over 50 seeds each in
  the tests, and runnable from the CLI):
  - `leader_partitioned`: the leader is cut off from the majority; the
    majority elects a new leader while the old one keeps leading alone and
    cannot commit.
  - `minority_partition`: a minority side cannot commit anything.
  - `follower_catches_up`: a crashed follower restarts and catches up.
  - `split_vote`: the vote splits and then resolves to one leader.
  - `stale_leader_steps_down`: an old leader steps down after healing.
  - `liveness`: with faults, a partition and a crash, then a heal: once a
    stable majority exists and no new faults happen, a leader is elected and
    a write is applied on every node within a bounded number of ticks
    (10 x the maximum election timeout).
  - `figure8`: a scripted run of the paper's Figure 8 situation.
- **Fuzz.** `fuzz(seed)` runs 6,000 ticks with 5% drop, duplicate and long
  delay. Every 20 to 300 ticks it picks a random partition, heal, crash (at
  most two nodes down), restart or client write. It ends by healing,
  restarting every node and checking liveness. `fuzz(seed, clients=3)` is
  the same run with three retrying clients on top. 1,000 seeds pass without
  clients and 1,000 with. A run with clients takes about 0.19 s against
  0.02 s without, because it handles about three times as many events. A longer
  hunt of 22,500 runs found nothing: seeds 1,000 to 10,999 plus heavier
  fault mixes (20% faults, up to four nodes down, very long stale messages,
  near-identical election timeouts, 40,000-tick runs). That hunt was run
  before clients existed and has not been repeated with them.
- **Mutation check.** Each mutant in `tests/mutants.py` breaks one rule, and
  the test fails unless the checker catches it on some seed within 300. First
  catching seed:

  | Mutant | Broken rule | Caught by | Seed |
  |---|---|---|---|
  | `DoubleVoter` | votes twice in a term | election safety | 2 |
  | `PhantomVote` | counts one vote too many | election safety | 7 |
  | `NoStepDown` | leader ignores a higher term | election safety | 0 |
  | `StaleLogVoter` | no election restriction | leader completeness | 0 |
  | `TruncateOnStale` | truncates on any AppendEntries | leader completeness | 7 |
  | `NoConsistencyCheck` | skips the prev-log check | log matching | 15 |
  | `NoNoop` | no no-op on election | liveness | 18 |
  | `StaleRead` | leader answers a GET from its own state, not the log | linearizability | 1 |
  | `DedupDisabled` | a retried `(client_id, seq)` is applied again | linearizability | 1 |
  | `ReplyBeforeCommit` | leader replies on append, not on commit | linearizability | 0 |

  The last three run with `clients=3`. All three were caught by the fuzz, so
  none needed a scripted scenario. On the 5 seeds the tests try, they break
  no Raft invariant and pass the fuzz without clients: only the
  linearizability check sees them. Of 300
  seeds, `StaleRead` is caught on 37, `DedupDisabled` on 113 and
  `ReplyBeforeCommit` on 285. `ReplyBeforeCommit` also fails the liveness
  check on 15 seeds, which was not looked into.

### What Figure 8 showed

Dropping the current-term condition from the commit rule (`CommitOldTerm`)
was not caught by any fuzz seed, and the scripted Figure 8 run passes with
it too. The reason is the no-op: a new leader's `match_index` starts at 0,
and every `AppendEntries` carries everything from `next_index` to the end
of the log, so any acknowledgement that covers an old entry also covers the
leader's no-op. The real rule then commits the no-op, and the old entry
with it, anyway. With the no-op, the Figure 8 rule is redundant in this
implementation.

To show the rule still matters, `CommitOldTermNoNoop` removes both. The
scripted `figure8` run breaks leader completeness with it on every seed: an
entry committed at index 1 in term 4 is missing from the term-7 leader's
log.

## Bugs found and fixed

**A client got `ok` for a write four nodes never applied.** The `liveness`
scenario failed on seeds 10, 16, 43 and 49. A leader committed an entry and
replied to the client, then stepped down before its next heartbeat could
spread the new commit index. Every other node held the entry with
`commit_index` 0. The new leader is not allowed to count replicas for an
entry from an older term, and with no new client command it had nothing of
its own term to commit, so the entry stayed uncommitted forever. Every
invariant still held; only the liveness check saw it. The fix, as in the
Raft paper, is for a new leader to append a no-op in its own term
(`fix(node): append a no-op entry when elected`). Committing the no-op
commits everything before it. The `NoNoop` mutant keeps this failure in the
test suite.

Found while reviewing the node against Figure 2 of the paper:

- **Leader demoting itself.** The `AppendEntries` handler set the role to
  follower for any message of the current term, including on a leader. A
  leader now ignores same-term `AppendEntries`.
- **Pointless resend.** A failure response that could not move `next_index`
  (because it was already at `match_index + 1`) still triggered a resend.
  Duplicated or stale failure responses could then cause extra traffic. Now
  nothing is sent unless `next_index` actually moved.

**Sessions and the linearizability checker found no protocol bug.** 1,000
client fuzz seeds passed the first time they were run. The one mistake they
were involved in was in the checker's own speed-up: a first version
remembered only how many applied records it had checked per node, and two
existing tests that edit an already-checked record failed. It now keeps a
copy of what it checked and compares.

The scripted Figure 8 scenario had two bugs of its own before it worked:
it polled for a leader every 10 ticks, so the new leader's first broadcast
leaked out before the script could isolate it (it now steps one event at a
time); and a crashed node's in-flight messages were delivered into whatever
partition group it landed in (each repartition now puts the crashed node in
a group by itself).

## Reproducing a run

Every run is a pure function of its seed. A failing scenario or fuzz run
prints its seed and a command that replays it exactly:

```
raftlite.checker.InvariantViolation: election safety: term 6 has leaders n3 and n4 (seed=0, t=3853)
repro: python -c "from raftlite.scenarios import *; from tests.mutants import NoStepDown; fuzz(0, NoStepDown)"
```

Paste the `repro:` line into a shell from the repository root. For named
scenarios, `python -m raftlite.cli <scenario> --seed N` gives the same run
with the full trace. The repro line has only been tried in POSIX shells;
the double-quoted `python -c` may need changes in Windows `cmd.exe`.

## Known simplifications

- **Sessions never expire.** The session table keeps one row per client
  forever. There is no expiry, no garbage collection and no way to close a
  session.
- **One operation per client at a time.** Dedup compares against the last
  `seq` only. A client that sent two operations at once could have the
  older one refused as stale.
- **Retries still take log space.** A retry is deduplicated when applied,
  not when the leader receives it, so it costs a log entry.
- **Clients have no network.** A client's request reaches the node in the
  same tick and the reply comes back in the same tick it is produced.
  Client traffic is never dropped, delayed or cut by a partition; a client
  only fails to reach a node that is crashed. Faults apply between nodes.
- **The stale-seq reply is not reached in the simulator.** It is unit
  tested in `KVStore` and `RaftNode`, but with one operation per client and
  no client network, no late copy of an old request ever arrives.
- **The linearizability check is weak on the scripted scenarios.** They
  send bare `PUT`s and no reads, so almost any history passes. The check
  earns its keep in the client fuzz.
- **Clients cannot be stopped** once added; they run to the end of the run.
- **The split-vote scenario is forced.** It splits the vote with a
  partition. Splits from colliding election timers are covered by the
  election tests over many seeds and by the fuzz runs.
- Reads go through the log like writes; there is no read-index or lease
  optimisation.
- No membership changes, log compaction or snapshots, real disk persistence
  or real sockets. Persistence is an in-memory object that survives a
  simulated crash.
- No conflict-term hint when backing up `next_index`, so a far-behind
  follower costs one round trip per missing entry before it matches.
