import heapq
import random
from dataclasses import dataclass

from raftlite.checker import Checker
from raftlite.client import Client
from raftlite.linearizability import ABSENT, Op, step
from raftlite.messages import ClientReply, Send, SetTimer, TimerKind
from raftlite.node import PersistentState, RaftNode


@dataclass(frozen=True)
class SimConfig:
    election_timeout_min: int = 150
    election_timeout_max: int = 300
    heartbeat_interval: int = 50
    message_delay_min: int = 1
    message_delay_max: int = 10
    drop_prob: float = 0.0
    duplicate_prob: float = 0.0
    delay_prob: float = 0.0
    long_delay_max: int = 200


class Simulator:
    def __init__(
        self, node_count=5, seed=0, config=None, node_cls=RaftNode, checker=None, observer=None
    ):
        self.seed = seed
        self.config = config or SimConfig()
        self.rng = random.Random(seed)
        self.checker = checker or Checker()
        self._observer = observer
        self.now = 0
        self.trace = []
        self.client_replies = []
        self.history = {}
        self.clients = {}
        self._queue = []
        self._seq = 0
        self._timer_generation = {}
        self._group = {}
        self._node_cls = node_cls
        self._node_ids = node_ids = range(node_count)
        self.persistent = {i: PersistentState() for i in node_ids}
        self.nodes = {i: node_cls(i, node_ids, self.persistent[i]) for i in node_ids}
        for node_id in node_ids:
            self._apply(node_id, self.nodes[node_id].start())
        self._after_step()

    def step(self):
        if not self._queue:
            return False
        self.now, _, event = heapq.heappop(self._queue)
        match event:
            case ("timer", node_id, kind, generation):
                if self._timer_generation[node_id, kind] == generation:
                    self._log(f"n{node_id} {kind.value} timeout")
                    self._apply(node_id, self.nodes[node_id].handle_timeout(kind))
            case ("client", client_id, generation):
                client = self.clients[client_id]
                if client.generation == generation:
                    client.wake(self)
            case ("deliver", src, dst, msg) if dst not in self.nodes:
                self._log(f"n{src} -> n{dst} dropped {msg}")
                if self._observer is not None:
                    self._observer.event("drop", src=src, dst=dst, msg=msg, reason="down")
            case ("deliver", src, dst, msg) if not self._connected(src, dst):
                self._log(f"n{src} -> n{dst} dropped (partition) {msg}")
                if self._observer is not None:
                    self._observer.event("drop", src=src, dst=dst, msg=msg, reason="partition")
            case ("deliver", src, dst, msg):
                self._log(f"n{src} -> n{dst} deliver {msg}")
                if self._observer is not None:
                    self._observer.event("deliver", src=src, dst=dst, msg=msg)
                self._apply(dst, self.nodes[dst].handle_message(msg))
        self._after_step()
        return True

    def run_until(self, time):
        while self._queue and self._queue[0][0] <= time:
            self.step()
        self.now = max(self.now, time)

    def crash(self, node_id):
        del self.nodes[node_id]
        for key in self._timer_generation:
            if key[0] == node_id:
                self._timer_generation[key] += 1
        self._log(f"n{node_id} crash")
        if self._observer is not None:
            self._observer.event("crash", node=node_id)
        self._after_step()

    def restart(self, node_id):
        if node_id in self.nodes:
            raise ValueError(f"n{node_id} is already running")
        node = self._node_cls(node_id, self._node_ids, self.persistent[node_id])
        self.nodes = dict(sorted({**self.nodes, node_id: node}.items()))
        self._log(f"n{node_id} restart")
        if self._observer is not None:
            self._observer.event("restart", node=node_id)
        self._apply(node_id, node.start())
        self._after_step()

    def partition(self, *groups):
        listed = [node_id for group in groups for node_id in group]
        if len(set(listed)) != len(listed) or not set(listed) <= set(self._node_ids):
            raise ValueError(f"bad partition groups: {groups}")
        rest = [node_id for node_id in self._node_ids if node_id not in listed]
        groups = [sorted(group) for group in groups if group] + ([rest] if rest else [])
        self._group = {node_id: i for i, group in enumerate(groups) for node_id in group}
        self._log("partition " + " | ".join(str(group) for group in groups))
        if self._observer is not None:
            self._observer.event("partition", groups=groups)
        self._after_step()

    def heal(self):
        self._group = {}
        self._log("heal")
        if self._observer is not None:
            self._observer.event("heal")
        self._after_step()

    def _connected(self, src, dst):
        return self._group.get(src) == self._group.get(dst)

    def submit(self, node_id, cmd):
        self._deliver_command(node_id, cmd)
        self._after_step()

    def add_clients(self, count, keys=("x", "y")):
        for client_id in range(len(self.clients), len(self.clients) + count):
            self.clients[client_id] = Client(client_id, keys)
            self.wake_client(self.clients[client_id], 0)

    def wake_client(self, client, delay):
        client.generation += 1
        self._push(self.now + delay, ("client", client.client_id, client.generation))

    def client_send(self, node_id, cmd):
        if node_id not in self.nodes:
            self._log(f"client -> n{node_id} unreachable {cmd}")
            return False
        self._deliver_command(node_id, cmd)
        return True

    def _deliver_command(self, node_id, cmd):
        self._log(f"client -> n{node_id} {cmd}")
        self._record_invoke(cmd)
        if self._observer is not None:
            self._observer.event("invoke", node=node_id, cmd=cmd)
        self._apply(node_id, self.nodes[node_id].handle_client_command(cmd))

    def _record_invoke(self, cmd):
        match cmd.command:
            case ("SESSION", _, _, op):
                pass
            case op:
                pass
        try:
            step(ABSENT, op)
        except ValueError:
            return
        self.history.setdefault(cmd.request_id, Op(op, self.now, request_id=cmd.request_id))

    def _record_return(self, reply):
        op = self.history.get(reply.request_id)
        if reply.ok and op is not None and op.ret is None:
            op.ret, op.result = self.now, reply.result

    def _after_step(self):
        if self._observer is not None:
            self._observer.snapshot(self)
        self.checker.check(self)

    def trace_text(self):
        return "".join(line + "\n" for line in self.trace)

    def _apply(self, node_id, effects):
        for effect in effects:
            match effect:
                case SetTimer(kind):
                    generation = self._timer_generation.get((node_id, kind), 0) + 1
                    self._timer_generation[node_id, kind] = generation
                    fire_at = self.now + self._timer_delay(kind)
                    self._push(fire_at, ("timer", node_id, kind, generation))
                    self._log(f"n{node_id} set {kind.value} timer for t={fire_at}")
                    if self._observer is not None:
                        self._observer.event("timer", node=node_id, timer=kind, fires=fire_at)
                case Send(dst, msg):
                    self._log(f"n{node_id} -> n{dst} send {msg}")
                    self._send(node_id, dst, msg)
                case ClientReply():
                    self.client_replies.append((node_id, effect))
                    self._record_return(effect)
                    for client in self.clients.values():
                        client.on_reply(self, node_id, effect)
                    self._log(f"n{node_id} -> client {effect}")
                    if self._observer is not None:
                        self._observer.event("return", node=node_id, reply=effect)
                case _:
                    raise TypeError(f"unknown effect from n{node_id}: {effect!r}")

    def _send(self, src, dst, msg):
        config = self.config
        if self._chance(config.drop_prob):
            self._log(f"n{src} -> n{dst} dropped (fault) {msg}")
            if self._observer is not None:
                self._observer.event("drop", src=src, dst=dst, msg=msg, reason="fault")
            return
        copies = 1
        if self._chance(config.duplicate_prob):
            copies = 2
            self._log(f"n{src} -> n{dst} duplicate {msg}")
            if self._observer is not None:
                self._observer.event("duplicate", src=src, dst=dst, msg=msg)
        for _ in range(copies):
            delay = self.rng.randint(config.message_delay_min, config.message_delay_max)
            if self._chance(config.delay_prob):
                extra = self.rng.randint(1, config.long_delay_max)
                delay += extra
                self._log(f"n{src} -> n{dst} delay +{extra} {msg}")
                if self._observer is not None:
                    self._observer.event("delay", src=src, dst=dst, msg=msg, extra=extra)
            self._push(self.now + delay, ("deliver", src, dst, msg))
            if self._observer is not None:
                self._observer.event("send", src=src, dst=dst, msg=msg, arrive=self.now + delay)

    def _chance(self, prob):
        return prob > 0 and self.rng.random() < prob

    def _timer_delay(self, kind):
        if kind is TimerKind.ELECTION:
            return self.rng.randint(
                self.config.election_timeout_min, self.config.election_timeout_max
            )
        return self.config.heartbeat_interval

    def _push(self, time, event):
        heapq.heappush(self._queue, (time, self._seq, event))
        self._seq += 1

    def _log(self, text):
        self.trace.append(f"{self.now:06d} {text}")
