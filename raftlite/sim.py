import heapq
import random
from dataclasses import dataclass

from raftlite.checker import Checker
from raftlite.messages import ClientReply, Send, SetTimer, TimerKind
from raftlite.node import PersistentState, RaftNode


@dataclass(frozen=True)
class SimConfig:
    election_timeout_min: int = 150
    election_timeout_max: int = 300
    heartbeat_interval: int = 50
    message_delay_min: int = 1
    message_delay_max: int = 10


class Simulator:
    def __init__(self, node_count=5, seed=0, config=None, node_cls=RaftNode, checker=None):
        self.seed = seed
        self.config = config or SimConfig()
        self.rng = random.Random(seed)
        self.checker = checker or Checker()
        self.now = 0
        self.trace = []
        self.client_replies = []
        self._queue = []
        self._seq = 0
        self._timer_generation = {}
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
            case ("deliver", src, dst, msg) if dst not in self.nodes:
                self._log(f"n{src} -> n{dst} dropped {msg}")
            case ("deliver", src, dst, msg):
                self._log(f"n{src} -> n{dst} deliver {msg}")
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
        self._after_step()

    def restart(self, node_id):
        if node_id in self.nodes:
            raise ValueError(f"n{node_id} is already running")
        node = self._node_cls(node_id, self._node_ids, self.persistent[node_id])
        self.nodes = dict(sorted({**self.nodes, node_id: node}.items()))
        self._log(f"n{node_id} restart")
        self._apply(node_id, node.start())
        self._after_step()

    def submit(self, node_id, cmd):
        self._log(f"client -> n{node_id} {cmd}")
        self._apply(node_id, self.nodes[node_id].handle_client_command(cmd))
        self._after_step()

    def _after_step(self):
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
                case Send(dst, msg):
                    delay = self.rng.randint(
                        self.config.message_delay_min, self.config.message_delay_max
                    )
                    self._push(self.now + delay, ("deliver", node_id, dst, msg))
                    self._log(f"n{node_id} -> n{dst} send {msg}")
                case ClientReply():
                    self.client_replies.append((node_id, effect))
                    self._log(f"n{node_id} -> client {effect}")
                case _:
                    raise TypeError(f"unknown effect from n{node_id}: {effect!r}")

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
