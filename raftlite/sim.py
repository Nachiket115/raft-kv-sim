import heapq
import random
from dataclasses import dataclass

from raftlite.messages import ClientReply, Send, SetTimer, TimerKind
from raftlite.node import RaftNode


@dataclass(frozen=True)
class SimConfig:
    election_timeout_min: int = 150
    election_timeout_max: int = 300
    heartbeat_interval: int = 50
    message_delay_min: int = 1
    message_delay_max: int = 10


class Simulator:
    def __init__(self, node_count=5, seed=0, config=None, node_cls=RaftNode):
        self.seed = seed
        self.config = config or SimConfig()
        self.rng = random.Random(seed)
        self.now = 0
        self.trace = []
        self.client_replies = []
        self._queue = []
        self._seq = 0
        self._timer_generation = {}
        node_ids = range(node_count)
        self.nodes = {i: node_cls(i, node_ids) for i in node_ids}
        for node_id in node_ids:
            self._apply(node_id, self.nodes[node_id].start())

    def step(self):
        if not self._queue:
            return False
        self.now, _, event = heapq.heappop(self._queue)
        match event:
            case ("timer", node_id, kind, generation):
                if self._timer_generation[node_id, kind] == generation:
                    self._log(f"n{node_id} {kind.value} timeout")
                    self._apply(node_id, self.nodes[node_id].handle_timeout(kind))
            case ("deliver", src, dst, msg):
                self._log(f"n{src} -> n{dst} deliver {msg}")
                self._apply(dst, self.nodes[dst].handle_message(msg))
        return True

    def run_until(self, time):
        while self._queue and self._queue[0][0] <= time:
            self.step()
        self.now = max(self.now, time)

    def submit(self, node_id, cmd):
        self._log(f"client -> n{node_id} {cmd}")
        self._apply(node_id, self.nodes[node_id].handle_client_command(cmd))

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
