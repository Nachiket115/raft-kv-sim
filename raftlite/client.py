from raftlite.messages import ClientCommand

RETRY_TIMEOUT = 300
REDIRECT_DELAY = 10
THINK_MIN = 10
THINK_MAX = 100
KINDS = ("PUT", "PUT", "GET", "GET", "DELETE")


class Client:
    def __init__(self, client_id, keys):
        self.client_id = client_id
        self.keys = tuple(keys)
        self.seq = 0
        self.op = None
        self.target = None
        self.generation = 0

    def wake(self, sim):
        if self.op is None:
            self.seq += 1
            self.op = self._next_op(sim.rng)
        target = self.target
        if target is None:
            target = sim.rng.choice(sorted(sim.persistent))
        self.target = None
        sim.wake_client(self, RETRY_TIMEOUT)
        if not sim.client_send(target, self.command()):
            sim.wake_client(self, REDIRECT_DELAY)

    def on_reply(self, sim, node_id, reply):
        if self.op is None or reply.request_id != (self.client_id, self.seq):
            return
        if reply.ok:
            self.op = None
            self.target = node_id
            sim.wake_client(self, sim.rng.randint(THINK_MIN, THINK_MAX))
        else:
            self.target = reply.leader_hint
            sim.wake_client(self, REDIRECT_DELAY)

    def command(self):
        request_id = (self.client_id, self.seq)
        return ClientCommand(request_id, ("SESSION", self.client_id, self.seq, self.op))

    def _next_op(self, rng):
        kind = rng.choice(KINDS)
        key = rng.choice(self.keys)
        if kind == "PUT":
            return ("PUT", key, f"c{self.client_id}s{self.seq}")
        return (kind, key)
