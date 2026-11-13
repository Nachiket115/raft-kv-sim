STALE_SEQ = "stale seq"


class KVStore:
    def __init__(self):
        self.data = {}
        self.sessions = {}

    def execute(self, command):
        match command:
            case ("SESSION", client_id, seq, op):
                session = self.sessions.get(client_id)
                if session and seq < session[0]:
                    return False, STALE_SEQ
                if session and self._is_duplicate(seq, session[0]):
                    return session[1]
                reply = self._outcome(op)
                self.sessions[client_id] = (seq, reply)
                return reply
            case _:
                return self._outcome(command)

    def _is_duplicate(self, seq, last_seq):
        return seq == last_seq

    def _outcome(self, command):
        try:
            return True, self.apply(command)
        except ValueError as error:
            return False, str(error)

    def apply(self, command):
        match command:
            case ("PUT", key, value):
                self.data[key] = value
                return None
            case ("GET", key):
                return self.data.get(key)
            case ("DELETE", key):
                existed = key in self.data
                self.data.pop(key, None)
                return existed
            case _:
                raise ValueError(f"unknown command: {command!r}")
