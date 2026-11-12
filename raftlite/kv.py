class KVStore:
    def __init__(self):
        self.data = {}

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
