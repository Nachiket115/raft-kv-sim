from dataclasses import dataclass


class Absent:
    def __repr__(self):
        return "ABSENT"


ABSENT = Absent()


@dataclass
class Op:
    command: tuple
    invoke: int
    ret: int | None = None
    result: object = None
    request_id: object = None


def step(state, command):
    match command:
        case ("PUT", _, value):
            return None, value
        case ("GET", _):
            return (None if state is ABSENT else state), state
        case ("DELETE", _):
            return state is not ABSENT, ABSENT
        case _:
            raise ValueError(f"unknown command: {command!r}")


def linearizable(ops):
    ops = sorted(ops, key=lambda op: op.invoke)
    returned = sum(1 << i for i, op in enumerate(ops) if op.ret is not None)
    failed = set()
    stack = [(0, ABSENT)]
    while stack:
        done, state = stack.pop()
        if not returned & ~done:
            return True
        if (done, state) in failed:
            continue
        failed.add((done, state))
        horizon = None
        first_open = (~done & (done + 1)).bit_length() - 1
        for i in range(first_open, len(ops)):
            op = ops[i]
            if horizon is not None and op.invoke > horizon:
                break
            if done >> i & 1:
                continue
            if op.ret is not None and (horizon is None or op.ret < horizon):
                horizon = op.ret
            result, after = step(state, op.command)
            if op.ret is None or result == op.result:
                stack.append((done | 1 << i, after))
    return False


def by_key(history):
    keys = {}
    for op in history:
        keys.setdefault(op.command[1], []).append(op)
    return keys


def failing_key(history):
    for key, ops in by_key(history).items():
        if not linearizable(ops):
            return key
    return None
