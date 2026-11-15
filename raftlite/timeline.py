import re

SEND_AE = re.compile(r"^(\d+) n(\d+) -> n\d+ send AppendEntries\(term=(\d+), leader_id=(\d+)")
SEND_RV = re.compile(r"^(\d+) n(\d+) -> n\d+ send RequestVote\(term=(\d+), candidate_id=(\d+)")
EVENT = re.compile(r"^(\d+) (n\d+ (?:crash|restart)|partition .*|heal|(?:cut|restore) n\d+-n\d+)$")


def timeline(trace):
    rows = []
    candidates = set()
    leaders = {}
    for line in trace:
        if m := SEND_RV.match(line):
            key = (m[3], m[4])
            if key not in candidates:
                candidates.add(key)
                rows.append(f"{m[1]}  term {m[3]}  n{m[4]} starts an election")
        elif m := SEND_AE.match(line):
            if m[3] not in leaders:
                leaders[m[3]] = m[4]
                rows.append(f"{m[1]}  term {m[3]}  n{m[4]} becomes leader")
        elif m := EVENT.match(line):
            rows.append(f"{m[1]}  {m[2]}")
    summary = ["leaders by term:"]
    summary += [f"  term {t}: n{leaders[t]}" for t in sorted(leaders, key=int)] or ["  none"]
    return "\n".join(rows + [""] + summary) + "\n"
