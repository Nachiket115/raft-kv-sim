import argparse
import os
import sys

from raftlite.record import dumps, record_scenario, validate
from raftlite.scenarios import SCENARIOS, ScenarioFailed, run
from raftlite.timeline import timeline


def show(trace, as_timeline):
    return timeline(trace) if as_timeline else "".join(line + "\n" for line in trace)


def record_run(args, out):
    recording = record_scenario(args.scenario, args.seed)
    try:
        validate(recording)
        with open(args.record, "w", encoding="utf-8") as file:
            file.write(dumps(recording))
    except (ValueError, OSError) as problem:
        print(f"cannot record {args.scenario}: {problem}", file=out)
        return 2
    meta = recording["meta"]
    if meta["result"] == "pass":
        print(f"PASS {args.scenario} seed={args.seed} ticks={meta['ticks']} recorded {args.record}", file=out)
        return 0
    violation = meta["violation"]
    print(
        f"FAIL {args.scenario} seed={args.seed} {violation['invariant']}: {violation['detail']} recorded {args.record}",
        file=out,
    )
    return 1


def main(argv=None, out=None):
    out = out or sys.stdout
    parser = argparse.ArgumentParser(prog="raftlite")
    parser.add_argument("scenario", nargs="?")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--timeline", action="store_true")
    parser.add_argument("--record", metavar="PATH")
    args = parser.parse_args(argv)
    if args.list:
        print("\n".join(sorted(SCENARIOS)), file=out)
        return 0
    if args.scenario not in SCENARIOS:
        print(f"unknown scenario {args.scenario!r}; choose from: {', '.join(sorted(SCENARIOS))}", file=out)
        return 2
    if args.record:
        return record_run(args, out)
    try:
        sim = run(args.scenario, args.seed)
    except ScenarioFailed as failure:
        out.write(show(failure.trace, args.timeline))
        print(f"FAIL {args.scenario} {failure}", file=out)
        return 1
    except AssertionError as failure:
        print(f"FAIL {args.scenario} seed={args.seed}\n{failure}", file=out)
        return 1
    out.write(show(sim.trace, args.timeline))
    print(f"PASS {args.scenario} seed={args.seed} ticks={sim.now}", file=out)
    return 0


if __name__ == "__main__":
    try:
        code = main()
        sys.stdout.flush()
    except BrokenPipeError:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        code = 0
    sys.exit(code)
