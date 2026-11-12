import argparse
import sys

from raftlite.scenarios import SCENARIOS, ScenarioFailed, run


def main(argv=None, out=None):
    out = out or sys.stdout
    parser = argparse.ArgumentParser(prog="raftlite")
    parser.add_argument("scenario", nargs="?")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args(argv)
    if args.list:
        print("\n".join(sorted(SCENARIOS)), file=out)
        return 0
    if args.scenario not in SCENARIOS:
        print(f"unknown scenario {args.scenario!r}; choose from: {', '.join(sorted(SCENARIOS))}", file=out)
        return 2
    try:
        sim = run(args.scenario, args.seed)
    except ScenarioFailed as failure:
        out.write("".join(line + "\n" for line in failure.trace))
        print(f"FAIL {args.scenario} {failure}", file=out)
        return 1
    except AssertionError as failure:
        print(f"FAIL {args.scenario} seed={args.seed}\n{failure}", file=out)
        return 1
    out.write(sim.trace_text())
    print(f"PASS {args.scenario} seed={args.seed} ticks={sim.now}", file=out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
