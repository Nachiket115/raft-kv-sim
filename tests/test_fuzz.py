import os
import subprocess
import sys
from pathlib import Path

import pytest

from raftlite.checker import InvariantViolation
from raftlite.scenarios import ScenarioFailed, fuzz
from tests import mutants

FUZZ_SEEDS = int(os.environ.get("FUZZ_SEEDS", "100"))
MUTANT_SEED_CAP = 300
REPO = Path(__file__).resolve().parent.parent


def test_fuzz_seeds_pass():
    for seed in range(FUZZ_SEEDS):
        fuzz(seed)


def test_fuzz_runs_with_same_seed_are_byte_identical():
    assert fuzz(5).trace_text() == fuzz(5).trace_text()


def failure_kind(error):
    return error.invariant if isinstance(error, InvariantViolation) else "liveness"


@pytest.mark.parametrize(
    "mutant, kind",
    [
        (mutants.DoubleVoter, "election safety"),
        (mutants.PhantomVote, "election safety"),
        (mutants.NoStepDown, "election safety"),
        (mutants.StaleLogVoter, "leader completeness"),
        (mutants.TruncateOnStale, "leader completeness"),
        (mutants.NoConsistencyCheck, "log matching"),
        (mutants.NoNoop, "liveness"),
    ],
    ids=lambda value: getattr(value, "__name__", value),
)
def test_mutant_is_caught_on_some_seed(mutant, kind):
    for seed in range(MUTANT_SEED_CAP):
        try:
            fuzz(seed, mutant)
        except (InvariantViolation, ScenarioFailed) as error:
            if failure_kind(error) == kind:
                assert f"seed={seed}" in str(error)
                return
        except IndexError:
            pass
    pytest.fail(f"{mutant.__name__} never broke {kind} in {MUTANT_SEED_CAP} seeds")


def test_failure_prints_a_repro_command_that_reproduces_it():
    with pytest.raises(InvariantViolation) as caught:
        fuzz(0, mutants.NoStepDown)
    message = str(caught.value)
    assert "seed=0" in message
    command = message.rsplit("repro: ", 1)[1]
    assert command == (
        'python -c "from raftlite.scenarios import *; '
        'from tests.mutants import NoStepDown; fuzz(0, NoStepDown)"'
    )
    code = command.split('"')[1]
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True
    )
    assert result.returncode != 0
    assert "election safety" in result.stderr and "seed=0" in result.stderr
