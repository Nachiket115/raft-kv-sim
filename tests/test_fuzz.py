import os
import subprocess
import sys
from pathlib import Path

import pytest

from raftlite.checker import InvariantViolation
from raftlite.node import RaftNode
from raftlite.scenarios import ScenarioFailed, call_for, figure8, fuzz
from tests import mutants

FUZZ_SEEDS = int(os.environ.get("FUZZ_SEEDS", "100"))
MUTANT_SEED_CAP = 300
REPO = Path(__file__).resolve().parent.parent


def test_fuzz_seeds_pass():
    for seed in range(FUZZ_SEEDS):
        fuzz(seed)


def test_fuzz_runs_with_same_seed_are_byte_identical():
    assert fuzz(5).trace_text() == fuzz(5).trace_text()


def test_fuzz_seeds_with_clients_pass():
    for seed in range(FUZZ_SEEDS):
        fuzz(seed, clients=3)


def test_fuzz_runs_with_clients_are_byte_identical_for_a_seed():
    first = fuzz(5, clients=3).trace_text()
    assert first == fuzz(5, clients=3).trace_text()
    assert first != fuzz(5).trace_text()


def session_entries(log):
    return [entry.command[1:3] for entry in log if entry.command and entry.command[0] == "SESSION"]


def test_fuzz_with_clients_exercises_retries_and_unanswered_operations():
    finished = unanswered = logged_twice = 0
    for seed in range(10):
        sim = fuzz(seed, clients=3)
        ops = [op for op in sim.history.values() if isinstance(op.request_id, tuple)]
        assert {op.request_id[0] for op in ops} == {0, 1, 2}, seed
        finished += sum(op.ret is not None for op in ops)
        unanswered += sum(op.ret is None for op in ops)
        for state in sim.persistent.values():
            entries = session_entries(state.log)
            logged_twice += len(entries) - len(set(entries))
    assert finished > 1000
    assert unanswered > 0
    assert logged_twice > 0


def test_failure_with_clients_names_them_in_the_repro_command():
    with pytest.raises(InvariantViolation) as caught:
        fuzz(0, mutants.NoStepDown, clients=3)
    assert str(caught.value).endswith(
        'from tests.mutants import NoStepDown; fuzz(0, NoStepDown, clients=3)"'
    )
    assert call_for("fuzz", 4, RaftNode, 3) == "fuzz(4, clients=3)"
    assert call_for("fuzz", 4, RaftNode) == "fuzz(4)"


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
        (mutants.StaleRead, "linearizability"),
        (mutants.DedupDisabled, "linearizability"),
        (mutants.ReplyBeforeCommit, "linearizability"),
    ],
    ids=lambda value: getattr(value, "__name__", value),
)
def test_mutant_is_caught_on_some_seed(mutant, kind):
    clients = 3 if kind == "linearizability" else 0
    for seed in range(MUTANT_SEED_CAP):
        try:
            fuzz(seed, mutant, clients=clients)
        except (InvariantViolation, ScenarioFailed) as error:
            if failure_kind(error) == kind:
                assert f"seed={seed}" in str(error)
                return
        except IndexError:
            pass
    pytest.fail(f"{mutant.__name__} never broke {kind} in {MUTANT_SEED_CAP} seeds")


@pytest.mark.parametrize(
    "mutant", [mutants.StaleRead, mutants.DedupDisabled, mutants.ReplyBeforeCommit]
)
def test_client_mutants_break_no_raft_invariant_and_pass_without_clients(mutant):
    for seed in range(5):
        fuzz(seed, mutant)
        try:
            fuzz(seed, mutant, clients=3)
        except InvariantViolation as error:
            assert error.invariant == "linearizability", seed
        except ScenarioFailed:
            pass


def test_linearizability_failure_shows_the_key_history_and_a_repro_command():
    with pytest.raises(InvariantViolation) as caught:
        fuzz(1, mutants.StaleRead, clients=3)
    message = str(caught.value)
    assert "no valid order for the operations on key 'x'" in message
    assert "seed=1" in message
    assert " GET x  " in message and "(request (" in message
    assert message.endswith('from tests.mutants import StaleRead; fuzz(1, StaleRead, clients=3)"')


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


def test_figure8_catches_old_term_commit_once_noop_is_gone():
    for seed in range(20):
        with pytest.raises(InvariantViolation) as caught:
            figure8(seed, mutants.CommitOldTermNoNoop)
        assert caught.value.invariant == "leader completeness", seed
        assert "committed at index" in str(caught.value), seed
        assert "figure8(" in str(caught.value), seed


def test_old_term_commit_alone_is_harmless_while_leaders_append_a_noop():
    for seed in range(20):
        figure8(seed, mutants.CommitOldTerm)
        figure8(seed, mutants.NoNoop)
