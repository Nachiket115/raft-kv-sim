import io

from raftlite import cli
from raftlite.scenarios import run
from raftlite.timeline import timeline

HAND = [
    "000100 n1 election timeout",
    "000100 n1 -> n0 send RequestVote(term=1, candidate_id=1, last_log_index=0, last_log_term=0)",
    "000100 n1 -> n2 send RequestVote(term=1, candidate_id=1, last_log_index=0, last_log_term=0)",
    "000110 n1 -> n0 send AppendEntries(term=1, leader_id=1, prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0)",
    "000110 n1 -> n2 send AppendEntries(term=1, leader_id=1, prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0)",
    "000200 n1 crash",
    "000300 n1 restart",
    "000400 partition [0] | [1, 2]",
    "000500 heal",
]


def test_hand_written_trace():
    assert timeline(HAND) == (
        "000100  term 1  n1 starts an election\n"
        "000110  term 1  n1 becomes leader\n"
        "000200  n1 crash\n"
        "000300  n1 restart\n"
        "000400  partition [0] | [1, 2]\n"
        "000500  heal\n"
        "\n"
        "leaders by term:\n"
        "  term 1: n1\n"
    )


def test_no_leader_says_none():
    assert timeline([]).endswith("leaders by term:\n  none\n")


def test_leader_partitioned_shows_old_and_new_leader():
    text = timeline(run("leader_partitioned", 1).trace)
    assert "term 1: n2" in text
    assert "partition [2] | [0, 1, 3, 4]" in text
    assert "term 2: n0" in text


def test_timeline_is_deterministic_and_wired_to_cli():
    out = io.StringIO()
    assert cli.main(["split_vote", "--seed", "3", "--timeline"], out) == 0
    again = io.StringIO()
    cli.main(["split_vote", "--seed", "3", "--timeline"], again)
    assert out.getvalue() == again.getvalue()
    assert "leaders by term:" in out.getvalue()
    assert out.getvalue().splitlines()[-1].startswith("PASS split_vote")
