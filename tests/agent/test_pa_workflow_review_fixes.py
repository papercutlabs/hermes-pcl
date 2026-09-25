"""PA-203 round 2 review fixes to the workflow core (review-r2.md B1, B3, B4, item 8)."""

from __future__ import annotations

import json

import pytest

from agent.pa_workflow import (
    MAX_PARALLEL_SPECS,
    ScriptRefused,
    WorkflowRunner,
    WorkflowStore,
    validate_script,
)
from tests.agent.test_pa_workflow import Recorder, _start

READ_ONLY = ("readonly-investigation",)
SCHEMA = {"type": "object"}


def _run(tmp_path, script, *, executor=None, kinds=READ_ONLY, **kwargs):
    store = WorkflowStore(tmp_path / "wf.sqlite")
    executor = executor or Recorder(lambda k, p, s: {"status": "ok", "text": "{}"})
    run_id = _start(store, script)
    status = WorkflowRunner(store, executor, kinds=kinds, **kwargs).run(run_id)
    return status, executor, store


# ── B1: no path starts a helper of a kind the runner does not offer ──────


@pytest.mark.parametrize("script", [
    "output = parallel([{'kind': 'explicit-case-edit', 'prompt': 'update SK/JOB/1', 'schema': {}}])",
    "output = run({'kind': 'explicit-case-edit', 'prompt': 'update SK/JOB/1', 'schema': {}})",
    "output = map([1, 2], lambda x: {'kind': 'readonly-investigation', 'prompt': 'q', 'schema': {}})",
    "output = run(task('explicit-case-edit', 'update SK/JOB/1', {}))",
])
def test_hand_written_or_other_kind_specs_start_no_helper(tmp_path, script):
    status, executor, _ = _run(tmp_path, script)
    assert status["status"] == "failed"
    assert executor.calls == []
    assert status["total_children"] == 0


def test_the_kind_is_rechecked_where_helpers_start(tmp_path):
    # Even a real spec is refused at parallel() when its kind is not offered.
    store = WorkflowStore(tmp_path / "wf.sqlite")
    executor = Recorder()
    run_id = _start(store, "output = run(task('explicit-case-edit', 'x', {}))")
    runner = WorkflowRunner(store, executor, kinds=READ_ONLY)
    runner.kinds = frozenset(READ_ONLY)
    assert runner.run(run_id)["status"] == "failed"
    assert executor.calls == []


def test_fan_out_is_capped(tmp_path):
    script = (
        f"output = map(range({MAX_PARALLEL_SPECS + 1}), "
        "lambda i: task('readonly-investigation', 'case ' + str(i), {'type': 'object'}))"
    )
    status, executor, _ = _run(tmp_path, script)
    assert status["status"] == "failed" and "at most" in status["error"]
    assert executor.calls == []
    over_run = (
        "for g in range(3):\n"
        "    map(range(100), lambda i: task('readonly-investigation', str(g) + ':' + str(i), {'type': 'object'}))\n"
    )
    status, executor, _ = _run(tmp_path / "b", over_run) if (tmp_path / "b").mkdir() is None else None
    # The limit stops the run; the 200 helpers that finished are kept (r2e).
    assert status["status"] == "completed" and "at most 200 helpers" in status["error"]
    assert len(status["output"]["results"]) == 200 and status["output"]["partial"] is True
    assert len(executor.calls) == 200


# ── B3: the script itself is bounded ─────────────────────────────────────


@pytest.mark.parametrize("script,needle", [
    # In-place growth while iterating: the step budget ends it.
    ("l = [1]\nfor x in l:\n    l[len(l):] = [x]\n", "steps"),
    ("s = 'a' * 10000000000\n", "at most"),
    ("n = 1\nfor i in range(10000):\n    n = n * 1000000000\n", "bits"),
    ("x = [a + b for a in range(10000) for b in range(10000)]\n", "steps"),
])
def test_runaway_scripts_end_failed(tmp_path, script, needle):
    status, executor, _ = _run(tmp_path, script)
    assert status["status"] == "failed", status
    assert needle in status["error"]


def test_shift_operator_is_refused():
    with pytest.raises(ScriptRefused):
        validate_script("x = 1 << 100000000000")


# ── item 8: stop reasons ─────────────────────────────────────────────────


def test_timeout_stops_the_run_before_its_next_helper(tmp_path):
    calls = []

    def stop():
        return "timeout" if len(calls) >= 2 else None

    executor = Recorder(lambda k, p, s: calls.append(p) or {"status": "ok", "text": "{}"})
    script = "for i in range(5):\n    run(task('readonly-investigation', 'q' + str(i), {'type': 'object'}))\n"
    status, _, _ = _run(tmp_path, script, executor=executor, stop_reason=stop)
    assert status["status"] == "failed" and status["error"] == "stopped: timeout"
    assert len(calls) == 2


def test_paused_run_stays_unfinished_and_resumes_without_redoing_work(tmp_path):
    state = {"reason": None}
    calls = []

    def answer(k, p, s):
        calls.append(p)
        if len(calls) == 2:
            state["reason"] = "paused"
        return {"status": "ok", "text": "{}"}

    store = WorkflowStore(tmp_path / "wf.sqlite")
    script = "output = [run(task('readonly-investigation', 'q' + str(i), {'type': 'object'}))['status'] for i in range(4)]\n"
    run_id = _start(store, script)
    first = WorkflowRunner(store, Recorder(answer), kinds=READ_ONLY, stop_reason=lambda: state["reason"]).run(run_id)
    assert first["status"] == "running"
    state["reason"] = None
    second = WorkflowRunner(store, Recorder(answer), kinds=READ_ONLY, stop_reason=lambda: state["reason"]).run(run_id)
    assert second["status"] == "completed" and second["output"] == ["ok"] * 4
    assert len(calls) == 4


# ── B4: results() is the same on resume ──────────────────────────────────


def test_results_reads_only_this_pass(tmp_path):
    class Crash(BaseException):
        pass

    count = {"n": 0}

    def answer(k, p, s):
        count["n"] += 1
        if count["n"] == 3:
            raise Crash()
        return {"status": "ok", "text": "{}"}

    store = WorkflowStore(tmp_path / "wf.sqlite")
    script = (
        "parallel([task('readonly-investigation', 'p1', {'type': 'object'}),"
        " task('readonly-investigation', 'p2', {'type': 'object'})])\n"
        "n = len(results())\n"
        "run(task('readonly-investigation', 'q' + str(n), {'type': 'object'}))\n"
        "output = n\n"
    )
    run_id = _start(store, script)
    with pytest.raises(Crash):
        WorkflowRunner(store, Recorder(answer), kinds=READ_ONLY).run(run_id)
    status = WorkflowRunner(store, Recorder(answer), kinds=READ_ONLY).run(run_id)
    assert status["output"] == 2
    assert sorted(c["task"] for c in status["children"]) == ["p1", "p2", "q2"]


def test_augmented_add_builds_a_new_list_so_iteration_ends(tmp_path):
    status, _, _ = _run(tmp_path, "l = [1]\nfor x in l:\n    l += [x]\noutput = l\n")
    assert status["status"] == "completed" and status["output"] == [1, 1]
