"""PA-203: the runtime-neutral workflow runner core (agent/pa_workflow.py)."""

from __future__ import annotations

import json
import threading

import pytest

from agent.pa_workflow import ScriptRefused, WorkflowRunner, WorkflowStore, validate_script

KINDS = ("readonly-investigation", "explicit-case-edit")
CASES = [f"AM/JOB/2609/{n:04d}" for n in range(1, 62)]  # 61 cases

SCHEMA = {
    "type": "object",
    "properties": {"cases": {"type": "array", "items": {"type": "object", "required": ["job", "finding"]}}},
    "required": ["cases"],
}

def _script(cases, size):
    return (
        f"cases = {json.dumps(cases)}\n"
        f"schema = {json.dumps(SCHEMA)}\n"
        f"groups = chunks(cases, {size})\n"
        "found = map(groups, lambda g: task('readonly-investigation', 'Check ' + join(g), schema))\n"
        "output = {'covered': [c['job'] for r in found if r['status'] == 'ok' for c in r['output']['cases']],\n"
        "          'failed': [r['task'] for r in found if r['status'] == 'failed']}\n"
    )


def _answer(prompt: str) -> str:
    jobs = [w for w in prompt.split() if "/JOB/" in w]
    return json.dumps({"cases": [{"job": j, "finding": "no update"} for j in jobs]})


class Recorder:
    def __init__(self, fn=None):
        self.calls: list[str] = []
        self.lock = threading.Lock()
        self.fn = fn or (lambda kind, prompt, schema: {"status": "ok", "text": _answer(prompt)})

    def __call__(self, kind, prompt, schema):
        with self.lock:
            self.calls.append(prompt)
        return self.fn(kind, prompt, schema)


def _start(store, script, run_id="run-1"):
    store.create_run(run_id, script=script, title="t", started_by={"chat": "c"}, origin={"chat_id": "c"})
    return run_id


@pytest.mark.parametrize("size,children", [(5, 13), (1, 61)])
def test_61_item_fan_out_returns_once_with_every_item_covered_once(tmp_path, size, children):
    store = WorkflowStore(tmp_path / "wf.db")
    executor = Recorder()
    run_id = _start(store, _script(CASES, size))

    status = WorkflowRunner(store, executor, kinds=KINDS, max_concurrency=8).run(run_id)

    assert status["status"] == "completed"
    assert status["counts"] == {"pending": 0, "running": 0, "ok": children, "failed": 0}
    assert len(executor.calls) == children
    assert sorted(status["output"]["covered"]) == sorted(CASES)
    assert len(status["output"]["covered"]) == 61
    # Exactly one completion may be emitted for the run.
    assert store.claim_completion(run_id) is True
    assert store.claim_completion(run_id) is False
    assert store.completion_claimed(run_id) is True


class Crash(BaseException):
    """Simulates the process dying mid-run."""


def test_run_resumes_after_a_crash_without_redoing_finished_children(tmp_path):
    path = tmp_path / "wf.db"
    store = WorkflowStore(path)
    run_id = _start(store, _script(CASES, 5))
    finished_before_crash = 4

    def crash_after(kind, prompt, schema):
        if len(first.calls) > finished_before_crash:
            raise Crash()
        return {"status": "ok", "text": _answer(prompt)}

    first = Recorder(crash_after)
    with pytest.raises(Crash):
        WorkflowRunner(store, first, kinds=KINDS, max_concurrency=1).run(run_id)

    mid = WorkflowStore(path).run_status(run_id)
    assert mid["status"] == "running"
    assert mid["counts"]["ok"] == finished_before_crash
    assert [r["run_id"] for r in WorkflowStore(path).unfinished_runs()] == [run_id]

    second = Recorder()
    status = WorkflowRunner(WorkflowStore(path), second, kinds=KINDS).run(run_id)

    assert status["status"] == "completed"
    assert len(second.calls) == 13 - finished_before_crash
    done_prompts = set(first.calls[:finished_before_crash])
    assert not done_prompts & set(second.calls)
    assert sorted(status["output"]["covered"]) == sorted(CASES)


@pytest.mark.parametrize(
    "script",
    [
        'terminal("ls")',
        'delegate_task(goal="x")',
        'send_message("120@g.us", "hi")',
        'write_file("/tmp/x", "y")',
        'open("/etc/passwd")',
        'exec("1")',
        'eval("1")',
        'getattr(results(), "x")',
        '__import__("os")',
        "x = results().__class__",
        "import os",
        "from os import path",
        "def f():\n    pass",
        "class A:\n    pass",
        "while True:\n    pass",
        "try:\n    x = 1\nexcept Exception:\n    pass",
        "with x:\n    pass",
        "x = 2 ** 1000",
        "_x = 1",
    ],
)
def test_script_api_refuses_raw_tools_and_escapes(script):
    with pytest.raises(ScriptRefused):
        validate_script(script)


def test_allowed_script_passes_and_names_what_is_allowed():
    validate_script("f = lambda x: task('readonly-investigation', x, {})\noutput = run(f('a'))")
    with pytest.raises(ScriptRefused) as exc:
        validate_script('terminal("ls")')
    assert "terminal" in str(exc.value) and "parallel" in str(exc.value)


def test_refused_script_or_unknown_kind_ends_the_run_failed_without_a_child(tmp_path):
    store = WorkflowStore(tmp_path / "wf.db")
    executor = Recorder()
    refused = _start(store, 'terminal("ls")', "run-refused")
    unknown = _start(store, "output = run(task('shell', 'ls', {}))", "run-unknown")

    for run_id in (refused, unknown):
        status = WorkflowRunner(store, executor, kinds=KINDS).run(run_id)
        assert status["status"] == "failed"
        assert "ScriptRefused" in status["error"]
    assert "shell" in store.run_status(unknown)["error"]
    assert executor.calls == []


def test_schema_check_retries_then_fails_without_stopping_the_run(tmp_path):
    store = WorkflowStore(tmp_path / "wf.db")
    seen: dict[str, int] = {}

    def flaky(kind, prompt, schema):
        name = prompt.split()[1]
        seen[name] = seen.get(name, 0) + 1
        if name == "good":
            text = "not json" if seen[name] == 1 else '```json\n{"cases": [{"job": "x", "finding": "y"}]}\n```'
        else:
            text = '{"cases": "wrong"}'
        return {"status": "ok", "text": text}

    executor = Recorder(flaky)
    script = (
        f"schema = {json.dumps(SCHEMA)}\n"
        "output = parallel([task('readonly-investigation', 'Check good', schema),"
        " task('readonly-investigation', 'Check bad', schema)])\n"
    )
    run_id = _start(store, script)
    status = WorkflowRunner(store, executor, kinds=KINDS, max_attempts=3).run(run_id)

    assert status["status"] == "completed"
    good, bad = status["output"]
    assert (good["status"], good["attempts"]) == ("ok", 2)
    assert good["output"] == {"cases": [{"job": "x", "finding": "y"}]}
    assert (bad["status"], bad["attempts"], bad["output"]) == ("failed", 3, None)
    assert bad["error"]
    assert "Return only JSON that matches this schema" in executor.calls[-1] or any(
        "Return only JSON" in c for c in executor.calls
    )


def test_executor_errors_become_failed_results(tmp_path):
    store = WorkflowStore(tmp_path / "wf.db")

    def broken(kind, prompt, schema):
        raise RuntimeError("provider down")

    run_id = _start(store, "output = run(task('readonly-investigation', 'x', {}))")
    status = WorkflowRunner(store, Recorder(broken), kinds=KINDS, max_attempts=2).run(run_id)
    assert status["status"] == "completed"
    assert status["output"]["status"] == "failed"
    assert "provider down" in status["output"]["error"]


def test_run_status_reports_real_counts_mid_run(tmp_path):
    store = WorkflowStore(tmp_path / "wf.db")
    release = threading.Event()
    started = threading.Semaphore(0)

    def blocking(kind, prompt, schema):
        started.release()
        release.wait(10)
        return {"status": "ok", "text": _answer(prompt)}

    run_id = _start(store, _script(CASES[:10], 1))
    runner = WorkflowRunner(store, Recorder(blocking), kinds=KINDS, max_concurrency=3)
    thread = threading.Thread(target=runner.run, args=(run_id,))
    thread.start()
    for _ in range(3):
        assert started.acquire(timeout=10)

    mid = store.run_status(run_id)
    assert mid["status"] == "running"
    assert mid["counts"]["running"] == 3
    assert mid["counts"]["pending"] == 7
    assert mid["counts"]["ok"] == 0
    assert "output" not in mid

    release.set()
    thread.join(10)
    final = store.run_status(run_id)
    assert final["status"] == "completed"
    assert final["counts"]["ok"] == 10
