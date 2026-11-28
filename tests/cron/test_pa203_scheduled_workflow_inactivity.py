"""PA-203 r2f replay s7: an inline workflow keeps a scheduled job alive.

A scheduled Management job ran workflow wf-f22f3d4c2115 inline with 18
helpers.  The cron watchdog killed it as "idle for 604s ... executing tool:
workflow_start"; the workflow finished 11 seconds later and its result
reached no one.  Here a scheduled job runs, through the real tick /
_process_job / run_job path, an inline workflow that lasts longer than the
inactivity limit while its helpers keep finishing.  The job completes and
delivers.  A job that does time out still keeps the workflow's results and
its failure notice says where they are.
"""

from __future__ import annotations

import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from agent.pa_delegation import MANAGEMENT_READONLY_CHILD_TOOLSET
from tests.cron.test_pa203_conversation_workspace import (  # noqa: F401
    LIVE_KEY,
    MANAGEMENT_CHAT,
    _job,
    home,
)

LIMIT = 1.5


@pytest.fixture
def workflow(home, tmp_path, monkeypatch):
    from cron import scheduler
    from tools import pa_workflow_tool

    monkeypatch.setattr(pa_workflow_tool, "store_path", lambda: tmp_path / "pa_workflows.sqlite")
    monkeypatch.setattr(scheduler, "_CRON_POLL_INTERVAL", 0.1)
    monkeypatch.setenv("HERMES_CRON_TIMEOUT", str(LIMIT))
    delay = {"seconds": 0.5}

    def slow_executor(_parent):
        def execute(kind, prompt, schema):
            time.sleep(delay["seconds"])
            return {"status": "ok", "text": json.dumps({"job": prompt.split()[-1], "finding": "no update"})}
        return execute

    monkeypatch.setattr(pa_workflow_tool, "_make_executor", slow_executor)
    return {"tool": pa_workflow_tool, "delay": delay}


def _activity_agent(run):
    """A stand-in agent with the real activity-tracker contract of AIAgent."""
    agent = MagicMock()
    agent.valid_tool_names = {"tgg_case_query"}
    agent._last_activity_ts = time.time()
    agent._last_activity_desc = "starting"

    def touch(desc):
        agent._last_activity_ts = time.time()
        agent._last_activity_desc = desc

    agent._touch_activity = touch
    agent.get_activity_summary = lambda: {
        "seconds_since_activity": time.time() - agent._last_activity_ts,
        "last_activity_desc": agent._last_activity_desc,
        "current_tool": "workflow_start",
        "api_call_count": 1,
        "max_iterations": 10,
    }
    agent.run_conversation.side_effect = lambda *_a, **_k: run(agent)
    return agent


def _tick(agent):
    from cron import scheduler

    delivered: dict = {}

    def capture(job_arg, content, **kwargs):
        delivered.update(kwargs, content=content)
        return None

    with patch("cron.scheduler.get_due_jobs", return_value=[_job()]), \
         patch("cron.scheduler.advance_next_run"), \
         patch("cron.scheduler.save_job_output", return_value="out.md"), \
         patch("cron.scheduler.mark_job_run"), \
         patch("cron.scheduler._deliver_result", side_effect=capture), \
         patch("dotenv.load_dotenv"), \
         patch("hermes_state.SessionDB", return_value=MagicMock()), \
         patch("tools.mcp_tool.discover_mcp_tools", return_value=[]), \
         patch("hermes_cli.runtime_provider.resolve_runtime_provider", return_value={
             "api_key": "test-key", "base_url": "https://provider.invalid/v1",
             "provider": "openrouter", "api_mode": "chat_completions"}), \
         patch("run_agent.AIAgent", return_value=agent):
        assert scheduler.tick(verbose=False) == 1
    return delivered


def _start_inline(tool, agent, jobs):
    # The Management scope the real run_job applies to a tgg_management job.
    agent.enabled_toolsets = [MANAGEMENT_READONLY_CHILD_TOOLSET]
    agent._pa_delegation_child_scope = True
    script = (
        f"jobs = {jobs!r}\n"
        "found = map(jobs, lambda job: task('readonly-investigation', 'Check ' + job, "
        "{'type': 'object', 'required': ['job']}))\n"
        "output = [r['output'] for r in found]\n"
    )
    agent._touch_activity("executing tool: workflow_start")
    return json.loads(tool.workflow_start(script=script, title="AM review", wait=True, parent_agent=agent))


def test_inline_workflow_with_steady_progress_outlives_the_inactivity_limit(workflow):
    tool = workflow["tool"]
    jobs = [f"AM/JOB/2609/{n:04d}" for n in range(1, 13)]  # 3 rounds of 4 helpers
    seen = {}

    def run(agent):
        started = time.time()
        summary = _start_inline(tool, agent, jobs)
        seen["seconds"] = time.time() - started
        seen["summary"] = summary
        return {"completed": True, "final_response": f"All {summary['counts']['ok']} AM cases checked."}

    delivered = _tick(_activity_agent(run))
    assert seen["seconds"] > LIMIT, seen  # longer than the inactivity limit
    assert seen["summary"]["status"] == "completed" and seen["summary"]["counts"]["ok"] == 12
    assert delivered["content"] == "All 12 AM cases checked."
    assert "idle for" not in delivered["content"]


def test_a_timed_out_job_keeps_the_workflow_results_and_says_where(workflow):
    tool = workflow["tool"]
    workflow["delay"]["seconds"] = LIMIT + 1.0  # one silent helper: really idle
    finished = threading.Event()

    def run(agent):
        try:
            _start_inline(tool, agent, ["AM/JOB/2609/0001"])
        finally:
            finished.set()
        return {"completed": True, "final_response": "late"}

    delivered = _tick(_activity_agent(run))
    content = delivered["content"]
    assert "idle for" in content
    assert "is still running" in content and "/work/workflow-" in content
    assert finished.wait(10)
    run_id = content.split("Workflow run ")[1].split()[0]
    status = tool._store().run_status(run_id)
    assert status["status"] == "completed" and status["counts"]["ok"] == 1
    from tools.work_artifacts import _work_dir

    results = json.loads((_work_dir(LIVE_KEY) / f"workflow-{run_id}-results.json").read_text())
    assert results["results"][0]["output"]["job"] == "AM/JOB/2609/0001"


def test_helper_heartbeats_reach_the_real_agent_through_the_run_parent():
    from tools.pa_workflow_tool import _ParentSpec

    touched = []
    parent = _ParentSpec({"enabled_toolsets": [MANAGEMENT_READONLY_CHILD_TOOLSET]}, {}, activity=touched.append)
    parent._touch_activity("delegate_task: subagent 0 working")
    assert touched == ["delegate_task: subagent 0 working"]
    assert _ParentSpec({}, {})._touch_activity("x") is None  # background runs: no-op
