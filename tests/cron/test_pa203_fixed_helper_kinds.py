"""PA-203 round 2: on the Management route the helper kind fixes its tools.

Teren, 25 Sep 2026: toolset names the model passes are ignored; the kind of
helper (read-only investigation, or explicit single-case edit) decides the
tools; a helper never starts with no tools and never has more than its parent.
These tests go through the real ``delegate_task`` with a scoped parent.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from agent.pa_delegation import scope_delegation_children
from tests.cron.test_pa203_readonly_delegation_children import (  # noqa: F401
    EXPLICIT,
    EXPLICIT_TOOLS,
    MANAGEMENT_ENABLED,
    READONLY,
    READONLY_TOOLS,
    _context,
    _parent,
    capability_toolsets,
)
from tools import delegate_tool

# Scenario 3's real request (candidate.md): these names reduced a round-1
# child to no tools at all, and it was still started.
SCENARIO_3_TOOLSETS = ["python-sandbox", "report-operations", "tgg-whatsapp-evidence", "custom", "file"]


def _scoped_parent(enabled=None):
    enabled = list(MANAGEMENT_ENABLED if enabled is None else enabled)
    parent = _parent(list(enabled))
    parent.valid_tool_names = set(EXPLICIT_TOOLS) | {"delegate_task", "python_sandbox", "pa_business_write"}
    scope_delegation_children(parent, enabled, _context())
    return parent


def _delegate(parent, **kwargs):
    """Call the real delegate_task; capture child construction and runs."""
    built = []

    def construct(**child_kwargs):
        child = MagicMock()
        child.valid_tool_names = set()
        for name in child_kwargs["enabled_toolsets"]:
            child.valid_tool_names |= set(
                READONLY_TOOLS if name == READONLY else EXPLICIT_TOOLS if name == EXPLICIT else []
            )
        built.append(child_kwargs["enabled_toolsets"])
        return child

    runs = []

    def run_child(index, goal, child, parent_agent, **_):
        runs.append(goal)
        return {"task_index": index, "status": "completed", "summary": "done", "api_calls": 1,
                "duration_seconds": 0.1}

    with patch("run_agent.AIAgent", side_effect=construct), \
         patch.object(delegate_tool, "_run_single_child", side_effect=run_child), \
         patch.object(delegate_tool, "_load_config", return_value={}):
        result = delegate_tool.delegate_task(parent_agent=parent, **kwargs)
    return json.loads(result), built, runs


def test_model_supplied_toolsets_are_ignored_and_the_kind_decides():
    parent = _scoped_parent()

    result, built, runs = _delegate(
        parent, goal="Check SK/JOB/2604/2098 in WhatsApp", toolsets=SCENARIO_3_TOOLSETS,
    )
    assert "error" not in result, result
    assert built == [[READONLY]]
    assert len(runs) == 1

    # Naming the write-capable toolset does not select it; only the kind does.
    _, built, _ = _delegate(parent, goal="Update one case", toolsets=[EXPLICIT, "pa-business"])
    assert built == [[READONLY]]
    _, built, _ = _delegate(
        parent, goal="Update one case", toolsets=["file"], profile="explicit-case-edit",
    )
    assert built == [[EXPLICIT]]


def test_batch_tasks_each_get_their_kind_regardless_of_requested_toolsets():
    parent = _scoped_parent()
    _, built, runs = _delegate(parent, tasks=[
        {"goal": "cases 1-5", "toolsets": ["file", "python-sandbox"]},
        {"goal": "cases 6-10", "toolsets": [EXPLICIT]},
        {"goal": "one explicit update", "profile": "whatsapp_case_investigator"},
    ])
    assert built == [[READONLY], [READONLY], [EXPLICIT]]
    assert len(runs) == 3


def test_no_helper_starts_with_no_tools():
    # A Management parent without the read-only toolset: round 1 started an
    # empty child here; round 2 refuses and starts nothing.
    enabled = [name for name in MANAGEMENT_ENABLED if name not in (READONLY, EXPLICIT)]
    parent = _scoped_parent(enabled)

    for kwargs in (
        {"goal": "review 61 cases", "toolsets": SCENARIO_3_TOOLSETS, "background": True},
        {"goal": "review 61 cases"},
        {"goal": "one case", "profile": "explicit-case-edit"},
    ):
        result, built, runs = _delegate(parent, **kwargs)
        assert "error" in result and "no helper was started" in result["error"], result
        assert built == [] and runs == []


def test_helper_whose_toolset_resolves_to_nothing_is_refused(monkeypatch):
    import toolsets as toolsets_module

    monkeypatch.setitem(
        toolsets_module.TOOLSETS, READONLY, {"description": "empty", "tools": [], "includes": []},
    )
    parent = _scoped_parent()
    result, built, runs = _delegate(parent, goal="review")
    assert "no tools" in result["error"]
    assert built == [] and runs == []


def test_helper_never_gets_more_tools_than_its_parent():
    parent = _scoped_parent()
    parent.valid_tool_names = set(READONLY_TOOLS) - {"vision_analyze"}
    result, built, runs = _delegate(parent, goal="review")
    assert "its parent does not have" in result["error"]
    assert "vision_analyze" in result["error"]
    assert runs == []


def test_unknown_kind_is_refused_not_widened():
    parent = _scoped_parent()
    result, built, runs = _delegate(parent, goal="review", profile="full-access")
    assert "Unknown helper kind" in result["error"]
    assert built == [] and runs == []


def test_orchestrator_role_cannot_add_delegation_on_the_scoped_route():
    parent = _scoped_parent()
    with patch.object(delegate_tool, "_get_orchestrator_enabled", return_value=True), \
         patch.object(delegate_tool, "_get_max_spawn_depth", return_value=3):
        _, built, _ = _delegate(parent, goal="review", role="orchestrator")
    assert built == [[READONLY]]


@pytest.mark.parametrize("context", [None, _context("tgg_ops_ingest")])
def test_other_routes_keep_native_toolset_selection(context):
    enabled = ["file", "web", "delegation"]
    parent = _parent(list(enabled))
    scope_delegation_children(parent, enabled, context)
    _, built, _ = _delegate(parent, goal="look something up", toolsets=["web"])
    assert built == [["web"]]
