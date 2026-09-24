"""PA-203: a delegated child on the Management route can never write.

Native delegation builds every child from the parent agent's
``enabled_toolsets``.  The PA layer scopes that attribute on the Management
route (interactive Management turns and scheduled ``tgg_management`` jobs) to
the capability's read-only investigation toolset.  These tests drive the real
cron ``run_job`` and gateway ``_run_agent`` construction paths, then build the
child with the real ``tools.delegate_tool._build_child_agent``.
"""

from __future__ import annotations

import sys
import threading
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

import gateway.run as gateway_run
import toolsets as toolsets_module
from agent.pa_delegation import (
    MANAGEMENT_READONLY_CHILD_TOOLSET,
    delegation_child_toolsets,
    scope_delegation_children,
)
from cron.scheduler import run_job
from gateway.config import Platform
from gateway.session import SessionSource
from tools.delegate_tool import _build_child_agent


FIXTURE = Path(__file__).parents[1] / "fixtures" / "pa" / "bobby_tgg_constitution.yaml"
READONLY = MANAGEMENT_READONLY_CHILD_TOOLSET

# The Management brief's enabled toolsets in the PA-203 capability candidate.
MANAGEMENT_ENABLED = [
    "memory",
    "file",
    "web",
    "vision",
    "custom",
    "pa-observability",
    "python-sandbox",
    "report-operations",
    "cronjob",
    "tgg-whatsapp-evidence",
    "tgg-per-case-whatsapp",
    "tgg-management-skill-readonly",
    READONLY,
    "delegation",
]

# Stream B's read-only investigation toolset (capability plugin).
READONLY_TOOLS = [
    "tgg_whatsapp_source_query",
    "tgg_whatsapp_message_media",
    "tgg_whatsapp_audio_inspect",
    "tgg_whatsapp_case_context",
    "tgg_whatsapp_case_search",
    "tgg_case_search",
    "tgg_case_query",
    "tgg_message_history_search",
    "vision_analyze",
    "skill_view",
]

# Capability-registered write toolset stand-in, so an explicit request for it
# is a real toolset name that the parent has.
PER_CASE_WRITE_TOOLS = [
    "tgg_whatsapp_case_status",
    "tgg_whatsapp_case_prepare",
    "tgg_whatsapp_case_record_result",
    "tgg_whatsapp_case_submit",
]

WRITE_REQUEST = [
    "pa-business",
    "tgg-per-case-whatsapp",
    "file",
    "cronjob",
    "python-sandbox",
    "report-operations",
]

WRITE_TOOLS = {
    "pa_business_write",
    "tgg_case_create",
    "tgg_case_update_state",
    "tgg_case_observation",
    "tgg_whatsapp_case_prepare",
    "tgg_whatsapp_case_record_result",
    "tgg_whatsapp_case_submit",
    "tgg_file_return",
    "python_sandbox",
    "write_file",
    "patch",
    "cronjob",
    "delegate_task",
}


@pytest.fixture(autouse=True)
def capability_toolsets(monkeypatch):
    """Register the capability's plugin toolsets the way its plugin does."""
    monkeypatch.setitem(
        toolsets_module.TOOLSETS,
        READONLY,
        {"description": "read-only investigation", "tools": list(READONLY_TOOLS), "includes": []},
    )
    monkeypatch.setitem(
        toolsets_module.TOOLSETS,
        "tgg-per-case-whatsapp",
        {"description": "per-case update", "tools": list(PER_CASE_WRITE_TOOLS), "includes": []},
    )
    monkeypatch.setitem(
        toolsets_module.TOOLSETS,
        "tgg-management-skill-readonly",
        {"description": "skill read", "tools": ["skill_view"], "includes": []},
    )


def _context(job_type: str | None = "tgg_management"):
    return SimpleNamespace(job_type=job_type)


def _parent(enabled_toolsets):
    parent = MagicMock()
    parent.base_url = "https://provider.invalid/v1"
    parent.api_key = "***"
    parent.provider = "openrouter"
    parent.api_mode = "chat_completions"
    parent.model = "test-model"
    parent.platform = "whatsapp"
    parent.providers_allowed = None
    parent.providers_ignored = None
    parent.providers_order = None
    parent.provider_sort = None
    parent._session_db = None
    parent._delegate_depth = 0
    parent._active_children = []
    parent._active_children_lock = threading.Lock()
    parent._print_fn = None
    parent.tool_progress_callback = None
    parent.thinking_callback = None
    parent.enabled_toolsets = enabled_toolsets
    return parent


def _child_toolsets(parent, requested):
    """Build one child with the real native builder; return its toolsets."""
    with patch("run_agent.AIAgent") as child_cls:
        child_cls.return_value = MagicMock()
        _build_child_agent(
            task_index=0,
            goal="Investigate the selected cases",
            context=None,
            toolsets=requested,
            model=None,
            max_iterations=10,
            task_count=1,
            parent_agent=parent,
        )
    return child_cls.call_args.kwargs["enabled_toolsets"]


def _tools(toolset_names) -> set[str]:
    tools: set[str] = set()
    for name in toolset_names:
        tools.update(toolsets_module.resolve_toolset(name))
    return tools


# ── The PA policy ─────────────────────────────────────────────────────────


def test_policy_scopes_only_the_management_route():
    assert delegation_child_toolsets(_context(), MANAGEMENT_ENABLED) == [READONLY]
    assert delegation_child_toolsets(_context(), None) == [READONLY]
    assert delegation_child_toolsets(_context("tgg_ops_ingest"), MANAGEMENT_ENABLED) is None
    assert delegation_child_toolsets(_context(None), MANAGEMENT_ENABLED) is None
    assert delegation_child_toolsets(None, MANAGEMENT_ENABLED) is None


def test_management_parent_without_the_readonly_toolset_gives_children_nothing():
    enabled = [name for name in MANAGEMENT_ENABLED if name != READONLY]
    assert delegation_child_toolsets(_context(), enabled) == []

    parent = _parent(list(enabled))
    scope_delegation_children(parent, enabled, _context())

    assert _child_toolsets(parent, None) == []
    assert _child_toolsets(parent, WRITE_REQUEST) == []


# ── (a) omitted toolsets → read-only only ─────────────────────────────────


def test_omitted_toolsets_give_the_child_only_the_readonly_toolset():
    parent = _parent(list(MANAGEMENT_ENABLED))
    scope_delegation_children(parent, list(MANAGEMENT_ENABLED), _context())

    child = _child_toolsets(parent, None)

    assert child == [READONLY]
    assert _tools(child) == set(READONLY_TOOLS)
    assert not (_tools(child) & WRITE_TOOLS)


# ── (b) explicit write request → stripped ─────────────────────────────────


def test_explicit_write_request_is_stripped_to_nothing():
    parent = _parent(list(MANAGEMENT_ENABLED))
    scope_delegation_children(parent, list(MANAGEMENT_ENABLED), _context())

    child = _child_toolsets(parent, WRITE_REQUEST)

    assert not (_tools(child) & WRITE_TOOLS)
    assert "pa-business" not in child
    assert "tgg-per-case-whatsapp" not in child


def test_explicit_mixed_request_keeps_only_the_readonly_part():
    parent = _parent(list(MANAGEMENT_ENABLED))
    scope_delegation_children(parent, list(MANAGEMENT_ENABLED), _context())

    child = _child_toolsets(
        parent,
        [*WRITE_REQUEST, READONLY, "tgg-management-skill-readonly"],
    )

    assert READONLY in child
    assert "tgg-management-skill-readonly" in child
    assert _tools(child) <= set(READONLY_TOOLS)
    assert not (_tools(child) & WRITE_TOOLS)


def test_scoping_does_not_change_the_parent_constructor_list():
    enabled = list(MANAGEMENT_ENABLED)
    parent = _parent(enabled)
    scope_delegation_children(parent, enabled, _context())

    assert enabled == MANAGEMENT_ENABLED
    assert parent.enabled_toolsets == [READONLY]


# ── (c) non-Management routes unchanged ───────────────────────────────────


@pytest.mark.parametrize("context", [None, _context("tgg_ops_ingest"), _context(None)])
def test_non_management_route_keeps_native_inheritance(context):
    enabled = ["file", "web", "pa-business", "delegation"]
    parent = _parent(enabled)

    scope_delegation_children(parent, enabled, context)

    assert parent.enabled_toolsets is enabled
    assert _child_toolsets(parent, None) == ["file", "web", "pa-business"]
    assert _child_toolsets(parent, ["pa-business", "terminal"]) == ["pa-business"]


# ── Scheduled route: real run_job construction ────────────────────────────


def _write_config(tmp_path: Path, constitution_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "model": "test-model",
                "pa": {"enabled": True, "constitution_path": str(constitution_path)},
            }
        ),
        encoding="utf-8",
    )


def _readonly_management_constitution(tmp_path: Path) -> Path:
    constitution = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    management = constitution["job_briefs"]["tgg_management"]
    management["enabled_toolsets"] = [
        *management["enabled_toolsets"],
        READONLY,
        "delegation",
    ]
    path = tmp_path / "readonly-management-constitution.yaml"
    path.write_text(yaml.safe_dump(constitution), encoding="utf-8")
    return path


def _run_cron_job(tmp_path: Path, job: dict, parent):
    _write_config(tmp_path, _readonly_management_constitution(tmp_path))
    parent.run_conversation.return_value = {
        "completed": True,
        "final_response": "done",
    }
    with patch("cron.scheduler._hermes_home", tmp_path), \
         patch("cron.scheduler._resolve_origin", return_value=None), \
         patch("dotenv.load_dotenv"), \
         patch("hermes_state.SessionDB", return_value=MagicMock()), \
         patch("tools.mcp_tool.discover_mcp_tools", return_value=[]), \
         patch(
             "hermes_cli.runtime_provider.resolve_runtime_provider",
             return_value={
                 "api_key": "test-key",
                 "base_url": "https://provider.invalid/v1",
                 "provider": "openrouter",
                 "api_mode": "chat_completions",
             },
         ), \
         patch("run_agent.AIAgent") as parent_cls:
        parent_cls.return_value = parent
        success, _output, _final, error = run_job(job)
    assert success is True, error
    return parent_cls.call_args.kwargs


def test_scheduled_management_job_children_are_readonly(tmp_path):
    parent = _parent(None)
    kwargs = _run_cron_job(
        tmp_path,
        {
            "id": "pa203-readonly-children",
            "name": "TGG scheduled review",
            "prompt": "review the selected cases",
            "pa_job_type": "tgg_management",
        },
        parent,
    )

    # The scheduled parent itself keeps its full brief and delegation.
    assert READONLY in kwargs["enabled_toolsets"]
    assert "delegation" in kwargs["enabled_toolsets"]
    assert "delegation" not in (kwargs["disabled_toolsets"] or [])
    # Its children do not.
    assert parent.enabled_toolsets == [READONLY]
    assert _child_toolsets(parent, None) == [READONLY]
    assert not (_tools(_child_toolsets(parent, WRITE_REQUEST)) & WRITE_TOOLS)


def test_scheduled_non_management_job_is_unchanged(tmp_path):
    sentinel = ["file", "pa-business"]
    parent = _parent(sentinel)
    _run_cron_job(
        tmp_path,
        {
            "id": "plain-cron-job",
            "name": "plain",
            "prompt": "say hello",
        },
        parent,
    )

    assert parent.enabled_toolsets is sentinel
    assert "_pa_delegation_child_scope" not in vars(parent)


# ── Interactive route: real gateway _run_agent construction ───────────────


class _TurnAgent:
    """Stands in for AIAgent: records the constructor list like AIAgent does."""

    instances: list["_TurnAgent"] = []

    def __init__(self, *args, **kwargs):
        self.enabled_toolsets = kwargs.get("enabled_toolsets")
        self.constructed_enabled_toolsets = kwargs.get("enabled_toolsets")
        self.tools = []
        self.session_id = kwargs.get("session_id")
        self.model = kwargs.get("model", "test-model")
        self.provider = kwargs.get("provider", "test-provider")
        self.base_url = "https://provider.invalid/v1"
        self.api_key = "***"
        self.api_mode = "chat_completions"
        self.platform = kwargs.get("platform", "whatsapp")
        self._delegate_depth = 0
        self._active_children = []
        self._active_children_lock = threading.Lock()
        self._session_db = None
        self.child_scopes: list = []
        type(self).instances.append(self)

    @property
    def is_interrupted(self) -> bool:
        return False

    def run_conversation(self, message, conversation_history=None, task_id=None):
        self.child_scopes.append(self.enabled_toolsets)
        return {
            "final_response": "ordinary text",
            "messages": [{"role": "assistant", "content": "ordinary text"}],
            "api_calls": 1,
            "completed": True,
            "final_response_validation": None,
        }


@pytest.fixture()
def gateway_runner(tmp_path, monkeypatch):
    (tmp_path / "config.yaml").write_text("{}\n", encoding="utf-8")
    _TurnAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _TurnAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "test-model")
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {"api_key": "fake", "provider": "test-provider"},
    )
    monkeypatch.setattr(gateway_run, "_render_pa_ephemeral_prompt", lambda _context: "")
    monkeypatch.setattr(gateway_run, "_apply_pa_compression_policy", lambda *_args: None)
    monkeypatch.setattr(gateway_run, "_record_pa_behavior_event", lambda *_args, **_kwargs: None)

    import hermes_cli.tools_config as tools_config

    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: {"file", "web"})
    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)

    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {}
    runner._ephemeral_system_prompt = ""
    runner._prefill_messages = []
    runner._reasoning_config = None
    runner._service_tier = None
    runner._provider_routing = {}
    runner._fallback_model = None
    runner._running_agents = {}
    runner._pending_model_notes = {}
    runner._pending_skills_reload_notes = {}
    runner._session_db = None
    runner._agent_cache = {}
    runner._agent_cache_lock = threading.Lock()
    runner._session_model_overrides = {}
    runner._queued_events = {}
    runner.hooks = SimpleNamespace(loaded_hooks=False)
    runner.config = SimpleNamespace(streaming=None)
    runner._agent_config_signature = lambda *_args, **_kwargs: "same-signature"
    runner._resolve_session_agent_runtime = lambda **_kwargs: (
        "test-model",
        {"api_key": "fake", "provider": "test-provider"},
    )
    runner._resolve_session_reasoning_config = lambda **_kwargs: None
    return runner


def _turn(session_key: str = "management-session") -> dict:
    return {
        "message": "investigate the selected cases",
        "context_prompt": "",
        "history": [],
        "source": SessionSource(
            platform=Platform.WHATSAPP,
            chat_id="management@g.us",
            chat_type="group",
        ),
        "session_id": "session-1",
        "session_key": session_key,
        "suppress_delivery": True,
    }


def _merge_as_brief(enabled_by_job_type: dict):
    def _merge(enabled, disabled, context):
        job_type = getattr(context, "job_type", None)
        return enabled_by_job_type.get(job_type, enabled), disabled

    return _merge


@pytest.mark.asyncio
async def test_interactive_management_turn_children_are_readonly(gateway_runner, monkeypatch):
    monkeypatch.setattr(
        gateway_run,
        "_resolve_pa_context",
        lambda *_args, **_kwargs: _context(),
    )
    monkeypatch.setattr(
        gateway_run,
        "_merge_pa_toolsets",
        _merge_as_brief({"tgg_management": list(MANAGEMENT_ENABLED)}),
    )

    await gateway_runner._run_agent(**_turn())

    agent = _TurnAgent.instances[0]
    # The parent was constructed with the whole Management brief.
    assert agent.constructed_enabled_toolsets == MANAGEMENT_ENABLED
    # During its turn, delegation sees only the read-only child scope.
    assert agent.child_scopes == [[READONLY]]
    assert _child_toolsets(agent, None) == [READONLY]
    stripped = _child_toolsets(agent, WRITE_REQUEST)
    assert not (_tools(stripped) & WRITE_TOOLS)


@pytest.mark.asyncio
async def test_interactive_non_management_turn_is_unchanged(gateway_runner, monkeypatch):
    monkeypatch.setattr(
        gateway_run,
        "_resolve_pa_context",
        lambda *_args, **_kwargs: _context("tgg_ops_ingest"),
    )
    monkeypatch.setattr(gateway_run, "_merge_pa_toolsets", _merge_as_brief({}))

    await gateway_runner._run_agent(**_turn())

    agent = _TurnAgent.instances[0]
    assert agent.child_scopes == [["file", "web"]]
    assert agent.enabled_toolsets is agent.constructed_enabled_toolsets
    assert _child_toolsets(agent, None) == ["file", "web"]


@pytest.mark.asyncio
async def test_cached_agent_gets_its_own_list_back_on_another_route(gateway_runner, monkeypatch):
    contexts = iter([_context(), _context("ordinary")])
    monkeypatch.setattr(
        gateway_run,
        "_resolve_pa_context",
        lambda *_args, **_kwargs: next(contexts),
    )
    monkeypatch.setattr(gateway_run, "_merge_pa_toolsets", _merge_as_brief({}))
    monkeypatch.setattr(
        __import__("hermes_cli.tools_config", fromlist=["_get_platform_tools"]),
        "_get_platform_tools",
        lambda *_args: {"file", "web", READONLY, "delegation"},
    )

    await gateway_runner._run_agent(**_turn())
    await gateway_runner._run_agent(**_turn())

    assert len(_TurnAgent.instances) == 1
    agent = _TurnAgent.instances[0]
    constructed = sorted(["file", "web", READONLY, "delegation"])
    assert agent.child_scopes == [[READONLY], constructed]
    assert agent.enabled_toolsets == constructed
