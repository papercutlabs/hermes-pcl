"""The nightly analyzer receipt gate stays inside one live agent session."""

import re
import sys
import threading
import types
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.config import Platform
from gateway.session import SessionSource


class _FakeAgent:
    instances = []

    def __init__(self, *args, **kwargs):
        self.tools = []
        self.calls = []
        type(self).instances.append(self)

    def run_conversation(self, message, conversation_history=None, task_id=None):
        self.calls.append({
            "message": message,
            "conversation_history": conversation_history,
            "task_id": task_id,
        })
        if len(self.calls) == 1:
            return {
                "final_response": "first turn",
                "messages": [{"role": "assistant", "content": "first turn"}],
                "api_calls": 1,
                "completed": True,
            }
        if len(self.calls) == 2:
            return {
                "final_response": "second turn",
                "messages": [{"role": "assistant", "content": "second turn"}],
                "api_calls": 1,
                "completed": True,
            }
        return {
            "final_response": "receipt submitted",
            "messages": [{"role": "assistant", "content": "receipt submitted"}],
            "api_calls": 1,
            "completed": True,
        }


def _runner():
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
    return runner


_LANE_BATCH_ID = "nightly:2026-08-17:abcdef012345"
_LANE_CHAT_ID = "120363421153247095@g.us"
_NIGHTLY = "nightly"
_CONTINUOUS = "continuous"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("original_message", "lane"),
    [
        ("nightly analyzer", _NIGHTLY),
        (
            f"[System] Nightly WhatsApp analyzer. batch_id={_LANE_BATCH_ID}. "
            f"authoritative_chat_id={_LANE_CHAT_ID}. Load the required WhatsApp skill, "
            "process only that frozen chat into its singular ledger, and seal the immutable chat receipt.",
            _NIGHTLY,
        ),
        (
            f"[System] Nightly WhatsApp analyzer retry. batch_id={_LANE_BATCH_ID}. "
            f"authoritative_chat_id={_LANE_CHAT_ID}. attempt_id=amk-ctx-lost-20261002a. "
            "Resume only this frozen chat, retain its durable work, and seal its immutable chat receipt.",
            _NIGHTLY,
        ),
        (
            f"[System] Continuous WhatsApp interval. batch_id={_LANE_BATCH_ID}. "
            f"authoritative_chat_id={_LANE_CHAT_ID}. Load the required WhatsApp skill, "
            "process only that frozen chat into its singular ledger, and seal the immutable chat receipt.",
            _CONTINUOUS,
        ),
        (
            f"Continuous WhatsApp interval. batch_id={_LANE_BATCH_ID}. authoritative_chat_id={_LANE_CHAT_ID}. "
            "This is a recovery of the same frozen interval: do not re-analyse.",
            _CONTINUOUS,
        ),
    ],
    ids=["bare", "nightly-trigger", "nightly-retry", "continuous-interval", "continuous-recovery"],
)
async def test_nightly_analyzer_continues_same_agent_session_until_receipt(
    monkeypatch, tmp_path, original_message, lane,
):
    batch_id = _LANE_BATCH_ID
    authoritative_chat_id = _LANE_CHAT_ID
    _FakeAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _FakeAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-5.6")
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "fake"})

    import hermes_cli.tools_config as tools_config
    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: {"core"})

    status_calls = []

    def status_handler(args):
        status_calls.append(args)
        index = len(status_calls)
        return {
            "ok": True,
            "completed_chat_ids": [] if index < 4 else [authoritative_chat_id],
            "chat_progress": {
                authoritative_chat_id: {
                    "sha256": "baseline" if index == 1 else f"progress-{index - 1}",
                    "page_classifications": 3 if index == 1 else index + 2,
                }
            },
        }

    import tools.registry as tool_registry
    monkeypatch.setattr(
        tool_registry.registry,
        "get_entry",
        lambda name: SimpleNamespace(handler=status_handler) if name == "tgg_nightly_get_batch_status" else None,
    )

    runner = _runner()
    result = await runner._run_agent(
        message=original_message,
        context_prompt="",
        history=[],
        source=SessionSource(platform=Platform.WHATSAPP, chat_id="synthetic@g.us", chat_type="group"),
        session_id="nightly-session",
        session_key="nightly-session-key",
        pa_job_type="tgg_nightly_whatsapp",
        pa_context={"nightly_batch_id": batch_id, "authoritative_chat_id": authoritative_chat_id},
        suppress_delivery=True,
    )

    assert result["final_response"] == "receipt submitted"
    assert len(_FakeAgent.instances) == 1
    calls = _FakeAgent.instances[0].calls
    assert len(calls) == 3
    assert [call["task_id"] for call in calls] == ["nightly-session"] * 3
    assert calls[0]["conversation_history"] == []
    assert calls[1]["conversation_history"] == []
    assert calls[2]["conversation_history"] == []
    receipt_binding = re.compile(
        r"\bContinuous WhatsApp interval\. batch_id=(nightly:\d{4}-\d{2}-\d{2}:[0-9a-f]{12})\. "
        r"authoritative_chat_id=(\d+@g\.us)\."
    )
    prior_continuation = (
        "The immutable nightly chat receipt is still missing for "
        f"batch_id={batch_id} and authoritative_chat_id={authoritative_chat_id}."
    )
    assert receipt_binding.search(prior_continuation) is None
    if lane == _CONTINUOUS:
        # The continuous lane's own re-prompt is byte-for-byte what it was.
        assert [receipt_binding.search(call["message"]).groups() for call in calls[1:]] == [
            (batch_id, authoritative_chat_id),
            (batch_id, authoritative_chat_id),
        ]
        assert calls[1]["message"] == (
            f"Continuous WhatsApp interval. batch_id={batch_id}. "
            f"authoritative_chat_id={authoritative_chat_id}. "
            "The immutable nightly chat receipt is still missing for "
            f"batch_id={batch_id} and authoritative_chat_id={authoritative_chat_id}. "
            "Previous pages, image inspections and findings are durably saved. "
            "The compact durable page inventory records all earlier pages as "
            "classified. Start at exact cursor=100. Do not load the full chat ledger and do not revisit "
            "earlier pages: their output can exhaust this continuation's context. "
            "Unless the inventory says every item is already covered, process exactly "
            "that one page in cursor order, including every required image inspection, "
            "then commit its ledger decisions and groups. If it is the final page, seal "
            "the chat receipt after that commit; otherwise end "
            "the turn immediately. The runner will verify durable progress and start "
            "the next compact continuation. Do not summarize completion until the "
            "immutable chat receipt exists, unless a genuine terminal tool failure "
            "prevents further progress."
        )
    else:
        # PA-203: a nightly analyzer's continuation must not pose as the
        # continuous lane, or the plugin refuses every case lookup.
        for call in calls[1:]:
            assert receipt_binding.search(call["message"]) is None
            assert call["message"].startswith(
                f"Nightly WhatsApp analyzer continuation. batch_id={batch_id}. "
                f"authoritative_chat_id={authoritative_chat_id}. "
            )
    assert "Start at exact cursor=100" in calls[1]["message"]
    assert "Start at exact cursor=125" in calls[2]["message"]
    assert "Do not load the full chat ledger" in calls[1]["message"]
    assert "process exactly that one page" in calls[1]["message"].lower()
    assert len(status_calls) == 4
    assert status_calls == [{"batch_id": batch_id}] * 4
    assert runner._queued_events == {}


@pytest.mark.asyncio
async def test_nightly_analyzer_skips_continuation_when_receipt_already_exists(monkeypatch, tmp_path):
    _FakeAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _FakeAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-5.6")
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "fake"})

    import hermes_cli.tools_config as tools_config
    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: {"core"})
    import tools.registry as tool_registry
    monkeypatch.setattr(
        tool_registry.registry,
        "get_entry",
        lambda name: SimpleNamespace(handler=lambda _args: {"ok": True, "completed_chat_ids": ["amk@g.us"]}) if name == "tgg_nightly_get_batch_status" else None,
    )

    await _runner()._run_agent(
        message="nightly analyzer",
        context_prompt="",
        history=[],
        source=SessionSource(platform=Platform.WHATSAPP, chat_id="synthetic@g.us", chat_type="group"),
        session_id="nightly-session",
        session_key="nightly-session-key",
        pa_job_type="tgg_nightly_whatsapp",
        pa_context={"nightly_batch_id": "nightly:2026-08-17:test", "authoritative_chat_id": "amk@g.us"},
        suppress_delivery=True,
    )

    assert len(_FakeAgent.instances) == 1
    assert len(_FakeAgent.instances[0].calls) == 1


@pytest.mark.asyncio
async def test_nightly_analyzer_seals_directly_when_coverage_cursor_reaches_chat_total(monkeypatch, tmp_path):
    _FakeAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _FakeAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-5.6")
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "fake"})
    import hermes_cli.tools_config as tools_config
    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: {"core"})
    calls = []
    import tools.registry as tool_registry

    def status(_args):
        calls.append(1)
        return {
            "ok": True,
            "completed_chat_ids": ["amk@g.us"] if len(calls) == 3 else [],
            "chat_progress": {
                "amk@g.us": {
                    "sha256": "before" if len(calls) == 1 else "all-covered",
                    "first_incomplete_cursor": 175,
                    "all_items_covered": True,
                }
            },
        }

    monkeypatch.setattr(
        tool_registry.registry, "get_entry",
        lambda name: SimpleNamespace(handler=status) if name == "tgg_nightly_get_batch_status" else None,
    )
    await _runner()._run_agent(
        message="nightly analyzer", context_prompt="", history=[],
        source=SessionSource(platform=Platform.WHATSAPP, chat_id="synthetic@g.us", chat_type="group"),
        session_id="nightly-session", session_key="nightly-session-key",
        pa_job_type="tgg_nightly_whatsapp",
        pa_context={"nightly_batch_id": "nightly:2026-08-17:test", "authoritative_chat_id": "amk@g.us"},
        suppress_delivery=True,
    )

    assert len(_FakeAgent.instances[0].calls) == 2
    assert "Do not fetch another page; seal the chat receipt now" in _FakeAgent.instances[0].calls[1]["message"]


@pytest.mark.asyncio
async def test_nightly_analyzer_stops_after_a_continuation_with_no_durable_progress(monkeypatch, tmp_path):
    _FakeAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _FakeAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-5.6")
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "fake"})

    import hermes_cli.tools_config as tools_config
    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: {"core"})
    import tools.registry as tool_registry
    monkeypatch.setattr(
        tool_registry.registry,
        "get_entry",
        lambda name: SimpleNamespace(handler=lambda _args: {
            "ok": True,
            "completed_chat_ids": [],
            "chat_progress": {"amk@g.us": {"sha256": "unchanged"}},
        }) if name == "tgg_nightly_get_batch_status" else None,
    )

    result = await _runner()._run_agent(
        message="nightly analyzer",
        context_prompt="",
        history=[],
        source=SessionSource(platform=Platform.WHATSAPP, chat_id="synthetic@g.us", chat_type="group"),
        session_id="nightly-session",
        session_key="nightly-session-key",
        pa_job_type="tgg_nightly_whatsapp",
        pa_context={"nightly_batch_id": "nightly:2026-08-17:test", "authoritative_chat_id": "amk@g.us"},
        suppress_delivery=True,
    )

    assert len(_FakeAgent.instances[0].calls) == 1
    assert "no durable analyzer progress" in result["final_response"]


class _IncompleteAgent(_FakeAgent):
    def run_conversation(self, message, conversation_history=None, task_id=None):
        self.calls.append({
            "message": message,
            "conversation_history": conversation_history,
            "task_id": task_id,
        })
        return {
            "final_response": "ledger update committed; response interrupted",
            "messages": [],
            "api_calls": 1,
            "completed": False,
            "partial": True,
        }


def _install_incomplete_agent(monkeypatch, tmp_path):
    _IncompleteAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _IncompleteAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-5.6")
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "fake"})
    import hermes_cli.tools_config as tools_config
    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: {"core"})


async def _run_incomplete_nightly():
    return await _runner()._run_agent(
        message="nightly analyzer",
        context_prompt="",
        history=[],
        source=SessionSource(platform=Platform.WHATSAPP, chat_id="synthetic@g.us", chat_type="group"),
        session_id="nightly-session",
        session_key="nightly-session-key",
        pa_job_type="tgg_nightly_whatsapp",
        pa_context={"nightly_batch_id": "nightly:2026-08-17:test", "authoritative_chat_id": "amk@g.us"},
        suppress_delivery=True,
    )


@pytest.mark.asyncio
async def test_nightly_incomplete_after_durable_progress_continues(monkeypatch, tmp_path):
    _install_incomplete_agent(monkeypatch, tmp_path)
    calls = []
    import tools.registry as tool_registry

    def status(_args):
        calls.append(1)
        # Pre-turn baseline, post-turn durable write, then sealed receipt.
        progress = ["before", "after", "after"][len(calls) - 1]
        return {
            "ok": True,
            "completed_chat_ids": ["amk@g.us"] if len(calls) == 3 else [],
            "chat_progress": {"amk@g.us": {"sha256": progress, "page_classifications": len(calls)}},
        }

    monkeypatch.setattr(
        tool_registry.registry, "get_entry",
        lambda name: SimpleNamespace(handler=status) if name == "tgg_nightly_get_batch_status" else None,
    )
    result = await _run_incomplete_nightly()

    assert len(_IncompleteAgent.instances[0].calls) == 2
    assert result["completed"] is True
    assert result["partial"] is False


@pytest.mark.asyncio
async def test_nightly_incomplete_after_seal_is_completed_from_receipt(monkeypatch, tmp_path):
    _install_incomplete_agent(monkeypatch, tmp_path)
    calls = []
    import tools.registry as tool_registry

    def status(_args):
        calls.append(1)
        return {
            "ok": True,
            "completed_chat_ids": ["amk@g.us"] if len(calls) == 2 else [],
            "chat_progress": {"amk@g.us": {"sha256": "sealed", "page_classifications": 1}},
        }

    monkeypatch.setattr(
        tool_registry.registry, "get_entry",
        lambda name: SimpleNamespace(handler=status) if name == "tgg_nightly_get_batch_status" else None,
    )
    result = await _run_incomplete_nightly()

    assert len(_IncompleteAgent.instances[0].calls) == 1
    assert result["completed"] is True
    assert result["partial"] is False


@pytest.mark.asyncio
async def test_nightly_incomplete_without_durable_progress_remains_terminal(monkeypatch, tmp_path):
    _install_incomplete_agent(monkeypatch, tmp_path)
    import tools.registry as tool_registry
    monkeypatch.setattr(
        tool_registry.registry, "get_entry",
        lambda name: SimpleNamespace(handler=lambda _args: {
            "ok": True,
            "completed_chat_ids": [],
            "chat_progress": {"amk@g.us": {"sha256": "unchanged", "page_classifications": 0}},
        }) if name == "tgg_nightly_get_batch_status" else None,
    )
    result = await _run_incomplete_nightly()

    assert len(_IncompleteAgent.instances[0].calls) == 1
    assert result["partial"] is True


@pytest.mark.asyncio
async def test_nightly_analyzer_uses_elapsed_deadline_not_turn_count(monkeypatch, tmp_path):
    _FakeAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _FakeAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-5.6")
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "fake"})
    monkeypatch.setattr(gateway_run, "_TGG_NIGHTLY_MAX_ANALYZER_SECONDS", 0)

    import hermes_cli.tools_config as tools_config
    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: {"core"})
    import tools.registry as tool_registry
    monkeypatch.setattr(
        tool_registry.registry,
        "get_entry",
        lambda name: SimpleNamespace(
            handler=lambda _args: {"ok": True, "completed_chat_ids": []}
        ) if name == "tgg_nightly_get_batch_status" else None,
    )

    result = await _runner()._run_agent(
        message="nightly analyzer",
        context_prompt="",
        history=[],
        source=SessionSource(platform=Platform.WHATSAPP, chat_id="synthetic@g.us", chat_type="group"),
        session_id="nightly-session",
        session_key="nightly-session-key",
        pa_job_type="tgg_nightly_whatsapp",
        pa_context={"nightly_batch_id": "nightly:2026-08-17:test", "authoritative_chat_id": "amk@g.us"},
        suppress_delivery=True,
    )

    assert len(_FakeAgent.instances[0].calls) == 1
    assert "two-hour same-session deadline" in result["final_response"]
