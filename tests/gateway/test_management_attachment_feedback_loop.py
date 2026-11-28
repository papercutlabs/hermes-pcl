"""PA-129 Management attachment correction-loop integration tests."""

from __future__ import annotations

import json
import sys
import threading
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

import gateway.run as gateway_run
from gateway.config import Platform
from gateway.durable_jsonl_consumer import (
    DurableInbox,
    InboxRecord,
    build_management_attachment_response_validator,
    deliver_management_replies,
)
from gateway.pa_observability import build_turn_record
from gateway.session import SessionSource
from run_agent import AIAgent
from tools.python_sandbox_tool import _workspace_key


MANAGEMENT_CHAT = "management@g.us"
GATE_CHANGED_AT = "2026-09-17T00:00:00+00:00"


def _tool_defs() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": "python_sandbox",
                "description": "Run Python in the sandbox.",
                "parameters": {"type": "object", "properties": {}},
            },
        }
    ]


def _tool_call(call_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=call_id,
        type="function",
        function=SimpleNamespace(name="python_sandbox", arguments="{}"),
    )


def _response(
    content: str,
    *,
    tool_calls: list[SimpleNamespace] | None = None,
    finish_reason: str = "stop",
) -> SimpleNamespace:
    message = SimpleNamespace(content=content, tool_calls=tool_calls)
    return SimpleNamespace(
        choices=[SimpleNamespace(message=message, finish_reason=finish_reason)],
        model="test/model",
        usage=None,
    )


def _config(tmp_path: Path) -> Path:
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(
        "selectors:\n"
        "- job_type: tgg_management\n"
        "  match:\n"
        "    source.platform: whatsapp\n"
        f"    source.chat_id: {MANAGEMENT_CHAT}\n",
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "pa": {
                    "enabled": True,
                    "constitution_path": str(constitution),
                }
            }
        ),
        encoding="utf-8",
    )
    return config


def _record(message_id: str) -> InboxRecord:
    return InboxRecord(
        seq=1,
        message_id=message_id,
        chat_id=MANAGEMENT_CHAT,
        start_offset=0,
        end_offset=1,
        raw={
            "messageId": message_id,
            "chatId": MANAGEMENT_CHAT,
            "timestamp": "2026-09-17T00:01:00+00:00",
        },
    )


def _captured(response: str, owner: str, message_id: str) -> dict:
    return {
        "message_id": f"capture-{message_id}",
        "kind": "send",
        "args": [MANAGEMENT_CHAT, response],
        "kwargs": {"reply_to": message_id},
        "workspace_owner": owner,
        "delivery_mode": "capture",
    }


def _agent() -> AIAgent:
    with (
        patch("run_agent.get_tool_definitions", return_value=_tool_defs()),
        patch("run_agent.check_toolset_requirements", return_value={}),
        patch("run_agent.OpenAI"),
    ):
        agent = AIAgent(
            api_key="test-key-1234567890",
            base_url="https://example.invalid/v1",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
        )
    agent.client = MagicMock()
    agent._cached_system_prompt = "You are Christopher."
    agent._use_prompt_caching = False
    agent.tool_delay = 0
    agent.compression_enabled = False
    agent.save_trajectories = False
    return agent


def _sandbox_result(path: str, size: int) -> str:
    return json.dumps(
        {
            "status": "success",
            "files": [
                {
                    "path": path,
                    "bytes": size,
                    "client_url": "https://example.invalid/artifact",
                }
            ],
            "run_id": "r_current",
            "exit_code": 0,
        }
    )


def test_stale_media_reference_is_corrected_then_two_exact_files_reach_sender(
    tmp_path: Path,
    monkeypatch,
) -> None:
    owner = "stable-management-session"
    work = tmp_path / "sandbox_workspaces" / _workspace_key(owner) / "work"
    work.mkdir(parents=True)
    first = work / "current.xlsx"
    first_bytes = b"PK\x03\x04first-edited-workbook"
    first.write_bytes(first_bytes)
    second = work / "second.xlsx"
    second_bytes = b"PK\x03\x04second-workbook"
    second.write_bytes(second_bytes)
    monkeypatch.setattr(
        "tools.python_sandbox_tool.get_hermes_home",
        lambda: tmp_path,
    )

    config = _config(tmp_path)
    agent = _agent()
    agent.session_log_file = tmp_path / "session.json"
    agent.final_response_validator = build_management_attachment_response_validator(
        config_path=config,
        workspace_owner=owner,
    )
    agent.final_response_validation_max_retries = 1
    agent.final_response_validation_failure = "Attachment not delivered."

    old_history = [
        {"role": "user", "content": "Send the earlier workbook."},
        {
            "role": "tool",
            "tool_call_id": "old-call",
            "content": json.dumps(
                {
                    "status": "success",
                    "files": [
                        {
                            "path": "work/old.xlsx",
                            "media_ref": "/media/tgg/hermes/old__r_old.xlsx",
                        }
                    ],
                }
            ),
        },
        {
            "role": "assistant",
            "content": "MEDIA:/media/tgg/hermes/old__r_old.xlsx",
        },
    ]
    stale = "/media/tgg/hermes/current__r_current.xlsx"
    agent.client.chat.completions.create.side_effect = [
        _response("", tool_calls=[_tool_call("current-call")], finish_reason="tool_calls"),
        _response(f"Here is the workbook.\n\nMEDIA:{stale}"),
        _response("Here is the workbook.\n\nMEDIA:work/current.xlsx"),
    ]
    with (
        patch(
            "run_agent.handle_function_call",
            return_value=_sandbox_result("work/current.xlsx", len(first_bytes)),
        ),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        first_result = agent.run_conversation(
            "Create and send the current workbook.",
            conversation_history=old_history,
        )

    assert first_result["final_response"].endswith("MEDIA:work/current.xlsx")
    validation = first_result["final_response_validation"]
    assert validation["rejections"] == 1
    assert validation["exhausted"] is False
    assert "Runtime attachment validation" in validation["observations"][0]
    assert "`work/current.xlsx`" in validation["observations"][0]
    assert stale not in first_result["final_response"]
    assert not any(
        message.get("_final_validation_synthetic")
        for message in first_result["messages"]
    )
    persisted = json.loads(agent.session_log_file.read_text(encoding="utf-8"))
    assert persisted["messages"][-1]["content"].endswith(
        "MEDIA:work/current.xlsx"
    )
    assert not any(
        message.get("_final_validation_synthetic")
        for message in persisted["messages"]
    )
    correction_request = agent.client.chat.completions.create.call_args_list[2].kwargs[
        "messages"
    ]
    assert any(
        message.get("role") == "user"
        and "Runtime attachment validation" in str(message.get("content"))
        and "work/current.xlsx" in str(message.get("content"))
        for message in correction_request
    )
    turn_record = build_turn_record(
        session_id="private-session",
        agent_id="christopher",
        chat_id=MANAGEMENT_CHAT,
        agent_result=first_result,
        final_response=first_result["final_response"],
        started_at=1.0,
        completed_at=2.0,
    )
    recorded_validation = turn_record.raw_turn_envelope[
        "final_response_validation"
    ]
    assert recorded_validation["rejections"] == 1
    assert "work/current.xlsx" in recorded_validation["observations"][0]

    inbox = DurableInbox(tmp_path / "inbox.db")
    uploaded: list[bytes] = []

    class _Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return b'{"success":true,"messageId":"WA-DOC","outcome":"delivered"}'

    def fake_urlopen(request, timeout=0):
        payload = json.loads(request.data)
        assert request.full_url.endswith("/send-media")
        uploaded.append(Path(payload["filePath"]).read_bytes())
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    first_delivery = deliver_management_replies(
        inbox,
        config_path=config,
        captured_outbound=[_captured(first_result["final_response"], owner, "MSG-1")],
        batch_records=[_record("MSG-1")],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=[{"message_ids": ["MSG-1"], "turn_id": "turn-1"}],
    )
    assert first_delivery["delivered"] == 1
    assert uploaded == [first_bytes]

    agent.client.chat.completions.create.side_effect = [
        _response("", tool_calls=[_tool_call("second-call")], finish_reason="tool_calls"),
        _response("Here is the next workbook.\n\nMEDIA:work/second.xlsx"),
    ]
    with (
        patch(
            "run_agent.handle_function_call",
            return_value=_sandbox_result("work/second.xlsx", len(second_bytes)),
        ),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
        patch.object(agent, "_save_session_log"),
    ):
        second_result = agent.run_conversation(
            "Create and send the second workbook.",
            conversation_history=first_result["messages"],
        )
    assert second_result["final_response"].endswith("MEDIA:work/second.xlsx")
    assert second_result["final_response_validation"] is None

    second_delivery = deliver_management_replies(
        inbox,
        config_path=config,
        captured_outbound=[_captured(second_result["final_response"], owner, "MSG-2")],
        batch_records=[_record("MSG-2")],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=[{"message_ids": ["MSG-2"], "turn_id": "turn-2"}],
    )
    assert second_delivery["delivered"] == 1
    assert uploaded == [first_bytes, second_bytes]


def test_repeated_invalid_attachment_exhausts_to_visible_text_without_media(
    tmp_path: Path,
    monkeypatch,
) -> None:
    owner = "stable-management-session"
    work = tmp_path / "sandbox_workspaces" / _workspace_key(owner) / "work"
    work.mkdir(parents=True)
    report = work / "report.xlsx"
    report.write_bytes(b"PK\x03\x04valid-workbook")
    monkeypatch.setattr(
        "tools.python_sandbox_tool.get_hermes_home",
        lambda: tmp_path,
    )
    config = _config(tmp_path)
    agent = _agent()
    agent.final_response_validator = build_management_attachment_response_validator(
        config_path=config,
        workspace_owner=owner,
    )
    agent.final_response_validation_max_retries = 1
    agent.final_response_validation_failure = (
        "I couldn't send the attachment because its file reference was invalid. "
        "Please ask me to try again."
    )
    invalid = "MEDIA:/media/tgg/hermes/report__r_missing.xlsx"
    agent.client.chat.completions.create.side_effect = [
        _response("", tool_calls=[_tool_call("report-call")], finish_reason="tool_calls"),
        _response(invalid),
        _response(invalid),
    ]
    with (
        patch(
            "run_agent.handle_function_call",
            return_value=_sandbox_result("work/report.xlsx", report.stat().st_size),
        ),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
        patch.object(agent, "_save_session_log"),
    ):
        result = agent.run_conversation("Send the workbook.")

    assert result["final_response"].startswith("I couldn't send the attachment")
    assert "MEDIA:" not in result["final_response"]
    assert result["turn_exit_reason"] == "final_response_validation_exhausted"
    assert result["final_response_validation"]["rejections"] == 2
    assert result["final_response_validation"]["exhausted"] is True
    assert not any(
        message.get("_final_validation_synthetic")
        for message in result["messages"]
    )

    requests: list[str] = []

    class _Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return b'{"success":true,"messageId":"WA-TEXT"}'

    def fake_urlopen(request, timeout=0):
        requests.append(request.full_url)
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    summary = deliver_management_replies(
        DurableInbox(tmp_path / "exhausted-inbox.db"),
        config_path=config,
        captured_outbound=[_captured(result["final_response"], owner, "MSG-FAIL")],
        batch_records=[_record("MSG-FAIL")],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=[{"message_ids": ["MSG-FAIL"], "turn_id": "turn-fail"}],
    )
    assert summary["delivered"] == 1
    assert [url.rsplit("/", 1)[-1] for url in requests] == ["send"]


@pytest.mark.parametrize("tool_call_first", [False, True])
def test_invalid_attachment_at_iteration_limit_uses_visible_failure(
    tmp_path: Path,
    monkeypatch,
    tool_call_first: bool,
) -> None:
    owner = "stable-management-session"
    work = tmp_path / "sandbox_workspaces" / _workspace_key(owner) / "work"
    work.mkdir(parents=True)
    report = work / "report.xlsx"
    report.write_bytes(b"PK\x03\x04valid-workbook")
    monkeypatch.setattr(
        "tools.python_sandbox_tool.get_hermes_home",
        lambda: tmp_path,
    )
    config = _config(tmp_path)
    agent = _agent()
    agent.max_iterations = 2 if tool_call_first else 1
    agent.final_response_validator = build_management_attachment_response_validator(
        config_path=config,
        workspace_owner=owner,
    )
    agent.final_response_validation_max_retries = 1
    agent.final_response_validation_failure = "Attachment not delivered."
    invalid = "MEDIA:/media/tgg/hermes/report__r_missing.xlsx"
    responses = [_response(invalid)]
    if tool_call_first:
        responses.insert(
            0,
            _response(
                "",
                tool_calls=[_tool_call("report-call")],
                finish_reason="tool_calls",
            ),
        )
    agent.client.chat.completions.create.side_effect = responses
    history = [] if tool_call_first else [
        {
            "role": "tool",
            "tool_call_id": "prepared-result",
            "content": _sandbox_result("work/report.xlsx", report.stat().st_size),
        }
    ]
    with (
        patch(
            "run_agent.handle_function_call",
            return_value=_sandbox_result("work/report.xlsx", report.stat().st_size),
        ),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
        patch.object(agent, "_save_session_log"),
    ):
        result = agent.run_conversation(
            "Send the workbook.",
            conversation_history=history,
        )

    assert result["final_response"] == "Attachment not delivered."
    assert "MEDIA:" not in result["final_response"]
    assert result["turn_exit_reason"] == "final_response_validation_exhausted"
    assert result["final_response_validation"] == {
        "rejections": 1,
        "exhausted": True,
        "observations": result["final_response_validation"]["observations"],
    }
    assert agent.client.chat.completions.create.call_count == agent.max_iterations
    assert not any(
        message.get("_final_validation_synthetic")
        for message in result["messages"]
    )


def test_normal_text_response_does_not_enter_attachment_retry(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    agent = _agent()
    agent.final_response_validator = build_management_attachment_response_validator(
        config_path=config,
        workspace_owner="stable-management-session",
    )
    agent.final_response_validation_max_retries = 1
    agent.client.chat.completions.create.return_value = _response(
        "The workbook review is still in progress."
    )
    with (
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
        patch.object(agent, "_save_session_log"),
    ):
        result = agent.run_conversation("Status?")

    assert result["final_response"] == "The workbook review is still in progress."
    assert result["final_response_validation"] is None
    assert agent.client.chat.completions.create.call_count == 1


class _GatewayWiringAgent:
    instances: list["_GatewayWiringAgent"] = []

    def __init__(self, *args, **kwargs):
        self.tools = []
        self.session_id = kwargs.get("session_id")
        self.model = kwargs.get("model", "test-model")
        self.provider = kwargs.get("provider", "test-provider")
        self.validation_states: list[bool] = []
        type(self).instances.append(self)

    @property
    def is_interrupted(self) -> bool:
        return False

    def run_conversation(self, message, conversation_history=None, task_id=None):
        self.validation_states.append(self.final_response_validator is not None)
        return {
            "final_response": "ordinary text",
            "messages": [{"role": "assistant", "content": "ordinary text"}],
            "api_calls": 1,
            "completed": True,
            "final_response_validation": None,
        }


@pytest.mark.asyncio
async def test_gateway_installs_management_guard_then_clears_it_on_cached_agent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "config.yaml").write_text("{}\n", encoding="utf-8")
    _GatewayWiringAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _GatewayWiringAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(
        gateway_run,
        "_resolve_gateway_model",
        lambda config=None: "test-model",
    )
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {"api_key": "fake", "provider": "test-provider"},
    )
    resolved_contexts = iter(
        [
            SimpleNamespace(job_type="tgg_management"),
            SimpleNamespace(job_type="ordinary"),
        ]
    )
    monkeypatch.setattr(
        gateway_run,
        "_resolve_pa_context",
        lambda *_args, **_kwargs: next(resolved_contexts),
    )
    monkeypatch.setattr(
        gateway_run,
        "_merge_pa_toolsets",
        lambda enabled, disabled, _context: (enabled, disabled),
    )
    monkeypatch.setattr(gateway_run, "_render_pa_ephemeral_prompt", lambda _context: "")
    monkeypatch.setattr(gateway_run, "_apply_pa_compression_policy", lambda *_args: None)
    monkeypatch.setattr(gateway_run, "_record_pa_behavior_event", lambda *_args, **_kwargs: None)

    import hermes_cli.tools_config as tools_config

    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: set())
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

    source = SessionSource(
        platform=Platform.WHATSAPP,
        chat_id=MANAGEMENT_CHAT,
        chat_type="group",
    )
    call = {
        "message": "hello",
        "context_prompt": "",
        "history": [],
        "source": source,
        "session_id": "session-1",
        "session_key": "stable-management-session",
        "suppress_delivery": True,
    }
    await runner._run_agent(**call)
    await runner._run_agent(**call)

    assert len(_GatewayWiringAgent.instances) == 1
    assert _GatewayWiringAgent.instances[0].validation_states == [True, False]
