"""PA-203: a nightly analyzer's assignment lives in its session instructions.

On 1 Oct the AMK analyzer's preserve-recent compaction kept only the last 20
messages, all tool results of a 26-call parallel turn whose calling message
had been cut.  The sanitizer removed them as orphans and the successor held
one message (``messages=110->1``); the trigger carrying the batch_id was gone.
The assignment is now carried in the agent's ephemeral system prompt, which
is appended to the system instructions on every provider call and never
passes through the compressor.
"""

import json
import sys
import threading
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import gateway.run as gateway_run
from gateway.config import Platform
from gateway.session import SessionSource

_BATCH_ID = "nightly:2026-10-01:84441a1b0296"
_ROLE_CHATS = {
    "amk": "120363421424519051@g.us",
    "hg": "120363422582425366@g.us",
    "pg": "120363423568509280@g.us",
    "sk": "120363403845802098@g.us",
    "rental": "120363421153247095@g.us",
    "backend": "120363404682000990@g.us",
}
_CONTEXT_PROMPT = "Session context: synthetic nightly chat."


def _nightly_trigger(chat_id: str) -> str:
    # Body as run-nightly-whatsapp.py event_for() writes it, with the sender
    # prefix the replay adapter adds.
    return (
        f"[System] Nightly WhatsApp analyzer. batch_id={_BATCH_ID}. authoritative_chat_id={chat_id}. "
        "Load the required WhatsApp skill, process only that frozen chat into its singular ledger, "
        "and seal the immutable chat receipt."
    )


def _nightly_retry(chat_id: str, attempt_id: str) -> str:
    return (
        f"[System] Nightly WhatsApp analyzer retry. batch_id={_BATCH_ID}. "
        f"authoritative_chat_id={chat_id}. attempt_id={attempt_id}. "
        "Resume only this frozen chat, retain its durable work, and seal its immutable chat receipt."
    )


def _continuous_interval(chat_id: str) -> str:
    return (
        f"[System] Continuous WhatsApp interval. batch_id={_BATCH_ID}. authoritative_chat_id={chat_id}. "
        "Load the required WhatsApp skill, process only that frozen chat into its singular ledger, "
        "and seal the immutable chat receipt."
    )


def _continuous_recovery(chat_id: str) -> str:
    return (
        f"Continuous WhatsApp interval. batch_id={_BATCH_ID}. authoritative_chat_id={chat_id}. "
        "This is a recovery of the same frozen interval: do not re-analyse."
    )


class _FakeAgent:
    instances = []

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        self.ephemeral_system_prompt = kwargs.get("ephemeral_system_prompt")
        self.tools = []
        self.calls = []
        type(self).instances.append(self)

    def run_conversation(self, message, conversation_history=None, task_id=None):
        self.calls.append({"message": message, "ephemeral": self.ephemeral_system_prompt})
        return {
            "final_response": f"turn {len(self.calls)}",
            "messages": [{"role": "assistant", "content": f"turn {len(self.calls)}"}],
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


def _install_fakes(monkeypatch, tmp_path, chat_id, *, seal_after_status_calls):
    """Fake agent plus a batch-status tool that seals after N reads."""
    _FakeAgent.instances = []
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _FakeAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-6.1-sol")
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "fake"})
    import hermes_cli.tools_config as tools_config

    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda *_args: {"core"})
    status_calls = []

    def status_handler(_args):
        status_calls.append(1)
        index = len(status_calls)
        return {
            "ok": True,
            "completed_chat_ids": [chat_id] if index >= seal_after_status_calls else [],
            "chat_progress": {
                chat_id: {
                    "sha256": f"progress-{index}",
                    "first_incomplete_cursor": 100 + 25 * index,
                }
            },
        }

    import tools.registry as tool_registry

    monkeypatch.setattr(
        tool_registry.registry,
        "get_entry",
        lambda name: SimpleNamespace(handler=status_handler)
        if name == "tgg_nightly_get_batch_status"
        else None,
    )
    return status_calls


async def _run(runner, message, *, pa_job_type, pa_context, chat_id="900000000000000001@g.us"):
    return await runner._run_agent(
        message=message,
        context_prompt=_CONTEXT_PROMPT,
        history=[],
        source=SessionSource(platform=Platform.WHATSAPP, chat_id=chat_id, chat_type="group"),
        session_id="nightly-session",
        session_key="nightly-session-key",
        pa_job_type=pa_job_type,
        pa_context=pa_context,
        suppress_delivery=True,
    )


def _assert_assignment(text: str, *, role: str, chat_id: str) -> None:
    assert "[PA NIGHTLY ANALYZER ASSIGNMENT]" in text
    assert f"batch_id: {_BATCH_ID}" in text
    assert f"authoritative_chat_id: {chat_id}" in text
    assert f"role: {role}" in text
    assert f"frozen evidence: the frozen chat of batch_id={_BATCH_ID} for authoritative_chat_id={chat_id}" in text


# 1 and 3: present in the instructions of every analyzer role, and still there
# for every same-session continuation the runner issues.
@pytest.mark.asyncio
@pytest.mark.parametrize("role", sorted(_ROLE_CHATS))
async def test_every_analyzer_role_carries_its_assignment_through_its_continuations(
    monkeypatch, tmp_path, role,
):
    chat_id = _ROLE_CHATS[role]
    status_calls = _install_fakes(monkeypatch, tmp_path, chat_id, seal_after_status_calls=4)
    trigger = _nightly_trigger(chat_id)

    await _run(
        _runner(), trigger,
        pa_job_type="tgg_nightly_whatsapp",
        pa_context={
            "job_type": "tgg_nightly_whatsapp",
            "nightly_batch_id": _BATCH_ID,
            "nightly_role": role,
            "authoritative_chat_id": chat_id,
        },
    )

    assert len(_FakeAgent.instances) == 1
    agent = _FakeAgent.instances[0]
    ephemeral = agent.kwargs["ephemeral_system_prompt"]
    _assert_assignment(ephemeral, role=role, chat_id=chat_id)
    assert f"original trigger (verbatim): {trigger}" in ephemeral
    assert ephemeral.startswith(_CONTEXT_PROMPT + "\n\n[PA NIGHTLY ANALYZER ASSIGNMENT]\n")
    # The first run plus two continuations, all on the one agent and so all
    # under the same session instructions.
    assert len(agent.calls) == 3
    assert agent.calls[1]["message"].startswith("Nightly WhatsApp analyzer continuation.")
    assert all(call["ephemeral"] == ephemeral for call in agent.calls)
    assert len(status_calls) == 4


# 3: a retry is a new event and a new agent; it carries the same assignment
# plus the attempt id its trigger names.
@pytest.mark.asyncio
async def test_retry_carries_the_assignment_and_its_attempt(monkeypatch, tmp_path):
    chat_id = _ROLE_CHATS["amk"]
    _install_fakes(monkeypatch, tmp_path, chat_id, seal_after_status_calls=3)
    retry = _nightly_retry(chat_id, "amk-ctx-lost-20261002a")

    await _run(
        _runner(), retry,
        pa_job_type="tgg_nightly_whatsapp",
        pa_context={
            "job_type": "tgg_nightly_whatsapp",
            "nightly_batch_id": _BATCH_ID,
            "nightly_role": "amk",
            "authoritative_chat_id": chat_id,
        },
    )

    agent = _FakeAgent.instances[0]
    ephemeral = agent.kwargs["ephemeral_system_prompt"]
    _assert_assignment(ephemeral, role="amk", chat_id=chat_id)
    assert "attempt_id=amk-ctx-lost-20261002a." in ephemeral
    assert len(agent.calls) == 2
    assert all(call["ephemeral"] == ephemeral for call in agent.calls)


def test_trigger_naming_another_batch_is_not_copied_verbatim():
    chat_id = _ROLE_CHATS["pg"]
    block = gateway_run._tgg_nightly_assignment_prompt(
        "tgg_nightly_whatsapp",
        {"nightly_batch_id": _BATCH_ID, "nightly_role": "pg", "authoritative_chat_id": chat_id},
        _nightly_trigger(chat_id).replace(_BATCH_ID, "nightly:2026-09-30:000000000000"),
    )
    _assert_assignment(block, role="pg", chat_id=chat_id)
    assert "original trigger (verbatim)" not in block
    assert "nightly:2026-09-30" not in block


# 4: Management, the continuous lane (interval and recovery) and the
# consolidator get exactly the instructions they had before.
_UNCHANGED_LANES = [
    pytest.param(
        "Krisleen: please check the AMK list.", "tgg_management", None,
        "120363000000000000@g.us", id="management",
    ),
    pytest.param(
        _continuous_interval(_ROLE_CHATS["amk"]), "tgg_nightly_whatsapp",
        {
            "job_type": "tgg_nightly_whatsapp", "nightly_batch_id": _BATCH_ID,
            "nightly_role": "amk", "authoritative_chat_id": _ROLE_CHATS["amk"],
            "continuous_interval": True,
            "continuous_contract": "tgg-christopher-continuous-interval/v1",
        },
        "900000000000000001@g.us", id="continuous-interval",
    ),
    pytest.param(
        _continuous_recovery(_ROLE_CHATS["sk"]), "tgg_nightly_whatsapp",
        {
            "job_type": "tgg_nightly_whatsapp", "nightly_batch_id": _BATCH_ID,
            "nightly_role": "sk", "authoritative_chat_id": _ROLE_CHATS["sk"],
        },
        "900000000000000004@g.us", id="continuous-recovery",
    ),
    pytest.param(
        f"[System] Nightly WhatsApp consolidation. batch_id={_BATCH_ID}. "
        "Wait for and read all six sealed singular ledgers, reconcile only duplicate exact jobs, and seal the batch.",
        "tgg_nightly_whatsapp",
        {
            "job_type": "tgg_nightly_whatsapp", "nightly_batch_id": _BATCH_ID,
            "nightly_role": "consolidator", "authoritative_chat_id": None,
        },
        "900000000000000007@g.us", id="consolidator",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("message", "pa_job_type", "pa_context", "source_chat"), _UNCHANGED_LANES)
async def test_other_lanes_keep_byte_identical_instructions(
    monkeypatch, tmp_path, message, pa_job_type, pa_context, source_chat,
):
    authoritative = (pa_context or {}).get("authoritative_chat_id") or "none@g.us"

    async def ephemeral_for() -> list[str]:
        _install_fakes(monkeypatch, tmp_path, authoritative, seal_after_status_calls=1)
        await _run(
            _runner(), message, pa_job_type=pa_job_type, pa_context=pa_context, chat_id=source_chat,
        )
        return [agent.kwargs["ephemeral_system_prompt"] for agent in _FakeAgent.instances]

    with monkeypatch.context() as patched:
        # The af61c302e assembly: no assignment block at all.
        patched.setattr(gateway_run, "_tgg_nightly_assignment_prompt", lambda *_a: "")
        before = await ephemeral_for()
    after = await ephemeral_for()

    assert after == before
    assert after == [_CONTEXT_PROMPT]
    assert gateway_run._tgg_nightly_assignment_prompt(pa_job_type, pa_context, message) == ""


# 2: the 1 Oct shape, on the real agent loop and the real PA compaction policy.
def _tool_defs() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": name,
                "parameters": {"type": "object", "properties": {}},
            },
        }
        for name in ("tgg_whatsapp_case_context", "tgg_whatsapp_message_media")
    ]


def _response(content, *, tool_calls=None, finish_reason="stop"):
    message = SimpleNamespace(content=content, tool_calls=tool_calls)
    return SimpleNamespace(
        choices=[SimpleNamespace(message=message, finish_reason=finish_reason)],
        model="gpt-6.1-sol",
        usage=None,
    )


def _wide_parallel_turn() -> list[SimpleNamespace]:
    # 5 case lookups and 21 media fetches in one assistant turn, as at 16:31 UTC.
    names = ["tgg_whatsapp_case_context"] * 5 + ["tgg_whatsapp_message_media"] * 21
    return [
        SimpleNamespace(
            id=f"call_{index:02d}",
            type="function",
            function=SimpleNamespace(name=name, arguments=json.dumps({"item": index})),
        )
        for index, name in enumerate(names)
    ]


def test_assignment_survives_compaction_that_keeps_only_orphan_tool_results(tmp_path):
    from run_agent import AIAgent
    from agent.transports import get_transport

    chat_id = _ROLE_CHATS["amk"]
    trigger = _nightly_trigger(chat_id)
    assignment = gateway_run._tgg_nightly_assignment_prompt(
        "tgg_nightly_whatsapp",
        {"nightly_batch_id": _BATCH_ID, "nightly_role": "amk", "authoritative_chat_id": chat_id},
        trigger,
    )
    assert assignment

    with (
        patch("run_agent.get_tool_definitions", return_value=_tool_defs()),
        patch("run_agent.check_toolset_requirements", return_value={}),
        patch("run_agent.OpenAI"),
    ):
        agent = AIAgent(
            api_key="test-key-1234567890",
            base_url="https://example.invalid/v1",
            model="gpt-6.1-sol",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
            ephemeral_system_prompt=assignment,
        )
    agent.client = MagicMock()
    agent._cached_system_prompt = "You are Christopher."
    agent._use_prompt_caching = False
    agent.tool_delay = 0
    agent.save_trajectories = False
    agent.session_log_file = tmp_path / "session.json"
    agent.compression_enabled = True
    compressor = agent.context_compressor
    compressor.set_pa_compression_policy({"strategy": "preserve-recent", "window_size": 20})
    compressor.threshold_tokens = 10**9

    def tool_result(*_args, **_kwargs):
        # Cross the compaction threshold as the wide turn's results land.
        compressor.threshold_tokens = 1
        return json.dumps({"ok": True})

    agent.client.chat.completions.create.side_effect = [
        _response("", tool_calls=_wide_parallel_turn(), finish_reason="tool_calls"),
        _response("Blocked: the batch is not in view."),
        _response("Continuing the frozen AMK chat."),
    ]
    with (
        patch("run_agent.handle_function_call", side_effect=tool_result),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_session_log"),
    ):
        agent.run_conversation(trigger, conversation_history=[], task_id="nightly-session")
        # A same-session continuation: the runner's own re-prompt, empty history.
        agent.run_conversation(
            f"Nightly WhatsApp analyzer continuation. batch_id={_BATCH_ID}. "
            f"authoritative_chat_id={chat_id}. ",
            conversation_history=[],
            task_id="nightly-session",
        )

    assert compressor.compression_count == 1
    requests = [call.kwargs["messages"] for call in agent.client.chat.completions.create.call_args_list]
    assert len(requests) == 3

    first, after_compaction, continuation = requests
    assert first[1] == {"role": "user", "content": trigger}

    # The 1 Oct shape: the trigger and the calling assistant turn were cut,
    # the 20 kept tool results were orphans and were removed; only the
    # compaction marker is left of the conversation.
    conversation = after_compaction[1:]
    assert len(conversation) == 1
    assert conversation[0]["content"].startswith("[PA CONTEXT COMPACTION] preserve-recent")
    assert _BATCH_ID not in json.dumps(conversation)

    for request in requests:
        assert request[0]["role"] == "system"
        assert request[0]["content"].endswith(assignment)
        _assert_assignment(request[0]["content"], role="amk", chat_id=chat_id)

    # On Christopher's provider the system message becomes the Responses
    # API "instructions" field.
    codex = get_transport("codex_responses").build_kwargs(
        model="gpt-6.1-sol", messages=after_compaction, tools=None,
    )
    assert codex["instructions"].endswith(assignment)
    assert _BATCH_ID not in json.dumps(codex["input"])
    assert continuation[0]["content"] == after_compaction[0]["content"]
