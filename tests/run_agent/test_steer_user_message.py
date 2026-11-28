"""PA-611: a message that arrives mid-turn enters the conversation as a real user message.

Modelled on Codex CLI: the message is queued, appended as a ``role: user``
message at the next model-call boundary, and a turn that would end while one
is still pending makes one more model call in the same turn.
"""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from run_agent import AIAgent

ATTACHMENT_TEXT = (
    "here is the file\n"
    "[Attachment retained for analysis in python_sandbox: /inputs/media/sheet.xlsx]\n"
    "[Message id: MSG-STEER]"
)


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


def _response(content, *, tool_calls=None, finish_reason="stop") -> SimpleNamespace:
    message = SimpleNamespace(content=content, tool_calls=tool_calls)
    return SimpleNamespace(
        choices=[SimpleNamespace(message=message, finish_reason=finish_reason)],
        model="test/model",
        usage=None,
    )


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


def _run(agent: AIAgent, prompt: str, tool_side_effect=None):
    with (
        patch("run_agent.handle_function_call", side_effect=tool_side_effect or (lambda *a, **k: "ok")),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
        patch.object(agent, "_save_session_log"),
    ):
        return agent.run_conversation(prompt)


def _capture_calls(agent: AIAgent, responses, *, during_call=None):
    """Make the fake client return ``responses`` in order and record each request."""
    requests: list[list[dict]] = []
    queue = list(responses)

    def create(**kwargs):
        requests.append(copy.deepcopy(kwargs["messages"]))
        if during_call is not None:
            during_call(len(requests))
        return queue.pop(0)

    agent.client.chat.completions.create.side_effect = create
    return requests


def test_steer_during_tool_call_arrives_as_user_message_not_in_tool_result():
    agent = _agent()

    def tool(*_args, **_kwargs):
        # The gateway steers while the tool is running.
        assert agent.steer_user_message(ATTACHMENT_TEXT, source_message_ids=["MSG-STEER"])
        return "tool output"

    requests = _capture_calls(
        agent,
        [
            _response("", tool_calls=[_tool_call("c1")], finish_reason="tool_calls"),
            _response("answered both"),
        ],
    )
    result = _run(agent, "first request", tool_side_effect=tool)

    assert len(requests) == 2
    second = requests[1]
    assert second[-1]["role"] == "user"
    assert second[-1]["content"] == ATTACHMENT_TEXT
    tool_messages = [m for m in second if m["role"] == "tool"]
    assert tool_messages and all("User guidance" not in m["content"] for m in tool_messages)
    # Internal markers never reach the provider.
    assert "_steered_user_message" not in second[-1]
    assert "_source_message_ids" not in second[-1]
    assert result["final_response"] == "answered both"
    assert "pending_steer" not in result


def test_steer_pending_when_model_would_end_turn_triggers_one_more_call_in_same_turn():
    agent = _agent()

    def during_call(call_no):
        if call_no == 1:
            agent.steer_user_message("also send the totals", source_message_ids=["MSG-LATE"])

    requests = _capture_calls(
        agent,
        [_response("first answer"), _response("first answer plus totals")],
        during_call=during_call,
    )
    result = _run(agent, "first request")

    assert len(requests) == 2, "exactly one more call in the same turn"
    # The first answer is part of the conversation, then the steered user message.
    assert [m["role"] for m in requests[1][-2:]] == ["assistant", "user"]
    assert requests[1][-2]["content"] == "first answer"
    assert requests[1][-1]["content"] == "also send the totals"
    assert result["final_response"] == "first answer plus totals"
    assert result["api_calls"] == 2
    assert "pending_steer" not in result
    roles = [m["role"] for m in result["messages"]]
    assert roles[-4:] == ["user", "assistant", "user", "assistant"]


def test_no_extra_call_when_nothing_is_pending():
    agent = _agent()
    requests = _capture_calls(agent, [_response("only answer")])
    result = _run(agent, "first request")
    assert len(requests) == 1
    assert result["final_response"] == "only answer"


def test_attachment_information_and_source_ids_survive_into_the_conversation():
    agent = _agent()

    def during_call(call_no):
        if call_no == 1:
            agent.steer_user_message(ATTACHMENT_TEXT, source_message_ids=["MSG-STEER"])

    _capture_calls(agent, [_response("a"), _response("b")], during_call=during_call)
    result = _run(agent, "first request")

    steered = [m for m in result["messages"] if m.get("_steered_user_message")]
    assert len(steered) == 1
    assert "/inputs/media/sheet.xlsx" in steered[0]["content"]
    assert steered[0]["_source_message_ids"] == ["MSG-STEER"]


def test_follow_up_call_respects_the_iteration_budget():
    agent = _agent()
    agent.max_iterations = 1

    def during_call(call_no):
        agent.steer_user_message("too late for another call", source_message_ids=["MSG-X"])

    requests = _capture_calls(agent, [_response("only answer")], during_call=during_call)
    result = _run(agent, "first request")

    assert len(requests) == 1
    assert result["final_response"] == "only answer"
    # It was not lost: it goes back to the caller as the next turn.
    assert result["pending_steer"] == "too late for another call"


def test_empty_text_is_rejected():
    agent = _agent()
    assert agent.steer_user_message("   ") is False
    assert agent._has_pending_user_messages() is False
