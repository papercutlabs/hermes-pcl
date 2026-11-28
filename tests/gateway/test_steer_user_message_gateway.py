"""PA-611: gateway side of steering a mid-turn message in as a user message."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType, SessionSource
from gateway.session_context import (
    clear_session_vars,
    get_session_env,
    pop_steered_source_refs,
    set_session_vars,
)


def _runner():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._running_agents = {}
    runner._running_agents_ts = {}
    return runner


def _event(text="here is the file", message_id="MSG-STEER", media=None):
    source = SessionSource(
        platform=Platform.WHATSAPP, chat_id="chat@g.us", chat_type="group", user_id="u1"
    )
    return MessageEvent(
        text=text,
        message_type=MessageType.DOCUMENT if media else MessageType.TEXT,
        source=source,
        message_id=message_id,
        media_urls=list(media or []),
        media_types=["application/vnd.ms-excel"] if media else [],
    )


@pytest.fixture(autouse=True)
def _clean():
    pop_steered_source_refs("sk")
    yield
    pop_steered_source_refs("sk")
    clear_session_vars([])


def test_steer_hands_the_agent_text_attachment_line_and_source_ids():
    runner = _runner()
    agent = MagicMock()
    agent.steer_user_message.return_value = True
    event = _event(media=["/inputs/media/sheet.xlsx"])

    assert runner._steer_event_into_running_agent(agent, event, "sk") is True

    (text,), kwargs = agent.steer_user_message.call_args
    assert "here is the file" in text
    assert "/inputs/media/sheet.xlsx" in text  # attachment information intact
    assert "MSG-STEER" in text
    assert kwargs == {"source_message_ids": ["MSG-STEER"]}


def test_attachment_only_message_is_steered_with_a_placeholder():
    runner = _runner()
    agent = MagicMock()
    agent.steer_user_message.return_value = True
    event = _event(text="", media=["/inputs/media/sheet.xlsx"])
    assert runner._steer_event_into_running_agent(agent, event, "sk") is True
    (text,), _ = agent.steer_user_message.call_args
    assert "/inputs/media/sheet.xlsx" in text


def test_agent_that_rejects_or_cannot_steer_returns_false():
    runner = _runner()
    rejecting = MagicMock()
    rejecting.steer_user_message.return_value = False
    assert runner._steer_event_into_running_agent(rejecting, _event(), "sk") is False
    assert runner._steer_event_into_running_agent(object(), _event(), "sk") is False
    assert pop_steered_source_refs("sk") == []


def test_steered_attachment_is_resolvable_where_current_message_refs_are_read():
    """The tools that find the current attachment read this env value."""
    runner = _runner()
    agent = MagicMock()
    agent.steer_user_message.return_value = True
    set_session_vars(
        chat_id="chat@g.us",
        session_key="sk",
        source_message_refs=json.dumps(["MSG-OWN"]),
    )
    assert json.loads(get_session_env("HERMES_SESSION_SOURCE_MESSAGE_REFS")) == ["MSG-OWN"]

    assert runner._steer_event_into_running_agent(agent, _event(media=["/inputs/media/a.xlsx"]), "sk")

    assert json.loads(get_session_env("HERMES_SESSION_SOURCE_MESSAGE_REFS")) == [
        "MSG-OWN",
        "MSG-STEER",
    ]
    # Another session never sees it.
    set_session_vars(session_key="other", source_message_refs=json.dumps(["MSG-OTHER"]))
    assert json.loads(get_session_env("HERMES_SESSION_SOURCE_MESSAGE_REFS")) == ["MSG-OTHER"]


def test_steer_is_recorded_on_the_replay_context_for_the_intake():
    from gateway.replay import ReplayPlan, replay_context

    runner = _runner()
    agent = MagicMock()
    agent.steer_user_message.return_value = True
    plan = ReplayPlan(platform="whatsapp", messages=(), run_id="r1", attempt_id="a1")
    with replay_context(plan) as ctx:
        assert runner._steer_event_into_running_agent(agent, _event(), "sk")
        assert ctx.steered_message_ids == ["MSG-STEER"]


def test_turn_record_lists_the_steered_message_with_the_running_turns_own():
    runner = _runner()
    captured = {}

    async def fake_record(**kwargs):
        captured.update(kwargs)

    runner._record_pa_turn_safe = fake_record
    runner.session_store = MagicMock()
    runner.session_store.get_or_create_session.return_value = None
    own = _event(message_id="MSG-OWN")
    steer_agent = MagicMock()
    steer_agent.steer_user_message.return_value = True
    runner._steer_event_into_running_agent(steer_agent, _event(), "sk")

    asyncio.run(
        runner._record_pa_turn_boundary(
            source=own.source,
            quick_key="sk",
            run_generation=1,
            event=own,
            agent_result={"final_response": "done", "messages": [{"role": "user", "content": "x"}]},
            started_at=0.0,
        )
    )

    assert captured["agent_result"]["turn_source_message_ids"] == ["MSG-OWN", "MSG-STEER"]
    assert pop_steered_source_refs("sk") == []  # consumed by the boundary
