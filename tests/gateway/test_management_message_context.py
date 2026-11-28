from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
import yaml

from gateway import durable_jsonl_consumer as consumer
from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.management_message_context import (
    contextual_replay_message,
    previous_skipped_context,
)
from gateway.platforms.whatsapp import WhatsAppAdapter
from gateway.replay import ReplayPlan
from gateway.run import GatewayRunner


MANAGEMENT_CHAT = "management@g.us"
OTHER_CHAT = "other@g.us"
PASSIVE_ID = "3EB00A2B968D43E2A6E4E2"
TRIGGER_ID = "3EB06C800D2B3A6D345280"
BOT_ID = "139973755973678@lid"


def _message(message_id: str, chat_id: str, timestamp: int, body: str) -> dict:
    return {
        "messageId": message_id,
        "chatId": chat_id,
        "chatName": "Management fixture",
        "senderId": "fixture-user",
        "senderName": "Fixture User",
        "isGroup": True,
        "body": body,
        "hasMedia": False,
        "mediaType": "",
        "mediaUrls": [],
        "timestamp": timestamp,
        "fromMe": False,
        "botIds": [BOT_ID],
        "mentionedIds": [],
    }


def _trigger(message_id: str, timestamp: int) -> dict:
    value = _message(
        message_id,
        MANAGEMENT_CHAT,
        timestamp,
        f"@{BOT_ID.split('@', 1)[0]} can u see the above",
    )
    value["mentionedIds"] = [BOT_ID]
    return value


def _write_jsonl(path: Path, values: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(value) + "\n" for value in values),
        encoding="utf-8",
    )


def _config(tmp_path: Path) -> Path:
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(
        yaml.safe_dump(
            {
                "selectors": [
                    {
                        "job_type": "tgg_management",
                        "match": {
                            "source.platform": "whatsapp",
                            "source.chat_id": MANAGEMENT_CHAT,
                        },
                    }
                ],
                # The observed messages are 80 seconds apart.  The context
                # repair must not misuse this eight-second reply debounce as
                # a context-lookback limit.
                "job_briefs": {
                    "tgg_management": {
                        "debounce_passive_ms": 8000,
                        "debounce_addressed_ms": 1500,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "model": {"provider": "fixture", "default": "fixture"},
                "pa": {"enabled": True, "constitution_path": str(constitution)},
            }
        ),
        encoding="utf-8",
    )
    return config


def _case_db(path: Path) -> Path:
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE bridge_message_log("
            "local_id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT NOT NULL,"
            "source_ref TEXT NOT NULL UNIQUE,chat_jid TEXT NOT NULL,chat_name TEXT,"
            "zone TEXT,channel_type TEXT,sender_id TEXT,from_me INTEGER,ts INTEGER,"
            "sgt TEXT,text TEXT,message_kind TEXT,has_media INTEGER,media_refs TEXT,"
            "quoted_text TEXT,reply_to_source_ref TEXT,raw_json TEXT)"
        )
    return path


@pytest.mark.asyncio
async def test_skipped_pg_attachment_reaches_later_trigger_once_without_auto_reply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    image = tmp_path / "PG-JOB-2604-0842.jpg"
    image.write_bytes(b"fixture-image")
    passive = _message(
        PASSIVE_ID,
        MANAGEMENT_CHAT,
        1789139627,
        "PG/JOB/2604/0842 screenshot question",
    )
    passive.update(
        {
            "hasMedia": True,
            "mediaType": "image/jpeg",
            "mediaUrls": [str(image)],
            "mediaMimes": ["image/jpeg"],
        }
    )
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [passive])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    inbox.stage_from_source(source, cursor)
    with inbox.connect() as conn:
        conn.execute(
            "UPDATE ingress_events SET retention_state='complete' WHERE message_id=?",
            (PASSIVE_ID,),
        )
    config = _config(tmp_path)
    state_db = tmp_path / "state.db"

    async def forbidden_model(*_args, **_kwargs):
        raise AssertionError("untagged attachment opened a model turn")

    monkeypatch.setattr(consumer, "process_live_records", forbidden_model)
    await consumer._process_claimed_chat_batch_unlocked(
        inbox,
        inbox.pending(limit=1),
        config_path=config,
        state_db=state_db,
        case_db=_case_db(tmp_path / "case.db"),
        source_before_image_dir=tmp_path / "before-images",
        gate_changed_at="2030-01-01T00:00:00+00:00",
        runner=object(),
        direct_trigger_required=True,
    )
    assert inbox.counts() == {"skipped": 1}

    with source.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_trigger(TRIGGER_ID, 1789139707)) + "\n")
    inbox.stage_from_source(source, cursor)
    projected: list[list[str]] = []
    replay_inputs: list[tuple[dict, ...] | None] = []

    def capture_projection(_case_db, records, **_kwargs):
        projected.append([record.message_id for record in records])
        return {}

    async def capture_model(records, **kwargs):
        replay_messages = kwargs.get("replay_messages")
        replay_inputs.append(replay_messages)
        record_ids = [record.message_id for record in records]
        if record_ids == [TRIGGER_ID]:
            assert replay_messages is not None and len(replay_messages) == 1
            assert replay_messages[0]["sourceMessageIds"] == [PASSIVE_ID, TRIGGER_ID]
            assert replay_messages[0]["timestamp"] == 1789139707
            assert replay_messages[0]["mediaUrls"] == [str(image)]
            handled_ids = [PASSIVE_ID, TRIGGER_ID]
            turn_id = "turn-context"
        else:
            assert record_ids == ["later-trigger"]
            assert replay_messages is None
            handled_ids = record_ids
            turn_id = "turn-later"
        return {
            "submitted_message_ids": record_ids,
            "handled": [
                {
                    "message_ids": handled_ids,
                    "turn_id": turn_id,
                }
            ],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "_inject_bounded_source_evidence", capture_projection)
    monkeypatch.setattr(consumer, "process_live_records", capture_model)
    monkeypatch.setattr(
        consumer,
        "deliver_management_replies",
        lambda *_args, **_kwargs: {"delivered": 0, "undelivered": 0},
    )
    await consumer._process_claimed_chat_batch_unlocked(
        inbox,
        inbox.pending(limit=1),
        config_path=config,
        state_db=state_db,
        case_db=tmp_path / "case.db",
        source_before_image_dir=tmp_path / "before-images",
        gate_changed_at="2030-01-01T00:00:00+00:00",
        runner=object(),
        direct_trigger_required=True,
    )
    assert projected == [[PASSIVE_ID, TRIGGER_ID]]
    assert inbox.counts() == {"completed": 1, "skipped": 1}
    with inbox.connect() as conn:
        passive_row = conn.execute(
            "SELECT status,last_error,pa_turn_id FROM ingress_events WHERE message_id=?",
            (PASSIVE_ID,),
        ).fetchone()
    assert tuple(passive_row) == (
        "skipped",
        "PRIORITY_DIRECT_TRIGGER_NOT_RECOGNIZED",
        None,
    )

    with source.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_trigger("later-trigger", 1789139737)) + "\n")
    inbox.stage_from_source(source, cursor)
    await consumer._process_claimed_chat_batch_unlocked(
        inbox,
        inbox.pending(limit=1),
        config_path=config,
        state_db=state_db,
        case_db=tmp_path / "case.db",
        source_before_image_dir=tmp_path / "before-images",
        gate_changed_at="2030-01-01T00:00:00+00:00",
        runner=object(),
        direct_trigger_required=True,
    )
    assert replay_inputs[-1] is None
    assert inbox.counts() == {"completed": 2, "skipped": 1}


def test_context_selection_never_crosses_chat(tmp_path: Path) -> None:
    source = tmp_path / "events.jsonl"
    _write_jsonl(
        source,
        [
            _message("other-passive", OTHER_CHAT, 100, "other chat attachment"),
            _trigger("current-trigger", 180),
        ],
    )
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    inbox.stage_from_source(source, cursor)
    records = inbox.pending(limit=2)
    inbox.claim([records[0]])
    inbox.finish(
        [records[0]],
        status="skipped",
        error="PRIORITY_DIRECT_TRIGGER_NOT_RECOGNIZED",
    )
    assert previous_skipped_context(
        inbox_db=inbox.db_path,
        state_db=tmp_path / "state.db",
        current_records=[records[1]],
    ) == ()


@pytest.mark.asyncio
async def test_contextual_output_is_one_real_replayplan_turn_with_attachment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    image = tmp_path / "PG-JOB-2604-0842.jpg"
    image.write_bytes(b"fixture-image")
    passive = _message(
        PASSIVE_ID,
        MANAGEMENT_CHAT,
        1789139627,
        "PG/JOB/2604/0842 screenshot question",
    )
    passive.update(
        {
            "hasMedia": True,
            "mediaType": "image/jpeg",
            "mediaUrls": [str(image)],
            "mediaMimes": ["image/jpeg"],
        }
    )
    trigger = _trigger(TRIGGER_ID, 1789139707)
    messages = contextual_replay_message([passive, trigger])
    assert len(messages) == 1

    platform_config = PlatformConfig(
        enabled=True,
        extra={
            "group_policy": "open",
            "require_mention": True,
            "debounce_passive_ms": 8000,
        },
    )
    runner = GatewayRunner(
        GatewayConfig(platforms={Platform.WHATSAPP: platform_config})
    )
    runner._session_db = None
    adapter = WhatsAppAdapter(platform_config)
    captured = []

    async def fake_build(_platform, _platform_config, *, connect=True):
        runner._wire_adapter(adapter)
        return adapter, None

    async def capture_event(event):
        captured.append(event)

    monkeypatch.setattr(runner, "_build_adapter", fake_build)
    monkeypatch.setattr(runner, "_handle_message", capture_event)
    # The unadapted producer output proves this instrument can see the defect:
    # the native eight-second debounce opens two turns across the real
    # 80-second gap.
    await runner.replay(
        ReplayPlan(
            platform="whatsapp",
            messages=(passive, trigger),
            delivery_mode="capture",
        )
    )
    assert len(captured) == 2
    captured.clear()

    result = await runner.replay(
        ReplayPlan(platform="whatsapp", messages=messages, delivery_mode="capture")
    )

    assert result.processed == 1
    assert len(captured) == 1
    event = captured[0]
    assert event.raw_message["sourceMessageIds"] == [PASSIVE_ID, TRIGGER_ID]
    assert event.raw_message["timestamp"] == 1789139707
    assert event.media_urls == [str(image)]
    assert "PG/JOB/2604/0842" in event.text
    assert "can u see the above" in event.text
