from __future__ import annotations

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.whatsapp import WhatsAppAdapter


CHAT = "120363403845802098@g.us"


def _message(message_id: str, ts: int, body: str, chat_id: str = CHAT) -> dict:
    return {
        "messageId": message_id,
        "chatId": chat_id,
        "chatName": "MM2 Maintenance (SK)",
        "senderId": "251547711758376@lid",
        "senderName": "251547711758376",
        "isGroup": True,
        "timestamp": ts,
        "body": body,
        "hasMedia": False,
        "mediaUrls": [],
    }


@pytest.fixture(autouse=True)
def _isolate_whatsapp_group_policy(monkeypatch):
    """Keep replay fixtures independent of config-loader env side effects."""
    monkeypatch.delenv("WHATSAPP_GROUP_POLICY", raising=False)
    monkeypatch.delenv("WHATSAPP_GROUP_ALLOWED_USERS", raising=False)


def _adapter(extra: dict | None = None) -> WhatsAppAdapter:
    config = {"group_policy": "open", **(extra or {})}
    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra=config))
    assert adapter._group_policy == config["group_policy"]
    return adapter


async def _capture_replay(adapter: WhatsAppAdapter, messages: list[dict]) -> list:
    captured = []

    async def handle(event):
        captured.append(event)

    adapter.handle_message = handle  # type: ignore[method-assign]
    processed = await adapter.replay_bridge_messages(messages)
    assert processed == len(messages)
    return captured


@pytest.mark.asyncio
async def test_ingest_chat_bypasses_require_mention_for_ops_capture():
    adapter = _adapter({"require_mention": True, "ingest_chats": [CHAT]})

    event = await adapter._build_message_event(_message("m1", 1779679800, "normal worker update"))

    assert event is not None
    assert event.text == "normal worker update"
    assert event.source.chat_id == CHAT


@pytest.mark.asyncio
async def test_replay_uses_timestamp_debounce_without_wall_sleep():
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "turn_policy": {
            CHAT: {"debounce_seconds": 300, "direct_mention_immediate": False},
        },
    })

    captured = await _capture_replay(adapter, [
        _message("m1", 1000, "before"),
        _message("m2", 1100, "install done"),
        _message("m3", 1501, "new job"),
    ])

    assert len(captured) == 2
    assert captured[0].raw_message["sourceMessageIds"] == ["m1", "m2"]
    assert captured[1].message_id == "m3"


@pytest.mark.asyncio
async def test_direct_trigger_closes_replay_turn_immediately():
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "turn_policy": {
            CHAT: {"debounce_seconds": 300, "direct_mention_immediate": True},
        },
    })

    captured = await _capture_replay(adapter, [
        _message("m1", 1000, "worker context"),
        _message("m2", 1010, "/status please"),
        _message("m3", 1020, "after direct trigger"),
    ])

    assert len(captured) == 2
    assert captured[0].raw_message["sourceMessageIds"] == ["m1", "m2"]
    assert captured[1].message_id == "m3"


@pytest.mark.asyncio
async def test_replay_bundle_hard_capped_at_ten_messages():
    """A run of messages all inside the debounce window must not accumulate
    into one mega-bundle: the replay cap flushes at 10, spillover starts the
    next turn."""
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "turn_policy": {
            CHAT: {"debounce_seconds": 300, "direct_mention_immediate": False},
        },
    })

    # 22 messages, each 100s apart (< debounce) — would previously bundle
    # into a single 22-message turn spanning ~35 minutes.
    captured = await _capture_replay(adapter, [
        _message(f"m{i:02d}", 1000 + i * 100, f"update {i}") for i in range(22)
    ])

    assert len(captured) == 3
    sizes = [
        len(e.raw_message["sourceMessageIds"]) if e.raw_message.get("bundle") else 1
        for e in captured
    ]
    assert sizes == [10, 10, 2]
    assert captured[0].raw_message["sourceMessageIds"][0] == "m00"
    assert captured[1].raw_message["sourceMessageIds"][0] == "m10"
    assert captured[2].raw_message["sourceMessageIds"][0] == "m20"


@pytest.mark.asyncio
async def test_replay_bundle_cap_override_uncapped_matches_live_no_cap():
    """replay_bundle_message_cap: 0 disables the replay-only cap so grouping
    matches live semantics (live is trailing-quiet debounce, no message cap)."""
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "replay_bundle_message_cap": 0,
        "turn_policy": {
            CHAT: {"debounce_seconds": 300, "direct_mention_immediate": False},
        },
    })

    captured = await _capture_replay(adapter, [
        _message(f"m{i:02d}", 1000 + i * 100, f"update {i}") for i in range(22)
    ])

    assert len(captured) == 1
    assert len(captured[0].raw_message["sourceMessageIds"]) == 22


@pytest.mark.asyncio
async def test_replay_bundle_cap_override_custom_value():
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "replay_bundle_message_cap": 5,
        "turn_policy": {
            CHAT: {"debounce_seconds": 300, "direct_mention_immediate": False},
        },
    })

    captured = await _capture_replay(adapter, [
        _message(f"m{i:02d}", 1000 + i * 100, f"update {i}") for i in range(12)
    ])

    sizes = [
        len(e.raw_message["sourceMessageIds"]) if e.raw_message.get("bundle") else 1
        for e in captured
    ]
    assert sizes == [5, 5, 2]


@pytest.mark.asyncio
async def test_replay_without_turn_policy_uses_passive_window_not_legacy_default():
    """Without an explicit turn_policy, replay must group on the same
    addressed/passive window live turns run on (config/brief
    debounce_passive_ms), not the 1.5s legacy turn_debounce_ms default —
    otherwise evals judge bundle shapes live would never produce."""
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "debounce_passive_ms": 300000,
    })

    captured = await _capture_replay(adapter, [
        _message("m1", 1000, "album photo"),
        _message("m2", 1090, "caption 90s later"),
        _message("m3", 1290, "200s later — still inside 5min window"),
        _message("m4", 1650, "after a 360s gap — new turn"),
    ])

    assert len(captured) == 2
    assert captured[0].raw_message["sourceMessageIds"] == ["m1", "m2", "m3"]
    assert captured[1].message_id == "m4"


@pytest.mark.asyncio
async def test_replay_without_any_explicit_window_keeps_legacy_default():
    """No turn_policy and no explicit brief/config window: the legacy
    turn_debounce_ms fallback (1.5s default) still governs — existing replay
    rigs keep their golden groupings."""
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
    })

    captured = await _capture_replay(adapter, [
        _message("m1", 1000, "first"),
        _message("m2", 1001, "one second later — bundles at 1.5s fallback"),
        _message("m3", 1006, "five seconds after m2 — new turn"),
    ])

    assert len(captured) == 2
    assert captured[0].raw_message["sourceMessageIds"] == ["m1", "m2"]
    assert captured[1].message_id == "m3"


@pytest.mark.asyncio
async def test_replay_bundle_still_splits_on_timestamp_gap():
    """Original-timestamp gap > debounce starts a new turn even under the cap."""
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "turn_policy": {
            CHAT: {"debounce_seconds": 300, "direct_mention_immediate": False},
        },
    })

    captured = await _capture_replay(adapter, [
        _message("m1", 1000, "first"),
        _message("m2", 1100, "second"),
        _message("m3", 9000, "after a 2h-style gap"),
        _message("m4", 9100, "fourth"),
    ])

    assert len(captured) == 2
    assert captured[0].raw_message["sourceMessageIds"] == ["m1", "m2"]
    assert captured[1].raw_message["sourceMessageIds"] == ["m3", "m4"]


def _album_photo(message_id: str, ts: int, quoted_text: str, quoted_id: str) -> dict:
    """Album-shaped photo message: no body, quote carries the job-sheet context."""
    return {
        "messageId": message_id,
        "chatId": CHAT,
        "chatName": "MM2 Maintenance (SK)",
        "senderId": "251547711758376@lid",
        "senderName": "251547711758376",
        "isGroup": True,
        "timestamp": ts,
        "body": "",
        "hasMedia": True,
        "mediaType": "image",
        "mediaUrls": [f"/tmp/tgg-eval-synthetic-{message_id}.jpg"],
        "quotedText": quoted_text,
        "quotedMessageId": quoted_id,
    }


@pytest.mark.asyncio
async def test_album_bundle_renders_shared_quote_once_with_case_refs():
    """Album messages with no body but with quotedText must carry the quote
    (incl. extracted case refs) into the bundle text — and a quote shared by
    several images in one album renders once, not once per image."""
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "turn_policy": {
            CHAT: {"debounce_seconds": 300, "direct_mention_immediate": False},
        },
    })

    quote = "Job sheet SK/JOB/2605/1954\nBlk 123 #04-567 toilet door install"
    captured = await _capture_replay(adapter, [
        _album_photo("a1", 1000, quote, "q-jobsheet-1"),
        _album_photo("a2", 1005, quote, "q-jobsheet-1"),
    ])

    assert len(captured) == 1
    bundle = captured[0]
    assert bundle.raw_message.get("bundle") is True
    text = bundle.text
    assert "Quoted WhatsApp message case refs: SK/JOB/2605/1954" in text
    assert "toilet door install" in text
    # Shared quote renders once per album, not per image.
    assert text.count("Quoted WhatsApp message case refs") == 1
    assert text.count("[Replying to:") == 1


@pytest.mark.asyncio
async def test_album_bundle_renders_distinct_quotes_separately():
    """Two messages quoting different prior messages keep both quotes."""
    adapter = _adapter({
        "require_mention": True,
        "ingest_chats": [CHAT],
        "turn_policy": {
            CHAT: {"debounce_seconds": 300, "direct_mention_immediate": False},
        },
    })

    captured = await _capture_replay(adapter, [
        _album_photo("b1", 1000, "Job sheet SK/JOB/2605/1954", "q-1"),
        _album_photo("b2", 1005, "Job sheet SK/WC/2605/2001", "q-2"),
    ])

    assert len(captured) == 1
    text = captured[0].text
    assert "SK/JOB/2605/1954" in text
    assert "SK/WC/2605/2001" in text
    assert text.count("[Replying to:") == 2
