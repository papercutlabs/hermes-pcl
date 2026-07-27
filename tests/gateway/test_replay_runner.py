import asyncio
import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import GatewayConfig, HomeChannel, Platform, PlatformConfig
from gateway.delivery import DeliveryTarget
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult
from gateway.replay import ReplayAttempt, ReplayCorpus, ReplayPlan, current_replay_context, replay_context
from gateway.run import GatewayRunner
from gateway.session import SessionSource, SessionStore, build_session_key


class FakeReplayAdapter(BasePlatformAdapter):
    def __init__(self, config=None):
        super().__init__(
            config or PlatformConfig(enabled=True, extra={"group_sessions_per_user": False, "thread_sessions_per_user": False}),
            Platform.WHATSAPP,
        )
        self.connect_called = False

    async def connect(self) -> bool:
        self.connect_called = True
        return True

    async def disconnect(self) -> None:
        return None

    async def send(self, chat_id: str, content: str, reply_to=None, metadata=None) -> SendResult:
        return SendResult(success=False, error="live send should be guarded in replay")

    async def get_chat_info(self, chat_id: str):
        return {"id": chat_id, "name": chat_id}

    async def replay_bridge_messages(self, messages, *, bypass_require_mention=True) -> int:
        for message in messages:
            event = MessageEvent(
                text=message.get("body", ""),
                message_type=MessageType.TEXT,
                source=SessionSource(
                    platform=Platform.WHATSAPP,
                    chat_id=message.get("chatId", "120363@g.us"),
                    chat_name=message.get("chatName", "Ops"),
                    chat_type="group" if message.get("isGroup", True) else "dm",
                    user_id=message.get("senderId", "user@s.whatsapp.net"),
                    user_name=message.get("senderName", "Sky"),
                ),
                raw_message=dict(message),
                message_id=message.get("messageId"),
            )
            await self.handle_message(event)
        return len(messages)


@pytest.mark.asyncio
async def test_gateway_runner_replay_uses_no_connect_build_and_captures_outbound(monkeypatch):
    monkeypatch.delenv("WHATSAPP_HOME_CHANNEL", raising=False)
    runner = GatewayRunner(GatewayConfig(platforms={Platform.WHATSAPP: PlatformConfig(enabled=True, extra={})}))
    runner._session_db = None
    adapter = FakeReplayAdapter()
    build_calls = []

    async def fake_build(platform, platform_config, *, connect=True):
        build_calls.append((platform, connect))
        runner._wire_adapter(adapter)
        return adapter, None

    async def fake_handle(event):
        ctx = current_replay_context()
        assert ctx is not None
        assert ctx.execution_mode == "replay"
        assert ctx.run_id == "run-1"
        assert os.getenv("WHATSAPP_HOME_CHANNEL") is None
        assert runner._home_channel_is_configured("whatsapp") is True
        return "captured reply"

    monkeypatch.setattr(runner, "_build_adapter", fake_build)
    monkeypatch.setattr(runner, "_handle_message", AsyncMock(side_effect=fake_handle))

    result = await runner.replay(ReplayPlan(
        platform="whatsapp",
        run_id="run-1",
        attempt_id="attempt-1",
        messages=({"messageId": "m1", "chatId": "eval-home@g.us", "body": "hello", "timestamp": 100},),
    ))

    assert build_calls == [(Platform.WHATSAPP, False)]
    assert adapter.connect_called is False
    assert runner._handle_message.await_count == 1
    assert result.processed == 1
    assert result.outbound[0]["kind"] == "send"
    assert result.outbound[0]["kwargs"]["content"] == "captured reply"
    assert result.outbound[0]["message_id"] == "replay-1"
    assert result.outbound[0]["replay_run_id"] == "run-1"
    assert result.outbound[0]["headers"]["X-Replay-Attempt-Id"] == "attempt-1"
    assert result.attempt["run_id"] == "run-1"
    assert result.attempt["replay_namespace"] == "agent:replay:run-1"
    assert "WHATSAPP_HOME_CHANNEL" not in os.environ


@pytest.mark.asyncio
async def test_gateway_runner_replay_exception_carries_captured_outbound(monkeypatch):
    runner = GatewayRunner(GatewayConfig(
        platforms={Platform.WHATSAPP: PlatformConfig(enabled=True, extra={})}
    ))
    runner._session_db = None
    adapter = FakeReplayAdapter()

    async def fake_build(platform, platform_config, *, connect=True):
        runner._wire_adapter(adapter)
        return adapter, None

    async def capture_then_raise(messages, *, bypass_require_mention=True):
        await adapter.send("ops@g.us", "durable before failure")
        raise RuntimeError("HTTP 403 AuthorizationError")

    monkeypatch.setattr(runner, "_build_adapter", fake_build)
    monkeypatch.setattr(adapter, "replay_bridge_messages", capture_then_raise)

    with pytest.raises(RuntimeError, match="HTTP 403") as caught:
        await runner.replay(ReplayPlan(
            platform="whatsapp",
            run_id="run-capture-failure",
            attempt_id="attempt-capture-failure",
            messages=({"messageId": "m1", "chatId": "ops@g.us", "body": "hello"},),
        ))

    captures = caught.value.replay_outbound
    assert len(captures) == 1
    assert captures[0]["kind"] == "send"
    assert captures[0]["args"] == ["ops@g.us", "durable before failure"]
    assert captures[0]["replay_run_id"] == "run-capture-failure"


@pytest.mark.asyncio
async def test_gateway_runner_concurrent_replays_keep_adapter_context_home_and_sessions_isolated(
    monkeypatch,
):
    """Two overlapping chats must not inherit the last replay's adapter/context."""
    monkeypatch.delenv("WHATSAPP_HOME_CHANNEL", raising=False)
    runner = GatewayRunner(GatewayConfig(
        platforms={Platform.WHATSAPP: PlatformConfig(
            enabled=True,
            extra={"group_sessions_per_user": False, "thread_sessions_per_user": False},
        )}
    ))
    runner._session_db = None
    both_entered = asyncio.Event()
    entered = 0
    observed = {}

    class ConcurrentReplayAdapter(FakeReplayAdapter):
        def __init__(self, label):
            super().__init__()
            self.label = label

        async def replay_bridge_messages(self, messages, *, bypass_require_mention=True) -> int:
            nonlocal entered
            entered += 1
            if entered == 2:
                both_entered.set()
            await asyncio.wait_for(both_entered.wait(), timeout=2)
            message = messages[0]
            source = SessionSource(
                platform=Platform.WHATSAPP,
                chat_id=message["chatId"],
                chat_type="group",
                user_id=f"{self.label}@s.whatsapp.net",
            )
            ctx = current_replay_context()
            observed[self.label] = {
                "run_id": ctx.run_id if ctx else None,
                "session_key": runner._session_key_for_source(source),
                "home": runner._home_channel_is_configured("whatsapp"),
                "env": os.getenv("WHATSAPP_HOME_CHANNEL"),
            }
            await runner.delivery_router._deliver_to_platform(
                DeliveryTarget(Platform.WHATSAPP, chat_id=message["chatId"]),
                f"reply-{self.label}",
                {"label": self.label},
            )
            return 1

    async def fake_build(platform, platform_config, *, connect=True):
        ctx = current_replay_context()
        adapter = ConcurrentReplayAdapter(ctx.run_id)
        runner._wire_adapter(adapter)
        return adapter, None

    monkeypatch.setattr(runner, "_build_adapter", fake_build)
    plans = [
        ReplayPlan(
            platform="whatsapp",
            run_id=f"run-{label}",
            attempt_id=f"attempt-{label}",
            messages=({
                "messageId": f"message-{label}",
                "chatId": f"chat-{label}@g.us",
                "body": label,
                "timestamp": 100,
            },),
        )
        for label in ("a", "b")
    ]

    result_a, result_b = await asyncio.gather(*(runner.replay(plan) for plan in plans))

    assert [entry["args"][1] for entry in result_a.outbound] == ["reply-run-a"]
    assert [entry["args"][1] for entry in result_b.outbound] == ["reply-run-b"]
    assert observed["run-a"]["run_id"] == "run-a"
    assert observed["run-b"]["run_id"] == "run-b"
    assert observed["run-a"]["session_key"].startswith("agent:replay:run-a:")
    assert observed["run-b"]["session_key"].startswith("agent:replay:run-b:")
    assert observed["run-a"]["session_key"] != observed["run-b"]["session_key"]
    assert observed["run-a"]["home"] is True
    assert observed["run-b"]["home"] is True
    assert observed["run-a"]["env"] is None
    assert observed["run-b"]["env"] is None
    assert "WHATSAPP_HOME_CHANNEL" not in os.environ


@pytest.mark.asyncio
async def test_replay_blocks_slash_command_side_effects(monkeypatch):
    runner = GatewayRunner(GatewayConfig(platforms={Platform.WHATSAPP: PlatformConfig(enabled=True, extra={})}))
    runner._session_db = None
    adapter = FakeReplayAdapter()

    async def fake_build(platform, platform_config, *, connect=True):
        runner._wire_adapter(adapter)
        return adapter, None

    monkeypatch.setattr(runner, "_build_adapter", fake_build)

    result = await runner.replay(ReplayPlan(
        platform="whatsapp",
        run_id="run-cmd",
        attempt_id="attempt-cmd",
        messages=({"messageId": "m1", "body": "/reset", "timestamp": 100},),
    ))

    assert result.processed == 1
    assert result.blocked_commands == [{
        "command": "new",
        "platform": "whatsapp",
        "chat_id": "120363@g.us",
        "reason": "replay_command_side_effect_blocked",
    }]
    assert result.outbound == []


@pytest.mark.asyncio
async def test_gateway_runner_replay_persists_attempt_provenance(tmp_path, monkeypatch):
    from hermes_state import SessionDB

    runner = GatewayRunner(GatewayConfig(platforms={Platform.WHATSAPP: PlatformConfig(enabled=True, extra={})}))
    runner._session_db = SessionDB(db_path=tmp_path / "state.db")
    adapter = FakeReplayAdapter()

    async def fake_build(platform, platform_config, *, connect=True):
        runner._wire_adapter(adapter)
        return adapter, None

    async def fake_handle(event):
        return None

    monkeypatch.setattr(runner, "_build_adapter", fake_build)
    monkeypatch.setattr(runner, "_handle_message", AsyncMock(side_effect=fake_handle))

    result = await runner.replay(ReplayPlan(
        platform="whatsapp",
        run_id="run-provenance",
        attempt_id="attempt-provenance",
        messages=({"messageId": "m1", "body": "hello", "timestamp": 100},),
        target_descriptor_manifest={"provider": "systems-pcl", "auth_ref": "TOKEN_ENV"},
        target_baseline_manifest={"snapshot_id": "baseline-1"},
        code_manifest={"repo": "hermes", "git_commit": "abc123"},
    ))

    row = runner._session_db.get_replay_attempt(attempt_id="attempt-provenance")
    assert row["run_id"] == "run-provenance"
    assert row["replay_namespace"] == "agent:replay:run-provenance"
    assert row["status"] == "completed"
    assert row["target_descriptor_manifest"]["provider"] == "systems-pcl"
    assert row["target_descriptor_manifest"]["run_id"] == "run-provenance"
    assert row["corpus_digest"].startswith("sha256:")
    assert result.execution_report["summary"]["turn_count"] == 0
    runner._session_db.close()


def test_gateway_runner_session_key_namespace_failure_fails_closed(monkeypatch):
    runner = GatewayRunner(GatewayConfig(platforms={Platform.WHATSAPP: PlatformConfig(enabled=True, extra={})}))
    runner.session_store = None
    source = SessionSource(
        platform=Platform.WHATSAPP,
        chat_id="120363111@g.us",
        chat_type="group",
        user_id="60120000000@s.whatsapp.net",
    )

    def boom(self, session_key):
        assert session_key.startswith("agent:main:")
        raise ValueError("namespace broke")

    monkeypatch.setattr("gateway.replay.ReplayExecutionContext.namespace_session_key", boom)

    with replay_context(ReplayPlan(platform="whatsapp", run_id="run-bad", attempt_id="attempt-bad")):
        with pytest.raises(RuntimeError, match="refusing to fall back to live session key"):
            runner._session_key_for_source(source)


def _wa_adapter(tmp_path):
    from gateway.platforms.whatsapp import WhatsAppAdapter

    return WhatsAppAdapter(PlatformConfig(
        enabled=True,
        extra={
            "session_path": str(tmp_path / "wa-session"),
            "turn_debounce_ms": 1500,
            "group_policy": "open",
            "dm_policy": "open",
            "group_sessions_per_user": False,
            "thread_sessions_per_user": False,
        },
    ))


def _small_tgg_bridge_corpus():
    return [
        {
            "messageId": "m1",
            "chatId": "120363111@g.us",
            "chatName": "TGG Ops",
            "isGroup": True,
            "senderId": "60120000000@s.whatsapp.net",
            "senderName": "Sky",
            "body": "first update",
            "timestamp": 100,
        },
        {
            "messageId": "m2",
            "chatId": "120363111@g.us",
            "chatName": "TGG Ops",
            "isGroup": True,
            "senderId": "60120000000@s.whatsapp.net",
            "senderName": "Sky",
            "body": "second update",
            "timestamp": 101,
        },
        {
            "messageId": "m3",
            "chatId": "120363111@g.us",
            "chatName": "TGG Ops",
            "isGroup": True,
            "senderId": "60120000000@s.whatsapp.net",
            "senderName": "Sky",
            "body": "later update",
            "timestamp": 110,
        },
    ]


def _bridge_log_row(
    source_ref: str,
    ts: int,
    body: str,
    *,
    message_kind: str = "text",
    has_media: bool = False,
    media_refs=None,
    quoted_text: str = "",
    reply_to_source_ref: str = "",
    raw_json=None,
) -> tuple:
    return (
        source_ref,
        "120363111@g.us",
        "TGG Ops",
        "60120000000@s.whatsapp.net",
        ts,
        f"2026-05-24 00:{ts:02d}:00 SGT",
        body,
        message_kind,
        1 if has_media else 0,
        json.dumps(media_refs or []),
        quoted_text,
        reply_to_source_ref,
        json.dumps(raw_json or {}, ensure_ascii=False),
    )


def _write_bridge_message_log(tmp_path: Path, rows: list[tuple]) -> Path:
    db_path = tmp_path / "bridge.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE bridge_message_log (
              source_ref TEXT,
              chat_jid TEXT,
              chat_name TEXT,
              sender_id TEXT,
              ts INTEGER,
              sgt TEXT,
              text TEXT,
              message_kind TEXT,
              has_media INTEGER,
              media_refs TEXT,
              quoted_text TEXT,
              reply_to_source_ref TEXT,
              raw_json TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO bridge_message_log
              (source_ref, chat_jid, chat_name, sender_id, ts, sgt, text,
               message_kind, has_media, media_refs, quoted_text,
               reply_to_source_ref, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return db_path


class _BridgePostRecorder:
    """Connected-looking WhatsApp bridge session that records real POST attempts."""

    def __init__(self):
        self.posts = []

    def post(self, url, *, json=None, timeout=None):
        self.posts.append({"url": url, "json": json, "timeout": timeout})
        return _BridgePostResponse(message_id=f"bridge-leak-{len(self.posts)}")


class _BridgePostResponse:
    def __init__(self, *, message_id: str):
        self.status = 200
        self._message_id = message_id

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return {"messageId": self._message_id}

    async def text(self):
        return "ok"


class _FakeSessionStore:
    _entries = {}

    def _ensure_loaded(self) -> None:
        return None

    def get_or_create_session(self, source):
        return SimpleNamespace(session_key=build_session_key(source))

    def switch_session(self, session_key: str, target_session_id: str):
        return SimpleNamespace(session_key=session_key, session_id=target_session_id)


class _DirectSendProbeWhatsAppAdapter:
    """Mixin for replay probes that intentionally calls direct adapter.send paths."""

    async def replay_bridge_messages(self, messages, *, bypass_require_mention=True) -> int:
        runner = self._probe_runner

        # delivery.py:254 — cron/delivery router direct adapter.send path.
        await runner.delivery_router._deliver_to_platform(
            DeliveryTarget(Platform.WHATSAPP, chat_id="delivery-chat@g.us"),
            "delivery.py direct send should be captured",
            {"job_id": "replay-probe"},
        )

        # run.py:3528 — active-session shutdown/restart notice direct send.
        active_source = SessionSource(
            platform=Platform.WHATSAPP,
            chat_id="shutdown-active@g.us",
            chat_name="Active",
            chat_type="group",
            user_id="active-user@s.whatsapp.net",
            user_name="Active User",
        )
        active_key = build_session_key(
            active_source,
            group_sessions_per_user=False,
            thread_sessions_per_user=False,
        )
        runner._running_agents[active_key] = object()
        runner._cache_session_source(active_key, active_source)

        # run.py:3574 — home-channel shutdown/restart notice direct send
        # (home has thread metadata so the 3574 branch, not 3576, is exercised).
        await runner._notify_active_sessions_of_shutdown()

        # run.py:4709 — CLI→gateway handoff response direct send.
        runner.session_store = _FakeSessionStore()
        runner._handle_message = AsyncMock(return_value="handoff reply should be captured")
        await runner._process_handoff({
            "id": "cli-session-123456",
            "handoff_platform": "whatsapp",
            "title": "Replay Probe",
        })

        return len(messages)


@pytest.mark.asyncio
async def test_replay_guard_blocks_direct_whatsapp_sends_before_bridge_post(tmp_path, monkeypatch):
    """Direct adapter.send callers in replay must be captured before WA bridge POST."""
    from gateway.platforms.whatsapp import WhatsAppAdapter

    class ProbeAdapter(_DirectSendProbeWhatsAppAdapter, WhatsAppAdapter):
        def __init__(self, config, runner, http_session):
            super().__init__(config)
            self._probe_runner = runner
            self._running = True
            self._http_session = http_session

    platform_config = PlatformConfig(
        enabled=True,
        home_channel=HomeChannel(
            platform=Platform.WHATSAPP,
            chat_id="shutdown-home@g.us",
            name="Home",
            thread_id="home-thread",
        ),
        extra={
            "bridge_port": 39123,
            "session_path": str(tmp_path / "wa-session"),
            "turn_debounce_ms": 0,
            "group_policy": "open",
            "dm_policy": "open",
            "group_sessions_per_user": False,
            "thread_sessions_per_user": False,
        },
    )
    runner = GatewayRunner(GatewayConfig(platforms={Platform.WHATSAPP: platform_config}))
    runner.session_store = _FakeSessionStore()
    http_session = _BridgePostRecorder()
    adapter = ProbeAdapter(platform_config, runner, http_session)

    async def fake_build(platform, platform_config, *, connect=True):
        runner._wire_adapter(adapter)
        return adapter, None

    monkeypatch.setattr(runner, "_build_adapter", fake_build)

    result = await runner.replay(ReplayPlan(
        platform="whatsapp",
        run_id="run-direct-send",
        attempt_id="attempt-direct-send",
        messages=({"messageId": "m1", "body": "trigger direct-send probes", "timestamp": 100},),
    ))

    assert http_session.posts == []
    assert result.processed == 1
    outbound_contents = [
        entry["kwargs"].get("content") or (entry["args"][1] if len(entry["args"]) > 1 else "")
        for entry in result.outbound
    ]
    assert any("delivery.py direct send" in content for content in outbound_contents)
    assert any("Gateway shutting down" in content for content in outbound_contents)
    assert any("handoff reply should be captured" in content for content in outbound_contents)
    assert len([entry for entry in result.outbound if entry["kind"] == "send"]) >= 4


@pytest.mark.asyncio
async def test_replay_guard_blocks_yuanbao_adapter_send_direct_path():
    """YuanbaoAdapter.send delegates to outbound.send_text unless replay guard captures it."""
    from gateway.platforms.yuanbao import YuanbaoAdapter
    from gateway.replay import replay_context

    class FakeYuanbaoOutbound:
        def __init__(self):
            self.calls = []

        async def send_text(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return SendResult(success=True, message_id="yuanbao-leak")

    adapter = YuanbaoAdapter(PlatformConfig(
        enabled=True,
        extra={"app_id": "app", "app_secret": "secret"},
    ))
    fake_outbound = FakeYuanbaoOutbound()
    adapter._outbound = fake_outbound
    runner = GatewayRunner(GatewayConfig(platforms={Platform.YUANBAO: adapter.config}))

    with replay_context(ReplayPlan(platform="yuanbao", run_id="run-yuanbao")) as ctx:
        runner._install_replay_delivery_guard(adapter, ctx)
        result = await adapter.send("direct:account", "yuanbao direct send should be captured")

    assert result.success is True
    assert result.message_id == "replay-1"
    assert fake_outbound.calls == []
    assert ctx.outbound[0]["kind"] == "send"
    assert ctx.outbound[0]["args"][1] == "yuanbao direct send should be captured"


@pytest.mark.asyncio
async def test_native_replay_matches_current_whatsapp_harness_turn_grouping(tmp_path, monkeypatch):
    corpus = _small_tgg_bridge_corpus()
    golden_source_ids = [["m1", "m2"], ["m3"]]

    harness_adapter = _wa_adapter(tmp_path / "harness")
    harness_events = []

    async def capture_harness(event):
        harness_events.append(event)

    harness_adapter.handle_message = capture_harness
    await harness_adapter.replay_bridge_messages(corpus)
    harness_ids = [event.raw_message.get("sourceMessageIds", [event.message_id]) for event in harness_events]
    assert harness_ids == golden_source_ids

    runner = GatewayRunner(GatewayConfig(platforms={Platform.WHATSAPP: PlatformConfig(enabled=True, extra={})}))
    runner._session_db = None
    native_adapter = _wa_adapter(tmp_path / "native")
    native_events = []

    async def fake_build(platform, platform_config, *, connect=True):
        runner._wire_adapter(native_adapter)
        return native_adapter, None

    async def capture_native(event):
        native_events.append(event)
        return None

    monkeypatch.setattr(runner, "_build_adapter", fake_build)
    monkeypatch.setattr(runner, "_handle_message", AsyncMock(side_effect=capture_native))

    await runner.replay(ReplayPlan(platform="whatsapp", messages=tuple(corpus)))
    native_ids = [event.raw_message.get("sourceMessageIds", [event.message_id]) for event in native_events]

    assert native_ids == harness_ids == golden_source_ids


def test_replay_corpus_loads_bridge_message_log_with_explicit_determinism_report(tmp_path):
    from scripts import tgg_christopher_hermes_replay as legacy_harness

    missing_media = tmp_path / "missing-photo.jpg"
    rows = [
        _bridge_log_row("chat::skip0", 90, "offset skip"),
        # Deliberately inserted out of order; corpus ordering is ts, source_ref.
        _bridge_log_row("chat::m2", 101, "second update", quoted_text="Job sheet SK/JOB/2605/1954", reply_to_source_ref="q1"),
        _bridge_log_row("chat::m1", 100, "first update"),
        _bridge_log_row("chat::reaction", 102, "[reaction: 👍]", message_kind="reaction"),
        _bridge_log_row(
            "chat::m3",
            110,
            "later update",
            has_media=True,
            message_kind="image",
            media_refs=[{"local_path": str(missing_media)}],
        ),
    ]
    db_path = _write_bridge_message_log(tmp_path, rows)

    corpus = ReplayCorpus.from_bridge_message_log(
        db_path,
        chat_id="120363111@g.us",
        since="2026-05-24 00:00:00 SGT",
        tenant="tgg",
        agent_id="christopher",
        job_type="tgg_ops_ingest",
        skip_messages=1,
    )

    assert [message["messageId"] for message in corpus.messages] == ["m1", "m2", "m3"]
    assert corpus.messages[1]["quotedText"] == "Job sheet SK/JOB/2605/1954"
    assert corpus.messages[2]["mediaUrls"] == [str(missing_media)]
    assert corpus.messages[0]["_pa_source_ref"] == "chat::m1"
    assert corpus.messages[0]["_pa_local_time"]
    assert corpus.messages[0]["_tgg_source_ref"] == "chat::m1"
    assert corpus.messages[0]["_tgg_sgt"] == corpus.messages[0]["_pa_local_time"]
    assert corpus.messages[0]["_hermes_pa_context"] == {
        "tenant": "tgg",
        "agent_id": "christopher",
        "job_type": "tgg_ops_ingest",
    }
    assert corpus.report["messages_skipped"] == [
        {"reason": "offset", "count": 1},
        {"reason": "bare_reaction", "source_ref": "chat::reaction", "message_kind": "reaction"},
    ]
    assert corpus.report["missing_media"] == [
        {
            "source_ref": "chat::m3",
            "path": str(missing_media),
            "basename": "missing-photo.jpg",
            "reason": "media_path_missing",
        }
    ]
    policy = corpus.manifest()["policy"]
    assert policy["ordering"] == ["timestamp", "source_ref"]
    assert policy["dedup"] == "first_by_message_id_or_source_ref"
    assert policy["reaction_policy"] == "skip_bare_reactions"
    assert policy["future_read_fence"] == "per_turn_latest_message_timestamp_plus_one"

    # Parity with the current TGG harness corpus loader/converter: same ordered
    # feed after its offset + reaction policy, before the native ReplayPlan.
    legacy_records = legacy_harness._load_records(
        db_path,
        chat_id="120363111@g.us",
        since_sgt="2026-05-24 00:00:00 SGT",
        until_sgt=None,
        limit=None,
        skip_messages=1,
    )
    legacy_messages = [
        legacy_harness._record_to_bridge_message(record)
        for record in legacy_records
        if not legacy_harness._is_bare_reaction_record(record)
    ]
    assert [
        (m["messageId"], m["chatId"], m["body"], m["quotedText"], m["mediaUrls"])
        for m in corpus.messages
    ] == [
        (m["messageId"], m["chatId"], m["body"], m["quotedText"], m["mediaUrls"])
        for m in legacy_messages
    ]


def test_replay_corpus_dedup_reports_skipped_duplicates(tmp_path):
    db_path = _write_bridge_message_log(
        tmp_path,
        [
            _bridge_log_row("chat::m1", 100, "first", raw_json={"id": "same-id"}),
            _bridge_log_row("chat::m1-duplicate", 101, "duplicate", raw_json={"id": "same-id"}),
            _bridge_log_row("chat::m2", 102, "second", raw_json={"id": "m2"}),
        ],
    )

    corpus = ReplayCorpus.from_bridge_message_log(
        db_path,
        chat_id="120363111@g.us",
        since="2026-05-24 00:00:00 SGT",
        tenant="finexis",
        agent_id="mtu",
        job_type="advisor_ingest",
    )

    assert [message["messageId"] for message in corpus.messages] == ["same-id", "m2"]
    assert corpus.report["duplicates_skipped"] == [
        {"reason": "duplicate_message", "dedup_key": "same-id", "source_ref": "chat::m1-duplicate"}
    ]


@pytest.mark.asyncio
async def test_replay_corpus_fixture_matches_current_harness_turn_boundaries(tmp_path, monkeypatch):
    db_path = _write_bridge_message_log(
        tmp_path,
        [
            _bridge_log_row("chat::m1", 100, "first update"),
            _bridge_log_row("chat::m2", 101, "second update"),
            _bridge_log_row("chat::m3", 110, "later update"),
        ],
    )
    corpus = ReplayCorpus.from_bridge_message_log(
        db_path,
        chat_id="120363111@g.us",
        since="2026-05-24 00:00:00 SGT",
        tenant="tgg",
        agent_id="christopher",
        job_type="tgg_ops_ingest",
    )
    plan = ReplayPlan(
        platform="whatsapp",
        messages=corpus.messages,
        corpus_manifest=corpus.manifest(),
        replay_policy=corpus.replay_policy_manifest(),
    )

    harness_adapter = _wa_adapter(tmp_path / "harness")
    harness_events = []

    async def capture_harness(event):
        harness_events.append(event)

    harness_adapter.handle_message = capture_harness
    await harness_adapter.replay_bridge_messages(list(corpus.messages))
    harness_ids = [event.raw_message.get("sourceMessageIds", [event.message_id]) for event in harness_events]

    runner = GatewayRunner(GatewayConfig(platforms={Platform.WHATSAPP: PlatformConfig(enabled=True, extra={})}))
    runner._session_db = None
    native_adapter = _wa_adapter(tmp_path / "native")
    native_events = []

    async def fake_build(platform, platform_config, *, connect=True):
        runner._wire_adapter(native_adapter)
        return native_adapter, None

    async def capture_native(event):
        native_events.append(event)
        return None

    monkeypatch.setattr(runner, "_build_adapter", fake_build)
    monkeypatch.setattr(runner, "_handle_message", AsyncMock(side_effect=capture_native))

    result = await runner.replay(plan)
    native_ids = [event.raw_message.get("sourceMessageIds", [event.message_id]) for event in native_events]

    assert result.corpus_report == {}
    assert native_ids == harness_ids == [["m1", "m2"], ["m3"]]


def test_replay_plan_loads_typed_plan_and_corpus(tmp_path):
    corpus_path = tmp_path / "bridge.jsonl"
    corpus_path.write_text('\n'.join(json.dumps(row) for row in _small_tgg_bridge_corpus()), encoding="utf-8")
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps({
        "platform": "whatsapp",
        "delivery_mode": "drop",
        "corpus": {"path": corpus_path.name},
    }), encoding="utf-8")

    plan = ReplayPlan.from_path(plan_path)

    assert plan.platform == "whatsapp"
    assert plan.delivery_mode == "drop"
    assert len(plan.messages) == 3
    assert plan.source_path == str(corpus_path)
    assert plan.replay_namespace.startswith("agent:replay:")


def test_replay_plan_loads_bridge_message_log_corpus_spec(tmp_path):
    db_path = _write_bridge_message_log(
        tmp_path,
        [
            _bridge_log_row("chat::m1", 100, "first update"),
            _bridge_log_row("chat::reaction", 101, "[reaction: 👍]", message_kind="reaction"),
        ],
    )
    plan = ReplayPlan.from_mapping({
        "platform": "whatsapp",
        "corpus": {
            "source": "bridge_message_log",
            "db_path": str(db_path),
            "chat_id": "120363111@g.us",
            "tenant": "tgg",
            "agent_id": "christopher",
            "job_type": "tgg_ops_ingest",
            # Legacy plan fixtures remain readable; the CLI surface is now
            # the generic --since/--until pair.
            "since_sgt": "2026-05-24 00:00:00 SGT",
        },
    })

    assert [message["messageId"] for message in plan.messages] == ["m1"]
    assert plan.corpus_manifest["source_type"] == "bridge_message_log"
    assert plan.corpus_manifest["report"]["messages_skipped"][0]["reason"] == "bare_reaction"
    assert plan.replay_policy["future_read_fence"]["mode"] == "per_turn_latest_message_timestamp_plus_one"


def test_replay_plan_and_attempt_manifests_have_canonical_digests(tmp_path):
    corpus_path = tmp_path / "bridge.json"
    corpus_path.write_text(json.dumps({"messages": _small_tgg_bridge_corpus()}), encoding="utf-8")
    plan = ReplayPlan.from_mapping(
        {
            "platform": "whatsapp",
            "run_id": "run-fixed",
            "attempt_id": "attempt-fixed",
            "corpus": {"path": str(corpus_path)},
            "target_descriptor": {
                "provider": "systems-pcl",
                "base_url": "http://127.0.0.1:5191",
                "auth_ref": "SYSTEMS_PCL_TOKEN",
            },
            "target_baseline": {"snapshot_id": "snap-1", "table_counts": {"cases": 0}},
            "config_overlay": {"pa": {"enabled": True}},
            "code_manifest": {"repo": "hermes", "git_commit": "abc123"},
        }
    )
    attempt = ReplayAttempt.from_plan(plan)

    assert attempt.corpus_manifest["message_count"] == 3
    assert attempt.target_descriptor_manifest["run_id"] == "run-fixed"
    for digest in (
        attempt.corpus_digest,
        attempt.config_overlay_digest,
        attempt.target_descriptor_digest,
        attempt.target_baseline_digest,
        attempt.code_digest,
        attempt.replay_policy_digest,
        attempt.plan_digest,
    ):
        assert digest.startswith("sha256:")
        assert len(digest) == len("sha256:") + 64

    same_attempt = ReplayAttempt.from_plan(plan)
    assert same_attempt.corpus_digest == attempt.corpus_digest
    assert same_attempt.plan_digest == attempt.plan_digest


def test_replay_session_key_namespace_isolates_live_sessions(tmp_path, monkeypatch):
    import hermes_state

    monkeypatch.setattr(hermes_state, "SessionDB", lambda: None)
    config = GatewayConfig(
        sessions_dir=tmp_path / "sessions",
        group_sessions_per_user=False,
        thread_sessions_per_user=False,
    )
    store = SessionStore(config.sessions_dir, config)
    source = SessionSource(
        platform=Platform.WHATSAPP,
        chat_id="120363111@g.us",
        chat_name="TGG Ops",
        chat_type="group",
        user_id="60120000000@s.whatsapp.net",
    )

    live = store.get_or_create_session(source)
    assert live.session_key == "agent:main:whatsapp:group:120363111@g.us"

    with replay_context(ReplayPlan(platform="whatsapp", run_id="run-fixed", attempt_id="attempt-fixed")):
        replay = store.get_or_create_session(source)
        replay_again = store.get_or_create_session(source)

    assert replay.session_key == "agent:replay:run-fixed:whatsapp:group:120363111@g.us"
    assert replay.session_id == replay_again.session_id
    assert replay.session_id != live.session_id
    assert store._entries[live.session_key].session_id == live.session_id


def test_send_message_tool_is_captured_in_replay_context():
    from gateway.replay import replay_context
    from tools.send_message_tool import _handle_send

    plan = ReplayPlan(platform="whatsapp", run_id="run-tool", attempt_id="attempt-tool")
    with replay_context(plan) as ctx:
        payload = json.loads(_handle_send({"target": "whatsapp:120363@g.us", "message": "do not send live"}))

    assert payload == {"success": True, "message_id": "replay-1", "replay": "capture"}
    assert ctx.outbound[0]["kind"] == "send_message_tool"
    assert ctx.outbound[0]["kwargs"]["target"] == "whatsapp:120363@g.us"
    assert ctx.outbound[0]["headers"]["X-Replay-Run-Id"] == "run-tool"


def test_replay_context_sets_pa_history_cap():
    from gateway.replay import replay_context, set_replay_turn_history_before_ts
    from tools.pa_business_tools import _history_before_ts_cap

    plan = ReplayPlan(platform="whatsapp", history_before_ts=111)
    with replay_context(plan):
        assert _history_before_ts_cap() == 111
        set_replay_turn_history_before_ts(222)
        assert _history_before_ts_cap() == 222


@pytest.mark.asyncio
async def test_telegram_replay_bridge_messages_dispatches_normalized_events():
    from gateway.platforms.telegram import TelegramAdapter

    adapter = TelegramAdapter(PlatformConfig(
        enabled=True,
        token="unused-for-replay",
        extra={
            "require_mention": False,
            "group_sessions_per_user": False,
            "thread_sessions_per_user": True,
        },
    ))
    handled = []

    async def fake_handle(event):
        handled.append(event)
        return None

    adapter.set_message_handler(fake_handle)

    with replay_context(ReplayPlan(platform="telegram", run_id="tg-run", attempt_id="tg-attempt")):
        processed = await adapter.replay_bridge_messages([
            {
                "messageId": "tg-2",
                "chatId": "-100100200300",
                "chatName": "Pip PA Replay",
                "isGroup": True,
                "senderId": "276672685",
                "senderName": "Teren",
                "body": "later",
                "timestamp": 102,
                "messageThreadId": 9,
            },
            {
                "messageId": "tg-1",
                "chatId": "-100100200300",
                "chatName": "Pip PA Replay",
                "isGroup": True,
                "senderId": "276672685",
                "senderName": "Teren",
                "body": "status please",
                "timestamp": 101,
                "messageThreadId": 9,
            },
        ])

    assert processed == 2
    assert [event.message_id for event in handled] == ["tg-1", "tg-2"]
    first = handled[0]
    assert first.source.platform == Platform.TELEGRAM
    assert first.source.chat_id == "-100100200300"
    assert first.source.chat_type == "group"
    assert first.source.user_id == "276672685"
    assert first.source.thread_id == "9"
    assert first.text == "status please"
    assert first.raw_message["messageId"] == "tg-1"


@pytest.mark.asyncio
async def test_telegram_replay_honors_group_mention_gate_when_not_bypassed():
    from gateway.platforms.telegram import TelegramAdapter

    adapter = TelegramAdapter(PlatformConfig(
        enabled=True,
        token="unused-for-replay",
        extra={"require_mention": True},
    ))
    handled = []

    async def fake_handle(event):
        handled.append(event)
        return None

    adapter.set_message_handler(fake_handle)

    with replay_context(ReplayPlan(platform="telegram", run_id="tg-gate", attempt_id="tg-gate-attempt")):
        processed = await adapter.replay_bridge_messages([
            {
                "messageId": "tg-drop",
                "chatId": "-100100200300",
                "isGroup": True,
                "senderId": "276672685",
                "body": "unaddressed group text",
                "timestamp": 101,
            }
        ], bypass_require_mention=False)

    assert processed == 0
    assert handled == []
