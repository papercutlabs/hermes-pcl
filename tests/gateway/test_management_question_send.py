"""PA-396: one operator-approved Management question for one record.

The contract under test:
- the destination must be a WhatsApp management selector;
- the record is read once through the Systems operator document read, with
  the tenant header when given;
- the approved text is sent verbatim, as the first photo's caption or as
  plain text, and registered as the record's initial notice;
- a second send for the same record and destination is refused;
- no model turn runs and the document-outbox cursor never moves;
- a quoted reply to the notice correlates to the record.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
import yaml

from gateway import durable_jsonl_consumer as consumer
from gateway.durable_jsonl_consumer import (
    ConsumerError,
    DurableInbox,
    InboxRecord,
    ManagementQuestionSource,
    send_management_question,
)

MGMT_CHAT = "120363426509183563@g.us"
SITE_CHAT = "120363421424519051@g.us"
RECORD_ID = "3cc12710-0000-4000-8000-000000000001"
ENTRY_ID = "f26ab4f6-0000-4000-8000-000000000002"
QUESTION = "Krisleen, which job is the burst pipe at Blk 641 #10-822 on?"
SYSTEMS = "http://systems.test"
BRIDGE = "http://bridge.test"


class _FakeResponse:
    def __init__(self, payload: dict, status: int = 200):
        self._payload = json.dumps(payload).encode()
        self.status = status

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


@pytest.fixture()
def media_root(tmp_path: Path) -> Path:
    root = tmp_path / "retained"
    root.mkdir()
    return root


@pytest.fixture()
def config_path(tmp_path: Path, media_root: Path) -> Path:
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(
        "selectors:\n"
        "- job_type: tgg_ops_ingest\n"
        "  match:\n"
        "    source.platform: whatsapp\n"
        f"    source.chat_id: {SITE_CHAT}\n"
        "- job_type: tgg_management\n"
        "  match:\n"
        "    source.platform: whatsapp\n"
        f"    source.chat_id: {MGMT_CHAT}\n",
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({"pa": {
        "enabled": True,
        "constitution_path": str(constitution),
        "media_retention": {
            "enabled": True,
            "media_root": str(media_root),
            "source_roots": [str(media_root)],
            "operation": "tgg_media_retention",
            "media_ref_prefix": "/media/tgg/hermes",
        },
    }}), encoding="utf-8")
    return config


@pytest.fixture()
def inbox(tmp_path: Path) -> DurableInbox:
    return DurableInbox(tmp_path / "inbox.db")


@pytest.fixture(autouse=True)
def _token_and_no_drain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    monkeypatch.delenv("TGG_MANAGEMENT_DOCUMENT_API_URL", raising=False)
    monkeypatch.delenv("TGG_MANAGEMENT_DOCUMENT_CHAT_ID", raising=False)

    def no_cursor(*_args, **_kwargs):
        raise AssertionError("the outbox cursor must never move")

    async def no_turn(*_args, **_kwargs):
        raise AssertionError("no model turn may run")

    monkeypatch.setattr(DurableInbox, "advance_management_document_cursor", no_cursor)
    monkeypatch.setattr(consumer, "_run_management_document_turn", no_turn)


def _jpeg(path: Path, salt: int) -> str:
    data = b"\xff\xd8\xff\xe0" + bytes([salt]) * 32
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def _media(digest: str, *, ref: str | None, mime: str = "image/jpeg", availability: str = "available") -> dict:
    return {
        "messageId": f"msg-{digest[:6]}", "sourceKey": "k", "ordinal": 0,
        "digest": digest, "mime": mime, "availability": availability,
        "sourceIntegrity": "matches_cited_media", "retrievalUrl": ref,
    }


def _document(media: list[dict] | None = None, *, entries: list[dict] | None = None) -> dict:
    return {"data": {
        "contract": "tgg-human-resolution-document-entry/v1",
        "document": {
            "record": {
                "id": RECORD_ID, "kind": "unknown_case_identity",
                "evidenceMedia": media or [],
                "citedSnapshot": {"messages": [], "media": []},
            },
            "entries": entries if entries is not None else [
                {"id": ENTRY_ID, "recordId": RECORD_ID, "entryKind": "initial_default",
                 "createdAt": 1791115772, "body": {}, "effects": []},
            ],
        },
    }}


def _install(monkeypatch: pytest.MonkeyPatch, document: dict | None, *, bridge_fail: bool = False) -> list:
    calls: list[tuple[str, str, dict | None, dict]] = []

    def fake_urlopen(request, timeout=0):
        body = json.loads(request.data) if request.data else None
        calls.append((request.get_method(), request.full_url, body, dict(request.headers)))
        if request.full_url.startswith(SYSTEMS):
            if document is None:
                from urllib.error import HTTPError
                raise HTTPError(request.full_url, 404, "not found", {}, None)
            return _FakeResponse(document)
        if bridge_fail:
            return _FakeResponse({"success": False, "outcome": "unknown"}, status=202)
        bridge_calls = [call for call in calls if call[1].startswith(BRIDGE)]
        return _FakeResponse({"success": True, "messageId": f"WA-{len(bridge_calls)}"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    return calls


def _send(inbox: DurableInbox, config_path: Path, *, chat_id: str = MGMT_CHAT, **kwargs) -> dict:
    return send_management_question(
        inbox,
        config_path=config_path,
        record_id=RECORD_ID,
        chat_id=chat_id,
        question_text=QUESTION + "\n",
        source=ManagementQuestionSource(api_url=SYSTEMS, token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", tenant="tgg"),
        bridge_url=BRIDGE,
        **kwargs,
    )


def _rows(inbox: DurableInbox) -> list[dict]:
    with inbox.connect() as conn:
        return [dict(row) for row in conn.execute(
            "SELECT delivery_key,status,bridge_message_id,reply_to_message_id,correlation_json "
            "FROM reply_deliveries ORDER BY created_at,delivery_key"
        )]


def _bridge(calls: list) -> list[tuple[str, dict]]:
    return [(url, body) for method, url, body, _ in calls if url.startswith(BRIDGE)]


def test_non_selector_destination_is_refused_before_any_read(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install(monkeypatch, _document())
    with pytest.raises(ConsumerError, match="not a WhatsApp management selector"):
        _send(inbox, config_path, chat_id=SITE_CHAT)
    assert calls == [] and _rows(inbox) == []


def test_missing_record_is_refused_without_a_send(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install(monkeypatch, None)
    with pytest.raises(ConsumerError, match="record not found"):
        _send(inbox, config_path)
    assert _bridge(calls) == [] and _rows(inbox) == []


def test_record_without_initial_entry_is_refused(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install(monkeypatch, _document(entries=[]))
    with pytest.raises(ConsumerError, match="no initial document entry"):
        _send(inbox, config_path)
    assert _bridge(calls) == [] and _rows(inbox) == []


def test_text_only_send_is_verbatim_and_registered_as_initial_notice(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install(monkeypatch, _document())
    receipt = _send(inbox, config_path)
    method, url, _, headers = calls[0]
    assert (method, url) == ("GET", f"{SYSTEMS}/api/operator/human-resolution-documents/{RECORD_ID}")
    assert headers["X-ps-tenant"] == "tgg" and headers["Authorization"] == "Bearer test-token"
    assert _bridge(calls) == [(f"{BRIDGE}/send", {"chatId": MGMT_CHAT, "message": QUESTION})]
    assert receipt["notice"] == {
        "delivery_key": f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}",
        "endpoint": "send", "status": "delivered", "message_id": "WA-1",
    }
    [row] = _rows(inbox)
    assert row["status"] == "delivered" and row["bridge_message_id"] == "WA-1"
    assert row["reply_to_message_id"] is None
    correlation = json.loads(row["correlation_json"])
    assert correlation["document_id"] == RECORD_ID and correlation["entry_id"] == ENTRY_ID
    assert correlation["notice_role"] == "initial_notice" and correlation["notice_body"] == QUESTION
    # The outbox sees this destination as already notified for the entry,
    # and a later lifecycle notice would quote this message.
    assert inbox.management_document_delivery_status(entry_id=ENTRY_ID, chat_id=MGMT_CHAT) == "delivered"
    assert inbox.initial_management_document_notice(chat_id=MGMT_CHAT, document_id=RECORD_ID) == {
        "delivery_key": f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}",
        "message_id": "WA-1", "body": QUESTION,
    }
    assert inbox.management_document_cursor() is None


def test_photos_send_caption_on_first_and_resolve_archived_bytes(
    inbox: DurableInbox, config_path: Path, media_root: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_a = media_root / "a.jpg"
    digest_a = _jpeg(local_a, 1)
    local_b = media_root / "b.jpg"
    digest_b = _jpeg(local_b, 2)
    cache = tmp_path / "cache"
    cache.mkdir()
    archived = cache / "opaque"
    digest_c = _jpeg(archived, 3)
    resolver = tmp_path / "resolver" / "resolve.py"
    resolver.parent.mkdir()
    resolver.write_text(
        "import json,sys\n"
        "request=json.load(sys.stdin)\n"
        f"assert request['digest']=={digest_c!r} and request['operation']=='resolve'\n"
        f"print(json.dumps({{'ok':True,'path':{str(archived)!r},'digest':request['digest'],'mime':'image/jpeg'}}))\n",
        encoding="utf-8",
    )
    media = [
        _media(digest_a, ref="/media/tgg/hermes/a.jpg"),
        _media(digest_b, ref="/media/tgg/hermes/b.jpg"),
        _media(digest_a, ref="/media/tgg/hermes/a.jpg"),  # same bytes cited twice
        _media(digest_c, ref="/media/tgg/hermes/missing.jpg"),
        _media("f" * 64, ref=None, availability="unavailable"),
        _media("e" * 64, ref=None, mime="application/pdf"),
    ]
    calls = _install(monkeypatch, _document(media))
    receipt = _send(inbox, config_path, resolver_command=(sys.executable, "-I", str(resolver)))
    copied = media_root / f"wa-evidence-{digest_c[:24]}.jpg"
    assert copied.read_bytes() == archived.read_bytes()
    assert _bridge(calls) == [
        (f"{BRIDGE}/send-media", {"chatId": MGMT_CHAT, "filePath": str(local_a.resolve()), "mediaType": "image", "caption": QUESTION}),
        (f"{BRIDGE}/send-media", {"chatId": MGMT_CHAT, "filePath": str(local_b.resolve()), "mediaType": "image"}),
        (f"{BRIDGE}/send-media", {"chatId": MGMT_CHAT, "filePath": str(copied), "mediaType": "image"}),
    ]
    assert receipt["notice"]["message_id"] == "WA-1"
    assert [photo["status"] for photo in receipt["photos"]] == ["delivered", "delivered"]
    rows = _rows(inbox)
    notice_key = f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}"
    notice = [row for row in rows if row["delivery_key"] == notice_key]
    assert len(notice) == 1 and notice[0]["bridge_message_id"] == "WA-1"
    assert len(rows) == 3
    photos = [row for row in rows if row["delivery_key"].startswith(f"{notice_key}:photo:")]
    assert [row["delivery_key"] for row in photos] == [f"{notice_key}:photo:1", f"{notice_key}:photo:2"]
    assert [row["bridge_message_id"] for row in photos] == ["WA-2", "WA-3"]
    for row in photos:
        correlation = json.loads(row["correlation_json"])
        assert {key: correlation[key] for key in ("document_id", "entry_id", "entry_kind", "notice_role")} == {
            "document_id": RECORD_ID, "entry_id": ENTRY_ID,
            "entry_kind": "initial_default", "notice_role": "initial_notice",
        }
        assert correlation["notice_part"] == "photo" and "notice_body" not in correlation


def test_unconfirmed_notice_sends_no_photos_and_is_never_retried(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest_a = _jpeg(media_root / "a.jpg", 1)
    digest_b = _jpeg(media_root / "b.jpg", 2)
    media = [_media(digest_a, ref="/media/tgg/hermes/a.jpg"), _media(digest_b, ref="/media/tgg/hermes/b.jpg")]
    calls = _install(monkeypatch, _document(media), bridge_fail=True)
    receipt = _send(inbox, config_path)
    assert receipt["notice"]["status"] == "undelivered" and receipt["photos"] == []
    assert len(_bridge(calls)) == 1
    with pytest.raises(ConsumerError, match="already has a delivery"):
        _send(inbox, config_path)
    assert len(_bridge(calls)) == 1


def test_second_send_for_same_record_and_destination_is_refused(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install(monkeypatch, _document())
    _send(inbox, config_path)
    with pytest.raises(ConsumerError, match="already has a delivery"):
        _send(inbox, config_path)
    assert len(_bridge(calls)) == 1


def test_prior_outbox_notice_for_the_record_also_refuses(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inbox.claim_reply_delivery(
        f"human-resolution:other-entry:destination:{MGMT_CHAT}", chat_id=MGMT_CHAT,
        reply_to_message_id=None, correlation={"document_id": RECORD_ID, "entry_id": "other-entry"},
    )
    calls = _install(monkeypatch, _document())
    with pytest.raises(ConsumerError, match="already has a delivery"):
        _send(inbox, config_path)
    assert _bridge(calls) == []


def test_check_only_reads_and_resolves_but_sends_and_claims_nothing(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest_a = _jpeg(media_root / "a.jpg", 1)
    calls = _install(monkeypatch, _document([_media(digest_a, ref="/media/tgg/hermes/a.jpg")]))
    receipt = _send(inbox, config_path, check_only=True)
    assert receipt["check_only"] is True and receipt["photo_digests"] == [digest_a]
    assert _bridge(calls) == [] and _rows(inbox) == []


def test_quoted_reply_to_the_question_correlates_to_the_record(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest_a = _jpeg(media_root / "a.jpg", 1)
    digest_b = _jpeg(media_root / "b.jpg", 2)
    media = [_media(digest_a, ref="/media/tgg/hermes/a.jpg"), _media(digest_b, ref="/media/tgg/hermes/b.jpg")]
    _install(monkeypatch, _document(media))
    _send(inbox, config_path)

    def reply(message_id: str, quoted: str) -> InboxRecord:
        return InboxRecord(
            seq=1, message_id=message_id, chat_id=MGMT_CHAT, start_offset=0, end_offset=1,
            raw={"messageId": message_id, "chatId": MGMT_CHAT, "body": "AM/JOB/2610/0001",
                 "quotedMessageId": quoted, "fromMe": False},
        )

    assert inbox.management_document_correlation(reply("reply-1", "WA-1")) == {
        "document_id": RECORD_ID,
        "document_entry_id": ENTRY_ID,
        "outbound_notice_id": "WA-1",
        "reply_message_id": "reply-1",
        "confidence": "quoted_outbound_notice_exact",
    }
    # Krisleen may swipe-reply to a photo instead of the captioned question.
    assert inbox.management_document_correlation(reply("reply-2", "WA-2")) == {
        "document_id": RECORD_ID,
        "document_entry_id": ENTRY_ID,
        "outbound_notice_id": "WA-2",
        "reply_message_id": "reply-2",
        "confidence": "quoted_outbound_notice_exact",
        "question_text": QUESTION,
    }
    # A message that is not one of ours still has no correlation.
    assert inbox.management_document_correlation(reply("reply-3", "WA-other")) is None


def test_photo_rows_never_become_the_initial_notice_anchor(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest_a = _jpeg(media_root / "a.jpg", 1)
    digest_b = _jpeg(media_root / "b.jpg", 2)
    digest_c = _jpeg(media_root / "c.jpg", 3)
    media = [
        _media(digest_a, ref="/media/tgg/hermes/a.jpg"),
        _media(digest_b, ref="/media/tgg/hermes/b.jpg"),
        _media(digest_c, ref="/media/tgg/hermes/c.jpg"),
    ]
    _install(monkeypatch, _document(media))
    _send(inbox, config_path)
    # Even if a photo row sorted first, the lookup must skip it.
    with inbox.connect() as conn:
        conn.execute(
            "UPDATE reply_deliveries SET created_at='0000' WHERE delivery_key LIKE '%:photo:%'"
        )
    assert inbox.initial_management_document_notice(chat_id=MGMT_CHAT, document_id=RECORD_ID) == {
        "delivery_key": f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}",
        "message_id": "WA-1",
        "body": QUESTION,
    }
    assert inbox.management_document_delivery_status(entry_id=ENTRY_ID, chat_id=MGMT_CHAT) == "delivered"


def test_photo_key_without_photo_marker_is_still_a_key_mismatch(inbox: DurableInbox) -> None:
    key = f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}:photo:1"
    inbox.claim_reply_delivery(
        key, chat_id=MGMT_CHAT, reply_to_message_id=None,
        correlation={"document_id": RECORD_ID, "entry_id": ENTRY_ID},
    )
    inbox.record_reply_delivery(key, status="delivered", bridge_message_id="WA-x")
    reply = InboxRecord(
        seq=1, message_id="reply-x", chat_id=MGMT_CHAT, start_offset=0, end_offset=1,
        raw={"messageId": "reply-x", "chatId": MGMT_CHAT, "quotedMessageId": "WA-x", "fromMe": False},
    )
    with pytest.raises(ConsumerError, match="key mismatch"):
        inbox.management_document_correlation(reply)


def test_cli_runs_check_then_send_without_enabling_the_drain(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install(monkeypatch, _document())
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    question = tmp_path / "question.txt"
    question.write_text(QUESTION + "\n", encoding="utf-8")
    argv = [
        "management-question-send", "--record-id", RECORD_ID, "--chat-id", MGMT_CHAT,
        "--question-file", str(question), "--inbox", str(inbox.db_path),
        "--config", str(config_path), "--systems-url", SYSTEMS, "--tenant", "tgg",
        "--media-resolver-command", "",
    ]
    assert consumer.main(argv + ["--check"]) == 0
    assert _bridge(calls) == []
    assert consumer.main(argv) == 0
    assert _bridge(calls) == [(f"{BRIDGE}/send", {"chatId": MGMT_CHAT, "message": QUESTION})]
    assert consumer.main(argv) == 2
    assert len(_bridge(calls)) == 1
    assert consumer._management_document_event_config(config_path) is None


# ---------------------------------------------------------------------------
# Review fixes: the record is visible in the reply turn; replies to
# Christopher's follow-up correlate; --check shows each photo.
# ---------------------------------------------------------------------------

GATE_CHANGED_AT = "2026-07-21T04:00:00+00:00"
FRESH_TS = "2026-07-21T04:01:00+00:00"
RECORD_LINE = f"This reply answers the human-resolution record with record id {RECORD_ID}."


def _whatsapp_adapter():
    import asyncio
    from unittest.mock import MagicMock

    from gateway.config import Platform
    from gateway.platforms.whatsapp import WhatsAppAdapter

    adapter = WhatsAppAdapter.__new__(WhatsAppAdapter)
    adapter.platform = Platform.WHATSAPP
    adapter.config = MagicMock()
    adapter.config.extra = {}
    adapter._reply_prefix = None
    adapter._running = True
    adapter._message_handler = None
    adapter._active_sessions = {}
    adapter._pending_messages = {}
    adapter._background_tasks = set()
    adapter._auto_tts_disabled_chats = set()
    adapter._message_queue = asyncio.Queue()
    adapter._http_session = MagicMock()
    adapter._mention_patterns = []
    adapter._dm_policy = "open"
    adapter._allow_from = set()
    adapter._group_policy = "open"
    adapter._group_allow_from = set()
    return adapter


def _gateway_runner():
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(
        platforms={Platform.WHATSAPP: PlatformConfig(enabled=True)},
    )
    runner.adapters = {}
    runner._model = "openai/gpt-4.1-mini"
    runner._base_url = None
    return runner


def _inbound(message_id: str, quoted_id: str, *, quoted_text: str, correlation: dict | None) -> dict:
    data = {
        "messageId": message_id,
        "chatId": MGMT_CHAT,
        "senderId": "6591234567@s.whatsapp.net",
        "senderName": "Krisleen",
        "chatName": "TGG Management",
        "isGroup": True,
        "body": "AM/JOB/2610/0418",
        "hasMedia": False,
        "mediaUrls": [],
        "quotedMessageId": quoted_id,
        "hasQuotedMessage": True,
        "quotedText": quoted_text,
        "timestamp": FRESH_TS,
    }
    if correlation is not None:
        data["_hermes_pa_context"] = {"management_document_correlation": correlation}
    return data


async def _rendered_model_input(data: dict) -> str:
    event = await _whatsapp_adapter()._build_message_event(data, bypass_require_mention=True)
    assert event is not None
    text = await _gateway_runner()._prepare_inbound_message_text(
        event=event, source=event.source, history=[],
    )
    assert text is not None
    return text


def _correlation(reply_id: str, notice_id: str) -> dict:
    return {
        "document_id": RECORD_ID, "document_entry_id": ENTRY_ID,
        "outbound_notice_id": notice_id, "reply_message_id": reply_id,
        "confidence": "quoted_outbound_notice_exact",
    }


@pytest.mark.asyncio
async def test_correlated_reply_turn_shows_the_record_id_outside_the_body() -> None:
    text = await _rendered_model_input(
        _inbound("reply-1", "WA-1", quoted_text=QUESTION, correlation=_correlation("reply-1", "WA-1"))
    )
    assert RECORD_LINE in text
    assert text.startswith(f'[Replying to: "{RECORD_LINE}\n{QUESTION}"]')
    # A quote of the captioned question renders exactly as before.
    assert "Your question was" not in text
    # The human's words stay exactly as written, after the reply context.
    assert text.endswith("AM/JOB/2610/0418")


@pytest.mark.asyncio
async def test_correlated_reply_to_an_uncaptioned_photo_still_shows_the_record_id() -> None:
    text = await _rendered_model_input(
        _inbound("reply-2", "WA-2", quoted_text="", correlation=_correlation("reply-2", "WA-2"))
    )
    assert text.startswith(f'[Replying to: "{RECORD_LINE}"]')


@pytest.mark.asyncio
async def test_uncorrelated_reply_turn_has_no_record_id() -> None:
    text = await _rendered_model_input(
        _inbound("reply-3", "WA-other", quoted_text=QUESTION, correlation=None)
    )
    assert RECORD_ID not in text and "human-resolution record" not in text
    assert text.startswith(f'[Replying to: "{QUESTION}"]')


def test_turn_bundle_renders_the_record_line_in_its_quote() -> None:
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource
    from gateway.config import Platform

    adapter = _whatsapp_adapter()
    source = SessionSource(
        platform=Platform.WHATSAPP, chat_id=MGMT_CHAT, chat_name="TGG Management",
        chat_type="group", user_id="6591234567@s.whatsapp.net", user_name="Krisleen",
    )
    correlated = _inbound("reply-1", "WA-1", quoted_text=QUESTION, correlation=_correlation("reply-1", "WA-1"))
    plain = _inbound("m2", "", quoted_text="", correlation=None)
    events = [
        MessageEvent(text=item["body"], message_type=MessageType.TEXT, source=source,
                     raw_message=item, message_id=item["messageId"])
        for item in (correlated, plain)
    ]
    bundled = adapter._build_turn_event(events)
    assert RECORD_LINE in bundled.text
    assert bundled.text.count(RECORD_ID) == 1


def _deliver_follow_up(inbox: DurableInbox, config_path: Path, *, correlations: dict | None) -> dict:
    reply = InboxRecord(
        seq=1, message_id="reply-1", chat_id=MGMT_CHAT, start_offset=0, end_offset=1,
        raw={"messageId": "reply-1", "chatId": MGMT_CHAT, "timestamp": FRESH_TS,
             "body": "not sure", "quotedMessageId": "WA-1", "fromMe": False},
    )
    return consumer.deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[{
            "message_id": "replay-1", "kind": "send",
            "args": [MGMT_CHAT, "Which job number is it, Krisleen?"],
            "kwargs": {"reply_to": "reply-1"}, "delivery_mode": "capture",
        }],
        batch_records=[reply],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=[{"message_ids": ["reply-1"], "turn_id": "turn-1"}],
        document_correlations=correlations,
    )


def _quoting(message_id: str, quoted: str) -> InboxRecord:
    return InboxRecord(
        seq=2, message_id=message_id, chat_id=MGMT_CHAT, start_offset=0, end_offset=1,
        raw={"messageId": message_id, "chatId": MGMT_CHAT, "body": "AM/JOB/2610/0418",
             "quotedMessageId": quoted, "fromMe": False},
    )


def test_reply_to_christophers_follow_up_correlates_to_the_record(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install(monkeypatch, _document())
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    _send(inbox, config_path)  # question WA-1
    first = inbox.management_document_correlation(_quoting("reply-1", "WA-1"))
    assert first is not None
    summary = _deliver_follow_up(inbox, config_path, correlations={"reply-1": first})
    assert summary["delivered"] == 1
    assert _bridge(calls)[-1][0] == f"{BRIDGE}/send"  # follow-up WA-2
    assert inbox.management_document_correlation(_quoting("reply-2", "WA-2")) == {
        "document_id": RECORD_ID,
        "document_entry_id": ENTRY_ID,
        "outbound_notice_id": "WA-2",
        "reply_message_id": "reply-2",
        "confidence": "quoted_outbound_notice_exact",
        "question_text": QUESTION,
    }
    # The question stays the only quote anchor the outbox would use.
    assert inbox.initial_management_document_notice(chat_id=MGMT_CHAT, document_id=RECORD_ID) == {
        "delivery_key": f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}",
        "message_id": "WA-1",
        "body": QUESTION,
    }
    [row] = [row for row in _rows(inbox) if ":follow-up:" in row["delivery_key"]]
    assert row["delivery_key"] == f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}:follow-up:WA-2"
    stored = json.loads(row["correlation_json"])
    assert stored["notice_part"] == "follow_up" and "notice_body" not in stored
    assert stored["follow_up_delivery_key"] == f"{MGMT_CHAT}::reply-1"


def test_reply_in_an_uncorrelated_turn_registers_no_follow_up(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _document())
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    _send(inbox, config_path)
    summary = _deliver_follow_up(inbox, config_path, correlations=None)
    assert summary["delivered"] == 1
    assert not [row for row in _rows(inbox) if ":follow-up:" in row["delivery_key"]]
    assert inbox.management_document_correlation(_quoting("reply-2", "WA-2")) is None


def test_follow_up_is_not_registered_when_turn_names_two_documents() -> None:
    other = dict(_correlation("reply-9", "WA-9"), document_id="another-record", document_entry_id="e-2")
    assert consumer._follow_up_document_correlation(
        {"reply-1": _correlation("reply-1", "WA-1"), "reply-9": other}, "m-unrelated",
    ) is None
    assert consumer._follow_up_document_correlation(
        {"reply-1": _correlation("reply-1", "WA-1"), "reply-9": other}, "reply-9",
    ) == other


def test_check_lists_each_photo_in_send_order_with_caption_marker(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest_a = _jpeg(media_root / "a.jpg", 1)
    digest_b = _jpeg(media_root / "b.jpg", 2)
    media = [
        dict(_media(digest_a, ref="/media/tgg/hermes/a.jpg"), messageId="M-A", ordinal=0),
        dict(_media(digest_b, ref="/media/tgg/hermes/b.jpg"), messageId="M-B", ordinal=1),
    ]
    document = _document(media)
    document["data"]["document"]["record"]["evidenceMessages"] = [
        {"messageId": "M-A", "senderId": "muthu@lid", "sgt": "2026-10-04T19:42:38+08:00"},
        {"messageId": "M-B", "senderId": "muthu@lid", "sgt": "2026-10-04T19:42:54+08:00"},
    ]
    calls = _install(monkeypatch, document)
    receipt = _send(inbox, config_path, check_only=True)
    assert receipt["photo_order"] == "record_evidence_order"
    assert receipt["photos_planned"] == [
        {"send_order": 0, "carries_caption": True, "message_id": "M-A", "media_ordinal": 0,
         "mime": "image/jpeg", "sender_id": "muthu@lid", "sent_at": "2026-10-04T19:42:38+08:00",
         "digest": digest_a},
        {"send_order": 1, "carries_caption": False, "message_id": "M-B", "media_ordinal": 1,
         "mime": "image/jpeg", "sender_id": "muthu@lid", "sent_at": "2026-10-04T19:42:54+08:00",
         "digest": digest_b},
    ]
    assert _bridge(calls) == [] and _rows(inbox) == []


QUESTION_LINE = f"Your question was: {QUESTION}"


@pytest.mark.asyncio
async def test_photo_quote_renders_record_line_and_the_question_text(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest_a = _jpeg(media_root / "a.jpg", 1)
    digest_b = _jpeg(media_root / "b.jpg", 2)
    _install(monkeypatch, _document([
        _media(digest_a, ref="/media/tgg/hermes/a.jpg"), _media(digest_b, ref="/media/tgg/hermes/b.jpg"),
    ]))
    _send(inbox, config_path)  # question WA-1 (captioned photo), photo WA-2
    photo = inbox.management_document_correlation(_quoting("reply-c3", "WA-2"))
    text = await _rendered_model_input(_inbound("reply-c3", "WA-2", quoted_text="", correlation=photo))
    assert text == f'[Replying to: "{RECORD_LINE}\n{QUESTION_LINE}"]\n\nAM/JOB/2610/0418'
    question = inbox.management_document_correlation(_quoting("reply-c1", "WA-1"))
    assert "question_text" not in question
    text = await _rendered_model_input(_inbound("reply-c1", "WA-1", quoted_text=QUESTION, correlation=question))
    assert text == f'[Replying to: "{RECORD_LINE}\n{QUESTION}"]\n\nAM/JOB/2610/0418'


@pytest.mark.asyncio
async def test_follow_up_quote_renders_the_question_text(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _document())
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    _send(inbox, config_path)  # question WA-1
    first = inbox.management_document_correlation(_quoting("reply-1", "WA-1"))
    _deliver_follow_up(inbox, config_path, correlations={"reply-1": first})  # follow-up WA-2
    follow_up = inbox.management_document_correlation(_quoting("reply-2", "WA-2"))
    text = await _rendered_model_input(_inbound(
        "reply-2", "WA-2", quoted_text="Which job number is it, Krisleen?", correlation=follow_up,
    ))
    assert text.startswith(
        f'[Replying to: "{RECORD_LINE}\n{QUESTION_LINE}\nWhich job number is it, Krisleen?"]'
    )


@pytest.mark.asyncio
async def test_long_question_is_shortened_so_the_record_line_survives_the_cut() -> None:
    long_question = "Which job is this? " + "detail " * 120
    correlation = dict(_correlation("reply-9", "WA-9"), question_text=long_question)
    data = _inbound("reply-9", "WA-9", quoted_text="", correlation=correlation)
    context = _whatsapp_adapter()._reply_context_text(data)
    assert len(context) <= 500
    record_line, question_line = context.split("\n")
    assert record_line == RECORD_LINE
    assert question_line.startswith("Your question was: Which job is this? detail") and question_line.endswith("…")
    text = await _rendered_model_input(data)
    assert text.startswith(f'[Replying to: "{context}"]')
