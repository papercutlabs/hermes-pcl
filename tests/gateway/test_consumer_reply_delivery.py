"""Management-selector reply delivery (teren 2026-07-21 ruling) tests.

The contract under test:
- delivery keys on the inbound chat's SELECTOR class: management only —
  a site/ingest-selector response must NEVER deliver (negative test);
- at-most-once per inbound WhatsApp message via a durable pre-send claim;
- a bridge refusal / transport failure marks undelivered and never raises
  into the ingest path, and never retries.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from gateway import durable_jsonl_consumer as consumer
from gateway.config import Platform, PlatformConfig

from gateway.durable_jsonl_consumer import (
    DurableInbox,
    InboxRecord,
    _management_typing_presence,
    _management_selector_chats,
    _replay_messages_with_retained_documents,
    _deliver_management_document_notice,
    _internal_management_document_message,
    _normalize_management_document_captured_outbound,
    _parse_captured_send,
    _pa75_capture_reply_batch,
    _run_management_document_turn,
    deliver_management_replies,
    process_live_records,
    process_management_document_canary_event,
    process_management_document_events,
)

MGMT_CHAT = "120363426509183563@g.us"
PRODUCTION_MGMT_CHAT = "120363407903158826@g.us"
SITE_CHAT = "120363421424519051@g.us"
GATE_CHANGED_AT = "2026-07-21T04:00:00+00:00"
FRESH_TS = "2026-07-21T04:01:00+00:00"
STALE_TS = "2026-07-21T03:59:00+00:00"


@pytest.fixture()
def config_path(tmp_path: Path) -> Path:
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
        f"    source.chat_id: {MGMT_CHAT}\n"
        "- job_type: tgg_management\n"
        "  match:\n"
        "    source.platform: telegram\n"
        "    source.chat_id: '-5295904349'\n",
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text(
        f"pa:\n  enabled: true\n  constitution_path: {constitution}\n",
        encoding="utf-8",
    )
    return config


@pytest.fixture()
def inbox(tmp_path: Path) -> DurableInbox:
    return DurableInbox(tmp_path / "inbox.db")


def _captured(chat_id: str, content: str = "reply text", reply_to: str | None = "MSG1") -> dict:
    return {
        "message_id": "replay-1",
        "kind": "send",
        "args": [chat_id, content],
        "kwargs": {"reply_to": reply_to} if reply_to else {},
        "delivery_mode": "capture",
    }


def _record(
    chat_id: str, message_id: str = "MSG1", timestamp: str = FRESH_TS
) -> InboxRecord:
    return InboxRecord(
        seq=1,
        message_id=message_id,
        chat_id=chat_id,
        start_offset=0,
        end_offset=1,
        raw={"messageId": message_id, "chatId": chat_id, "timestamp": timestamp},
    )


def _handled(*message_ids: str) -> list[dict]:
    return [{"message_ids": list(message_ids), "turn_id": "turn-current"}]


def _enable_media_retention(
    config_path: Path, media_root: Path, *, ref_prefix: str = "/media"
) -> None:
    import yaml
    data = yaml.safe_load(config_path.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True,
        "media_root": str(media_root),
        "source_roots": [str(media_root)],
        "operation": "tgg_media_retention",
        "media_ref_prefix": ref_prefix,
    }
    config_path.write_text(yaml.safe_dump(data), encoding="utf-8")


def _captured_images(chat_id: str, paths: list[Path], reply_to: str = "MSG1") -> dict:
    return {
        "message_id": "replay-media",
        "kind": "send_multiple_images",
        "args": [chat_id, [[f"file://{path}", f"photo {i}"] for i, path in enumerate(paths)]],
        "kwargs": {"reply_to": reply_to},
        "delivery_mode": "capture",
    }


def _captured_document(
    chat_id: str, path: Path, *, caption: str | None = None, reply_to: str = "MSG1"
) -> dict:
    return {
        "message_id": "replay-document",
        "kind": "send_document",
        "args": [chat_id, str(path)],
        "kwargs": {"caption": caption, "reply_to": reply_to},
        "delivery_mode": "capture",
    }


def test_selector_chats_are_whatsapp_management_only(config_path: Path) -> None:
    chats = _management_selector_chats(config_path)
    assert chats == frozenset({MGMT_CHAT})


def test_parse_extracts_send_and_rejects_other_kinds() -> None:
    parsed = _parse_captured_send(_captured(MGMT_CHAT))
    assert parsed == {
        "chat_id": MGMT_CHAT,
        "content": "reply text",
        "reply_to": "MSG1",
        "workspace_owner": None,
    }
    assert _parse_captured_send({**_captured(MGMT_CHAT), "kind": "send_image"}) is None
    assert _parse_captured_send({"kind": "send", "args": [], "kwargs": {}}) is None


def _document_entry(entry_id: str, *, created_at: int = 100, kind: str = "initial_default") -> dict:
    return {
        "id": entry_id,
        "recordId": "record-1",
        "entryKind": kind,
        "createdAt": created_at,
        "body": {"statement": "I need a decision."},
        "effects": [{"caseJobNo": "AM/JOB/2608/1234", "effectRole": "provisional"}],
    }


def _canary_event(entry_id: str = "pa75-canary-entry:batch3") -> dict:
    source = {"id": "record-1", "sourceRunRef": "read-only:batch3", "evidence": ["msg-1"]}
    source_sha = __import__("hashlib").sha256(
        (json.dumps(source, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    event = {
        "contract": "tgg-pa75-typed-canary-event/v1",
        "id": "pa75-canary:batch3",
        "destination_chat_id": MGMT_CHAT,
        "entry": _document_entry(entry_id),
        "source_record_projection": source,
        "source_record_projection_sha256": source_sha,
    }
    event["event_sha256"] = __import__("hashlib").sha256(
        (json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    return event


def _backend15_empty_effects_event() -> dict:
    """Match the real Backend15 initial-default shape without copying its source text."""
    event = _canary_event(
        "pa75-canary-entry:backend15-review-boundary-20260911-04095a4acc004f5394037935"
    )
    event.update({
        "id": "pa75-canary:backend15-review-boundary-20260911-04095a4acc004f5394037935",
        "source_record_id": "1af369d2-4fe7-44c2-ab85-96a446c1517e",
        "source_entry_id": "aa0a274c-a2ed-4a3a-b568-8afc2f5e4166",
        "test_attempt_id": "backend15-review-boundary-20260911",
    })
    event["entry"].update({
        "recordId": event["source_record_id"],
        "sourceEntryId": event["source_entry_id"],
        "effects": [],
    })
    event["source_record_projection"] = {
        "recordId": event["source_record_id"],
        "initialEntryId": event["source_entry_id"],
        "kind": "unknown_case_identity",
    }
    event["source_record_projection_sha256"] = __import__("hashlib").sha256(
        (json.dumps(event["source_record_projection"], ensure_ascii=False, sort_keys=True,
                    separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    event["event_sha256"] = __import__("hashlib").sha256(
        (json.dumps({key: value for key, value in event.items() if key != "event_sha256"},
                    ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    return event


def _enable_document_events(monkeypatch: pytest.MonkeyPatch, *, chat_id: str = MGMT_CHAT) -> None:
    monkeypatch.setenv("TGG_MANAGEMENT_DOCUMENT_API_URL", "http://systems.test")
    monkeypatch.setenv("TGG_MANAGEMENT_DOCUMENT_CHAT_ID", chat_id)
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")


@pytest.mark.asyncio
async def test_document_event_drains_typed_outbox_without_creating_whatsapp_ingress(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    calls: list[tuple[str, dict | None]] = []

    def fake_urlopen(request, timeout=0):
        body = json.loads(request.data) if request.data else None
        calls.append((request.full_url, body))
        if request.full_url.startswith("http://systems.test/"):
            return _FakeResponse({"data": {"contract": "tgg-human-resolution-document-entry/v1", "entries": [_document_entry("entry-1")]}})
        return _FakeResponse({"success": True, "messageId": "WA-notice-1"})

    async def fake_turn(entry, **kwargs):
        assert entry["id"] == "entry-1"
        assert kwargs["destination_chat_id"] == MGMT_CHAT
        return [_captured(MGMT_CHAT, "Could you confirm which case this workbook updates?", reply_to=None)]

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", fake_turn)
    result = await process_management_document_events(
        inbox, config_path=config_path, runner=object()
    )
    assert result == {"examined": 1, "delivered": 1, "undelivered": 0, "skipped": 0}
    assert inbox.management_document_cursor() == (100, "entry-1")
    assert inbox.total() == 0
    assert calls == [
        ("http://systems.test/api/operator/human-resolution-document-entries?limit=100", None),
        ("http://127.0.0.1:3011/send", {"chatId": MGMT_CHAT, "message": "Could you confirm which case this workbook updates?"}),
    ]
    with inbox.connect() as conn:
        row = conn.execute("SELECT delivery_key,reply_to_message_id,status,bridge_message_id FROM reply_deliveries").fetchone()
    assert tuple(row) == (
        "human-resolution:entry-1:destination:120363426509183563@g.us",
        None,
        "delivered",
        "WA-notice-1",
    )


@pytest.mark.asyncio
async def test_pa75_typed_canary_event_uses_real_notice_delivery_without_poll_or_cursor(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    calls: list[str] = []

    def fake_urlopen(request, timeout=0):
        calls.append(request.full_url)
        assert request.full_url == "http://127.0.0.1:3011/send"
        return _FakeResponse({"success": True, "messageId": "WA-canary-1"})

    async def fake_turn(entry, **kwargs):
        assert entry["id"] == "pa75-canary-entry:batch3"
        assert kwargs["destination_chat_id"] == MGMT_CHAT
        assert kwargs["canary_projection"]["source_record_projection_sha256"]
        return [_captured(MGMT_CHAT, "Which cases should I update from this workbook?", reply_to=None)]

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", fake_turn)
    event = _canary_event("pa75-canary-entry:batch3")
    result = await process_management_document_canary_event(
        inbox, config_path=config_path, runner=object(), event=event,
    )
    assert result == {
        "contract": "tgg-pa75-typed-canary-event-receipt/v1",
        "canary_event_id": "pa75-canary:batch3",
        "canary_event_sha256": event["event_sha256"],
        "record_id": "record-1",
        "entry_id": "pa75-canary-entry:batch3",
        "destination_chat_id": MGMT_CHAT,
        "delivery_outcome": "delivered",
        "outbound_count": 1,
    }
    assert calls == ["http://127.0.0.1:3011/send"]
    assert inbox.management_document_cursor() is None
    assert inbox.total() == 0


@pytest.mark.asyncio
async def test_pa75_typed_canary_accepts_backend15_initial_default_with_no_effects(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    calls: list[str] = []

    def fake_urlopen(request, timeout=0):
        calls.append(request.full_url)
        return _FakeResponse({"success": True, "messageId": "WA-backend15-canary"})

    async def fake_turn(entry, **kwargs):
        assert entry["recordId"] == "1af369d2-4fe7-44c2-ab85-96a446c1517e"
        assert entry["sourceEntryId"] == "aa0a274c-a2ed-4a3a-b568-8afc2f5e4166"
        assert entry["effects"] == []
        return [_captured(MGMT_CHAT, "Has this report been reviewed?", reply_to=None)]

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", fake_turn)
    event = _backend15_empty_effects_event()
    result = await process_management_document_canary_event(
        inbox, config_path=config_path, runner=object(), event=event,
    )
    assert result["record_id"] == "1af369d2-4fe7-44c2-ab85-96a446c1517e"
    assert result["entry_id"] == event["entry"]["id"]
    assert result["delivery_outcome"] == "delivered"
    assert result["outbound_count"] == 1
    assert calls == ["http://127.0.0.1:3011/send"]


def test_pa75_canary_cli_uses_real_notice_path_and_immutable_receipt(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
) -> None:
    _enable_document_events(monkeypatch)
    event = _canary_event("pa75-canary-entry:cli")
    event_path = tmp_path / "event.json"
    receipt_path = tmp_path / "receipts" / "canary.json"
    event_path.write_text(json.dumps(event), encoding="utf-8")
    calls: list[str] = []

    def fake_urlopen(request, timeout=0):
        calls.append(request.full_url)
        return _FakeResponse({"success": True, "messageId": "WA-cli-1"})

    async def fake_turn(entry, **kwargs):
        assert kwargs["runner"] is fake_runner
        return [_captured(MGMT_CHAT, "Canary question", reply_to=None)]

    fake_runner = object()
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda path: fake_runner)
    monkeypatch.setattr(consumer, "_run_management_document_turn", fake_turn)
    assert consumer.main([
        "management-document-canary", "--event", str(event_path), "--inbox", str(inbox.db_path),
        "--config", str(config_path), "--receipt", str(receipt_path),
        "--activity-lock-file", str(tmp_path / "activity.lock"),
    ]) == 0
    receipt = json.loads(receipt_path.read_text())
    assert receipt["canary_event_sha256"] == event["event_sha256"]
    assert calls == ["http://127.0.0.1:3011/send"]
    assert inbox.management_document_cursor() is None
    # Exact retry reads the immutable receipt; it does not construct a runner
    # or re-enter the bridge/model path.
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda path: pytest.fail("runner reopened"))
    assert consumer.main([
        "management-document-canary", "--event", str(event_path), "--inbox", str(inbox.db_path),
        "--config", str(config_path), "--receipt", str(receipt_path),
    ]) == 0
    assert calls == ["http://127.0.0.1:3011/send"]
    assert json.loads(capsys.readouterr().out.splitlines()[-1]) == receipt


def test_pa75_canary_cli_refuses_divergent_receipt(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, capsys
) -> None:
    event = _canary_event()
    event_path = tmp_path / "event.json"
    receipt_path = tmp_path / "receipt.json"
    event_path.write_text(json.dumps(event), encoding="utf-8")
    receipt_path.write_text(json.dumps({"contract": "other"}), encoding="utf-8")
    assert consumer.main([
        "management-document-canary", "--event", str(event_path), "--inbox", str(inbox.db_path),
        "--config", str(config_path), "--receipt", str(receipt_path),
    ]) == 2
    assert "receipt collision is divergent" in capsys.readouterr().err

    # Matching event IDs alone cannot bless a malformed or cross-destination
    # terminal receipt and suppress the real canary path.
    receipt_path.write_text(json.dumps({
        "contract": "tgg-pa75-typed-canary-event-receipt/v1",
        "canary_event_id": event["id"],
        "canary_event_sha256": event["event_sha256"],
        "record_id": event["entry"]["recordId"],
        "entry_id": event["entry"]["id"],
        "destination_chat_id": "production-chat@g.us",
        "delivery_outcome": "delivered",
        "outbound_count": 1,
    }), encoding="utf-8")
    assert consumer.main([
        "management-document-canary", "--event", str(event_path), "--inbox", str(inbox.db_path),
        "--config", str(config_path), "--receipt", str(receipt_path),
    ]) == 2
    assert "receipt collision is divergent" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_pa75_claimed_reply_persists_raw_mutation_receipt_before_terminal(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    event = _canary_event()
    projection = {
        "contract": "tgg-pa75-typed-canary-projection/v1",
        "event_id": event["id"], "event_sha256": event["event_sha256"],
        "entry": event["entry"], "source_record_projection": event["source_record_projection"],
        "source_record_projection_sha256": event["source_record_projection_sha256"],
        "event_payload": {key: value for key, value in event.items() if key != "event_sha256"},
    }
    config_path.write_text(config_path.read_text() + "model:\n  provider: test-provider\n  default: test-model\n")
    source = tmp_path / "reply.jsonl"
    source.write_text(json.dumps({"messageId": "reply-1", "chatId": MGMT_CHAT, "timestamp": FRESH_TS}) + "\n")
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    records = inbox.pending(limit=1)
    monkeypatch.setenv("TGG_PA75_CAPTURE_RECEIPT_DIR", str(tmp_path / "capture-receipts"))
    monkeypatch.setattr(inbox, "management_document_correlation", lambda record: {"pa75_canary_projection": projection})
    monkeypatch.setattr(consumer, "ensure_record_media_retained", lambda *args, **kwargs: None)
    monkeypatch.setattr(consumer, "_inject_bounded_source_evidence", lambda *args, **kwargs: None)

    async def fake_process(records, **kwargs):
        assert kwargs["capture_business_writes_for_test_management"] is True
        return {
            "submitted_message_ids": ["reply-1"],
            "handled": [{"message_ids": ["reply-1"], "turn_id": "turn-canary"}],
            "captured_outbound": [],
            "captured_business_mutations": [{"operation": "captured-only"}],
        }

    original_finish = inbox.finish_processed_batch
    def receipt_before_finish(records, **kwargs):
        receipt_files = list((tmp_path / "capture-receipts").glob("*.json"))
        assert len(receipt_files) == 1
        receipt = json.loads(receipt_files[0].read_text())
        assert receipt["message_ids"] == ["reply-1"]
        assert receipt["provider"] == "test-provider"
        assert receipt["captured_business_mutations"] == [{"operation": "captured-only"}]
        return original_finish(records, **kwargs)

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(inbox, "finish_processed_batch", receipt_before_finish)
    await consumer._process_claimed_chat_batch(
        inbox, records, config_path=config_path, state_db=tmp_path / "state.db",
        case_db=tmp_path / "case.db", source_before_image_dir=tmp_path / "before",
        gate_changed_at=GATE_CHANGED_AT, runner=object(),
    )
    assert inbox.counts() == {"completed": 1}


def test_pa75_raw_capture_receipt_is_idempotent_and_refuses_divergence(
    config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    event = _canary_event()
    projection = {
        "contract": "tgg-pa75-typed-canary-projection/v1", "event_id": event["id"],
        "event_sha256": event["event_sha256"], "entry": event["entry"],
        "source_record_projection": event["source_record_projection"],
        "source_record_projection_sha256": event["source_record_projection_sha256"],
        "event_payload": {key: value for key, value in event.items() if key != "event_sha256"},
    }
    config_path.write_text(config_path.read_text() + "model:\n  provider: test-provider\n  default: test-model\n")
    monkeypatch.setenv("TGG_PA75_CAPTURE_RECEIPT_DIR", str(tmp_path / "receipts"))
    record = _record(MGMT_CHAT, "reply-1")
    result = {"handled": [{"message_ids": ["reply-1"], "turn_id": "turn-1"}], "captured_business_mutations": []}
    correlations = {"reply-1": {"pa75_canary_projection": projection}}
    first = consumer._pa75_capture_receipt(records=[record], correlations=correlations, result=result, config_path=config_path)
    assert consumer._pa75_capture_receipt(records=[record], correlations=correlations, result=result, config_path=config_path) == first
    with pytest.raises(consumer.ConsumerError, match="collision is divergent"):
        consumer._pa75_capture_receipt(
            records=[record], correlations=correlations,
            result={**result, "captured_business_mutations": [{"different": True}]}, config_path=config_path,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate,reason",
    [
        (lambda event: event["entry"].pop("body"), "entry projection is incomplete"),
        (lambda event: event["entry"].pop("effects"), "entry projection is incomplete"),
        (lambda event: event["entry"].update(effects=None), "entry projection is incomplete"),
        (lambda event: event.update(source_record_projection_sha256="0" * 64), "source projection hash mismatch"),
        (lambda event: event["entry"].update(id="3d781e49-242e-46ea-951c-1cec68675953"), "entry ID must use the canary namespace"),
    ],
)
async def test_pa75_typed_canary_event_rejects_incomplete_or_colliding_projection(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch, mutate, reason: str,
) -> None:
    _enable_document_events(monkeypatch)
    event = _canary_event()
    mutate(event)
    with pytest.raises(consumer.ConsumerError, match=reason):
        await process_management_document_canary_event(
            inbox, config_path=config_path, runner=object(), event=event,
        )


def test_pa75_capture_selection_requires_authenticated_quoted_canary_correlation() -> None:
    event = _canary_event()
    projection = {
        "contract": "tgg-pa75-typed-canary-projection/v1",
        "event_id": event["id"], "event_sha256": event["event_sha256"],
        "entry": event["entry"],
        "source_record_projection": event["source_record_projection"],
        "source_record_projection_sha256": event["source_record_projection_sha256"],
        "event_payload": {key: value for key, value in event.items() if key != "event_sha256"},
    }
    record = _record(MGMT_CHAT, "reply-1")
    correlation = {"reply-1": {"pa75_canary_projection": projection}}
    assert _pa75_capture_reply_batch([record], correlation) is True
    assert _pa75_capture_reply_batch([record], {}) is False  # bare test-chat message
    forged = {"reply-1": {"pa75_canary_projection": {**projection, "event_sha256": "0" * 64}}}
    assert _pa75_capture_reply_batch([record], forged) is False
    divergent = {"reply-1": {"pa75_canary_projection": {
        **projection, "entry": {**projection["entry"], "body": {"statement": "forged"}},
    }}}
    assert _pa75_capture_reply_batch([record], divergent) is False
    assert _pa75_capture_reply_batch([_record("120363407903158826@g.us", "reply-1")], correlation) is False


@pytest.mark.asyncio
async def test_document_event_restart_does_not_repeat_prior_send(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    sends = 0
    document_requests: list[str] = []

    def fake_urlopen(request, timeout=0):
        nonlocal sends
        if request.full_url.startswith("http://systems.test/"):
            document_requests.append(request.full_url)
            return _FakeResponse({"data": {"contract": "tgg-human-resolution-document-entry/v1", "entries": [_document_entry("entry-1", created_at=100)]}})
        sends += 1
        return _FakeResponse({"success": True, "messageId": "WA-notice-1"})

    async def fake_turn(entry, **kwargs):
        return [_captured(MGMT_CHAT, "A natural question", reply_to=None)]

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", fake_turn)
    assert (await process_management_document_events(inbox, config_path=config_path, runner=object()))["delivered"] == 1
    # A new process sees the persisted cursor and gets an empty exclusive page.
    def empty_page(request, timeout=0):
        assert "after_created_at=100" in request.full_url and "after_id=entry-1" in request.full_url
        return _FakeResponse({"data": {"contract": "tgg-human-resolution-document-entry/v1", "entries": []}})
    monkeypatch.setattr("urllib.request.urlopen", empty_page)
    assert await process_management_document_events(inbox, config_path=config_path, runner=object()) == {
        "examined": 0, "delivered": 0, "undelivered": 0, "skipped": 0,
    }
    assert sends == 1


@pytest.mark.asyncio
async def test_document_event_unknown_bridge_outcome_is_terminal_and_never_retried(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    bridge_calls = 0

    def fake_urlopen(request, timeout=0):
        nonlocal bridge_calls
        if request.full_url.startswith("http://systems.test/"):
            return _FakeResponse({"data": {"contract": "tgg-human-resolution-document-entry/v1", "entries": [_document_entry("entry-202")]}})
        bridge_calls += 1
        return _FakeResponse({"success": False, "outcome": "unknown", "retrySafe": False}, status=202)

    async def fake_turn(entry, **kwargs):
        return [_captured(MGMT_CHAT, "A natural question", reply_to=None)]

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", fake_turn)
    result = await process_management_document_events(inbox, config_path=config_path, runner=object())
    assert result == {"examined": 1, "delivered": 0, "undelivered": 1, "skipped": 0}
    assert inbox.management_document_cursor() == (100, "entry-202")
    assert inbox.reply_delivery_status(
        "human-resolution:entry-202:destination:120363426509183563@g.us"
    ) == "undelivered"
    assert bridge_calls == 1


@pytest.mark.asyncio
async def test_document_event_crash_after_send_advances_cursor_without_reopening_session(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    inbox.claim_reply_delivery(
        "human-resolution:entry-crash", chat_id=MGMT_CHAT, reply_to_message_id=None,
    )
    inbox.record_reply_delivery(
        "human-resolution:entry-crash", status="delivered", bridge_message_id="WA-previous",
    )

    def fake_urlopen(request, timeout=0):
        assert request.full_url.startswith("http://systems.test/")
        return _FakeResponse({"data": {"contract": "tgg-human-resolution-document-entry/v1", "entries": [_document_entry("entry-crash")]}})

    async def forbidden_turn(*args, **kwargs):
        raise AssertionError("already-terminal document entry must not reopen Christopher")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", forbidden_turn)
    result = await process_management_document_events(inbox, config_path=config_path, runner=object())
    assert result == {"examined": 1, "delivered": 1, "undelivered": 0, "skipped": 0}
    assert inbox.management_document_cursor() == (100, "entry-crash")


def _seed_initial_notice(inbox: DurableInbox) -> None:
    inbox.claim_reply_delivery(
        "human-resolution:entry-initial", chat_id=MGMT_CHAT, reply_to_message_id=None,
        correlation={
            "document_id": "record-1", "entry_id": "entry-initial",
            "entry_kind": "initial_default", "notice_body": "Which workbook should I apply?",
        },
    )
    inbox.record_reply_delivery(
        "human-resolution:entry-initial", status="delivered", bridge_message_id="WA-initial",
    )


def test_initial_pre_send_body_survives_confirmation_crash_for_lifecycle_quote(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    # This is the exact crash window: provider confirmation has been recorded,
    # but no separate post-send metadata write ever ran.
    _seed_initial_notice(inbox)
    sent: list[dict] = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request, timeout=0: (sent.append(json.loads(request.data)) or _FakeResponse({"success": True, "messageId": "WA-amendment"})),
    )
    config = consumer._management_document_event_config(config_path)
    assert config is not None
    assert _deliver_management_document_notice(
        inbox, config=config, entry=_document_entry("entry-crash-recovery", kind="amendment"),
        captured_outbound=[_captured(MGMT_CHAT, "The scope has been corrected.", reply_to="human-resolution-document-entry:entry-crash-recovery")],
    ) == "delivered"
    assert sent[0]["replyTo"]["body"] == "Which workbook should I apply?"


def test_lifecycle_closeout_quotes_only_the_stored_initial_notice(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    _seed_initial_notice(inbox)
    sent: list[dict] = []
    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": "WA-closure"})
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    config = consumer._management_document_event_config(config_path)
    assert config is not None
    entry = _document_entry("entry-closure", created_at=101, kind="closure")
    result = _deliver_management_document_notice(
        inbox, config=config, entry=entry,
        captured_outbound=[_captured(MGMT_CHAT, "I have updated the 82 cases.", reply_to="human-resolution-document-entry:entry-closure")],
    )
    assert result == "delivered"
    assert sent == [{
        "chatId": MGMT_CHAT,
        "message": "I have updated the 82 cases.",
        "replyTo": {"messageId": "WA-initial", "body": "Which workbook should I apply?", "fromMe": True},
    }]


def test_lifecycle_wrong_notice_is_suppressed_before_bridge(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    _seed_initial_notice(inbox)
    monkeypatch.setattr("urllib.request.urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("bridge must not be called")))
    config = consumer._management_document_event_config(config_path)
    assert config is not None
    with pytest.raises(consumer.ConsumerError, match="invalid lifecycle notice anchor"):
        _deliver_management_document_notice(
            inbox, config=config, entry=_document_entry("entry-wrong", kind="amendment"),
            captured_outbound=[_captured(MGMT_CHAT, "Wrong quote", reply_to="WA-unrelated")],
        )


def test_first_lifecycle_notice_in_a_destination_is_fresh_and_becomes_anchor(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    sent: list[dict] = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request, timeout=0: (
            sent.append(json.loads(request.data)) or _FakeResponse({"success": True, "messageId": "WA-first"})
        ),
    )
    config = consumer._management_document_event_config(config_path)
    assert config is not None
    assert _deliver_management_document_notice(
        inbox,
        config=config,
        entry=_document_entry("entry-no-initial", kind="closure"),
        captured_outbound=[_captured(MGMT_CHAT, "I need a decision.", reply_to=None)],
    ) == "delivered"
    assert sent == [{"chatId": MGMT_CHAT, "message": "I need a decision."}]
    anchor = inbox.initial_management_document_notice(chat_id=MGMT_CHAT, document_id="record-1")
    assert anchor and anchor["message_id"] == "WA-first"


def test_lifecycle_duplicate_and_unknown_outcome_never_resend(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    _seed_initial_notice(inbox)
    calls = 0
    def fake_urlopen(request, timeout=0):
        nonlocal calls
        calls += 1
        return _FakeResponse({"success": False, "outcome": "unknown", "retrySafe": False}, status=202)
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    config = consumer._management_document_event_config(config_path)
    assert config is not None
    entry = _document_entry("entry-202-close", kind="closure")
    outbound = [_captured(MGMT_CHAT, "Close out", reply_to="human-resolution-document-entry:entry-202-close")]
    assert _deliver_management_document_notice(inbox, config=config, entry=entry, captured_outbound=outbound) == "undelivered"
    assert _deliver_management_document_notice(inbox, config=config, entry=entry, captured_outbound=outbound) == "undelivered"
    assert calls == 1


@pytest.mark.asyncio
async def test_source_fired_closure_runs_turn_and_quotes_initial_notice(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    _seed_initial_notice(inbox)
    sent: list[dict] = []
    def fake_urlopen(request, timeout=0):
        if request.full_url.startswith("http://systems.test/"):
            return _FakeResponse({"data": {"contract": "tgg-human-resolution-document-entry/v1", "entries": [_document_entry("entry-source-closure", created_at=101, kind="closure")]}})
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": "WA-closure"})
    async def fake_turn(entry, **kwargs):
        assert entry["entryKind"] == "closure"
        return [_captured(MGMT_CHAT, "I have now updated the cases.", reply_to="human-resolution-document-entry:entry-source-closure")]
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", fake_turn)
    assert await process_management_document_events(inbox, config_path=config_path, runner=object()) == {
        "examined": 1, "delivered": 1, "undelivered": 0, "skipped": 0,
    }
    assert sent[0]["replyTo"]["messageId"] == "WA-initial"


@pytest.mark.asyncio
async def test_test_chat_anchor_does_not_block_fresh_production_notice_and_later_quote(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A historical test-chat notice is not a production quote anchor."""
    constitution = config_path.parent / "constitution.yaml"
    constitution.write_text(
        constitution.read_text()
        + "- job_type: tgg_management\n"
        + "  match:\n"
        + "    source.platform: whatsapp\n"
        + f"    source.chat_id: {PRODUCTION_MGMT_CHAT}\n",
        encoding="utf-8",
    )
    _enable_document_events(monkeypatch, chat_id=PRODUCTION_MGMT_CHAT)
    _seed_initial_notice(inbox)  # Existing immutable test-chat history.
    sent: list[dict] = []

    def fake_urlopen(request, timeout=0):
        if request.full_url.startswith("http://systems.test/"):
            return _FakeResponse({"data": {"contract": "tgg-human-resolution-document-entry/v1", "entries": [
                _document_entry("entry-production-first", kind="closure"),
            ]}})
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": f"WA-production-{len(sent)}"})

    async def fake_turn(entry, **kwargs):
        assert kwargs["destination_chat_id"] == PRODUCTION_MGMT_CHAT
        assert kwargs["fresh_initial_notice"] is True
        return [_captured(PRODUCTION_MGMT_CHAT, "Please decide where this belongs.", reply_to=None)]

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", fake_turn)
    assert await process_management_document_events(inbox, config_path=config_path, runner=object()) == {
        "examined": 1, "delivered": 1, "undelivered": 0, "skipped": 0,
    }
    assert sent == [{"chatId": PRODUCTION_MGMT_CHAT, "message": "Please decide where this belongs."}]

    config = consumer._management_document_event_config(config_path)
    assert config is not None
    later = _document_entry("entry-production-later", created_at=101, kind="closure")
    assert _deliver_management_document_notice(
        inbox,
        config=config,
        entry=later,
        captured_outbound=[_captured(
            PRODUCTION_MGMT_CHAT,
            "Recorded against the selected case.",
            reply_to="human-resolution-document-entry:entry-production-later",
        )],
    ) == "delivered"
    assert sent[-1]["replyTo"] == {
        "messageId": "WA-production-1",
        "body": "Please decide where this belongs.",
        "fromMe": True,
    }


@pytest.mark.asyncio
async def test_existing_lifecycle_delivery_suppresses_later_poll_turn(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _enable_document_events(monkeypatch)
    inbox.claim_reply_delivery(
        "human-resolution:entry-already-closed", chat_id=MGMT_CHAT, reply_to_message_id="WA-initial",
        correlation={"document_id": "record-1", "entry_id": "entry-already-closed", "entry_kind": "closure"},
    )
    inbox.record_reply_delivery("human-resolution:entry-already-closed", status="delivered", bridge_message_id="WA-closure")
    def fake_urlopen(request, timeout=0):
        return _FakeResponse({"data": {"contract": "tgg-human-resolution-document-entry/v1", "entries": [_document_entry("entry-already-closed", kind="closure")]}})
    async def forbidden_turn(*args, **kwargs):
        raise AssertionError("already delivered lifecycle entry must not recreate a turn")
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("gateway.durable_jsonl_consumer._run_management_document_turn", forbidden_turn)
    assert await process_management_document_events(inbox, config_path=config_path, runner=object()) == {
        "examined": 1, "delivered": 1, "undelivered": 0, "skipped": 0,
    }


def test_document_event_model_input_is_internal_not_a_capture_envelope() -> None:
    message = _internal_management_document_message(_document_entry("entry-1"), chat_id=MGMT_CHAT)
    assert message["senderId"] == "system@internal"
    assert message["isGroup"] is True
    assert message["chatName"] == "TGG Management"
    assert message["metadata"]["contract"] == "tgg_management_document_event/v1"
    assert "Decision-ready brief:" in message["body"]
    assert '"document_id": "record-1"' in message["body"]
    assert message["metadata"]["decision_ready_brief"] == {
        "document_id": "record-1",
        "entry_id": "entry-1",
        "entry_kind": "initial_default",
        "body": {"statement": "I need a decision."},
        "effects": [{"caseJobNo": "AM/JOB/2608/1234", "effectRole": "provisional"}],
    }
    assert "sourceKey" not in message and "source_key" not in message
    assert message["messageId"].startswith("human-resolution-document-entry:")


def test_document_event_reaches_real_whatsapp_group_gate() -> None:
    """Internal management input must not be rejected as a system DM."""
    from gateway.platforms.whatsapp import WhatsAppAdapter

    adapter = object.__new__(WhatsAppAdapter)
    adapter.platform = Platform.WHATSAPP
    adapter.config = PlatformConfig(enabled=True, extra={
        "dm_policy": "allowlist",
        "allow_from": [],
        "group_policy": "allowlist",
        "group_allow_from": [MGMT_CHAT],
        "require_mention": True,
    })
    adapter._dm_policy = "allowlist"
    adapter._allow_from = WhatsAppAdapter._coerce_allow_list([])
    adapter._group_policy = "allowlist"
    adapter._group_allow_from = WhatsAppAdapter._coerce_allow_list([MGMT_CHAT])
    adapter._mention_patterns = adapter._compile_mention_patterns()
    adapter._free_response_chats = adapter._whatsapp_free_response_chats()

    message = _internal_management_document_message(_document_entry("entry-1"), chat_id=MGMT_CHAT)
    assert adapter._should_process_message(message, bypass_require_mention=True) is True

    dm_shaped = {**message, "isGroup": False}
    assert adapter._should_process_message(dm_shaped, bypass_require_mention=True) is False


def test_initial_document_transport_strips_only_its_synthetic_reply_anchor() -> None:
    entry = _document_entry("entry-initial")
    synthetic_anchor = "human-resolution-document-entry:entry-initial"
    captured = _captured(MGMT_CHAT, "Could you confirm the workbook?", reply_to=synthetic_anchor)

    normalized = _normalize_management_document_captured_outbound(entry, [captured])

    assert _parse_captured_send(normalized[0])["reply_to"] is None
    assert _parse_captured_send(captured)["reply_to"] == synthetic_anchor


def test_lifecycle_transport_rewrites_current_event_anchor_to_delivered_initial_notice(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_document_events(monkeypatch)
    _seed_initial_notice(inbox)
    entry = _document_entry("entry-amendment", kind="amendment")
    synthetic_anchor = "human-resolution-document-entry:entry-amendment"
    normalized = _normalize_management_document_captured_outbound(
        entry, [_captured(MGMT_CHAT, "I have updated the cases.", reply_to=synthetic_anchor)],
    )
    assert _parse_captured_send(normalized[0])["reply_to"] == synthetic_anchor
    sent: list[dict] = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request, timeout=0: (sent.append(json.loads(request.data)) or _FakeResponse({"success": True, "messageId": "WA-amendment"})),
    )
    config = consumer._management_document_event_config(config_path)
    assert config is not None
    assert _deliver_management_document_notice(
        inbox, config=config, entry=entry, captured_outbound=normalized,
    ) == "delivered"
    assert sent == [{
        "chatId": MGMT_CHAT,
        "message": "I have updated the cases.",
        "replyTo": {"messageId": "WA-initial", "body": "Which workbook should I apply?", "fromMe": True},
    }]


def test_quoted_management_reply_maps_to_its_document_without_interpreting_body(
    inbox: DurableInbox,
) -> None:
    inbox.claim_reply_delivery(
        "human-resolution:entry-1",
        chat_id=MGMT_CHAT,
        reply_to_message_id=None,
        correlation={"document_id": "record-1", "entry_id": "entry-1"},
    )
    inbox.record_reply_delivery(
        "human-resolution:entry-1", status="delivered", bridge_message_id="WA-notice-1",
    )
    reply = _record(MGMT_CHAT, "reply-1")
    reply.raw.update({"body": "yes, please do it", "quotedMessageId": "WA-notice-1", "fromMe": False})
    assert inbox.management_document_correlation(reply) == {
        "document_id": "record-1",
        "document_entry_id": "entry-1",
        "outbound_notice_id": "WA-notice-1",
        "reply_message_id": "reply-1",
        "confidence": "quoted_outbound_notice_exact",
    }


def test_bare_management_yes_has_no_document_correlation(inbox: DurableInbox) -> None:
    inbox.claim_reply_delivery(
        "human-resolution:entry-1",
        chat_id=MGMT_CHAT,
        reply_to_message_id=None,
        correlation={"document_id": "record-1", "entry_id": "entry-1"},
    )
    inbox.record_reply_delivery(
        "human-resolution:entry-1", status="delivered", bridge_message_id="WA-notice-1",
    )
    reply = _record(MGMT_CHAT, "bare-yes")
    reply.raw.update({"body": "yes", "fromMe": False})
    assert inbox.management_document_correlation(reply) is None


@pytest.mark.asyncio
async def test_correlated_reply_reaches_recreated_management_session_as_metadata(
    config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    correlation = {
        "document_id": "record-1", "document_entry_id": "entry-1",
        "outbound_notice_id": "WA-notice-1", "reply_message_id": "reply-1",
        "confidence": "quoted_outbound_notice_exact",
    }
    record = _record(MGMT_CHAT, "reply-1")
    record.raw.update({"body": "Please update the cases.", "quotedMessageId": "WA-notice-1"})
    rendered = _replay_messages_with_retained_documents(
        [record], config_path=config_path,
        management_document_correlations={"reply-1": correlation},
    )
    assert rendered[0]["_hermes_pa_context"]["management_document_correlation"] == correlation

    monkeypatch.setattr("gateway.durable_jsonl_consumer.configured_engine", lambda _: ("openai-codex", "gpt-5.6-terra"))
    class Runner:
        def __init__(self): self.plans = []
        async def replay(self, plan):
            self.plans.append(plan)
            return type("Result", (), {"processed": 1, "outbound": []})()
    runner = Runner()
    state_db = tmp_path / "state.db"
    await process_live_records(
        [record], config_path=config_path, state_db=state_db, persistent_session=True,
        runner=runner, management_document_correlations={"reply-1": correlation},
    )
    assert runner.plans[0].replay_namespace == "agent:live-drain:conversation"
    assert runner.plans[0].messages[0]["_hermes_pa_context"]["management_document_correlation"] == correlation


@pytest.mark.asyncio
async def test_document_event_recreates_the_same_persistent_management_session(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("gateway.durable_jsonl_consumer.configured_engine", lambda _: ("openai-codex", "gpt-5.6-terra"))

    class Runner:
        def __init__(self): self.plans = []
        async def replay(self, plan):
            self.plans.append(plan)
            return type("Result", (), {"outbound": []})()

    runner = Runner()
    await _run_management_document_turn(_document_entry("entry-1"), config_path=config_path, destination_chat_id=MGMT_CHAT, runner=runner)
    await _run_management_document_turn(_document_entry("entry-2", created_at=101), config_path=config_path, destination_chat_id=MGMT_CHAT, runner=runner)
    assert [plan.replay_namespace for plan in runner.plans] == [
        "agent:live-drain:conversation",
        "agent:live-drain:conversation",
    ]
    assert all(plan.live_business_writes is False for plan in runner.plans)


def test_multi_photo_delivery_uses_send_media_and_distinct_durable_keys(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    media_root = tmp_path / "retained"
    media_root.mkdir()
    paths = [media_root / "a.png", media_root / "b.png"]
    for index, path in enumerate(paths):
        path.write_bytes(b"\x89PNG\r\n\x1a\n" + bytes([index]))
    _enable_media_retention(config_path, media_root)
    sent: list[tuple[str, dict]] = []
    def fake_urlopen(request, timeout=0):
        sent.append((request.full_url, json.loads(request.data)))
        return _FakeResponse({"success": True, "messageId": f"WA-{len(sent)}"})
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    kwargs = dict(
        config_path=config_path,
        captured_outbound=[_captured_images(MGMT_CHAT, paths)],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    first = deliver_management_replies(inbox, **kwargs)
    second = deliver_management_replies(inbox, **kwargs)
    assert first["delivered"] == 2
    assert second["duplicate"] == 2
    assert len(sent) == 2
    assert all(url.endswith("/send-media") for url, _ in sent)
    assert [body["filePath"] for _, body in sent] == [str(path) for path in paths]
    assert all(body["chatId"] == MGMT_CHAT and body["replyTo"] == "MSG1" for _, body in sent)
    with inbox.connect() as conn:
        keys = [row[0] for row in conn.execute(
            "SELECT delivery_key FROM reply_deliveries ORDER BY delivery_key"
        )]
    assert len(keys) == 2 and all(key.startswith(f"media::{MGMT_CHAT}::MSG1::") for key in keys)


def test_zip_document_delivery_uses_send_media_with_filename_and_caption(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    media_root = tmp_path / "retained"
    media_root.mkdir()
    report = media_root / "weekly-reports.zip"
    report.write_bytes(b"PK\x03\x04weekly-report")
    _enable_media_retention(config_path, media_root)
    sent: list[tuple[str, dict]] = []

    def fake_urlopen(request, timeout=0):
        sent.append((request.full_url, json.loads(request.data)))
        return _FakeResponse({"success": True, "messageId": "WA-ZIP"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[
            _captured_document(
                MGMT_CHAT,
                report,
                caption="Weekly report for 10–15 August 2026.",
            )
        ],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary == {
        "delivered": 1,
        "undelivered": 0,
        "suppressed": 0,
        "duplicate": 0,
    }
    assert sent == [
        (
            "http://127.0.0.1:3011/send-media",
            {
                "chatId": MGMT_CHAT,
                "replyTo": "MSG1",
                "filePath": str(report),
                "mediaType": "document",
                "fileName": "weekly-reports.zip",
                "caption": "Weekly report for 10–15 August 2026.",
            },
        )
    ]


def test_sandbox_document_uses_captured_workspace_owner(
    inbox: DurableInbox,
    config_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools.python_sandbox_tool import _workspace_key

    owner = "stable-gateway-owner"
    work = tmp_path / "sandbox_workspaces" / _workspace_key(owner) / "work"
    work.mkdir(parents=True)
    report = work / "report.xlsx"
    report.write_bytes(b"PK\x03\x04edited-workbook")
    monkeypatch.setattr(
        "tools.python_sandbox_tool.get_hermes_home", lambda: tmp_path
    )
    sent: list[tuple[str, dict]] = []

    def fake_urlopen(request, timeout=0):
        sent.append((request.full_url, json.loads(request.data)))
        return _FakeResponse({"success": True, "messageId": "WA-WORKBOOK"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    captured = _captured_document(MGMT_CHAT, Path("work/report.xlsx"))
    captured["workspace_owner"] = owner
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[captured],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary["delivered"] == 1
    assert sent == [
        (
            "http://127.0.0.1:3011/send-media",
            {
                "chatId": MGMT_CHAT,
                "replyTo": "MSG1",
                "filePath": str(report),
                "mediaType": "document",
                "fileName": "report.xlsx",
            },
        )
    ]


def test_missing_sandbox_document_records_undelivered_without_media_request(
    inbox: DurableInbox,
    config_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "tools.python_sandbox_tool.get_hermes_home", lambda: tmp_path
    )
    requests: list[tuple[str, dict]] = []

    def fake_urlopen(request, timeout=0):
        requests.append((request.full_url, json.loads(request.data)))
        assert not request.full_url.endswith("/send-media")
        return _FakeResponse({"success": True, "messageId": "WA-FAILURE-NOTE"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    captured = _captured_document(MGMT_CHAT, Path("work/missing.xlsx"))
    captured["workspace_owner"] = "stable-gateway-owner"
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[captured],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary["undelivered"] == 1
    assert [url.rsplit("/", 1)[-1] for url, _ in requests] == ["send"]
    assert "exact files[].path" in requests[0][1]["message"]
    with inbox.connect() as conn:
        rows = conn.execute(
            "SELECT delivery_key,status,error FROM reply_deliveries ORDER BY delivery_key"
        ).fetchall()
    refused = [row for row in rows if row["delivery_key"].startswith("media-refused::")]
    assert len(refused) == 1
    assert refused[0]["status"] == "undelivered"
    assert "sandbox attachment refused" in refused[0]["error"]


def test_composite_bundle_anchor_resolves_to_handled_component(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A multi-message turn bundle anchors its reply to a synthetic composite
    id ("MSG1+MSG2"). The gate must resolve it to a handled component instead
    of silently suppressing (2026-08-04 incident: every multi-message ask
    composed a reply that never reached the bridge)."""
    sent: list[tuple[str, dict]] = []

    def fake_urlopen(request, timeout=0):
        sent.append((request.full_url, json.loads(request.data)))
        return _FakeResponse({"success": True, "messageId": "WA-1"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT, reply_to="MSG1+MSG2")],
        batch_records=[
            _record(MGMT_CHAT, message_id="MSG1"),
            _record(MGMT_CHAT, message_id="MSG2"),
        ],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1", "MSG2"),
    )
    assert summary["delivered"] == 1
    assert summary["suppressed"] == 0
    # newest component wins the anchor
    assert sent and sent[0][1]["replyTo"] == {"messageId": "MSG2"}


def test_composite_anchor_with_no_handled_component_stays_suppressed(
    inbox: DurableInbox, config_path: Path
) -> None:
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT, reply_to="MSGX+MSGY")],
        batch_records=[_record(MGMT_CHAT, message_id="MSG1")],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert summary["delivered"] == 0
    assert summary["suppressed"] == 1


def test_streamed_media_directive_becomes_one_native_media_send(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    media_root = tmp_path / "retained"
    media_root.mkdir()
    photo = media_root / "case-photo.jpg"
    photo.write_bytes(b"\xff\xd8\xffcase-photo")
    _enable_media_retention(config_path, media_root)
    sent: list[tuple[str, dict]] = []

    def fake_urlopen(request, timeout=0):
        sent.append((request.full_url, json.loads(request.data)))
        return _FakeResponse({"success": True, "messageId": "WA-MEDIA-DIRECTIVE"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[
            _captured(
                MGMT_CHAT,
                f"SK/JOB/2606/2372 — first retained photo:\n\nMEDIA:{photo}",
            )
        ],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary == {
        "delivered": 1,
        "undelivered": 0,
        "suppressed": 0,
        "duplicate": 0,
    }
    assert sent == [
        (
            "http://127.0.0.1:3011/send-media",
            {
                "chatId": MGMT_CHAT,
                "replyTo": "MSG1",
                "filePath": str(photo),
                "mediaType": "image",
                "caption": "SK/JOB/2606/2372 — first retained photo:",
            },
        )
    ]


def test_failed_media_send_delivers_answer_as_separate_text_fallback(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from urllib.error import HTTPError

    media_root = tmp_path / "retained"
    media_root.mkdir()
    photo = media_root / "case-photo.jpg"
    photo.write_bytes(b"\xff\xd8\xffcase-photo")
    _enable_media_retention(config_path, media_root)
    sent: list[tuple[str, dict]] = []

    def fake_urlopen(request, timeout=0):
        payload = json.loads(request.data)
        sent.append((request.full_url, payload))
        if request.full_url.endswith("/send-media"):
            raise HTTPError(request.full_url, 403, "unreadable", {}, None)
        return _FakeResponse({"success": True, "messageId": "WA-TEXT-FALLBACK"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    answer = "PG/JOB/2607/0702 — WhatsApp says the basin was replaced."
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT, f"{answer}\n\nMEDIA:{photo}")],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary == {
        "delivered": 1,
        "undelivered": 1,
        "suppressed": 0,
        "duplicate": 0,
    }
    assert [url.rsplit("/", 1)[-1] for url, _ in sent] == ["send-media", "send"]
    assert sent[1][1]["message"] == (
        answer + "\n\nI couldn't send one or more of the selected images."
    )
    with inbox.connect() as conn:
        rows = conn.execute(
            "SELECT delivery_key,status FROM reply_deliveries ORDER BY delivery_key"
        ).fetchall()
    assert {(row["delivery_key"].split("::", 1)[0], row["status"]) for row in rows} == {
        ("media", "undelivered"),
        ("media-fallback", "delivered"),
    }


def test_later_media_failure_sends_only_attachment_note_when_caption_arrived(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from urllib.error import HTTPError

    media_root = tmp_path / "retained"
    media_root.mkdir()
    photos = [media_root / "a.jpg", media_root / "b.jpg"]
    for path in photos:
        path.write_bytes(b"\xff\xd8\xffcase-photo" + path.name.encode())
    _enable_media_retention(config_path, media_root)
    sent: list[tuple[str, dict]] = []

    def fake_urlopen(request, timeout=0):
        payload = json.loads(request.data)
        sent.append((request.full_url, payload))
        if request.full_url.endswith("/send-media") and len(sent) == 2:
            raise HTTPError(request.full_url, 403, "unreadable", {}, None)
        return _FakeResponse({"success": True, "messageId": f"WA-{len(sent)}"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    answer = "PG/JOB/2607/0702 — these are the useful photos."
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[
            _captured(
                MGMT_CHAT,
                answer + "\n\n" + "\n".join(f"MEDIA:{path}" for path in photos),
            )
        ],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary["delivered"] == 2
    assert summary["undelivered"] == 1
    assert sent[-1][0].endswith("/send")
    assert sent[-1][1]["message"] == "I couldn't send one or more of the selected images."


def test_streamed_case_media_ref_resolves_to_retained_file_and_native_send(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    media_root = tmp_path / "retained"
    media_root.mkdir()
    basename = "export_backfill_hg_7F263A59C4E0C211C37C29A1.jpg"
    photo = media_root / basename
    photo.write_bytes(b"\xff\xd8\xffcase-photo")
    _enable_media_retention(
        config_path, media_root, ref_prefix="/media/tgg/hermes"
    )
    sent: list[tuple[str, dict]] = []

    def fake_urlopen(request, timeout=0):
        sent.append((request.full_url, json.loads(request.data)))
        return _FakeResponse({"success": True, "messageId": "WA-CASE-MEDIA-REF"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[
            _captured(
                MGMT_CHAT,
                "SK/JOB/2606/2372 — first retained photo:\n\n"
                f"MEDIA:/media/tgg/hermes/{basename}",
            )
        ],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary == {
        "delivered": 1,
        "undelivered": 0,
        "suppressed": 0,
        "duplicate": 0,
    }
    assert sent == [
        (
            "http://127.0.0.1:3011/send-media",
            {
                "chatId": MGMT_CHAT,
                "replyTo": "MSG1",
                "filePath": str(photo),
                "mediaType": "image",
                "caption": "SK/JOB/2606/2372 — first retained photo:",
            },
        )
    ]


@pytest.mark.parametrize(
    "media_ref",
    [
        "/media/tgg/hermes/../case-photo.jpg",
        "/media/tgg/hermes/nested/case-photo.jpg",
        "/media/tgg/hermes/case-photo.jpg?download=1",
        "/media/tgg/hermes/case-photo.jpg#fragment",
        "/media/tgg/other/case-photo.jpg",
    ],
)
def test_streamed_case_media_ref_refuses_non_opaque_or_mismatched_reference(
    media_ref: str,
    inbox: DurableInbox,
    config_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    media_root = tmp_path / "retained"
    media_root.mkdir()
    (media_root / "case-photo.jpg").write_bytes(b"\xff\xd8\xffcase-photo")
    _enable_media_retention(
        config_path, media_root, ref_prefix="/media/tgg/hermes"
    )
    calls: list = []
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: calls.append(a))

    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT, f"MEDIA:{media_ref}")],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary == {
        "delivered": 0,
        "undelivered": 0,
        "suppressed": 1,
        "duplicate": 0,
    }
    assert not calls


def test_streamed_media_directive_outside_retained_root_is_not_sent_as_text(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    media_root = tmp_path / "retained"
    media_root.mkdir()
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"\xff\xd8\xffoutside")
    _enable_media_retention(config_path, media_root)
    calls: list = []
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: calls.append(a))

    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT, f"MEDIA:{outside}")],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )

    assert summary == {
        "delivered": 0,
        "undelivered": 0,
        "suppressed": 1,
        "duplicate": 0,
    }
    assert not calls


def test_media_delivery_refuses_non_management_missing_and_path_escape(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    media_root = tmp_path / "retained"
    media_root.mkdir()
    valid = media_root / "valid.png"
    valid.write_bytes(b"\x89PNG\r\n\x1a\nvalid")
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"\x89PNG\r\n\x1a\noutside")
    _enable_media_retention(config_path, media_root)
    calls = []
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: calls.append(a))
    captured = [
        _captured_images(SITE_CHAT, [valid]),
        _captured_images(MGMT_CHAT, [outside]),
        _captured_images(MGMT_CHAT, [media_root / "missing.png"]),
    ]
    summary = deliver_management_replies(
        inbox, config_path=config_path, captured_outbound=captured,
        batch_records=[_record(SITE_CHAT), _record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT, handled_groups=_handled("MSG1"),
    )
    assert summary["suppressed"] == 3
    assert not calls


def test_media_unknown_outcome_is_not_retried(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    media_root = tmp_path / "retained"
    media_root.mkdir()
    path = media_root / "photo.png"
    path.write_bytes(b"\x89PNG\r\n\x1a\nphoto")
    _enable_media_retention(config_path, media_root)
    calls = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request, timeout=0: calls.append(request) or _FakeResponse(
            {"outcome": "unknown", "retrySafe": False}, status=202
        ),
    )
    kwargs = dict(
        config_path=config_path, captured_outbound=[_captured_images(MGMT_CHAT, [path])],
        batch_records=[_record(MGMT_CHAT)], gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert deliver_management_replies(inbox, **kwargs)["undelivered"] == 1
    assert deliver_management_replies(inbox, **kwargs)["duplicate"] == 1
    assert len(calls) == 1
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT status,provider_outcome FROM reply_deliveries"
        ).fetchone()
    assert (row["status"], row["provider_outcome"]) == ("undelivered", "unknown")


def test_site_selector_response_never_delivers(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *a, **k: calls.append(a) or (_ for _ in ()).throw(AssertionError("no send expected")),
    )
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(SITE_CHAT)],
        batch_records=[_record(SITE_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert summary == {"delivered": 0, "undelivered": 0, "suppressed": 1, "duplicate": 0}
    assert not calls


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


def test_mgmt_delivery_is_at_most_once(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list = []

    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": "WAMSG9"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    kwargs = dict(
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT)],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    first = deliver_management_replies(inbox, **kwargs)
    second = deliver_management_replies(inbox, **kwargs)
    assert first["delivered"] == 1 and second["delivered"] == 0
    assert second["duplicate"] == 1
    assert len(sent) == 1
    assert sent[0] == {
        "chatId": MGMT_CHAT,
        "message": "reply text",
        "replyTo": {"messageId": "MSG1"},
    }


def test_reply_quote_payload_carries_anchor_sender_and_body(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bridge renders the quote from replyTo.participant/body; a bare
    messageId produces a phantom "[message]" quote (2026-08-06 incident)."""
    sent: list = []

    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": "WAMSG10"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    record = InboxRecord(
        seq=1,
        message_id="MSG1",
        chat_id=MGMT_CHAT,
        start_offset=0,
        end_offset=1,
        raw={
            "messageId": "MSG1",
            "chatId": MGMT_CHAT,
            "timestamp": FRESH_TS,
            "senderId": "230407865937940@lid",
            "body": "idk what to eat for lunch",
            "fromMe": False,
        },
    )
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT)],
        batch_records=[record],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert summary["delivered"] == 1
    assert sent[0]["replyTo"] == {
        "messageId": "MSG1",
        "participant": "230407865937940@lid",
        "body": "idk what to eat for lunch",
    }


def test_media_anchor_body_falls_back_to_media_type(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list = []

    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": "WAMSG11"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    record = InboxRecord(
        seq=1,
        message_id="MSG1",
        chat_id=MGMT_CHAT,
        start_offset=0,
        end_offset=1,
        raw={
            "messageId": "MSG1",
            "chatId": MGMT_CHAT,
            "timestamp": FRESH_TS,
            "senderId": "230407865937940@lid",
            "body": "",
            "mediaType": "image",
        },
    )
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT)],
        batch_records=[record],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert summary["delivered"] == 1
    assert sent[0]["replyTo"] == {
        "messageId": "MSG1",
        "participant": "230407865937940@lid",
        "body": "[image]",
    }


def test_distinct_renderings_for_same_anchor_still_deliver_only_once(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list = []

    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": f"WAMSG{len(sent)}"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[
            _captured(MGMT_CHAT, content="first answer"),
            _captured(MGMT_CHAT, content="second answer"),
        ],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert summary["delivered"] == 1 and summary["duplicate"] == 1
    assert len(sent) == 1
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT delivery_key FROM reply_deliveries"
        ).fetchone()
    assert row["delivery_key"] == f"{MGMT_CHAT}::MSG1"


def test_indeterminate_202_outcome_marks_undelivered(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request, timeout=0: _FakeResponse(
            {"outcome": "unknown", "retrySafe": False}, status=202
        ),
    )
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT)],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert summary == {"delivered": 0, "undelivered": 1, "suppressed": 0, "duplicate": 0}
    # claim consumed: no retry ever re-sends the indeterminate message
    retry = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT)],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert retry["duplicate"] == 1 and retry["delivered"] == 0


def test_bridge_refusal_marks_undelivered_and_never_raises(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from urllib.error import HTTPError

    def refusing_urlopen(request, timeout=0):
        raise HTTPError(
            "http://bridge/send", 403, "refused", {}, None  # type: ignore[arg-type]
        )

    monkeypatch.setattr("urllib.request.urlopen", refusing_urlopen)
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT)],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert summary["undelivered"] == 1
    # the claim consumed the key: a retry never re-sends
    retry = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT)],
        batch_records=[_record(MGMT_CHAT)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("MSG1"),
    )
    assert retry == {"delivered": 0, "undelivered": 0, "suppressed": 0, "duplicate": 1}


def test_same_content_uncited_anchor_is_suppressed_but_fresh_cited_anchor_delivers(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list = []

    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": "WAMSG-FRESH"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[
            _captured(MGMT_CHAT, content="same answer", reply_to="STALE-UNCITED"),
            _captured(MGMT_CHAT, content="same answer", reply_to="FRESH-CITED"),
        ],
        batch_records=[
            _record(MGMT_CHAT, "STALE-UNCITED"),
            _record(MGMT_CHAT, "FRESH-CITED"),
        ],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("FRESH-CITED"),
    )
    assert summary == {"delivered": 1, "undelivered": 0, "suppressed": 1, "duplicate": 0}
    assert sent == [
        {
            "chatId": MGMT_CHAT,
            "message": "same answer",
            "replyTo": {"messageId": "FRESH-CITED"},
        }
    ]


def test_preactivation_cited_anchor_is_suppressed(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *a, **k: calls.append(a) or (_ for _ in ()).throw(AssertionError("no send expected")),
    )
    summary = deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=[_captured(MGMT_CHAT, reply_to="PREACTIVATION")],
        batch_records=[_record(MGMT_CHAT, "PREACTIVATION", timestamp=STALE_TS)],
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=_handled("PREACTIVATION"),
    )
    assert summary == {"delivered": 0, "undelivered": 0, "suppressed": 1, "duplicate": 0}
    assert not calls


def test_fresh_cited_anchor_delivers_exactly_once(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list = []

    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return _FakeResponse({"success": True, "messageId": "WAMSG-ONCE"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    kwargs = {
        "config_path": config_path,
        "captured_outbound": [_captured(MGMT_CHAT, reply_to="FRESH")],
        "batch_records": [_record(MGMT_CHAT, "FRESH")],
        "gate_changed_at": GATE_CHANGED_AT,
        "handled_groups": _handled("FRESH"),
    }
    assert deliver_management_replies(inbox, **kwargs)["delivered"] == 1
    assert deliver_management_replies(inbox, **kwargs)["duplicate"] == 1
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_typing_presence_wraps_only_fresh_management_processing(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    presence: list[dict] = []

    def fake_urlopen(request, timeout=0):
        presence.append(json.loads(request.data))
        return _FakeResponse({"success": True})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    records = [
        _record(MGMT_CHAT, "FRESH-MGMT"),
        _record(MGMT_CHAT, "STALE-MGMT", timestamp=STALE_TS),
        _record(SITE_CHAT, "FRESH-SITE"),
    ]
    async with _management_typing_presence(
        records,
        config_path=config_path,
        gate_changed_at=GATE_CHANGED_AT,
        reassert_seconds=60,
    ):
        assert presence == [{"chatId": MGMT_CHAT, "presence": "composing"}]
    assert presence == [
        {"chatId": MGMT_CHAT, "presence": "composing"},
        {"chatId": MGMT_CHAT, "presence": "paused"},
    ]


@pytest.mark.asyncio
async def test_typing_reasserts_and_clears_on_failure(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    presence: list[dict] = []

    def fake_urlopen(request, timeout=0):
        presence.append(json.loads(request.data))
        return _FakeResponse({"success": True})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    with pytest.raises(RuntimeError, match="delivery exploded"):
        async with _management_typing_presence(
            [_record(MGMT_CHAT, "FRESH-MGMT")],
            config_path=config_path,
            gate_changed_at=GATE_CHANGED_AT,
            reassert_seconds=0.01,
        ):
            import asyncio

            await asyncio.sleep(0.025)
            raise RuntimeError("delivery exploded")
    assert [item["presence"] for item in presence].count("composing") >= 2
    assert presence[-1] == {"chatId": MGMT_CHAT, "presence": "paused"}


def test_typing_presence_default_reassert_is_six_seconds() -> None:
    import inspect

    assert (
        inspect.signature(_management_typing_presence)
        .parameters["reassert_seconds"]
        .default
        == 6
    )
