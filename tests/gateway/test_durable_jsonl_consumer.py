import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import zipfile
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path

import pytest
import yaml

from gateway import durable_jsonl_consumer as consumer
from gateway.config import GatewayConfig, Platform
from gateway.replay import replay_context
from gateway.session import SessionSource, SessionStore


def _message(message_id: str, chat_id: str = "test-group@g.us") -> dict:
    return {
        "messageId": message_id,
        "chatId": chat_id,
        "senderId": "fixture-user",
        "senderName": "Fixture User",
        "chatName": "Fixture Chat",
        "isGroup": True,
        "body": "fixture message",
        "hasMedia": False,
        "mediaType": None,
        "mediaUrls": [],
        "timestamp": 100,
        "fromMe": False,
    }


def _write_jsonl(path: Path, values: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(value) + "\n" for value in values),
        encoding="utf-8",
    )


def _write_case_db(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE bridge_message_log (
          local_id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          source_ref TEXT NOT NULL UNIQUE,
          chat_jid TEXT NOT NULL,
          chat_name TEXT,
          zone TEXT,
          channel_type TEXT,
          sender_id TEXT,
          from_me INTEGER,
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
    conn.commit()
    conn.close()
    return path


def _png_bytes(payload: bytes = b"fixture") -> bytes:
    return b"\x89PNG\r\n\x1a\n" + payload


def _xlsx(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Override PartName="/xl/workbook.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.'
                'spreadsheetml.sheet.main+xml"/>'
                "</Types>"
            ),
        )
        archive.writestr("xl/workbook.xml", "<workbook/>")


def _docx(path: Path, *, macro_enabled: bool = False) -> None:
    content_type = (
        "application/vnd.ms-word.document.macroenabled.main+xml"
        if macro_enabled
        else "application/vnd.openxmlformats-officedocument."
        "wordprocessingml.document.main+xml"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Override PartName="/word/document.xml" '
                f'ContentType="{content_type}"/>'
                "</Types>"
            ),
        )
        archive.writestr("word/document.xml", "<document/>")
        if macro_enabled:
            archive.writestr("word/vbaProject.bin", b"macro")


def test_source_projection_uses_exact_stored_capture_envelope_and_is_independent(
    tmp_path, monkeypatch
):
    source = tmp_path / "capture.jsonl"
    envelope = {"normalized": _message("projection-1"), "capture": {"trace": "exact"}}
    _write_jsonl(source, [envelope])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    assert inbox.stage_from_source(source, cursor) == 1
    assert inbox.source_projection_counts() == {
        "source_projection_pending": 1,
        "source_projection_complete": 0,
        "source_projection_held": 0,
    }
    received: list[dict] = []

    def fake_project(_config, *, operation, source_envelope):
        assert operation == "tgg_whatsapp_source_ingest"
        received.append(dict(source_envelope))
        return {"ok": True, "data": {"idempotent": True}}

    monkeypatch.setattr(consumer, "_project_source_envelope", fake_project)
    summary = consumer.project_pending_source_events(
        inbox, config_path=_source_projection_config(tmp_path), limit=10
    )
    assert summary == {"attempted": 1, "complete": 1, "held": 0, "skipped": 0, "disabled": 0}
    assert received == [envelope]
    assert inbox.counts() == {"pending": 1}
    assert inbox.source_projection_counts() == {
        "source_projection_pending": 0,
        "source_projection_complete": 1,
        "source_projection_held": 0,
    }


def test_source_projection_failure_holds_only_that_event_and_later_events_continue(
    tmp_path, monkeypatch
):
    source = tmp_path / "capture.jsonl"
    _write_jsonl(source, [_message("projection-fails"), _message("projection-next")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    inbox.stage_from_source(source, cursor)
    calls: list[str] = []

    def fake_project(_config, *, operation, source_envelope):
        message_id = source_envelope.get("messageId")
        calls.append(str(message_id))
        if message_id == "projection-fails":
            raise consumer.ConsumerError("Systems temporarily unavailable")
        return {"ok": True}

    monkeypatch.setattr(consumer, "_project_source_envelope", fake_project)
    summary = consumer.project_pending_source_events(
        inbox, config_path=_source_projection_config(tmp_path), limit=10
    )
    assert summary == {"attempted": 2, "complete": 1, "held": 1, "skipped": 0, "disabled": 0}
    assert calls == ["projection-fails", "projection-next"]
    assert inbox.counts() == {"pending": 2}
    assert inbox.source_projection_counts() == {
        "source_projection_pending": 0,
        "source_projection_complete": 1,
        "source_projection_held": 1,
    }
    # A retry within the backoff does not spin; the independent business rows
    # remain untouched either way.
    assert consumer.project_pending_source_events(
        inbox, config_path=_source_projection_config(tmp_path), limit=10
    ) == {"attempted": 0, "complete": 0, "held": 0, "skipped": 0, "disabled": 0}


def test_source_projection_marks_non_authoritative_chats_complete_without_calling_systems(
    tmp_path, monkeypatch
):
    source = tmp_path / "capture.jsonl"
    _write_jsonl(source, [_message("management-message", "management@g.us")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    inbox.stage_from_source(source, cursor)
    monkeypatch.setattr(
        consumer, "_project_source_envelope",
        lambda *_args, **_kwargs: pytest.fail("non-authoritative chat reached Systems"),
    )

    summary = consumer.project_pending_source_events(
        inbox, config_path=_source_projection_config(tmp_path), limit=10,
    )

    assert summary == {"attempted": 1, "complete": 0, "held": 0, "skipped": 1, "disabled": 0}
    assert inbox.source_projection_counts()["source_projection_complete"] == 1


def test_source_projection_crash_after_systems_commit_replays_idempotently(
    tmp_path, monkeypatch
):
    source = tmp_path / "capture.jsonl"
    envelope = {"normalized": _message("projection-crash"), "capture": {"trace": "same"}}
    _write_jsonl(source, [envelope])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    inbox.stage_from_source(source, cursor)
    # Model the only ambiguous window: Systems accepted the idempotent source
    # ref, then the consumer exited before its local complete state committed.
    record = inbox.source_projection_candidates(limit=1)[0]
    systems_commits: list[str] = []

    def fake_project(_config, *, operation, source_envelope):
        systems_commits.append(str(source_envelope["normalized"]["messageId"]))
        return {"ok": True, "data": {"idempotent": True}}

    monkeypatch.setattr(consumer, "_project_source_envelope", fake_project)
    consumer._project_source_envelope(
        _source_projection_config(tmp_path),
        operation="tgg_whatsapp_source_ingest",
        source_envelope=record.source_envelope,
    )
    assert inbox.source_projection_counts()["source_projection_pending"] == 1
    assert consumer.project_pending_source_events(
        inbox, config_path=_source_projection_config(tmp_path), limit=1
    )["complete"] == 1
    assert systems_commits == ["projection-crash", "projection-crash"]
    assert inbox.source_projection_counts()["source_projection_complete"] == 1


@pytest.mark.asyncio
async def test_standby_consumer_stages_and_projects_while_business_gate_is_off(
    tmp_path, monkeypatch
):
    source = tmp_path / "capture.jsonl"
    envelope = {"normalized": _message("standby-project"), "capture": {"sequence": 1}}
    _write_jsonl(source, [envelope])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    gate = tmp_path / "gate.json"
    gate.write_text(json.dumps({"version": 1, "enabled": False, "generation": 7}), encoding="utf-8")
    received: list[dict] = []

    def fake_project(_config, *, operation, source_envelope):
        received.append(dict(source_envelope))
        return {"ok": True}

    monkeypatch.setattr(consumer, "_project_source_envelope", fake_project)
    args = SimpleNamespace(
        config=str(_source_projection_config(tmp_path)),
        source=str(source), cursor=str(cursor), inbox=str(tmp_path / "inbox.db"),
        status_file=str(tmp_path / "status.json"), processing_gate=str(gate),
        state_db=str(tmp_path / "state.db"), case_db=str(tmp_path / "case.db"),
        source_before_image_dir=str(tmp_path / "before"), lock_file=str(tmp_path / "lock"),
        activity_lock_file=None, site_concurrency=1, chat_batch_size=2,
        retention_batch_size=2, source_projection_batch_size=10,
        poll_seconds=0.01, max_records=10, once=True,
    )
    assert await consumer.run_consumer(args) == 0
    assert received == [envelope]
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    assert inbox.counts() == {"pending": 1}
    assert inbox.source_projection_counts()["source_projection_complete"] == 1
    status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert status["state"] == "standby"
    assert status["source_opened"] is True
    assert status["cursor_advanced"] is True


def _retention_config(tmp_path: Path, source_root: Path, media_root: Path) -> Path:
    path = tmp_path / "retention-config.yaml"
    path.write_text(yaml.safe_dump({
        "pa": {
            "enabled": True,
            "media_retention": {
                "enabled": True,
                "media_root": str(media_root),
                "media_ref_prefix": "/media/tgg/hermes",
                "source_roots": [str(source_root)],
                "operation": "tgg_media_retention",
            },
        },
    }), encoding="utf-8")
    return path


def _source_projection_config(tmp_path: Path, *, enabled: bool = True) -> Path:
    path = tmp_path / "source-projection-config.yaml"
    path.write_text(yaml.safe_dump({
        "pa": {
            "enabled": False,
            "source_projection": {
                "enabled": enabled,
                "operation": "tgg_whatsapp_source_ingest",
                "chat_ids": ["test-group@g.us"],
                "max_attempts": 2,
                "retry_interval_seconds": 1,
            },
        },
    }), encoding="utf-8")
    return path


class _ReplyResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return b'{"success":true,"messageId":"WA-DOC"}'


def test_retained_xlsx_media_ref_delivers_as_document_and_images_are_unchanged(
    tmp_path, monkeypatch
):
    management_chat = "management@g.us"
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(
        "selectors:\n"
        "- job_type: tgg_management\n"
        "  match:\n"
        "    source.platform: whatsapp\n"
        f"    source.chat_id: {management_chat}\n",
        encoding="utf-8",
    )
    retained = tmp_path / "retained"
    retained.mkdir()
    workbook = retained / "sandbox_r_12ab34cd_deadbeef.xlsx"
    _xlsx(workbook)
    image = retained / "case-photo.png"
    image.write_bytes(_png_bytes())
    config = _retention_config(tmp_path, retained, retained)
    data = yaml.safe_load(config.read_text())
    data["pa"]["constitution_path"] = str(constitution)
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    sent: list[dict] = []

    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return _ReplyResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    def deliver(path: str, message_id: str):
        raw = _message(message_id, management_chat)
        raw["timestamp"] = "2026-07-21T04:01:00+00:00"
        record = consumer.InboxRecord(
            seq=1,
            message_id=message_id,
            chat_id=management_chat,
            start_offset=0,
            end_offset=1,
            raw=raw,
        )
        return consumer.deliver_management_replies(
            inbox,
            config_path=config,
            captured_outbound=[
                {
                    "kind": "send",
                    "args": [management_chat, f"MEDIA:{path}"],
                    "kwargs": {"reply_to": message_id},
                }
            ],
            batch_records=[record],
            gate_changed_at="2026-07-21T04:00:00+00:00",
            handled_groups=[{"message_ids": [message_id], "turn_id": "turn"}],
        )

    assert deliver(
        f"/media/tgg/hermes/{workbook.name}", "DOC-MSG"
    )["delivered"] == 1
    assert sent[-1] == {
        "chatId": management_chat,
        "replyTo": "DOC-MSG",
        "filePath": str(workbook),
        "mediaType": "document",
        "fileName": workbook.name,
    }

    assert deliver(str(image), "IMAGE-MSG")["delivered"] == 1
    assert sent[-1] == {
        "chatId": management_chat,
        "replyTo": "IMAGE-MSG",
        "filePath": str(image),
        "mediaType": "image",
    }


def test_media_retention_is_atomic_idempotent_and_provenance_bound(tmp_path, monkeypatch):
    source_root = tmp_path / "capture"
    source_root.mkdir()
    source = source_root / "photo.png"
    source.write_bytes(_png_bytes())
    media_root = tmp_path / "systems-media"
    config = _retention_config(tmp_path, source_root, media_root)
    calls: list[dict] = []
    monkeypatch.setattr(
        consumer, "_converge_retained_media",
        lambda config_path, **kwargs: calls.append(kwargs["payload"])
        or {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    raw = _message("M-IMAGE", "management@g.us")
    raw.update({"hasMedia": True, "mediaType": "image/png", "mediaUrls": [str(source)]})
    record = consumer.InboxRecord(1, "M-IMAGE", "management@g.us", 0, 1, raw)

    first = consumer.retain_record_media(record, config_path=config)
    second = consumer.retain_record_media(record, config_path=config)
    assert first == second == {"retained": 1, "bytes": len(_png_bytes()), "operation": True}
    files = list(media_root.glob("*.png"))
    assert len(files) == 1
    assert files[0].read_bytes() == _png_bytes()
    assert calls[0] == calls[1]
    assert calls[0]["message_id"] == "M-IMAGE"
    assert calls[0]["media"][0]["media_ordinal"] == 0
    assert calls[0]["media"][0]["ref"] == f"/media/tgg/hermes/{files[0].name}"

    files[0].write_bytes(_png_bytes(b"changed"))
    with pytest.raises(consumer.MediaRetentionError, match="PROVENANCE_DIVERGENCE"):
        consumer.retain_record_media(record, config_path=config)


def test_media_retention_refuses_source_path_escape(tmp_path, monkeypatch):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    outside = tmp_path / "outside.png"
    outside.write_bytes(_png_bytes())
    config = _retention_config(tmp_path, allowed, tmp_path / "retained")
    monkeypatch.setattr(consumer, "_converge_retained_media", lambda *a, **k: {})
    raw = _message("ESCAPE")
    raw.update(
        {"hasMedia": True, "mediaType": "image", "mediaUrls": [str(outside)]}
    )
    record = consumer.InboxRecord(1, "ESCAPE", "test-group@g.us", 0, 1, raw)
    with pytest.raises(consumer.MediaRetentionError, match="escapes configured roots"):
        consumer.retain_record_media(record, config_path=config)


def test_backend_document_with_caption_xlsx_converges_exact_retained_entry(
    tmp_path, monkeypatch
):
    capture = tmp_path / "capture"
    capture.mkdir()
    workbook = capture / "jobs.xlsx"
    _xlsx(workbook)
    retained = tmp_path / "retained"
    config = _retention_config(tmp_path, capture, retained)
    normalized = _message("SHEET-1")
    normalized.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(workbook)],
        }
    )
    raw = {
        "normalized": normalized,
        "raw": {
            "message": {
                "documentWithCaptionMessage": {
                    "message": {
                        "documentMessage": {
                            "mimetype": (
                                "application/vnd.openxmlformats-officedocument."
                                "spreadsheetml.sheet"
                            )
                        }
                    }
                }
            }
        },
    }
    convergence: list[dict] = []
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda _config, **kwargs: convergence.append(kwargs["payload"]) or {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    expected = {
        "retained": 1,
        "bytes": workbook.stat().st_size,
        "operation": True,
        "validated_spreadsheets": 1,
    }
    assert consumer.retain_record_media(record, config_path=config) == expected
    first_files = list(retained.glob("*.xlsx"))
    assert len(first_files) == 1
    assert first_files[0].read_bytes() == workbook.read_bytes()
    assert "_spreadsheet_0_" in first_files[0].name
    assert hashlib.sha256(workbook.read_bytes()).hexdigest()[:24] in first_files[0].name
    assert first_files[0].suffix == ".xlsx"

    assert consumer.retain_record_media(record, config_path=config) == expected
    assert list(retained.glob("*.xlsx")) == first_files
    assert convergence[0] == convergence[1]
    assert convergence[0]["message_id"] == "SHEET-1"
    assert convergence[0]["chat_jid"] == "test-group@g.us"
    assert convergence[0]["source_key"] == (
        "whatsapp-capture-v1:"
        + hashlib.sha256(b"test-group@g.us\0SHEET-1").hexdigest()
    )
    assert convergence[0]["media"] == [
        {
            "source_key": convergence[0]["source_key"],
            "media_ordinal": 0,
            "digest": hashlib.sha256(workbook.read_bytes()).hexdigest(),
            "mime": (
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
            "ref": f"/media/tgg/hermes/{first_files[0].name}",
        }
    ]

    assert consumer.ensure_record_media_retained(
        inbox, record, config_path=config
    ) == expected
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT retention_state,retained_media_count,retention_attempts "
            "FROM ingress_events WHERE message_id='SHEET-1'"
        ).fetchone()
    assert tuple(row) == ("complete", 1, 1)


def test_mixed_spreadsheet_and_image_still_retains_image(tmp_path, monkeypatch):
    capture = tmp_path / "capture"
    capture.mkdir()
    workbook = capture / "jobs.xlsx"
    _xlsx(workbook)
    image = capture / "evidence.png"
    image.write_bytes(_png_bytes())
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    convergence = []
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda *_args, **kwargs: convergence.append(kwargs["payload"]) or {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    raw = _message("SHEET-IMAGE")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(workbook), str(image)],
            "mediaMimes": [
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "image/png",
            ],
        }
    )
    record = consumer.InboxRecord(
        1, "SHEET-IMAGE", "test-group@g.us", 0, 1, raw
    )

    result = consumer.retain_record_media(record, config_path=config)

    assert result["retained"] == 2
    assert result["operation"] is True
    assert result["validated_spreadsheets"] == 1
    assert len(list((tmp_path / "retained").glob("*.png"))) == 1
    assert len(list((tmp_path / "retained").glob("*.xlsx"))) == 1
    assert len(convergence) == 1
    assert [entry["media_ordinal"] for entry in convergence[0]["media"]] == [0, 1]
    assert [entry["mime"] for entry in convergence[0]["media"]] == [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "image/png",
    ]


def test_pdf_only_retention_is_idempotent_and_completes(tmp_path, monkeypatch):
    capture = tmp_path / "capture"
    capture.mkdir()
    document = capture / "brief.pdf"
    document.write_bytes(b"%PDF-1.7\nfixture")
    retained = tmp_path / "retained"
    config = _retention_config(tmp_path, capture, retained)
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda *_args, **_kwargs: {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    raw = _message("PDF-1")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(document)],
            "mediaMimes": ["application/octet-stream"],
        }
    )
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    expected = {
        "retained": 1,
        "bytes": document.stat().st_size,
        "operation": True,
        "validated_documents": 1,
    }
    assert consumer.retain_record_media(record, config_path=config) == expected
    first_files = list(retained.glob("*.pdf"))
    assert len(first_files) == 1
    assert "_document_0_" in first_files[0].name
    assert first_files[0].read_bytes() == document.read_bytes()
    assert consumer.retain_record_media(record, config_path=config) == expected
    assert list(retained.glob("*.pdf")) == first_files

    assert consumer.ensure_record_media_retained(
        inbox, record, config_path=config
    ) == expected
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT retention_state,retained_media_count,retention_attempts "
            "FROM ingress_events WHERE message_id='PDF-1'"
        ).fetchone()
    assert tuple(row) == ("complete", 1, 1)

    document.write_bytes(b"%PDF-1.7\nchanged")
    with pytest.raises(consumer.MediaRetentionError, match="ordinal 0 changed"):
        consumer.retain_record_media(record, config_path=config)


def test_docx_only_retention_completes(tmp_path, monkeypatch):
    capture = tmp_path / "capture"
    capture.mkdir()
    document = capture / "brief.docx"
    _docx(document)
    retained = tmp_path / "retained"
    config = _retention_config(tmp_path, capture, retained)
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda *_args, **_kwargs: {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    raw = _message("DOCX-1")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(document)],
            "mediaMimes": [
                "application/vnd.openxmlformats-officedocument."
                "wordprocessingml.document"
            ],
        }
    )
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    result = consumer.ensure_record_media_retained(
        inbox, record, config_path=config
    )

    assert result == {
        "retained": 1,
        "bytes": document.stat().st_size,
        "operation": True,
        "validated_documents": 1,
    }
    files = list(retained.glob("*.docx"))
    assert len(files) == 1
    assert files[0].read_bytes() == document.read_bytes()
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT retention_state,retained_media_count "
            "FROM ingress_events WHERE message_id='DOCX-1'"
        ).fetchone()
    assert tuple(row) == ("complete", 1)


@pytest.mark.asyncio
async def test_retained_documents_are_annotated_only_in_replay_copy(
    tmp_path,
    monkeypatch,
):
    capture = tmp_path / "capture"
    capture.mkdir()
    workbook = capture / "jobs.xlsx"
    _xlsx(workbook)
    document = capture / "brief.pdf"
    document.write_bytes(b"%PDF-1.7\nfixture")
    retained = tmp_path / "retained"
    config = _retention_config(tmp_path, capture, retained)
    config_data = yaml.safe_load(config.read_text(encoding="utf-8"))
    config_data["model"] = {"provider": "test-provider", "default": "test-model"}
    config_data["python_sandbox"] = {
        "datasets": {"media": {"type": "path", "path": str(retained)}}
    }
    config.write_text(yaml.safe_dump(config_data), encoding="utf-8")
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda *_args, **_kwargs: {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    raw = _message("DOCUMENT-ANNOTATION")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(workbook), str(document)],
            "mediaMimes": [
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet",
                "application/pdf",
            ],
        }
    )
    retained_record = consumer.InboxRecord(
        1,
        "DOCUMENT-ANNOTATION",
        "test-group@g.us",
        0,
        1,
        raw,
    )
    assert consumer.retain_record_media(
        retained_record, config_path=config
    )["retained"] == 2
    complete_record = consumer.InboxRecord(
        1,
        "DOCUMENT-ANNOTATION",
        "test-group@g.us",
        0,
        1,
        raw,
        retention_state="complete",
    )

    case_db = _write_case_db(tmp_path / "case.db")
    consumer._inject_bounded_source_evidence(
        case_db,
        [complete_record],
        before_image_path=tmp_path / "before.json",
        run_id="projection-before-annotation",
        dry_run=False,
    )
    plans = []

    class Runner:
        async def replay(self, plan):
            plans.append(plan)
            return SimpleNamespace(processed=1, outbound=[])

    await consumer.process_live_records(
        [complete_record],
        config_path=config,
        state_db=tmp_path / "state.db",
        persistent_session=True,
        runner=Runner(),
    )

    annotated = plans[0].messages[0]["body"]
    retained_names = [
        path.name
        for path in sorted(retained.iterdir())
        if path.suffix in {".xlsx", ".pdf"}
    ]
    assert len(retained_names) == 2
    annotation_lines = [
        line for line in annotated.splitlines() if line.startswith("[Attachment retained")
    ]
    assert len(annotation_lines) == 2
    assert ".xlsx]" in annotation_lines[0]
    assert ".pdf]" in annotation_lines[1]
    assert retained_names[0] in annotated
    assert retained_names[1] in annotated
    assert all("/inputs/media/" in line for line in annotation_lines)
    assert raw["body"] == "fixture message"
    with sqlite3.connect(case_db) as conn:
        projected_body, projected_raw = conn.execute(
            "SELECT text,raw_json FROM bridge_message_log WHERE source_ref=?",
            ("DOCUMENT-ANNOTATION",),
        ).fetchone()
    assert projected_body == "fixture message"
    assert json.loads(projected_raw)["body"] == "fixture message"
    assert projected_body != annotated


def test_bypassed_document_is_not_annotated_even_if_retained_target_exists(
    tmp_path,
    monkeypatch,
):
    capture = tmp_path / "capture"
    capture.mkdir()
    workbook = capture / "jobs.xlsx"
    _xlsx(workbook)
    retained = tmp_path / "retained"
    config = _retention_config(tmp_path, capture, retained)
    config_data = yaml.safe_load(config.read_text(encoding="utf-8"))
    config_data["python_sandbox"] = {
        "datasets": {"media": {"type": "path", "path": str(retained)}}
    }
    config.write_text(yaml.safe_dump(config_data), encoding="utf-8")
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda *_args, **_kwargs: {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    raw = _message("DOCUMENT-BYPASSED")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(workbook)],
            "mediaMimes": [
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ],
        }
    )
    record = consumer.InboxRecord(
        1, "DOCUMENT-BYPASSED", "test-group@g.us", 0, 1, raw
    )
    consumer.retain_record_media(record, config_path=config)
    bypassed = consumer.InboxRecord(
        1,
        "DOCUMENT-BYPASSED",
        "test-group@g.us",
        0,
        1,
        raw,
        retention_state="bypassed",
    )

    messages = consumer._replay_messages_with_retained_documents(
        [bypassed], config_path=config
    )

    assert messages[0]["body"] == "fixture message"
    retained_targets = list(retained.glob("*.xlsx"))
    assert retained_targets

    retained_targets[0].unlink()
    completed_without_target = consumer.InboxRecord(
        1,
        "DOCUMENT-BYPASSED",
        "test-group@g.us",
        0,
        1,
        raw,
        retention_state="complete",
    )
    missing_target_messages = consumer._replay_messages_with_retained_documents(
        [completed_without_target], config_path=config
    )
    assert missing_target_messages[0]["body"] == "fixture message"


def test_macro_document_refusal_is_durable_and_recorded(tmp_path):
    capture = tmp_path / "capture"
    capture.mkdir()
    document = capture / "active.docm"
    _docx(document, macro_enabled=True)
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    source = tmp_path / "events.jsonl"
    raw = _message("DOCM-REFUSED")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(document)],
            "mediaMimes": [
                "application/vnd.ms-word.document.macroenabled.12"
            ],
        }
    )
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    result = consumer.ensure_record_media_retained(
        inbox, record, config_path=config
    )

    assert result["refused"] is True
    assert inbox.retention_candidates(limit=1) == []
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT retention_state,retention_last_error "
            "FROM ingress_events WHERE message_id='DOCM-REFUSED'"
        ).fetchone()
    assert row["retention_state"] == "bypassed"
    assert "macro-enabled document formats are refused" in row["retention_last_error"]


def test_mixed_pdf_and_image_converges_both_retained_entries(
    tmp_path, monkeypatch
):
    capture = tmp_path / "capture"
    capture.mkdir()
    document = capture / "brief.pdf"
    document.write_bytes(b"%PDF-1.7\nfixture")
    image = capture / "evidence.png"
    image.write_bytes(_png_bytes())
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    convergence = []
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda *_args, **kwargs: convergence.append(kwargs["payload"]) or {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    raw = _message("PDF-IMAGE")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(document), str(image)],
            "mediaMimes": ["application/pdf", "image/png"],
        }
    )
    record = consumer.InboxRecord(
        1, "PDF-IMAGE", "test-group@g.us", 0, 1, raw
    )

    result = consumer.retain_record_media(record, config_path=config)

    assert result["retained"] == 2
    assert result["operation"] is True
    assert result["validated_documents"] == 1
    assert len(list((tmp_path / "retained").glob("*.pdf"))) == 1
    assert len(list((tmp_path / "retained").glob("*.png"))) == 1
    assert len(convergence) == 1
    assert [entry["media_ordinal"] for entry in convergence[0]["media"]] == [0, 1]
    assert [entry["mime"] for entry in convergence[0]["media"]] == [
        "application/pdf",
        "image/png",
    ]


def test_spreadsheet_without_declared_mime_is_permanently_refused(tmp_path):
    capture = tmp_path / "capture"
    capture.mkdir()
    workbook = capture / "jobs.xlsx"
    _xlsx(workbook)
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    raw = _message("SHEET-NO-MIME")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(workbook)],
        }
    )
    record = consumer.InboxRecord(
        1, "SHEET-NO-MIME", "test-group@g.us", 0, 1, raw
    )

    with pytest.raises(
        consumer.PermanentMediaRefusal, match="provider-declared MIME"
    ):
        consumer.retain_record_media(record, config_path=config)


def test_permanent_spreadsheet_refusal_is_durable_and_not_retried(tmp_path):
    capture = tmp_path / "capture"
    capture.mkdir()
    executable = capture / "malware.xlsx"
    executable.write_bytes(b"MZ" + b"\x00" * 100)
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    source = tmp_path / "events.jsonl"
    raw = _message("SHEET-REFUSED")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(executable)],
            "mediaMimes": [
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ],
        }
    )
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    result = consumer.ensure_record_media_retained(
        inbox, record, config_path=config
    )

    assert result["refused"] is True
    assert inbox.retention_candidates(limit=1) == []
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT retention_state,retention_attempts,retention_failures,"
            "retention_last_error FROM ingress_events WHERE message_id=?",
            ("SHEET-REFUSED",),
        ).fetchone()
    assert tuple(row[:3]) == ("bypassed", 1, 0)
    assert "PROVENANCE_DIVERGENCE" in row["retention_last_error"]


def test_mandatory_media_retry_cap_quarantines_full_event_and_history(tmp_path):
    capture = tmp_path / "capture"
    capture.mkdir()
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"]["max_attempts"] = 2
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    source = tmp_path / "events.jsonl"
    missing = capture / "expired.jpg"
    raw = _message("MANDATORY-GIVE-UP", "site@g.us")
    raw.update({
        "hasMedia": True,
        "mediaType": "image/jpeg",
        "mediaUrls": [str(missing)],
        "providerMetadata": {"opaque": ["must", "survive"]},
    })
    source_envelope = {
        "type": "whatsapp_capture_event",
        "normalized": raw,
        "raw": {
            "message": {
                "imageMessage": {
                    "directPath": "/v/t62.7118-24/provider-only",
                    "mediaKey": "provider-key-must-survive",
                }
            },
            "providerSibling": {"opaque": [1, 2, 3]},
        },
    }
    _write_jsonl(source, [source_envelope])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    with pytest.raises(consumer.MediaRetentionError, match="unavailable"):
        consumer.ensure_record_media_retained(inbox, record, config_path=config)
    terminal = consumer.ensure_record_media_retained(inbox, record, config_path=config)

    assert terminal["retained"] == 0
    assert terminal["bytes"] == 0
    assert terminal["operation"] is False
    assert terminal["quarantined"] is True
    assert "media source is unavailable" in terminal["reason"]
    assert str(missing) in terminal["reason"]
    assert terminal["attempt"] == 2
    assert terminal["quarantine_attempt"] == 2
    assert terminal["retry_cap"] == 2
    assert inbox.retention_candidates(limit=10) == []
    assert inbox.retention_quarantine_status() == {"quarantined": 1}
    assert inbox.retention_quarantine_message_ids() == ["MANDATORY-GIVE-UP"]
    counts = inbox.retention_counts()
    assert counts["retention_quarantined"] == 1
    assert counts["retention_held"] == 0
    assert counts["retention_bypassed"] == 1
    with inbox.connect() as conn:
        event = conn.execute(
            "SELECT raw_json,source_envelope_json,retention_state,"
            "retention_attempts,retention_quarantine_attempts,"
            "retention_failures FROM ingress_events "
            "WHERE message_id='MANDATORY-GIVE-UP'"
        ).fetchone()
        quarantine = conn.execute(
            "SELECT raw_json,failure_history_json,status,terminal_error "
            "FROM media_retention_quarantine WHERE message_id='MANDATORY-GIVE-UP'"
        ).fetchone()
        failures = conn.execute(
            "SELECT attempt,error FROM media_retention_failures "
            "WHERE ingress_seq=? ORDER BY attempt", (record.seq,)
        ).fetchall()
    assert tuple(event[2:]) == ("bypassed", 2, 2, 2)
    assert json.loads(event["raw_json"])["providerMetadata"] == {
        "opaque": ["must", "survive"]
    }
    assert quarantine["raw_json"] == event["source_envelope_json"]
    preserved_envelope = json.loads(quarantine["raw_json"])
    assert preserved_envelope == source_envelope
    assert preserved_envelope["raw"]["message"]["imageMessage"] == {
        "directPath": "/v/t62.7118-24/provider-only",
        "mediaKey": "provider-key-must-survive",
    }
    assert preserved_envelope["raw"]["providerSibling"] == {"opaque": [1, 2, 3]}
    history = json.loads(quarantine["failure_history_json"])
    assert [entry["attempt"] for entry in history] == [1, 2]
    assert [(row["attempt"], row["error"]) for row in failures] == [
        (1, history[0]["error"]), (2, history[1]["error"])
    ]
    assert quarantine["status"] == "quarantined"
    assert quarantine["terminal_error"] == history[-1]["error"]
    assert inbox.retention_result(record)["quarantined"] is True


def test_legacy_attempts_do_not_count_toward_new_quarantine_budget(tmp_path):
    capture = tmp_path / "capture"
    capture.mkdir()
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"]["max_attempts"] = 2
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    source = tmp_path / "events.jsonl"
    raw = _message("LEGACY-ATTEMPTS", "site@g.us")
    raw.update({
        "hasMedia": True,
        "mediaType": "image/jpeg",
        "mediaUrls": [str(capture / "expired.jpg")],
    })
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]
    with inbox.connect() as conn:
        conn.execute(
            "UPDATE ingress_events SET retention_state='held',"
            "retention_attempts=4,retention_updated_at='2026-01-01T00:00:00+00:00' "
            "WHERE seq=?",
            (record.seq,),
        )

    with pytest.raises(consumer.ItemMediaRetentionError, match="unavailable"):
        consumer.ensure_record_media_retained(inbox, record, config_path=config)

    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT retention_state,retention_attempts,"
            "retention_quarantine_attempts FROM ingress_events WHERE seq=?",
            (record.seq,),
        ).fetchone()
    assert tuple(row) == ("held", 5, 1)
    assert inbox.retention_quarantine_status() == {}


def test_systemic_failures_are_spaced_and_never_burn_quarantine_budget(
    tmp_path, monkeypatch
):
    capture = tmp_path / "capture"
    capture.mkdir()
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"]["max_attempts"] = 1
    data["pa"]["media_retention"]["retry_interval_seconds"] = 60
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    source = tmp_path / "events.jsonl"
    raw = _message("SYSTEMIC-FAILURE", "site@g.us")
    raw.update({
        "hasMedia": True,
        "mediaType": "image/jpeg",
        "mediaUrls": [str(capture / "photo.jpg")],
    })
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    def fail_systemically(*_args, **_kwargs):
        raise consumer.MediaRetentionError("provider-wide outage")

    monkeypatch.setattr(consumer, "retain_record_media", fail_systemically)
    with pytest.raises(consumer.MediaRetentionError, match="provider-wide"):
        consumer.ensure_record_media_retained(inbox, record, config_path=config)

    assert inbox.retention_candidates(
        limit=1, retry_interval_seconds=60
    ) == []
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT retention_state,retention_attempts,"
            "retention_quarantine_attempts FROM ingress_events WHERE seq=?",
            (record.seq,),
        ).fetchone()
        conn.execute(
            "UPDATE ingress_events SET retention_updated_at='2026-01-01T00:00:00+00:00' "
            "WHERE seq=?",
            (record.seq,),
        )
    assert tuple(row) == ("held", 1, 0)
    assert inbox.retention_candidates(
        limit=1, retry_interval_seconds=60
    )[0].message_id == "SYSTEMIC-FAILURE"
    assert inbox.retention_quarantine_status() == {}


def test_quarantine_is_loud_in_status_and_model_replay(tmp_path, capsys):
    capture = tmp_path / "capture"
    capture.mkdir()
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"]["max_attempts"] = 1
    data["pa"]["media_retention"]["retry_interval_seconds"] = 1
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    source = tmp_path / "events.jsonl"
    raw = _message("LOUD-QUARANTINE", "site@g.us")
    raw.update({
        "hasMedia": True,
        "mediaType": "image/jpeg",
        "mediaUrls": [str(capture / "expired.jpg")],
    })
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)

    summary = asyncio.run(
        consumer.retain_pending_media(inbox, config_path=config, limit=1)
    )
    assert summary["quarantined"] == 1
    assert "media retention QUARANTINED" in capsys.readouterr().err
    status = consumer._retention_status(inbox, config, inspect_media=False)
    assert status["retention_quarantined"] == 1
    assert status["retention_quarantine_message_ids"] == ["LOUD-QUARANTINE"]

    management, site = inbox.pending_chat_batches(batch_size=10)
    records = (management or site)[0][1]
    assert records[0].retention_quarantined is True
    replay = consumer._replay_messages_with_retained_documents(
        records, config_path=config
    )
    body = consumer._bridge_item(replay[0])["body"]
    assert "Attachment unavailable: retention quarantine" in body
    assert "Do not infer or claim the attachment's contents" in body


def test_permanent_media_refusal_bypasses_without_quarantine(tmp_path):
    capture = tmp_path / "capture"
    capture.mkdir()
    workbook = capture / "bad.xlsx"
    workbook.write_bytes(b"MZ" + b"\x00" * 100)
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    source = tmp_path / "events.jsonl"
    raw = _message("PERMANENT-NOT-QUARANTINE")
    raw.update({
        "hasMedia": True,
        "mediaType": "document",
        "mediaUrls": [str(workbook)],
        "mediaMimes": [
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ],
    })
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    result = consumer.ensure_record_media_retained(inbox, record, config_path=config)

    assert result["refused"] is True
    assert inbox.retention_quarantine_status() == {}
    assert inbox.retention_counts()["retention_quarantined"] == 0


def test_media_retention_normalizes_source_read_oserror(tmp_path, monkeypatch):
    capture = tmp_path / "capture"
    capture.mkdir()
    source = capture / "photo.png"
    source.write_bytes(_png_bytes())
    config = _retention_config(tmp_path, capture, tmp_path / "retained")
    raw = _message("SOURCE-RACE")
    raw.update(
        {"hasMedia": True, "mediaType": "image", "mediaUrls": [str(source)]}
    )
    record = consumer.InboxRecord(
        1, "SOURCE-RACE", "test-group@g.us", 0, 1, raw
    )
    original_read_bytes = Path.read_bytes

    def fail_source_read(path):
        if path == source:
            raise OSError("capture download disappeared")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", fail_source_read)

    with pytest.raises(consumer.MediaRetentionError, match="retention I/O failed"):
        consumer.retain_record_media(record, config_path=config)


def test_production_normalized_video_is_not_retained(tmp_path, monkeypatch):
    capture = tmp_path / "capture"
    capture.mkdir()
    video = capture / "clip.mp4"
    video.write_bytes(b"not-an-image-video-fixture")
    retained = tmp_path / "retained"
    config = _retention_config(tmp_path, capture, retained)
    calls = []
    monkeypatch.setattr(
        consumer, "_converge_retained_media", lambda *a, **k: calls.append((a, k))
    )
    normalized = _message("VIDEO-1")
    normalized.update({
        "hasMedia": True, "mediaType": "video", "mediaUrls": [str(video)]
    })
    record = consumer.InboxRecord(
        1, "VIDEO-1", "test-group@g.us", 0, 1,
        {"type": "whatsapp_capture_event", "normalized": normalized},
    )
    assert consumer.retain_record_media(record, config_path=config) == {
        "retained": 0, "bytes": 0, "operation": False,
    }
    assert calls == []
    assert not retained.exists()


@pytest.mark.asyncio
async def test_pending_production_video_bypasses_retention_and_completes(
    tmp_path, monkeypatch
):
    args = _enabled_consumer_args(tmp_path, [])
    capture = tmp_path / "capture"
    capture.mkdir()
    video = capture / "pending.mp4"
    video.write_bytes(b"video-fixture")
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True,
        "media_root": str(tmp_path / "retained"),
        "source_roots": [str(capture)],
        "operation": "tgg_media_retention",
        "min_free_percent": 0,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    normalized = _message("VIDEO-PENDING", "site@g.us")
    normalized.update(
        {
            "hasMedia": True,
            "mediaType": "video",
            "mediaUrls": [str(video)],
        }
    )
    _write_jsonl(
        Path(args.source),
        [{"type": "whatsapp_capture_event", "normalized": normalized}],
    )

    async def fake_process(records, **kwargs):
        assert [record.message_id for record in records] == ["VIDEO-PENDING"]
        return {
            "submitted_message_ids": ["VIDEO-PENDING"],
            "handled": [
                {"message_ids": ["VIDEO-PENDING"], "turn_id": "turn-video"}
            ],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())

    assert await consumer.run_consumer(args) == 0
    inbox = consumer.DurableInbox(Path(args.inbox))
    assert inbox.counts() == {"completed": 1}
    assert inbox.retention_counts() == {
        "retention_total": 0,
        "retention_failures": 0,
        "retention_attempts": 0,
        "retention_pending": 0,
        "retention_complete": 0,
        "retention_bypassed": 1,
        "retention_held": 0,
        "retention_quarantined": 0,
    }


@pytest.mark.asyncio
async def test_production_normalized_missing_image_stays_pending_and_once_survives(
    tmp_path, monkeypatch
):
    args = _enabled_consumer_args(tmp_path, [])
    capture = tmp_path / "capture"
    capture.mkdir()
    retained = tmp_path / "retained"
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True, "media_root": str(retained),
        "source_roots": [str(capture)], "operation": "tgg_media_retention",
        "min_free_percent": 0,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    normalized = _message("MISSING-IMAGE", "management@g.us")
    normalized.update({
        "hasMedia": True, "mediaType": "image",
        "mediaUrls": [str(capture / "already-evicted.jpg")],
        "timestamp": 1784630163.917,
    })
    _write_jsonl(
        Path(args.source),
        [{"type": "whatsapp_capture_event", "normalized": normalized}],
    )
    assert await consumer.run_consumer(args) == 0
    inbox = consumer.DurableInbox(Path(args.inbox))
    assert inbox.counts() == {"pending": 1}
    assert inbox.retention_counts()["retention_failures"] == 1
    status = json.loads(Path(args.status_file).read_text())
    assert status["state"] == "held-pending"
    assert "media source is unavailable" in status["retention_hold"]


@pytest.mark.asyncio
async def test_one_chat_retention_hold_does_not_kill_other_chat(tmp_path, monkeypatch):
    args = _enabled_consumer_args(tmp_path, [])
    capture = tmp_path / "capture"
    capture.mkdir()
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True, "media_root": str(tmp_path / "retained"),
        "source_roots": [str(capture)], "operation": "tgg_media_retention",
        "min_free_percent": 0,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    missing = _message("MISSING", "management@g.us")
    missing.update({
        "hasMedia": True, "mediaType": "image",
        "mediaUrls": [str(capture / "gone.jpg")], "timestamp": 1784630163.917,
    })
    healthy = _message("HEALTHY", "site@g.us")
    _write_jsonl(Path(args.source), [
        {"type": "whatsapp_capture_event", "normalized": missing}, healthy,
    ])

    async def fake_process(records, **kwargs):
        assert [record.message_id for record in records] == ["HEALTHY"]
        return {
            "submitted_message_ids": ["HEALTHY"],
            "handled": [{"message_ids": ["HEALTHY"], "turn_id": "turn-healthy"}],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    assert await consumer.run_consumer(args) == 0
    inbox = consumer.DurableInbox(Path(args.inbox))
    assert inbox.counts() == {"completed": 1, "pending": 1}
    with inbox.connect() as conn:
        states = {
            row["message_id"]: row["status"]
            for row in conn.execute("SELECT message_id,status FROM ingress_events")
        }
    assert states == {"MISSING": "pending", "HEALTHY": "completed"}
    assert "media source is unavailable" in (inbox.retention_last_error() or "")


@pytest.mark.asyncio
async def test_demo_pause_retains_pending_site_image_without_runner_or_delivery(
    tmp_path, monkeypatch
):
    args = _enabled_consumer_args(tmp_path, [])
    capture = tmp_path / "capture"
    capture.mkdir()
    source = capture / "site-photo.png"
    source.write_bytes(_png_bytes())
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True,
        "media_root": str(tmp_path / "retained"),
        "source_roots": [str(capture)],
        "operation": "tgg_media_retention",
        "min_free_percent": 0,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    normalized = _message("PAUSED-SITE-IMAGE", "site@g.us")
    normalized.update({
        "hasMedia": True,
        "mediaType": "image/png",
        "mediaUrls": [str(source)],
        "timestamp": 1784630163.917,
    })
    _write_jsonl(
        Path(args.source),
        [{"type": "whatsapp_capture_event", "normalized": normalized}],
    )
    convergence = []
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda config_path, **kwargs: convergence.append(kwargs["payload"])
        or {
            "ok": True,
            "data": {"ledgerChanged": True, "observationsChanged": True},
        },
    )
    monkeypatch.setattr(
        consumer,
        "_new_gateway_runner",
        lambda: pytest.fail("paused site retention constructed a model runner"),
    )
    monkeypatch.setattr(
        consumer,
        "deliver_management_replies",
        lambda *args, **kwargs: pytest.fail("paused site retention invoked delivery"),
    )
    monkeypatch.setenv("TGG_DEMO_MANAGEMENT_ONLY", "1")

    assert await consumer.run_consumer(args) == 0
    inbox = consumer.DurableInbox(Path(args.inbox))
    assert inbox.counts() == {"pending": 1}
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT status,pa_turn_id,retention_state,retained_media_count "
            "FROM ingress_events WHERE message_id='PAUSED-SITE-IMAGE'"
        ).fetchone()
    assert tuple(row) == ("pending", None, "complete", 1)
    assert len(list((tmp_path / "retained").glob("*.png"))) == 1
    assert len(convergence) == 1
    status = json.loads(Path(args.status_file).read_text())
    assert status["retention_cycle"] == {
        "examined": 1,
        "retained": 1,
        "bypassed": 0,
        "held": 0,
        "quarantined": 0,
    }
    assert status["retention_total"] == 1
    assert status["retention_complete"] == 1
    assert status["inbox"] == {"pending": 1}


@pytest.mark.asyncio
async def test_demo_pause_non_image_bypasses_retention_without_runner(
    tmp_path, monkeypatch
):
    args = _enabled_consumer_args(tmp_path, [])
    normalized = _message("PAUSED-SITE-TEXT", "site@g.us")
    normalized["timestamp"] = 1784630163.917
    _write_jsonl(
        Path(args.source),
        [{"type": "whatsapp_capture_event", "normalized": normalized}],
    )
    monkeypatch.setenv("TGG_DEMO_MANAGEMENT_ONLY", "1")
    monkeypatch.setattr(
        consumer,
        "retain_record_media",
        lambda *args, **kwargs: pytest.fail("non-image entered retention I/O"),
    )
    monkeypatch.setattr(
        consumer,
        "_new_gateway_runner",
        lambda: pytest.fail("paused site text constructed a model runner"),
    )

    assert await consumer.run_consumer(args) == 0
    inbox = consumer.DurableInbox(Path(args.inbox))
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT status,retention_state,retention_attempts "
            "FROM ingress_events WHERE message_id='PAUSED-SITE-TEXT'"
        ).fetchone()
    assert tuple(row) == ("pending", "bypassed", 0)
    assert json.loads(Path(args.status_file).read_text())["retention_cycle"] == {
        "examined": 0,
        "retained": 0,
        "bypassed": 0,
        "held": 0,
        "quarantined": 0,
    }


@pytest.mark.asyncio
async def test_demo_pause_retention_hold_preserves_management_lane_and_retries(
    tmp_path, monkeypatch
):
    args = _enabled_consumer_args(tmp_path, [])
    args.retention_batch_size = 1
    capture = tmp_path / "capture"
    capture.mkdir()
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True,
        "media_root": str(tmp_path / "retained"),
        "source_roots": [str(capture)],
        "operation": "tgg_media_retention",
        "min_free_percent": 0,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    missing = _message("PAUSED-MISSING", "site@g.us")
    missing.update({
        "hasMedia": True,
        "mediaType": "image/jpeg",
        "mediaUrls": [str(capture / "raced-away.jpg")],
        "timestamp": 1784630163.917,
    })
    management = _message("MANAGEMENT-LIVE", "management@g.us")
    management.update({
        "timestamp": 1784630164.917,
        "botIds": ["6599999999@s.whatsapp.net"],
        "mentionedIds": ["6599999999@s.whatsapp.net"],
    })
    _write_jsonl(Path(args.source), [
        {"type": "whatsapp_capture_event", "normalized": missing},
        {"type": "whatsapp_capture_event", "normalized": management},
    ])
    processed = []

    async def fake_process(records, **kwargs):
        processed.extend(record.message_id for record in records)
        return {
            "submitted_message_ids": [record.message_id for record in records],
            "handled": [{
                "message_ids": [record.message_id for record in records],
                "turn_id": "turn-management",
            }],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    monkeypatch.setenv("TGG_DEMO_MANAGEMENT_ONLY", "1")

    assert await consumer.run_consumer(args) == 0
    assert processed == ["MANAGEMENT-LIVE"]
    inbox = consumer.DurableInbox(Path(args.inbox))
    with inbox.connect() as conn:
        rows = {
            row["message_id"]: (
                row["status"], row["retention_state"], row["retention_failures"]
            )
            for row in conn.execute(
                "SELECT message_id,status,retention_state,retention_failures "
                "FROM ingress_events"
            )
        }
    assert rows == {
        "PAUSED-MISSING": ("pending", "held", 1),
        "MANAGEMENT-LIVE": ("completed", "bypassed", 0),
    }
    status = json.loads(Path(args.status_file).read_text())
    assert status["state"] == "held-pending"
    assert status["retention_held"] == 1
    assert "media source is unavailable" in status["retention_hold"]
    assert await consumer.run_consumer(args) == 0
    assert processed == ["MANAGEMENT-LIVE"]
    retry_counts = consumer.DurableInbox(Path(args.inbox)).retention_counts()
    assert retry_counts["retention_failures"] == 1
    with consumer.DurableInbox(Path(args.inbox)).connect() as conn:
        conn.execute(
            "UPDATE ingress_events SET retention_updated_at='2026-01-01T00:00:00+00:00' "
            "WHERE message_id='PAUSED-MISSING'"
        )
    assert await consumer.run_consumer(args) == 0
    retry_counts = consumer.DurableInbox(Path(args.inbox)).retention_counts()
    assert retry_counts["retention_failures"] == 2
    assert retry_counts["retention_attempts"] == 2


@pytest.mark.asyncio
async def test_demo_pause_retention_rerun_is_counter_and_storage_idempotent(
    tmp_path, monkeypatch
):
    args = _enabled_consumer_args(tmp_path, [])
    capture = tmp_path / "capture"
    capture.mkdir()
    source = capture / "rerun.png"
    source.write_bytes(_png_bytes())
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True,
        "media_root": str(tmp_path / "retained"),
        "source_roots": [str(capture)],
        "operation": "tgg_media_retention",
        "min_free_percent": 0,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    normalized = _message("PAUSED-RERUN", "site@g.us")
    normalized.update({
        "hasMedia": True,
        "mediaType": "image/png",
        "mediaUrls": [str(source)],
        "timestamp": 1784630163.917,
    })
    _write_jsonl(
        Path(args.source),
        [{"type": "whatsapp_capture_event", "normalized": normalized}],
    )
    convergence = []
    monkeypatch.setattr(
        consumer,
        "_converge_retained_media",
        lambda config_path, **kwargs: convergence.append(kwargs["payload"])
        or {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        },
    )
    monkeypatch.setenv("TGG_DEMO_MANAGEMENT_ONLY", "1")
    monkeypatch.setattr(
        consumer,
        "_new_gateway_runner",
        lambda: pytest.fail("paused site rerun constructed a model runner"),
    )

    assert await consumer.run_consumer(args) == 0
    first = consumer.DurableInbox(Path(args.inbox)).retention_counts()
    first_files = [path.name for path in (tmp_path / "retained").glob("*")]
    assert await consumer.run_consumer(args) == 0
    second = consumer.DurableInbox(Path(args.inbox)).retention_counts()
    second_files = [path.name for path in (tmp_path / "retained").glob("*")]
    assert first == second
    assert first["retention_total"] == 1
    assert first["retention_attempts"] == 1
    assert first_files == second_files
    assert len(second_files) == 1
    assert len(convergence) == 1


def test_retention_operation_resolves_from_real_overlay_registry(tmp_path, monkeypatch):
    canonical = Path("deploy/tgg/christopher/config.yaml")
    data = yaml.safe_load(canonical.read_text())
    data["pa"]["enabled"] = True
    data["pa"]["constitution_path"] = str(
        Path("deploy/tgg/christopher/christopher_tgg_constitution.yaml").resolve()
    )
    config = tmp_path / "actual-shape.yaml"
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    observed = {}

    def fake_execute(bridge, operation, payload, **kwargs):
        observed.update(operation=operation, known=set(bridge.operations), payload=payload)
        return {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        }

    monkeypatch.setattr("tools.pa_business_tools.execute_business_operation", fake_execute)
    result = consumer._converge_retained_media(
        config,
        operation="tgg_media_retention",
        payload={"chat_jid": "120363421424519051@g.us", "message_id": "M1"},
    )
    assert result == {
        "ok": True,
        "data": {"ledgerChanged": False, "observationsChanged": False},
    }
    assert observed["operation"] == "tgg_media_retention"
    assert "tgg_media_retention" in observed["known"]


def test_retention_operation_uses_constitution_for_unselected_site_chat(
    tmp_path, monkeypatch
):
    canonical = Path("deploy/tgg/christopher/config.yaml")
    data = yaml.safe_load(canonical.read_text())
    data["pa"]["enabled"] = True
    data["pa"]["constitution_path"] = str(
        Path("deploy/tgg/christopher/christopher_tgg_constitution.yaml").resolve()
    )
    config = tmp_path / "actual-shape.yaml"
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    observed = {}

    def fake_execute(bridge, operation, payload, **kwargs):
        observed.update(operation=operation, known=set(bridge.operations), payload=payload)
        return {
            "ok": True,
            "data": {"ledgerChanged": False, "observationsChanged": False},
        }

    monkeypatch.setattr("tools.pa_business_tools.execute_business_operation", fake_execute)
    result = consumer._converge_retained_media(
        config,
        operation="tgg_media_retention",
        payload={"chat_jid": "unselected-site-chat@g.us", "message_id": "M1"},
    )

    assert result["ok"] is True
    assert observed["operation"] == "tgg_media_retention"
    assert "tgg_media_retention" in observed["known"]


@pytest.mark.parametrize(
    "response",
    [
        {"ok": False, "error": "rejected"},
        {"ok": True, "data": {}},
        {"ledgerChanged": False, "observationsChanged": False},
    ],
)
def test_retention_convergence_rejects_non_contract_systems_envelope(
    tmp_path, monkeypatch, response
):
    canonical = Path("deploy/tgg/christopher/config.yaml")
    data = yaml.safe_load(canonical.read_text())
    data["pa"]["enabled"] = True
    data["pa"]["constitution_path"] = str(
        Path("deploy/tgg/christopher/christopher_tgg_constitution.yaml").resolve()
    )
    config = tmp_path / "actual-shape.yaml"
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    monkeypatch.setattr(
        "tools.pa_business_tools.execute_business_operation",
        lambda *args, **kwargs: response,
    )

    with pytest.raises(
        consumer.MediaRetentionError, match="invalid Systems envelope"
    ):
        consumer._converge_retained_media(
            config,
            operation="tgg_media_retention",
            payload={
                "chat_jid": "120363421424519051@g.us",
                "message_id": "M1",
            },
        )


def test_retention_last_error_preserves_systems_status_code_and_error(
    tmp_path, monkeypatch
):
    capture = tmp_path / "capture"
    capture.mkdir()
    image = capture / "evidence.png"
    image.write_bytes(_png_bytes())
    canonical = Path("deploy/tgg/christopher/config.yaml")
    data = yaml.safe_load(canonical.read_text())
    data["pa"]["enabled"] = True
    data["pa"]["constitution_path"] = str(
        Path("deploy/tgg/christopher/christopher_tgg_constitution.yaml").resolve()
    )
    data["pa"]["media_retention"] = {
        "enabled": True,
        "media_root": str(tmp_path / "retained"),
        "media_ref_prefix": "/media/tgg/hermes",
        "source_roots": [str(capture)],
        "operation": "tgg_media_retention",
        "min_free_percent": 0,
    }
    config = tmp_path / "actual-shape.yaml"
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    monkeypatch.setattr(
        "tools.pa_business_tools.execute_business_operation",
        lambda *args, **kwargs: {
            "ok": False,
            "status_code": 400,
            "error": {
                "code": "MEDIA_NOT_FOUND",
                "message": "media[0].ref does not resolve to a retained file.",
            },
        },
    )
    raw = _message("SYSTEMS-ERROR", "120363421424519051@g.us")
    raw.update(
        {
            "hasMedia": True,
            "mediaType": "image/png",
            "mediaUrls": [str(image)],
        }
    )
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [raw])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.retention_candidates(limit=1)[0]

    with pytest.raises(consumer.MediaRetentionError, match="status_code=400"):
        consumer.ensure_record_media_retained(inbox, record, config_path=config)

    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT retention_state,retention_last_error FROM ingress_events "
            "WHERE message_id='SYSTEMS-ERROR'"
        ).fetchone()
    assert row["retention_state"] == "held"
    assert "status_code=400" in row["retention_last_error"]
    assert "code=MEDIA_NOT_FOUND" in row["retention_last_error"]
    assert (
        "message=media[0].ref does not resolve to a retained file."
        in row["retention_last_error"]
    )


@pytest.mark.asyncio
async def test_retention_failure_requeues_before_model(tmp_path, monkeypatch):
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [_message("retry-media")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    records = inbox.pending(limit=1)
    with inbox.connect() as conn:
        conn.execute(
            "UPDATE ingress_events SET retention_state='pending' WHERE seq=?",
            (records[0].seq,),
        )
    config = tmp_path / "config.yaml"
    config.write_text("pa:\n  enabled: true\n", encoding="utf-8")
    model_called = False
    def fail_retention(*args, **kwargs):
        raise consumer.MediaRetentionError("systems unavailable")
    async def model(*args, **kwargs):
        nonlocal model_called
        model_called = True
    monkeypatch.setattr(consumer, "retain_record_media", fail_retention)
    monkeypatch.setattr(consumer, "process_live_records", model)
    with pytest.raises(consumer.MediaRetentionError, match="systems unavailable"):
        await consumer._process_claimed_chat_batch(
            inbox, records, config_path=config, state_db=tmp_path / "state.db",
            case_db=_write_case_db(tmp_path / "case.db"),
            source_before_image_dir=tmp_path / "source-before-images",
            gate_changed_at="2026-07-21T00:00:00+00:00", runner=object(),
        )
    assert model_called is False
    assert inbox.counts() == {"pending": 1}
    assert inbox.retention_counts() == {
        "retention_total": 0,
        "retention_failures": 1,
        "retention_attempts": 1,
        "retention_pending": 0,
        "retention_complete": 0,
        "retention_bypassed": 0,
        "retention_held": 1,
        "retention_quarantined": 0,
    }


@pytest.mark.asyncio
async def test_live_batch_projects_source_before_model_and_repeat_is_idempotent(
    tmp_path, monkeypatch
):
    case_db = _write_case_db(tmp_path / "case.db")
    before_image_dir = tmp_path / "source-before-images"
    observed_counts: list[int] = []

    async def model(records, **kwargs):
        conn = sqlite3.connect(case_db)
        try:
            row = conn.execute(
                "SELECT chat_jid,text FROM bridge_message_log WHERE source_ref=?",
                (records[0].message_id,),
            ).fetchone()
            observed_counts.append(
                conn.execute(
                    "SELECT COUNT(*) FROM bridge_message_log WHERE source_ref=?",
                    (records[0].message_id,),
                ).fetchone()[0]
            )
        finally:
            conn.close()
        assert row == ("test-group@g.us", "fixture message")
        return {
            "submitted_message_ids": [records[0].message_id],
            "handled": [{
                "message_ids": [records[0].message_id],
                "turn_id": f"turn-{len(observed_counts)}",
            }],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", model)
    config = tmp_path / "config.yaml"
    config.write_text("pa:\n  enabled: true\n", encoding="utf-8")
    for suffix in ("first", "repeat"):
        inbox = consumer.DurableInbox(tmp_path / f"{suffix}-inbox.db")
        source = tmp_path / f"{suffix}.jsonl"
        _write_jsonl(source, [_message("SOURCE-1")])
        cursor = tmp_path / f"{suffix}-cursor.json"
        consumer.initialize_cursor(source, cursor, position="start")
        inbox.stage_from_source(source, cursor)
        records = inbox.pending(limit=1)
        await consumer._process_claimed_chat_batch(
            inbox,
            records,
            config_path=config,
            state_db=tmp_path / f"{suffix}-state.db",
            case_db=case_db,
            source_before_image_dir=before_image_dir,
            gate_changed_at="2026-07-21T00:00:00+00:00",
            runner=object(),
        )
        assert inbox.counts() == {"completed": 1}

    assert observed_counts == [1, 1]
    before_images = sorted(before_image_dir.glob("*.json"))
    assert len(before_images) == 2
    images = [json.loads(path.read_text()) for path in before_images]
    assert all(image["selected_message_ids"] == ["SOURCE-1"] for image in images)
    assert sorted(len(image["existing_rows"]) for image in images) == [0, 1]


@pytest.mark.asyncio
async def test_live_projection_identity_divergence_holds_without_model(
    tmp_path, monkeypatch
):
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [_message("SOURCE-IDENTITY")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    record = inbox.pending(limit=1)[0]
    divergent = consumer.InboxRecord(
        record.seq,
        record.message_id,
        record.chat_id,
        record.start_offset,
        record.end_offset,
        {**record.raw, "chatId": "other-chat@g.us"},
    )
    model_called = False

    async def model(*args, **kwargs):
        nonlocal model_called
        model_called = True

    monkeypatch.setattr(consumer, "process_live_records", model)
    config = tmp_path / "config.yaml"
    config.write_text("pa:\n  enabled: true\n", encoding="utf-8")
    with pytest.raises(
        consumer.SourceEvidenceProjectionError,
        match="identity diverges",
    ):
        await consumer._process_claimed_chat_batch(
            inbox,
            [divergent],
            config_path=config,
            state_db=tmp_path / "state.db",
            case_db=_write_case_db(tmp_path / "case.db"),
            source_before_image_dir=tmp_path / "source-before-images",
            gate_changed_at="2026-07-21T00:00:00+00:00",
            runner=object(),
        )
    assert model_called is False
    assert inbox.counts() == {"pending": 1}
    management, site = inbox.pending_chat_batches(
        batch_size=25,
        priority_chats=set(),
        exclude_chats={record.chat_id},
    )
    assert management == []
    assert site == []
    conn = sqlite3.connect(tmp_path / "case.db")
    try:
        assert conn.execute("SELECT COUNT(*) FROM bridge_message_log").fetchone()[0] == 0
    finally:
        conn.close()
    assert len(list((tmp_path / "source-before-images").glob("*.json"))) == 1


def test_stage_is_durable_before_cursor_and_idempotent(tmp_path):
    source = tmp_path / "events.jsonl"
    cursor_path = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    values = [_message("m1"), _message("m2")]
    _write_jsonl(source, values)
    first_line_end = len((json.dumps(values[0]) + "\n").encode())

    consumer.initialize_cursor(source, cursor_path, position="start")
    assert inbox.stage_from_source(source, cursor_path, max_records=1) == 1
    assert consumer.SourceCursor.from_path(cursor_path).offset == first_line_end
    assert inbox.counts() == {"pending": 1}

    assert inbox.stage_from_source(source, cursor_path, max_records=10) == 1
    assert inbox.counts() == {"pending": 2}

    # Simulate a crash after the DB commit but before cursor replacement by
    # rewinding the cursor. The unique message id absorbs the re-read.
    raw = json.loads(cursor_path.read_text())
    raw["offset"] = 0
    consumer._atomic_write_json(cursor_path, raw)
    assert inbox.stage_from_source(source, cursor_path, max_records=10) == 2
    assert inbox.counts() == {"pending": 2}
    assert consumer.SourceCursor.from_path(cursor_path).offset == source.stat().st_size


def test_stage_preserves_provider_document_mime_for_spreadsheet_gate(tmp_path):
    source = tmp_path / "events.jsonl"
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    normalized = _message("xlsx-provider-mime")
    normalized.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": ["/capture/jobs.xlsx"],
        }
    )
    wrapped = {
        "normalized": normalized,
        "raw": {
            "message": {
                "documentWithCaptionMessage": {
                    "message": {
                        "documentMessage": {
                            "mimetype": (
                                "application/vnd.openxmlformats-officedocument."
                                "spreadsheetml.sheet"
                            )
                        }
                    }
                }
            }
        },
    }
    _write_jsonl(source, [wrapped])
    consumer.initialize_cursor(source, cursor, position="start")

    assert inbox.stage_from_source(source, cursor) == 1
    record = inbox.pending(limit=1)[0]
    assert record.raw["mediaMimes"] == [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ]
    assert inbox.retention_candidates(limit=1)[0].message_id == "xlsx-provider-mime"


def test_inbox_v1_migration_classifies_existing_retention_queue(tmp_path):
    db = tmp_path / "v1-inbox.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE ingress_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE reply_deliveries (
            delivery_key TEXT PRIMARY KEY, chat_id TEXT NOT NULL,
            reply_to_message_id TEXT, status TEXT NOT NULL,
            bridge_message_id TEXT, error TEXT, created_at TEXT NOT NULL
        );
        CREATE TABLE ingress_events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE, chat_id TEXT NOT NULL,
            source_device INTEGER NOT NULL, source_inode INTEGER NOT NULL,
            start_offset INTEGER NOT NULL, end_offset INTEGER NOT NULL,
            raw_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0, pa_turn_id TEXT, last_error TEXT,
            retained_media_count INTEGER NOT NULL DEFAULT 0,
            retention_failures INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        """
    )
    image = _message("OLD-IMAGE", "site@g.us")
    image.update({
        "hasMedia": True,
        "mediaType": "image/png",
        "mediaUrls": [str(tmp_path / "old.png")],
    })
    text = _message("OLD-TEXT", "site@g.us")
    for item in (image, text):
        conn.execute(
            "INSERT INTO ingress_events(message_id,chat_id,source_device,source_inode,"
            "start_offset,end_offset,raw_json,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (
                item["messageId"], item["chatId"], 1, 1, 0, 1,
                json.dumps({"type": "whatsapp_capture_event", "normalized": item}),
                "2026-07-22T00:00:00+00:00", "2026-07-22T00:00:00+00:00",
            ),
        )
    conn.commit()
    conn.close()

    inbox = consumer.DurableInbox(db)
    with inbox.connect() as migrated:
        columns = {
            row[1] for row in migrated.execute("PRAGMA table_info(ingress_events)")
        }
        migrated_rows = migrated.execute(
            "SELECT message_id,retention_state,raw_json,source_envelope_json "
            "FROM ingress_events"
        ).fetchall()
        states = {
            row["message_id"]: row["retention_state"] for row in migrated_rows
        }
        envelope_backfill = {
            row["message_id"]: (row["raw_json"], row["source_envelope_json"])
            for row in migrated_rows
        }
        schema_version = migrated.execute(
            "SELECT value FROM ingress_meta WHERE key='schema_version'"
        ).fetchone()[0]
    assert {
        "retention_attempts", "retention_state", "retention_last_error",
        "retention_updated_at", "source_envelope_json", "projection_state",
        "projection_attempts", "projection_last_error", "projection_updated_at",
    } <= columns
    assert states == {"OLD-IMAGE": "pending", "OLD-TEXT": "bypassed"}
    assert all(raw_json == source_envelope_json for raw_json, source_envelope_json in envelope_backfill.values())
    assert schema_version == "5"


def test_partial_line_never_advances_cursor(tmp_path):
    source = tmp_path / "events.jsonl"
    cursor_path = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source.write_text(json.dumps(_message("partial")), encoding="utf-8")
    consumer.initialize_cursor(source, cursor_path, position="start")

    assert inbox.stage_from_source(source, cursor_path) == 0
    assert consumer.SourceCursor.from_path(cursor_path).offset == 0
    with source.open("a", encoding="utf-8") as handle:
        handle.write("\n")
    assert inbox.stage_from_source(source, cursor_path) == 1


def test_source_rotation_and_truncation_fail_closed(tmp_path):
    source = tmp_path / "events.jsonl"
    cursor_path = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    _write_jsonl(source, [_message("m1")])
    consumer.initialize_cursor(source, cursor_path, position="end")

    replacement = tmp_path / "replacement.jsonl"
    _write_jsonl(replacement, [_message("m2")])
    replacement.replace(source)
    with pytest.raises(consumer.ConsumerError, match="inode changed"):
        inbox.stage_from_source(source, cursor_path)


def test_singleton_guard_rejects_second_holder(tmp_path):
    lock_path = tmp_path / "consumer.lock"
    with consumer.SingletonLock(lock_path):
        with pytest.raises(consumer.ConsumerError, match="singleton"):
            with consumer.SingletonLock(lock_path):
                pass


def test_disabled_once_does_not_require_or_open_source(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("pa:\n  enabled: false\n", encoding="utf-8")
    gate = tmp_path / "processing-gate.json"
    gate.write_text(
        json.dumps({"version": 1, "enabled": False, "generation": 0}),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        config=str(config),
        source=str(tmp_path / "does-not-exist.jsonl"),
        cursor=str(tmp_path / "does-not-exist.cursor"),
        inbox=str(tmp_path / "inbox.db"),
        status_file=str(tmp_path / "status.json"),
        lock_file=str(tmp_path / "consumer.lock"),
        state_db=str(tmp_path / "state.db"),
        case_db=str(tmp_path / "case.db"),
        source_before_image_dir=str(tmp_path / "source-before-images"),
        processing_gate=str(gate),
        once=True,
        poll_seconds=0.01,
        max_records=10,
    )

    assert asyncio.run(consumer.run_consumer(args)) == 0
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["state"] == "standby"
    assert status["processing_enabled"] is False
    assert status["config_enabled"] is False
    assert status["gate_enabled"] is False
    assert status["source_opened"] is False
    assert status["cursor_advanced"] is False
    assert {
        "retention_total", "retention_failures", "media_root_count",
        "media_root_bytes", "media_volume_free_percent", "media_volume_free_bytes",
        "media_volume_total_bytes",
    } <= set(status)
    assert not Path(args.cursor).exists()


def test_disabled_retention_does_not_touch_media_root(tmp_path):
    media_root = tmp_path / "must-not-exist"
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({
        "pa": {
            "enabled": False,
            "media_retention": {
                "enabled": True, "media_root": str(media_root),
                "source_roots": [str(tmp_path / "capture")],
                "operation": "tgg_media_retention", "min_free_percent": 20,
            },
        },
    }), encoding="utf-8")
    gate = tmp_path / "gate.json"
    gate.write_text(json.dumps({"version": 1, "enabled": False, "generation": 0}))
    args = argparse.Namespace(
        config=str(config), source=str(tmp_path / "missing.jsonl"),
        cursor=str(tmp_path / "missing.cursor"), inbox=str(tmp_path / "inbox.db"),
        status_file=str(tmp_path / "status.json"), lock_file=str(tmp_path / "lock"),
        state_db=str(tmp_path / "state.db"), case_db=str(tmp_path / "case.db"),
        source_before_image_dir=str(tmp_path / "source-before-images"),
        processing_gate=str(gate), once=True,
        poll_seconds=.01, max_records=10,
    )
    assert asyncio.run(consumer.run_consumer(args)) == 0
    assert not media_root.exists()
    status = json.loads(Path(args.status_file).read_text())
    assert status["media_root_count"] == 0
    assert status["media_root_bytes"] == 0
    assert status["media_volume_free_percent"] is None


def test_low_media_volume_holds_before_source_open(tmp_path, monkeypatch):
    args = _enabled_consumer_args(tmp_path, [_message("not-staged")])
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True, "media_root": str(tmp_path / "retained"),
        "source_roots": [str(tmp_path)], "operation": "tgg_media_retention",
        "min_free_percent": 20,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    monkeypatch.setattr(
        consumer.shutil, "disk_usage",
        lambda path: consumer.shutil._ntuple_diskusage(total=100, used=90, free=10),
    )
    # A low-volume condition is a stable hold.  It must not escape the daemon
    # and trigger systemd's restart policy.
    assert asyncio.run(consumer.run_consumer(args)) == 0
    status = json.loads(Path(args.status_file).read_text())
    assert status["state"] == "held"
    assert status["source_opened"] is False
    assert status["media_volume_free_percent"] == 10.0
    assert status["media_volume_free_bytes"] == 10
    assert consumer.SourceCursor.from_path(Path(args.cursor)).offset == 0


def test_absolute_media_reserve_overrides_legacy_percentage_floor(tmp_path, monkeypatch):
    args = _enabled_consumer_args(tmp_path, [_message("not-staged")])
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True, "media_root": str(tmp_path / "retained"),
        "source_roots": [str(tmp_path)], "operation": "tgg_media_retention",
        "min_free_bytes": 50,
        # This deliberately conflicts: the byte reserve is authoritative.
        "min_free_percent": 99,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    monkeypatch.setattr(
        consumer.shutil, "disk_usage",
        lambda path: consumer.shutil._ntuple_diskusage(total=1000, used=910, free=90),
    )
    metrics = consumer._media_root_metrics(config, inspect=True)
    consumer._assert_media_headroom(config, metrics)
    assert metrics["media_volume_free_bytes"] == 90
    assert metrics["media_volume_total_bytes"] == 1000


def test_root_gate_blocks_accidentally_enabled_config_without_opening_source(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("pa:\n  enabled: true\n", encoding="utf-8")
    gate = tmp_path / "processing-gate.json"
    gate.write_text(
        json.dumps({"version": 1, "enabled": False, "generation": 0}),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        config=str(config),
        source=str(tmp_path / "does-not-exist.jsonl"),
        cursor=str(tmp_path / "does-not-exist.cursor"),
        inbox=str(tmp_path / "inbox.db"),
        status_file=str(tmp_path / "status.json"),
        lock_file=str(tmp_path / "consumer.lock"),
        state_db=str(tmp_path / "state.db"),
        case_db=str(tmp_path / "case.db"),
        source_before_image_dir=str(tmp_path / "source-before-images"),
        processing_gate=str(gate),
        once=True,
        poll_seconds=0.01,
        max_records=10,
    )

    assert asyncio.run(consumer.run_consumer(args)) == 0
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["processing_enabled"] is False
    assert status["config_enabled"] is True
    assert status["gate_enabled"] is False
    assert status["source_opened"] is False
    assert status["cursor_advanced"] is False
    assert not Path(args.cursor).exists()


def test_fixture_mode_uses_inbox_path_and_marks_completed(tmp_path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        yaml.safe_dump({
            "model": {
                "provider": "openai-direct-primary",
                "default": "gpt-5.4-mini",
            },
            "pa": {"enabled": True},
        }),
        encoding="utf-8",
    )
    source = tmp_path / "fixture.jsonl"
    _write_jsonl(source, [_message("fixture-1")])

    async def fake_process(records, **kwargs):
        assert [record.message_id for record in records] == ["fixture-1"]
        return {
            "turn_id": "pa-turn-fixture",
            "provider": "openai-direct-primary",
            "model": "gpt-5.4-mini",
            "processed": 1,
            "outbound_captured": 1,
            "captured_outbound": [{"kind": "send", "args": ["chat", "fixture"]}],
            "blocked_commands": 0,
        }

    monkeypatch.setattr(consumer, "process_replay_records", fake_process)
    args = argparse.Namespace(
        test_root=str(tmp_path),
        source=str(source),
        cursor=str(tmp_path / "cursor.json"),
        inbox=str(tmp_path / "inbox.db"),
        config=str(config),
        state_db=str(tmp_path / "state.db"),
        report=str(tmp_path / "report.json"),
        run_id="fixture-run",
        max_records=10,
    )
    assert asyncio.run(consumer.run_fixture(args)) == 0
    assert consumer.DurableInbox(tmp_path / "inbox.db").counts() == {"completed": 1}
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["ok"] is True
    assert report["result"]["turn_id"] == "pa-turn-fixture"
    assert report["result"]["captured_outbound"] == [
        {"kind": "send", "args": ["chat", "fixture"]}
    ]


def test_fixture_mode_rejects_path_outside_test_root(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    config = root / "config.yaml"
    config.write_text("pa:\n  enabled: true\n", encoding="utf-8")
    outside = tmp_path / "outside.jsonl"
    _write_jsonl(outside, [_message("m1")])
    args = argparse.Namespace(
        test_root=str(root),
        source=str(outside),
        cursor=str(root / "cursor.json"),
        inbox=str(root / "inbox.db"),
        config=str(config),
        state_db=str(root / "state.db"),
        report=str(root / "report.json"),
        run_id="fixture-run",
        max_records=10,
    )
    with pytest.raises(consumer.ConsumerError, match="escapes test root"):
        asyncio.run(consumer.run_fixture(args))


def test_bridge_wrapper_is_normalized_without_client_content_in_metadata():
    item = _message("wrapped")
    assert consumer._bridge_item({"event": item}) == item
    with pytest.raises(consumer.ConsumerError, match="messageId/chatId"):
        consumer._bridge_item({"event": {"type": "connection"}})


def _enabled_consumer_args(tmp_path: Path, messages: list[dict]) -> argparse.Namespace:
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(
        yaml.safe_dump({
            "selectors": [{
                "job_type": "tgg_management",
                "match": {
                    "source.platform": "whatsapp",
                    "source.chat_id": "management@g.us",
                },
            }],
        }),
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text(
        yaml.safe_dump({
            "model": {"provider": "openai-direct-primary", "default": "fixture"},
            "pa": {"enabled": True, "constitution_path": str(constitution)},
        }),
        encoding="utf-8",
    )
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, messages)
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    gate = tmp_path / "processing-gate.json"
    gate.write_text(json.dumps({
        "version": 1,
        "enabled": True,
        "generation": 3,
        "changed_at": "2026-07-21T00:00:00+00:00",
    }), encoding="utf-8")
    return argparse.Namespace(
        config=str(config), source=str(source), cursor=str(cursor),
        inbox=str(tmp_path / "inbox.db"), status_file=str(tmp_path / "status.json"),
        lock_file=str(tmp_path / "consumer.lock"), state_db=str(tmp_path / "state.db"),
        case_db=str(_write_case_db(tmp_path / "case.db")),
        source_before_image_dir=str(tmp_path / "source-before-images"),
        processing_gate=str(gate), once=True, poll_seconds=0.001, max_records=100,
        site_concurrency=4, chat_batch_size=25,
    )


def test_pending_chat_batches_are_fifo_and_split_management_capacity(tmp_path):
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source = tmp_path / "events.jsonl"
    values = [
        _message("site-a-1", "site-a@g.us"),
        _message("mgmt-1", "management@g.us"),
        _message("site-a-2", "site-a@g.us"),
        _message("site-b-1", "site-b@g.us"),
    ]
    _write_jsonl(source, values)
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    assert inbox.stage_from_source(source, cursor, max_records=10) == 4

    management, site = inbox.pending_chat_batches(
        batch_size=2, priority_chats={"management@g.us"}
    )
    assert [(chat, [r.message_id for r in rows]) for chat, rows in management] == [
        ("management@g.us", ["mgmt-1"])
    ]
    assert [(chat, [r.message_id for r in rows]) for chat, rows in site] == [
        ("site-a@g.us", ["site-a-1", "site-a-2"]),
        ("site-b@g.us", ["site-b-1"]),
    ]


def test_priority_selector_includes_internal_nightly_without_widening_management(tmp_path):
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(yaml.safe_dump({
        "selectors": [
            {"job_type": "ops_ingest", "match": {"source.platform": "whatsapp", "source.chat_id": "site@g.us"}},
            {"job_type": "tgg_management", "match": {"source.platform": "whatsapp", "source.chat_id": "management@g.us"}},
            {"job_type": "tgg_nightly_whatsapp", "match": {"source.platform": "whatsapp", "source.chat_id": "900000000000000001@g.us"}},
        ],
    }), encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({"pa": {"constitution_path": str(constitution)}}), encoding="utf-8")

    assert consumer._management_selector_chats(config) == {"management@g.us"}
    assert consumer._priority_selector_chats(config) == {
        "management@g.us", "900000000000000001@g.us",
    }
    assert consumer._nightly_selector_chats(config) == {
        "900000000000000001@g.us",
    }


def test_internal_nightly_prose_without_structured_identity_is_rejected(tmp_path):
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(yaml.safe_dump({
        "selectors": [
            {"job_type": "tgg_management", "match": {"source.platform": "whatsapp", "source.chat_id": "management@g.us"}},
            {"job_type": "tgg_nightly_whatsapp", "match": {"source.platform": "whatsapp", "source.chat_id": "900000000000000001@g.us"}},
        ],
    }), encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({"pa": {"constitution_path": str(constitution)}}), encoding="utf-8")

    exact = _message("nightly", "900000000000000001@g.us")
    exact.update({
        "senderId": "system@internal",
        "body": "[system] process TGG WhatsApp batch for 2026-08-17",
    })
    wrong_chat = dict(exact, chatId="management@g.us")
    wrong_sender = dict(exact, senderId="operator@internal")
    assert not consumer._priority_direct_trigger(consumer.InboxRecord(1, "nightly", exact["chatId"], 1, 1, exact), config)
    assert not consumer._priority_direct_trigger(consumer.InboxRecord(2, "wrong-chat", wrong_chat["chatId"], 2, 2, wrong_chat), config)
    assert not consumer._priority_direct_trigger(consumer.InboxRecord(3, "wrong-sender", wrong_sender["chatId"], 3, 3, wrong_sender), config)


def test_seven_session_nightly_trigger_validates_structured_role_assignment_not_body(tmp_path):
    constitution = tmp_path / "constitution.yaml"
    nightly_ids = [f"90000000000000000{index}@g.us" for index in range(1, 8)]
    constitution.write_text(yaml.safe_dump({
        "selectors": [
            {"job_type": "tgg_nightly_whatsapp", "match": {
                "source.platform": "whatsapp", "source.chat_id": chat_id,
            }}
            for chat_id in nightly_ids
        ],
    }), encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({"pa": {"constitution_path": str(constitution)}}), encoding="utf-8")
    batch_id = "nightly:2026-08-17:0123456789ab"
    exact = _message("nightly-six", nightly_ids[0])
    exact.update({
        "senderId": "system@internal",
        "body": (
            f"Nightly WhatsApp analyzer. batch_id={batch_id}. "
            "authoritative_chat_id=120363421424519051@g.us. "
            "Read only that frozen chat through the nightly plugin, investigate it, "
            "and submit its immutable chat receipt."
        ),
        "metadata": {
            "job_type": "tgg_nightly_whatsapp",
            "nightly_batch_id": batch_id,
            "nightly_role": "amk",
            "authoritative_chat_id": "120363421424519051@g.us",
        },
    })
    assert consumer._priority_direct_trigger(
        consumer.InboxRecord(1, "nightly-six", exact["chatId"], 1, 1, exact), config
    )
    recovery = json.loads(json.dumps(exact))
    recovery["body"] = (
        "Resume this AMK batch, repair the rejected finding, and continue "
        "until the immutable chat receipt is accepted."
    )
    assert consumer._priority_direct_trigger(
        consumer.InboxRecord(2, "nightly-recovery", recovery["chatId"], 2, 2, recovery), config
    )
    for key, value in (
        ("nightly_role", "pg"),
        ("authoritative_chat_id", "120363423568509280@g.us"),
        ("nightly_batch_id", "nightly:2026-08-17:not-a-digest"),
    ):
        tampered = json.loads(json.dumps(exact))
        tampered["metadata"][key] = value
        assert not consumer._priority_direct_trigger(
            consumer.InboxRecord(2, f"tampered-{key}", tampered["chatId"], 2, 2, tampered), config
        )

    backend = _message("nightly-backend", nightly_ids[5])
    backend.update({
        "senderId": "system@internal",
        "metadata": {
            "job_type": "tgg_nightly_whatsapp",
            "nightly_batch_id": batch_id,
            "nightly_role": "backend",
            "authoritative_chat_id": "120363404682000990@g.us",
        },
    })
    assert consumer._priority_direct_trigger(
        consumer.InboxRecord(3, "nightly-backend", backend["chatId"], 3, 3, backend), config
    )
    consolidator = _message("nightly-consolidator", nightly_ids[6])
    consolidator.update({
        "senderId": "system@internal",
        "metadata": {
            "job_type": "tgg_nightly_whatsapp",
            "nightly_batch_id": batch_id,
            "nightly_role": "consolidator",
            "authoritative_chat_id": None,
        },
    })
    assert consumer._priority_direct_trigger(
        consumer.InboxRecord(4, "nightly-consolidator", consolidator["chatId"], 4, 4, consolidator), config
    )
    for record in (backend, consolidator):
        swapped = json.loads(json.dumps(record))
        swapped["metadata"]["nightly_role"] = (
            "consolidator" if record is backend else "backend"
        )
        swapped["metadata"]["authoritative_chat_id"] = (
            None if record is backend else "120363404682000990@g.us"
        )
        assert not consumer._priority_direct_trigger(
            consumer.InboxRecord(5, "nightly-swapped", swapped["chatId"], 5, 5, swapped), config
        )


def test_verified_nightly_replay_carries_completion_gate_context(tmp_path):
    nightly_chat = "900000000000000001@g.us"
    authoritative_chat = "120363421424519051@g.us"
    batch_id = "nightly:2026-08-17:0123456789ab"
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(yaml.safe_dump({
        "selectors": [{
            "job_type": "tgg_nightly_whatsapp",
            "match": {
                "source.platform": "whatsapp",
                "source.chat_id": nightly_chat,
            },
        }],
    }), encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({
        "pa": {"constitution_path": str(constitution)},
    }), encoding="utf-8")
    raw = _message("nightly-context", nightly_chat)
    raw.update({
        "senderId": "system@internal",
        "metadata": {
            "job_type": "tgg_nightly_whatsapp",
            "nightly_batch_id": batch_id,
            "nightly_role": "amk",
            "authoritative_chat_id": authoritative_chat,
        },
    })
    record = consumer.InboxRecord(
        1, "nightly-context", nightly_chat, 1, 1, raw,
    )

    replay = consumer._replay_messages_with_retained_documents(
        [record], config_path=config,
    )[0]

    assert replay["_hermes_pa_job_type"] == "tgg_nightly_whatsapp"
    assert replay["_hermes_pa_context"] == {
        "job_type": "tgg_nightly_whatsapp",
        "nightly_batch_id": batch_id,
        "nightly_role": "amk",
        "authoritative_chat_id": authoritative_chat,
    }

    untrusted = json.loads(json.dumps(raw))
    untrusted["senderId"] = "fixture-user"
    untrusted_replay = consumer._replay_messages_with_retained_documents(
        [consumer.InboxRecord(2, "untrusted", nightly_chat, 2, 2, untrusted)],
        config_path=config,
    )[0]
    assert "_hermes_pa_job_type" not in untrusted_replay
    assert "_hermes_pa_context" not in untrusted_replay


@pytest.mark.asyncio
async def test_nightly_selector_runs_in_fresh_session_while_management_stays_persistent(
    tmp_path, monkeypatch
):
    nightly_chat = "900000000000000001@g.us"
    batch_id = "nightly:2026-08-17:0123456789ab"
    nightly = _message("nightly-fresh", nightly_chat)
    nightly.update({
        "senderId": "system@internal",
        "body": (
            f"Nightly WhatsApp analyzer. batch_id={batch_id}. "
            "authoritative_chat_id=120363421424519051@g.us. "
            "Read only that frozen chat through the nightly plugin, investigate it, "
            "and submit its immutable chat receipt."
        ),
        "metadata": {
            "job_type": "tgg_nightly_whatsapp",
            "nightly_batch_id": batch_id,
            "nightly_role": "amk",
            "authoritative_chat_id": "120363421424519051@g.us",
        },
    })
    args = _enabled_consumer_args(tmp_path, [nightly])
    constitution = Path(yaml.safe_load(Path(args.config).read_text())["pa"]["constitution_path"])
    constitution.write_text(yaml.safe_dump({
        "selectors": [
            {"job_type": "tgg_management", "match": {
                "source.platform": "whatsapp", "source.chat_id": "management@g.us",
            }},
            {"job_type": "tgg_nightly_whatsapp", "match": {
                "source.platform": "whatsapp", "source.chat_id": nightly_chat,
            }},
        ],
    }), encoding="utf-8")
    observed: list[bool] = []

    async def fake_process(records, **kwargs):
        observed.append(kwargs["persistent_session"])
        return {
            "processed": len(records),
            "submitted_message_ids": [record.message_id for record in records],
            "handled": [{
                "message_ids": [record.message_id for record in records],
                "turn_id": "turn-nightly-fresh",
            }],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    assert await consumer.run_consumer(args) == 0
    assert observed == [False]


@pytest.mark.asyncio
async def test_validated_continuous_interval_reuses_persistent_nightly_chat_session(
    tmp_path, monkeypatch
):
    nightly_chat = "900000000000000001@g.us"
    batch_id = "nightly:2026-08-27:0123456789ab"
    event = _message("continuous-amk", nightly_chat)
    event.update({
        "senderId": "system@internal",
        "metadata": {
            "job_type": "tgg_nightly_whatsapp",
            "nightly_batch_id": batch_id,
            "nightly_role": "amk",
            "authoritative_chat_id": "120363421424519051@g.us",
            "continuous_interval": True,
            "continuous_contract": "tgg-christopher-continuous-interval/v1",
        },
    })
    args = _enabled_consumer_args(tmp_path, [event])
    constitution = Path(yaml.safe_load(Path(args.config).read_text())["pa"]["constitution_path"])
    constitution.write_text(yaml.safe_dump({
        "selectors": [{"job_type": "tgg_nightly_whatsapp", "match": {
            "source.platform": "whatsapp", "source.chat_id": nightly_chat,
        }}],
    }), encoding="utf-8")
    record = consumer.InboxRecord(1, "continuous-amk", nightly_chat, 1, 1, event)
    assert consumer._continuous_interval_trigger(record, Path(args.config))
    tampered = json.loads(json.dumps(event))
    tampered["metadata"]["continuous_contract"] = "untrusted/v1"
    assert not consumer._continuous_interval_trigger(
        consumer.InboxRecord(2, "tampered", nightly_chat, 2, 2, tampered), Path(args.config),
    )
    observed = []

    async def fake_process(records, **kwargs):
        observed.append(kwargs["persistent_session"])
        return {
            "processed": len(records),
            "submitted_message_ids": [item.message_id for item in records],
            "handled": [{"message_ids": [item.message_id for item in records], "turn_id": "turn-continuous"}],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    assert await consumer.run_consumer(args) == 0
    assert observed == [True]


def test_continuous_and_full_nightly_triggers_never_steer_into_each_others_session(tmp_path):
    pseudo = "900000000000000001@g.us"
    batch_id = "nightly:2026-08-27:0123456789ab"
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(yaml.safe_dump({"selectors": [{
        "job_type": "tgg_nightly_whatsapp",
        "match": {"source.platform": "whatsapp", "source.chat_id": pseudo},
    }]}), encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({"pa": {"constitution_path": str(constitution)}}), encoding="utf-8")
    metadata = {
        "job_type": "tgg_nightly_whatsapp", "nightly_batch_id": batch_id,
        "nightly_role": "amk", "authoritative_chat_id": "120363421424519051@g.us",
    }
    nightly = _message("nightly", pseudo)
    nightly.update({"senderId": "system@internal", "metadata": dict(metadata)})
    continuous = _message("continuous", pseudo)
    continuous.update({"senderId": "system@internal", "metadata": {
        **metadata, "continuous_interval": True,
        "continuous_contract": "tgg-christopher-continuous-interval/v1",
    }})
    nightly_record = consumer.InboxRecord(1, "nightly", pseudo, 1, 1, nightly)
    continuous_record = consumer.InboxRecord(2, "continuous", pseudo, 2, 2, continuous)
    assert not consumer._same_session_steering_allowed([continuous_record], [nightly_record], config)
    assert not consumer._same_session_steering_allowed([nightly_record], [continuous_record], config)
    assert consumer._same_session_steering_allowed([continuous_record], [continuous_record], config)
    assert consumer._same_session_steering_allowed([nightly_record], [nightly_record], config)


def test_management_chat_waits_for_configured_trailing_quiet(tmp_path):
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [_message("mgmt-quiet", "management@g.us")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    with inbox.connect() as conn:
        conn.execute(
            "UPDATE ingress_events SET created_at=? WHERE message_id=?",
            ("2026-08-11T10:00:00+00:00", "mgmt-quiet"),
        )

    management, _ = inbox.pending_chat_batches(
        batch_size=10,
        priority_chats={"management@g.us"},
        priority_quiet_seconds=8,
        now=datetime.fromisoformat("2026-08-11T10:00:07+00:00"),
    )
    assert management == []
    management, _ = inbox.pending_chat_batches(
        batch_size=10,
        priority_chats={"management@g.us"},
        priority_quiet_seconds=8,
        now=datetime.fromisoformat("2026-08-11T10:00:08+00:00"),
    )
    assert [record.message_id for record in management[0][1]] == ["mgmt-quiet"]


def test_management_trigger_requires_mention_or_quoted_christopher_reply():
    ordinary = consumer.InboxRecord(1, "ordinary", "management@g.us", 0, 1, _message("ordinary"))
    mention_raw = _message("mention")
    mention_raw.update({
        "botIds": ["6599999999@s.whatsapp.net"],
        "mentionedIds": ["6599999999@s.whatsapp.net"],
    })
    reply_raw = _message("reply")
    reply_raw.update({
        "botIds": ["6599999999@lid"],
        "quotedParticipant": "6599999999@lid",
        "quotedMessageId": "christopher-outbound",
    })
    wrong_reply_raw = _message("wrong-reply")
    wrong_reply_raw.update({
        "botIds": ["6599999999@lid"],
        "quotedParticipant": "6511111111@lid",
        "quotedMessageId": "someone-else",
    })

    assert consumer._management_direct_trigger(ordinary) is False
    assert consumer._management_direct_trigger(consumer.InboxRecord(2, "mention", "management@g.us", 1, 2, mention_raw)) is True
    assert consumer._management_direct_trigger(consumer.InboxRecord(3, "reply", "management@g.us", 2, 3, reply_raw)) is True
    assert consumer._management_direct_trigger(consumer.InboxRecord(4, "wrong", "management@g.us", 3, 4, wrong_reply_raw)) is False


@pytest.mark.asyncio
async def test_management_brief_admits_unmentioned_messages_without_widening_nightly(
    tmp_path, monkeypatch
):
    real_management = "120363407903158826@g.us"
    test_management = "120363426509183563@g.us"
    nightly_chat = "900000000000000001@g.us"
    messages = [
        _message("real-unmentioned", real_management),
        _message("test-unmentioned", test_management),
        _message("nightly-unmentioned", nightly_chat),
    ]
    args = _enabled_consumer_args(tmp_path, messages)
    constitution = Path(
        yaml.safe_load(Path(args.config).read_text())["pa"]["constitution_path"]
    )
    constitution.write_text(
        yaml.safe_dump({
            "job_briefs": {
                "tgg_management": {
                    "require_mention": False,
                },
            },
            "selectors": [
                {
                    "job_type": "tgg_management",
                    "match": {
                        "source.platform": "whatsapp",
                        "source.chat_id": chat_id,
                    },
                }
                for chat_id in (real_management, test_management)
            ] + [{
                "job_type": "tgg_nightly_whatsapp",
                "match": {
                    "source.platform": "whatsapp",
                    "source.chat_id": nightly_chat,
                },
            }],
        }),
        encoding="utf-8",
    )
    processed_chats: list[str] = []

    async def fake_process(records, **_kwargs):
        chat_id = records[0].chat_id
        processed_chats.append(chat_id)
        message_ids = [record.message_id for record in records]
        return {
            "processed": len(records),
            "submitted_message_ids": message_ids,
            "handled": [{
                "message_ids": message_ids,
                "turn_id": f"turn-{chat_id}",
            }],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())

    assert await consumer.run_consumer(args) == 0
    assert set(processed_chats) == {real_management, test_management}
    inbox = consumer.DurableInbox(Path(args.inbox))
    with inbox.connect() as conn:
        rows = {
            row["message_id"]: (row["status"], row["last_error"])
            for row in conn.execute(
                "SELECT message_id,status,last_error FROM ingress_events"
            )
        }
    assert rows == {
        "real-unmentioned": ("completed", None),
        "test-unmentioned": ("completed", None),
        "nightly-unmentioned": (
            "skipped", "PRIORITY_DIRECT_TRIGGER_NOT_RECOGNIZED"
        ),
    }


def test_management_direct_trigger_setting_defaults_closed(tmp_path):
    args = _enabled_consumer_args(tmp_path, [])
    config = Path(args.config)
    assert consumer._management_requires_direct_trigger(config, "management@g.us")
    assert consumer._management_requires_direct_trigger(config, "other@g.us")


def test_startup_reconciles_successful_turn_refs_and_requeues_only_unmatched(tmp_path):
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [_message("done-ref"), _message("retry-ref")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor, max_records=10)
    records = inbox.pending(limit=10)
    inbox.claim(records)

    state_db = tmp_path / "state.db"
    conn = sqlite3.connect(state_db)
    conn.execute(
        "CREATE TABLE pa_turns(turn_id TEXT, message_refs_json TEXT, "
        "turn_status TEXT, error_json TEXT, completed_at REAL)"
    )
    conn.execute(
        "INSERT INTO pa_turns VALUES(?,?,?,?,?)",
        ("turn-done", json.dumps(["done-ref"]), "completed", None, 1.0),
    )
    conn.commit()
    conn.close()

    before = inbox.total()
    assert inbox.reconcile_orphan_processing(state_db) == {
        "completed": 1, "requeued": 1, "total": 2,
    }
    assert inbox.total() == before
    with inbox.connect() as conn:
        rows = {
            row["message_id"]: (row["status"], row["pa_turn_id"])
            for row in conn.execute(
                "SELECT message_id,status,pa_turn_id FROM ingress_events"
            )
        }
    assert rows == {
        "done-ref": ("completed", "turn-done"),
        "retry-ref": ("pending", None),
    }


def test_conservation_high_water_hard_aborts_on_row_deletion(tmp_path):
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [_message("keep-1"), _message("keep-2")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor, max_records=10)
    assert inbox.assert_and_record_conservation() == 2
    with inbox.connect() as conn:
        conn.execute("DELETE FROM ingress_events WHERE message_id='keep-2'")
    with pytest.raises(consumer.ConsumerError, match="conservation hard-abort"):
        inbox.assert_and_record_conservation()


@pytest.mark.asyncio
async def test_seq_3030_management_lane_runs_during_999_site_shape(
    tmp_path, monkeypatch
):
    # Match the incident's seq exactly without creating unrelated fixture rows.
    messages = [
        _message(f"site-a-{index}", "site-a@g.us")
        if index % 2 == 0 else _message(f"site-b-{index}", "site-b@g.us")
        for index in range(999)
    ]
    messages.append(_message("management-seq-3030", "management@g.us"))
    for message in messages:
        message["timestamp"] = 1784630163.917
    messages[-1].update({
        "botIds": ["6599999999@s.whatsapp.net"],
        "mentionedIds": ["6599999999@s.whatsapp.net"],
    })
    args = _enabled_consumer_args(tmp_path, messages)
    seeded = consumer.DurableInbox(Path(args.inbox))
    with seeded.connect() as conn:
        conn.execute("INSERT INTO sqlite_sequence(name,seq) VALUES('ingress_events',2030)")

    order: list[str] = []
    site_a_started = asyncio.Event()
    site_b_started = asyncio.Event()
    management_done = asyncio.Event()

    async def fake_process(records, **kwargs):
        assert kwargs["persistent_session"] is True
        chat = records[0].chat_id
        if chat == "site-a@g.us":
            site_a_started.set()
            await asyncio.wait_for(management_done.wait(), timeout=2)
            order.append("slow-site-complete")
        elif chat == "site-b@g.us":
            site_b_started.set()
            order.append("other-site-complete")
        else:
            await asyncio.wait_for(site_a_started.wait(), timeout=2)
            await asyncio.wait_for(site_b_started.wait(), timeout=2)
            order.append("management-complete")
            management_done.set()
        return {
            "processed": len(records),
            "submitted_message_ids": [r.message_id for r in records],
            "handled": [{
                "message_ids": [r.message_id for r in records],
                "turn_id": f"turn-{chat}",
            }],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    monkeypatch.delenv("TGG_DEMO_MANAGEMENT_ONLY", raising=False)

    assert await consumer.run_consumer(args) == 0
    assert order.index("management-complete") < order.index("slow-site-complete")
    inbox = consumer.DurableInbox(Path(args.inbox))
    assert inbox.total() == 1000
    with inbox.connect() as conn:
        management = conn.execute(
            "SELECT seq,status,pa_turn_id FROM ingress_events WHERE message_id=?",
            ("management-seq-3030",),
        ).fetchone()
    assert management["seq"] == 3030
    assert management["status"] == "completed"
    assert inbox.counts() == {"completed": 51, "pending": 949}
    status = json.loads(Path(args.status_file).read_text())
    assert status["scheduler_mode"] == "per-chat-parallel"
    assert status["site_concurrency"] == 4
    assert status["state_total"] == 1000


@pytest.mark.asyncio
async def test_cancelling_active_chat_requeues_without_double_processing(
    tmp_path, monkeypatch
):
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [_message("interrupt-me", "site@g.us")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    records = inbox.pending(limit=1)
    entered = asyncio.Event()

    async def never_finishes(records, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(consumer, "process_live_records", never_finishes)
    config = tmp_path / "config.yaml"
    config.write_text("pa:\n  enabled: true\n", encoding="utf-8")
    task = asyncio.create_task(consumer._process_claimed_chat_batch(
        inbox, records, config_path=config, state_db=tmp_path / "state.db",
        case_db=_write_case_db(tmp_path / "case.db"),
        source_before_image_dir=tmp_path / "source-before-images",
        gate_changed_at="2026-07-21T00:00:00+00:00", runner=object(),
    ))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert inbox.counts() == {"pending": 1}
    assert inbox.pending(limit=1)[0].message_id == "interrupt-me"


def test_passive_management_chatter_is_skipped_without_calling_model(
    tmp_path, monkeypatch
):
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [_message("passive", "management@g.us")])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    records = inbox.pending(limit=1)

    async def forbidden_model(*_args, **_kwargs):
        raise AssertionError("passive management chatter reached the model")

    monkeypatch.setattr(consumer, "process_live_records", forbidden_model)
    config = tmp_path / "config.yaml"
    config.write_text("pa:\n  enabled: true\n", encoding="utf-8")
    asyncio.run(consumer._process_claimed_chat_batch(
        inbox,
        records,
        config_path=config,
        state_db=tmp_path / "state.db",
        case_db=_write_case_db(tmp_path / "case.db"),
        source_before_image_dir=tmp_path / "source-before-images",
        gate_changed_at="2026-08-11T00:00:00+00:00",
        runner=object(),
        direct_trigger_required=True,
    ))
    assert inbox.counts() == {"skipped": 1}
    with inbox.connect() as conn:
        row = conn.execute(
            "SELECT last_error FROM ingress_events WHERE message_id='passive'"
        ).fetchone()
    assert row[0] == "PRIORITY_DIRECT_TRIGGER_NOT_RECOGNIZED"


def test_addressed_management_burst_includes_followups_and_steers_while_active(
    tmp_path, monkeypatch
):
    chat_id = "management@g.us"
    first = _message("mention", chat_id)
    first.update({
        "botIds": ["6599999999@s.whatsapp.net"],
        "mentionedIds": ["6599999999@s.whatsapp.net"],
    })
    source = tmp_path / "events.jsonl"
    _write_jsonl(source, [_message("burst-context", chat_id), first])
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)
    records = inbox.pending(limit=2)
    config = tmp_path / "config.yaml"
    config.write_text("pa:\n  enabled: true\n", encoding="utf-8")
    started = asyncio.Event()
    steered = asyncio.Event()
    calls: list[list[str]] = []
    shared_runner = object()

    async def fake_process(batch, **kwargs):
        assert kwargs["runner"] is shared_runner
        ids = [record.message_id for record in batch]
        calls.append(ids)
        if ids == ["burst-context", "mention"]:
            started.set()
            await asyncio.wait_for(steered.wait(), timeout=2)
            return {
                "processed": 2,
                "submitted_message_ids": ids,
                "handled": [{"message_ids": ids, "turn_id": "turn-main"}],
                "captured_outbound": [],
            }
        assert ids == ["followup"]
        steered.set()
        return {
            "processed": 1,
            "submitted_message_ids": ids,
            "handled": [],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "deliver_management_replies", lambda *_a, **_k: {"delivered": 0, "undelivered": 0})
    async def exercise():
        task = asyncio.create_task(consumer._process_claimed_chat_batch(
            inbox,
            records,
            config_path=config,
            state_db=tmp_path / "state.db",
            case_db=_write_case_db(tmp_path / "case.db"),
            source_before_image_dir=tmp_path / "source-before-images",
            gate_changed_at="2026-08-11T00:00:00+00:00",
            runner=shared_runner,
            direct_trigger_required=True,
            allow_active_steering=True,
            steering_poll_seconds=0.01,
        ))
        await asyncio.wait_for(started.wait(), timeout=2)
        with source.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_message("followup", chat_id)) + "\n")
        inbox.stage_from_source(source, cursor)
        await asyncio.wait_for(task, timeout=3)

    asyncio.run(exercise())

    assert calls == [["burst-context", "mention"], ["followup"]]
    assert inbox.counts() == {"completed": 2, "skipped": 1}


@pytest.mark.asyncio
async def test_live_drain_reuses_one_session_across_site_chat_turns(tmp_path):
    args = _enabled_consumer_args(tmp_path, [])
    plans = []

    class Runner:
        async def replay(self, plan):
            plans.append(plan)
            return SimpleNamespace(processed=1, outbound=[])

    chat_id = "site-ops@g.us"
    record = consumer.InboxRecord(
        seq=1, message_id="site-1", chat_id=chat_id,
        start_offset=0, end_offset=1, raw=_message("site-1", chat_id),
    )
    await consumer.process_live_records(
        [record],
        config_path=Path(args.config),
        state_db=Path(args.state_db),
        persistent_session=True,
        runner=Runner(),
    )
    record_2 = consumer.InboxRecord(
        seq=2, message_id="site-2", chat_id=chat_id,
        start_offset=1, end_offset=2, raw=_message("site-2", chat_id),
    )
    await consumer.process_live_records(
        [record_2],
        config_path=Path(args.config),
        state_db=Path(args.state_db),
        persistent_session=True,
        runner=Runner(),
    )
    assert [plan.replay_namespace for plan in plans] == [
        "agent:live-drain:conversation",
        "agent:live-drain:conversation",
    ]
    assert all({message["chatId"] for message in plan.messages} == {chat_id} for plan in plans)

    store = SessionStore(
        sessions_dir=tmp_path / "sessions",
        config=GatewayConfig(group_sessions_per_user=False),
    )
    store._db = None
    source = SessionSource(
        platform=Platform.WHATSAPP,
        chat_id=chat_id,
        chat_type="group",
        user_id="fixture-user",
    )
    session_ids = []
    for plan in plans:
        with replay_context(plan):
            session_ids.append(store.get_or_create_session(source).session_id)

    assert session_ids[0] == session_ids[1]
    with replay_context(plans[-1]):
        other_chat = store.get_or_create_session(
            SessionSource(
                platform=Platform.WHATSAPP,
                chat_id="other-site@g.us",
                chat_type="group",
                user_id="fixture-user",
            )
        )
    assert other_chat.session_id != session_ids[-1]


@pytest.mark.asyncio
async def test_live_drain_keeps_session_namespace_across_provider_and_model_switch(tmp_path):
    """PA-203 round 2: an engine change resumes the same conversation."""
    args = _enabled_consumer_args(tmp_path, [])
    plans = []

    class Runner:
        async def replay(self, plan):
            plans.append(plan)
            return SimpleNamespace(processed=1, outbound=[])

    record = consumer.InboxRecord(
        seq=1, message_id="provider-switch", chat_id="management@g.us",
        start_offset=0, end_offset=1, raw=_message("provider-switch", "management@g.us"),
    )
    await consumer.process_live_records(
        [record], config_path=Path(args.config), state_db=Path(args.state_db),
        persistent_session=True, runner=Runner(),
    )
    Path(args.config).write_text(yaml.safe_dump({
        "model": {"provider": "openai-codex", "default": "gpt-6-sol"},
        "pa": {"enabled": True},
    }))
    await consumer.process_live_records(
        [record], config_path=Path(args.config), state_db=Path(args.state_db),
        persistent_session=True, runner=Runner(),
    )
    assert plans[0].replay_namespace == "agent:live-drain:conversation"
    assert plans[1].replay_namespace == plans[0].replay_namespace


@pytest.mark.asyncio
async def test_bounded_backplay_keeps_session_namespaces_isolated(tmp_path):
    args = _enabled_consumer_args(tmp_path, [])
    plans = []

    class Runner:
        async def replay(self, plan):
            plans.append(plan)
            return SimpleNamespace(processed=1, outbound=[])

    record = consumer.InboxRecord(
        seq=1,
        message_id="backplay-1",
        chat_id="site-ops@g.us",
        start_offset=0,
        end_offset=1,
        raw=_message("backplay-1", "site-ops@g.us"),
    )
    for _ in range(2):
        await consumer.process_live_records(
            [record],
            config_path=Path(args.config),
            state_db=Path(args.state_db),
            persistent_session=False,
            runner=Runner(),
        )

    assert plans[0].replay_namespace != plans[1].replay_namespace
    assert all(
        plan.replay_namespace.startswith("agent:replay:live-drain-")
        for plan in plans
    )


@pytest.mark.asyncio
async def test_process_live_records_defers_structured_provider_error(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({
        "model": {"provider": "test-provider", "default": "test-model"},
        "pa": {"enabled": True},
    }))
    state_db = tmp_path / "state.db"
    with sqlite3.connect(state_db) as conn:
        conn.execute(
            "CREATE TABLE pa_turns("
            "turn_id TEXT,replay_run_id TEXT,message_refs_json TEXT,"
            "turn_status TEXT,provider TEXT,model TEXT,raw_turn_envelope_json TEXT,"
            "error_json TEXT,started_at REAL)"
        )

    class Runner:
        async def replay(self, plan):
            with sqlite3.connect(state_db) as conn:
                conn.execute(
                    "INSERT INTO pa_turns VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        "failed-turn", plan.run_id, json.dumps(["provider-fail"]),
                        "failed", "test-provider", "test-model", "{}",
                        json.dumps({"message": "HTTP 401 Missing Authentication header"}),
                        1.0,
                    ),
                )
            return SimpleNamespace(processed=1, outbound=[])

    record = consumer.InboxRecord(
        seq=1, message_id="provider-fail", chat_id="site@g.us",
        start_offset=0, end_offset=1, raw=_message("provider-fail", "site@g.us"),
    )
    result = await consumer.process_live_records(
        [record], config_path=config, state_db=state_db, runner=Runner(),
        persistent_session=True,
        defer_provider_errors=True,
    )
    assert result["handled"] == []
    assert "HTTP 401 Missing Authentication header" in result["provider_errors"][0]


def _seed_bounded_state(tmp_path: Path):
    chats = ["amk@g.us", "hg@g.us", "pg@g.us", "sk@g.us"]
    inbox = consumer.DurableInbox(tmp_path / "bounded-inbox.db")
    source = tmp_path / "bounded.jsonl"
    values = []
    for index, chat in enumerate(chats):
        message = _message(f"bounded-{index}", chat)
        message["timestamp"] = "2026-07-20T00:00:00+08:00"
        values.append(message)
    extra = _message("bounded-amk-orphan", chats[0])
    extra["timestamp"] = 1784476801
    values.append(extra)
    _write_jsonl(source, values)
    cursor = tmp_path / "bounded.cursor"
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor, max_records=20)
    records = inbox.pending(limit=20)
    inbox.claim([records[0], records[-1]])

    state_db = tmp_path / "state.db"
    with sqlite3.connect(state_db) as conn:
        conn.execute(
            "CREATE TABLE pa_turns(turn_id TEXT, message_refs_json TEXT, "
            "turn_status TEXT, error_json TEXT, completed_at TEXT)"
        )
        conn.execute(
            "INSERT INTO pa_turns VALUES(?,?,?,?,?)",
            ("turn-existing", json.dumps([records[0].message_id]), "completed", None,
             "2026-07-20T01:00:00+00:00"),
        )
    case_db = tmp_path / "case.db"
    with sqlite3.connect(case_db) as conn:
        conn.execute("CREATE TABLE cases(id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO cases DEFAULT VALUES")
        conn.execute(
            "CREATE TABLE ps_audit_log("
            "id TEXT,action TEXT,target_kind TEXT,target_id TEXT,"
            "source_surface TEXT,ts TEXT)"
        )
    canonical = tmp_path / "canonical.env"
    canonical.write_text("CHRISTOPHER_TGG_PS_SERVICE_TOKEN=test-token\n")
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({
        "model": {"provider": "test-provider", "default": "test-model"},
        "pa": {"enabled": True},
    }))
    return inbox, chats, state_db, case_db, canonical, config


def test_bounded_window_reconciliation_is_scoped_and_conserving(tmp_path):
    inbox, chats, state_db, *_ = _seed_bounded_state(tmp_path)
    cutoff = consumer._parse_ingress_timestamp("2026-07-20T00:00:00+08:00")
    selected = inbox.bounded_window(chat_ids=chats, cutoff=cutoff)
    assert len(selected) == 5
    before = inbox.total()
    preview = inbox.reconcile_window_processing(selected, state_db, dry_run=True)
    assert preview["completed"] == 1
    assert preview["requeued"] == 1
    assert preview["unresolved"] == []
    assert inbox.counts() == {"pending": 3, "processing": 2}

    applied = inbox.reconcile_window_processing(selected, state_db, dry_run=False)
    assert applied["completed"] == 1
    assert applied["requeued"] == 1
    assert inbox.total() == before
    assert inbox.counts() == {"completed": 1, "pending": 4}


def test_bounded_refusal_guards_cover_window_count_orphan_and_token(tmp_path):
    inbox, chats, state_db, _, canonical, _ = _seed_bounded_state(tmp_path)
    cutoff = consumer._parse_ingress_timestamp("2026-07-20T00:00:00+08:00")
    selected = inbox.bounded_window(chat_ids=chats, cutoff=cutoff)

    with pytest.raises(consumer.ConsumerError, match="denominator mismatch"):
        consumer.assert_bounded_selection(
            selected, chat_ids=chats, cutoff=cutoff, expected_total=6
        )
    outside = consumer.InboxRecord(
        seq=999, message_id="outside", chat_id="not-allowed@g.us",
        start_offset=0, end_offset=1,
        raw={**_message("outside", "not-allowed@g.us"), "timestamp": 1784476801},
    )
    with pytest.raises(consumer.ConsumerError, match="out-of-window"):
        consumer.assert_bounded_selection(
            [*selected, outside], chat_ids=chats, cutoff=cutoff, expected_total=6
        )
    with pytest.raises(consumer.ConsumerError, match="processing/orphan"):
        consumer.assert_no_window_orphans({"1": "pending", "2": "processing"})
    with pytest.raises(consumer.ConsumerError, match="mismatch"):
        consumer.assert_service_token_hash(
            canonical, "CHRISTOPHER_TGG_PS_SERVICE_TOKEN",
            environ={"CHRISTOPHER_TGG_PS_SERVICE_TOKEN": "wrong-token"},
        )


def test_bounded_dry_run_is_read_only_and_predicts_reconciliation(
    tmp_path, monkeypatch
):
    inbox, chats, state_db, case_db, canonical, config = _seed_bounded_state(tmp_path)
    cutoff = consumer._parse_ingress_timestamp("2026-07-20T00:00:00+08:00")
    selected = inbox.bounded_window(chat_ids=chats, cutoff=cutoff)

    def logical_state():
        conn = inbox.connect()
        try:
            inbox_rows = [
                tuple(row)
                for row in conn.execute(
                    "SELECT * FROM ingress_events ORDER BY seq"
                )
            ]
            meta_rows = [
                tuple(row)
                for row in conn.execute("SELECT * FROM ingress_meta ORDER BY key")
            ]
            delivery_rows = [
                tuple(row)
                for row in conn.execute(
                    "SELECT * FROM reply_deliveries ORDER BY delivery_key"
                )
            ]
        finally:
            conn.close()
        conn = sqlite3.connect(state_db)
        try:
            state_rows = conn.execute(
                "SELECT * FROM pa_turns ORDER BY turn_id"
            ).fetchall()
        finally:
            conn.close()
        conn = sqlite3.connect(case_db)
        try:
            case_rows = conn.execute("SELECT * FROM cases ORDER BY id").fetchall()
            audit_rows = conn.execute(
                "SELECT * FROM ps_audit_log ORDER BY id"
            ).fetchall()
        finally:
            conn.close()
        return {
            "inbox_rows": inbox_rows,
            "meta_rows": meta_rows,
            "delivery_rows": delivery_rows,
            "inbox_counts": inbox.counts(),
            "selected_statuses": inbox.window_statuses(selected),
            "state_rows": state_rows,
            "case_rows": case_rows,
            "audit_rows": audit_rows,
        }

    before = logical_state()
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    args = argparse.Namespace(
        inbox=str(inbox.db_path), config=str(config), state_db=str(state_db),
        case_db=str(case_db), canonical_env=str(canonical),
        service_token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", chat_id=chats,
        cutoff="2026-07-20T00:00:00+08:00", expected_total=5, batch_size=2,
        audit=str(tmp_path / "dry-audit.json"), run_id="dry-test", dry_run=True,
        lock_file=str(tmp_path / "consumer.lock"),
    )
    assert asyncio.run(consumer.run_bounded_backplay(args)) == 0
    assert logical_state() == before
    assert inbox.counts() == {"pending": 3, "processing": 2}
    audit = json.loads(Path(args.audit).read_text())
    assert audit["reconciliation"]["completed"] == 1
    assert audit["reconciliation"]["requeued"] == 1
    assert audit["processed_message_ids"] == []
    assert audit["zero_real_sends"] is True


def test_bounded_execution_never_calls_delivery(tmp_path, monkeypatch):
    inbox, chats, state_db, case_db, canonical, config = _seed_bounded_state(tmp_path)
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())

    async def fake_process(records, **kwargs):
        assert kwargs["persistent_session"] is False
        return {
            "submitted_message_ids": [record.message_id for record in records],
            "handled": [{
                "message_ids": [record.message_id for record in records],
                "turn_id": "turn-new",
            }],
            "captured_outbound": [{
                "kind": "send",
                "args": [records[0].chat_id, "captured fixture response"],
                "kwargs": {"reply_to": records[0].message_id},
                "replay_run_id": "fixture-run",
            }],
            "outbound_sent": 0,
        }

    def forbidden_delivery(*args, **kwargs):
        raise AssertionError("bounded execution must never call real delivery")

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "deliver_management_replies", forbidden_delivery)
    args = argparse.Namespace(
        inbox=str(inbox.db_path), config=str(config), state_db=str(state_db),
        case_db=str(case_db), canonical_env=str(canonical),
        service_token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", chat_id=chats,
        cutoff="2026-07-20T00:00:00+08:00", expected_total=5, batch_size=2,
        audit=str(tmp_path / "execute-audit.json"), run_id="execute-test", dry_run=False,
        lock_file=str(tmp_path / "consumer.lock"),
    )
    assert asyncio.run(consumer.run_bounded_backplay(args)) == 0
    audit = json.loads(Path(args.audit).read_text())
    assert audit["zero_real_sends"] is True
    assert audit["outbound_sent"] == 0
    assert len(audit["processed_message_ids"]) == 4
    assert audit["captured_outbound"] == 4
    assert audit["captured_outbound_entries"][0] == {
        "capture_index": 0,
        "kind": "send",
        "chat_id": "hg@g.us",
        "batch_chat_ids": ["hg@g.us"],
        "message_ids": ["bounded-1"],
        "turn_ids": ["turn-new"],
        "reply_to": "bounded-1",
        "body": "captured fixture response",
        "raw": {
            "kind": "send",
            "args": ["hg@g.us", "captured fixture response"],
            "kwargs": {"reply_to": "bounded-1"},
            "replay_run_id": "fixture-run",
        },
    }
    assert audit["conservation"]["preserved"] is True
    assert inbox.counts() == {"completed": 5}


def test_bounded_live_refuses_while_ordinary_consumer_holds_lock(
    tmp_path, monkeypatch
):
    inbox, chats, state_db, case_db, canonical, config = _seed_bounded_state(tmp_path)
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    lock_file = tmp_path / "consumer.lock"
    args = argparse.Namespace(
        inbox=str(inbox.db_path), config=str(config), state_db=str(state_db),
        case_db=str(case_db), canonical_env=str(canonical),
        service_token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", chat_id=chats,
        cutoff="2026-07-20T00:00:00+08:00", expected_total=5, batch_size=2,
        audit=str(tmp_path / "locked-audit.json"), run_id="locked-test",
        dry_run=False, lock_file=str(lock_file),
    )
    before = inbox.counts()
    before_file = (inbox.db_path.read_bytes(), inbox.db_path.stat().st_mtime_ns)
    with consumer.SingletonLock(lock_file):
        with pytest.raises(consumer.ConsumerError, match="singleton"):
            asyncio.run(consumer.run_bounded_backplay(args))
    assert inbox.counts() == before
    after_file = (inbox.db_path.read_bytes(), inbox.db_path.stat().st_mtime_ns)
    assert after_file == before_file
    assert not Path(args.audit).exists()


def test_bounded_partial_failure_persists_mutation_and_conservation_audit(
    tmp_path, monkeypatch
):
    inbox, chats, state_db, case_db, canonical, config = _seed_bounded_state(tmp_path)
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    calls = 0

    async def first_succeeds_second_provider_fails(records, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            body = "AuthenticationError: HTTP 401 Missing Authentication header"
            return {
                "submitted_message_ids": [record.message_id for record in records],
                "handled": [],
                "provider_errors": [body],
                "captured_outbound": [{
                    "kind": "send",
                    "args": [records[0].chat_id, body],
                    "kwargs": {},
                }],
                "outbound_sent": 0,
            }
        with sqlite3.connect(case_db) as conn:
            conn.execute("INSERT INTO cases DEFAULT VALUES")
            conn.execute(
                "INSERT INTO ps_audit_log VALUES(?,?,?,?,?,?)",
                ("audit-1", "update", "case", "case-1", "fixture", "now"),
            )
        return {
            "submitted_message_ids": [record.message_id for record in records],
            "handled": [{
                "message_ids": [record.message_id for record in records],
                "turn_id": "turn-new",
            }],
            "captured_outbound": [],
            "outbound_sent": 0,
        }

    monkeypatch.setattr(
        consumer, "process_live_records", first_succeeds_second_provider_fails
    )
    args = argparse.Namespace(
        inbox=str(inbox.db_path), config=str(config), state_db=str(state_db),
        case_db=str(case_db), canonical_env=str(canonical),
        service_token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", chat_id=chats,
        cutoff="2026-07-20T00:00:00+08:00", expected_total=5, batch_size=2,
        audit=str(tmp_path / "partial-audit.json"), run_id="partial-test",
        dry_run=False, lock_file=str(tmp_path / "consumer.lock"),
    )
    with pytest.raises(consumer.ConsumerError, match="HTTP 401"):
        asyncio.run(consumer.run_bounded_backplay(args))
    audit = json.loads(Path(args.audit).read_text())
    assert audit["ok"] is False
    assert audit["case_count_delta"]["cases"] == 1
    assert audit["conservation"]["preserved"] is True
    assert [row["status"] for row in audit["mutations"]] == ["completed", "retryable"]
    assert audit["captured_outbound_entries"][0]["body"].startswith(
        "AuthenticationError"
    )
    assert inbox.counts() == {"completed": 2, "pending": 3}
    assert audit["business_mutations"] == [{
        "rowid": 1, "id": "audit-1", "action": "update",
        "target_kind": "case", "target_id": "case-1",
        "source_surface": "fixture", "ts": "now",
    }]


def test_bounded_provider_error_is_retryable_and_capture_is_durable(
    tmp_path, monkeypatch
):
    inbox, chats, state_db, case_db, canonical, config = _seed_bounded_state(tmp_path)
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    seen_config_paths = []

    class Runner:
        async def replay(self, plan):
            assert Path(os.environ["HERMES_HOME"]) == config.parent.resolve()
            exc = RuntimeError("HTTP 403 AuthorizationError: provider-auth refused")
            exc.replay_outbound = [{
                "kind": "send",
                "args": [plan.messages[0]["chatId"], "captured before provider failure"],
                "kwargs": {},
                "replay_run_id": plan.run_id,
            }]
            raise exc

    def runner_factory(config_path=None):
        seen_config_paths.append(config_path)
        assert Path(config_path) == config.resolve()
        assert Path(os.environ["HERMES_HOME"]) == config.parent.resolve()
        return Runner()

    monkeypatch.setattr(consumer, "_new_gateway_runner", runner_factory)

    args = argparse.Namespace(
        inbox=str(inbox.db_path), config=str(config), state_db=str(state_db),
        case_db=str(case_db), canonical_env=str(canonical),
        service_token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", chat_id=chats,
        cutoff="2026-07-20T00:00:00+08:00", expected_total=5, batch_size=2,
        audit=str(tmp_path / "provider-error-audit.json"),
        run_id="provider-error-test", dry_run=False,
        lock_file=str(tmp_path / "consumer.lock"),
    )
    with pytest.raises(consumer.ConsumerError, match="HTTP 403 AuthorizationError"):
        asyncio.run(consumer.run_bounded_backplay(args))

    audit = json.loads(Path(args.audit).read_text())
    assert audit["ok"] is False
    assert "HTTP 403 AuthorizationError" in audit["error"]
    assert audit["captured_outbound"] == 1
    capture = audit["captured_outbound_entries"][0]
    assert capture["body"] == "captured before provider failure"
    assert capture["chat_id"] == "hg@g.us"
    assert capture["message_ids"] == ["bounded-1"]
    assert capture["turn_ids"] == []
    assert capture["raw"]["replay_run_id"].startswith("live-drain-")
    assert audit["mutations"][0]["status"] == "retryable"
    assert audit["conservation"]["preserved"] is True
    assert inbox.counts() == {"completed": 1, "pending": 4}
    assert seen_config_paths == [config.resolve()]


@pytest.mark.parametrize("body", [
    "HTTP 403 AuthorizationError: forbidden",
    "provider-auth failed while resolving credentials",
    "model resolution failed: unable to resolve model deployment",
])
def test_captured_provider_error_recognizes_auth_and_model_resolution(body):
    assert consumer._captured_provider_error([{
        "kind": "send", "args": ["ops@g.us", body], "kwargs": {},
    }]) == body


def test_bounded_captured_provider_error_fallback_is_retryable(tmp_path, monkeypatch):
    inbox, chats, state_db, case_db, canonical, config = _seed_bounded_state(tmp_path)
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())

    async def captured_error_without_structured_turn(records, **kwargs):
        return {
            "submitted_message_ids": [record.message_id for record in records],
            "handled": [],
            "captured_outbound": [{
                "kind": "send",
                "args": [records[0].chat_id, "HTTP 401 Missing Authentication header"],
                "kwargs": {},
            }],
            "outbound_sent": 0,
        }

    monkeypatch.setattr(
        consumer, "process_live_records", captured_error_without_structured_turn
    )
    args = argparse.Namespace(
        inbox=str(inbox.db_path), config=str(config), state_db=str(state_db),
        case_db=str(case_db), canonical_env=str(canonical),
        service_token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", chat_id=chats,
        cutoff="2026-07-20T00:00:00+08:00", expected_total=5, batch_size=2,
        audit=str(tmp_path / "captured-error-audit.json"), run_id="fallback-test",
        dry_run=False, lock_file=str(tmp_path / "consumer.lock"),
    )
    with pytest.raises(consumer.ConsumerError, match="HTTP 401"):
        asyncio.run(consumer.run_bounded_backplay(args))
    assert inbox.counts() == {"completed": 1, "pending": 4}


def test_bounded_legitimate_consumed_no_turn_is_terminal_skipped(tmp_path, monkeypatch):
    inbox, chats, state_db, case_db, canonical, config = _seed_bounded_state(tmp_path)
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())

    async def consumed_no_turn(records, **kwargs):
        return {
            "submitted_message_ids": [record.message_id for record in records],
            "handled": [],
            "captured_outbound": [],
            "provider_errors": [],
            "outbound_sent": 0,
        }

    monkeypatch.setattr(consumer, "process_live_records", consumed_no_turn)
    args = argparse.Namespace(
        inbox=str(inbox.db_path), config=str(config), state_db=str(state_db),
        case_db=str(case_db), canonical_env=str(canonical),
        service_token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", chat_id=chats,
        cutoff="2026-07-20T00:00:00+08:00", expected_total=5, batch_size=2,
        audit=str(tmp_path / "no-turn-audit.json"), run_id="no-turn-test",
        dry_run=False, lock_file=str(tmp_path / "consumer.lock"),
    )
    assert asyncio.run(consumer.run_bounded_backplay(args)) == 0
    audit = json.loads(Path(args.audit).read_text())
    assert audit["ok"] is True
    assert audit["captured_outbound_entries"] == []
    assert audit["zero_real_sends"] is True
    assert audit["conservation"]["preserved"] is True
    assert all(row["skipped"] == len(row["message_ids"]) for row in audit["mutations"])
    assert inbox.counts() == {"completed": 1, "skipped": 4}


def test_message_id_selection_is_exact_and_missing_refuses(tmp_path):
    inbox, *_ = _seed_bounded_state(tmp_path)
    selected = inbox.message_id_selection(["bounded-3", "bounded-1"])
    assert [record.message_id for record in selected] == ["bounded-1", "bounded-3"]
    consumer.assert_message_id_selection(
        selected,
        expected_message_ids=["bounded-3", "bounded-1"],
        expected_total=2,
    )
    with pytest.raises(consumer.ConsumerError, match="missing from inbox"):
        inbox.message_id_selection(["bounded-1", "not-present"])


def test_readjudication_reset_writes_before_image_and_conserves(tmp_path):
    inbox, *_ = _seed_bounded_state(tmp_path)
    records = inbox.message_id_selection(["bounded-1", "bounded-2"])
    with inbox.connect() as conn:
        conn.execute(
            "UPDATE ingress_events SET status='completed',pa_turn_id='old-turn' "
            "WHERE message_id='bounded-1'"
        )
    before_count = inbox.total()
    before_image = tmp_path / "readjudication-before.json"
    result = inbox.requeue_selected_for_readjudication(
        records,
        before_image_path=before_image,
        run_id="readjudication-test",
        dry_run=False,
    )
    assert result["selected"] == 2
    assert result["cas_updated"] == 2
    assert result["status_before"] == {"completed": 1, "pending": 1}
    image = json.loads(before_image.read_text())
    assert image["selected_count"] == 2
    assert {row["message_id"]: row["status"] for row in image["rows"]} == {
        "bounded-1": "completed",
        "bounded-2": "pending",
    }
    assert inbox.total() == before_count
    with inbox.connect() as conn:
        rows = conn.execute(
            "SELECT message_id,status,pa_turn_id,last_error FROM ingress_events "
            "WHERE message_id IN ('bounded-1','bounded-2') ORDER BY message_id"
        ).fetchall()
    assert [(row["message_id"], row["status"], row["pa_turn_id"]) for row in rows] == [
        ("bounded-1", "pending", None),
        ("bounded-2", "pending", None),
    ]
    assert all(row["last_error"] == "readjudication:readjudication-test" for row in rows)


def test_readjudication_dry_run_writes_nothing(tmp_path):
    inbox, *_ = _seed_bounded_state(tmp_path)
    records = inbox.message_id_selection(["bounded-1"])
    before = inbox.db_path.read_bytes()
    before_image = tmp_path / "must-not-exist.json"
    result = inbox.requeue_selected_for_readjudication(
        records,
        before_image_path=before_image,
        run_id="dry-readjudication",
        dry_run=True,
    )
    assert result["cas_updated"] == 0
    assert inbox.db_path.read_bytes() == before
    assert not before_image.exists()


def test_readjudication_draft_transitions_are_audited_and_dry_run_is_read_only(
    tmp_path,
):
    case_db = tmp_path / "case.db"
    conn = sqlite3.connect(case_db)
    conn.executescript("""
        CREATE TABLE draft_outbound (
          id INTEGER PRIMARY KEY, case_id INTEGER, channel TEXT NOT NULL,
          recipient TEXT, body TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'draft',
          created_by TEXT NOT NULL, approved_by TEXT, sent_at INTEGER,
          created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        CREATE TABLE ps_audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, tenant_slug TEXT NOT NULL,
          actor_kind TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
          target_kind TEXT, target_id TEXT, before_json TEXT, after_json TEXT,
          source_surface TEXT NOT NULL, summary TEXT NOT NULL, ts INTEGER NOT NULL
        );
        INSERT INTO draft_outbound VALUES
          (1,NULL,'clarification',NULL,'one','draft','christopher',NULL,NULL,1,1),
          (2,NULL,'clarification',NULL,'two','draft','christopher',NULL,NULL,1,1);
    """)
    conn.commit()
    conn.close()
    before = case_db.read_bytes()
    before_image = tmp_path / "draft-before.json"
    preview = consumer._transition_readjudication_drafts(
        case_db,
        readjudicated_ids=[1],
        pending_manager_ids=[2],
        manager_chat_id="manager@g.us",
        before_image_path=before_image,
        run_id="draft-test",
        dry_run=True,
    )
    assert preview == {"readjudicated": 1, "pending_manager": 1, "dry_run": True}
    assert case_db.read_bytes() == before
    assert not before_image.exists()

    result = consumer._transition_readjudication_drafts(
        case_db,
        readjudicated_ids=[1],
        pending_manager_ids=[2],
        manager_chat_id="manager@g.us",
        before_image_path=before_image,
        run_id="draft-test",
        dry_run=False,
    )
    assert result["readjudicated"] == 1
    assert result["pending_manager"] == 1
    image = json.loads(before_image.read_text())
    assert [row["id"] for row in image["draft_outbound"]] == [1, 2]
    conn = sqlite3.connect(case_db)
    rows = conn.execute(
        "SELECT id,state,recipient FROM draft_outbound ORDER BY id"
    ).fetchall()
    actions = conn.execute("SELECT action,target_id FROM ps_audit_log ORDER BY id").fetchall()
    conn.close()
    assert rows == [(1, "readjudicated", None), (2, "pending_manager", "manager@g.us")]
    assert actions == [
        ("clarification.readjudicated", "1"),
        ("clarification.pending_manager", "2"),
    ]
