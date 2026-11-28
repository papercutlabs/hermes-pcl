"""PA-203 r2d: Krisleen's file comes back under the name she sent.

19 Sep 2026, Krisleen: every file Christopher returned carried underscores
where her name had spaces and parentheses.  The capture bridge stores a
document as ``doc_<id>_<name with every other character as _>``; only the
provider message in the capture record keeps WhatsApp's ``fileName``.

1. The exact name survives intake: it reaches the turn as "Original
   filename" and the files log, while the stored file keeps its safe name.
2. Every send path shows WhatsApp an exact display name, spaces and
   parentheses included, while the stored copy keeps a safe name.
3. Two sends under one display name both go out.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.cron.test_pa203_agent_workspace import (  # noqa: F401 - fixtures
    LIVE_KEY,
    MANAGEMENT_CHAT,
    SANDBOX_CONFIG,
    _Bridge,
    _files_log,
    _job,
    _records,
    _run_and_deliver,
    _runner,
    _turn,
    _write,
    agent_home,
    home,
)

HER_NAME = "15 Jul 2026 - List Of Outstanding WCs (Savills).xlsx"
BRIDGE_STORED = "doc_98d297901510_15_Jul_2026_-_List_Of_Outstanding_WCs__Savills_.xlsx"
SEND_FOLDER = r"records/sent/send-\d{8}T\d{6}Z-[0-9a-f]{12}/"


def _capture_event(document: Path, message_id: str = "3EB0KRISDOC") -> dict:
    """The live capture record's shape (events.jsonl, 23 Jul 2026)."""
    return {
        "capturedAt": "2026-09-19T03:00:00.000Z",
        "type": "whatsapp_capture_event",
        "normalized": {
            "messageId": message_id,
            "chatId": MANAGEMENT_CHAT,
            "senderId": "188690831777827@lid",
            "senderName": "Krisleen",
            "isGroup": True,
            "body": "@139973755973678 please update",
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(document)],
            "timestamp": 1789779600,
        },
        "raw": {
            "key": {"remoteJid": MANAGEMENT_CHAT, "id": message_id},
            "message": {
                "documentWithCaptionMessage": {
                    "message": {
                        "documentMessage": {
                            "mimetype": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            "title": HER_NAME,
                            "fileName": HER_NAME,
                            "caption": "@139973755973678 please update",
                        }
                    }
                }
            },
        },
    }


def _workbook(path: Path) -> Path:
    from openpyxl import Workbook

    path.parent.mkdir(parents=True, exist_ok=True)
    book = Workbook()
    book.active["A1"] = "Job No"
    book.save(path)
    return path


# 1. The name survives intake ------------------------------------------------


def test_consumer_keeps_the_senders_file_name_from_the_capture_record(tmp_path):
    from gateway import durable_jsonl_consumer as consumer

    document = _workbook(tmp_path / "documents" / BRIDGE_STORED)
    source = tmp_path / "events.jsonl"
    source.write_text(json.dumps(_capture_event(document)) + "\n", encoding="utf-8")
    cursor = tmp_path / "cursor.json"
    inbox = consumer.DurableInbox(tmp_path / "inbox.db")
    consumer.initialize_cursor(source, cursor, position="start")
    inbox.stage_from_source(source, cursor)

    [record] = inbox.pending(limit=5)
    assert record.raw["mediaFileNames"] == [HER_NAME]
    assert record.raw["mediaUrls"] == [str(document)]


@pytest.mark.asyncio
async def test_whatsapp_event_carries_the_senders_file_name(tmp_path, monkeypatch):
    from gateway.config import PlatformConfig
    from gateway.platforms.whatsapp import WhatsAppAdapter

    monkeypatch.delenv("WHATSAPP_GROUP_POLICY", raising=False)
    document = _workbook(tmp_path / "documents" / BRIDGE_STORED)
    item = dict(_capture_event(document)["normalized"])
    item["mediaMimes"] = ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]
    item["mediaFileNames"] = [HER_NAME]
    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={"group_policy": "open"}))

    event = await adapter._build_message_event(item, bypass_require_mention=True)

    assert event.media_urls == [str(document)]
    assert event.media_file_names == [HER_NAME]


@pytest.mark.asyncio
async def test_turn_and_files_log_show_her_exact_name(agent_home, tmp_path, monkeypatch):
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    monkeypatch.setattr(
        "hermes_cli.config.read_raw_config", lambda: {"python_sandbox": SANDBOX_CONFIG}
    )
    document = tmp_path / "documents" / BRIDGE_STORED
    document.parent.mkdir()
    document.write_bytes(b"Krisleen's outstanding list")
    source = SessionSource(platform=Platform.WHATSAPP, chat_id=MANAGEMENT_CHAT, chat_type="group")
    event = MessageEvent(
        text="please update",
        message_type=MessageType.DOCUMENT,
        source=source,
        message_id="3EB0KRISDOC",
        media_urls=[str(document)],
        media_types=["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"],
        media_file_names=[HER_NAME],
    )

    text = await _runner()._prepare_inbound_message_text(event=event, source=source, history=[])

    assert f"[The user sent a document: '{HER_NAME}'." in text
    assert f"Original filename: '{HER_NAME}'." in text
    # The stored file keeps its safe name.
    assert f"Host path: {document}" in text
    assert f"Saved in your workspace as /records/received/{BRIDGE_STORED}." in text
    [entry] = _files_log(agent_home)
    assert entry["direction"] == "received"
    assert entry["original_name"] == HER_NAME
    assert entry["path"] == f"records/received/{BRIDGE_STORED}"


@pytest.mark.asyncio
async def test_without_a_declared_name_the_turn_is_unchanged(agent_home, tmp_path, monkeypatch):
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    monkeypatch.setattr(
        "hermes_cli.config.read_raw_config", lambda: {"python_sandbox": SANDBOX_CONFIG}
    )
    document = tmp_path / "documents" / BRIDGE_STORED
    document.parent.mkdir()
    document.write_bytes(b"older capture")
    source = SessionSource(platform=Platform.WHATSAPP, chat_id=MANAGEMENT_CHAT, chat_type="group")
    event = MessageEvent(
        text="please update",
        message_type=MessageType.DOCUMENT,
        source=source,
        media_urls=[str(document)],
        media_types=["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"],
    )

    text = await _runner()._prepare_inbound_message_text(event=event, source=source, history=[])

    assert f"Original filename: '{BRIDGE_STORED}'." in text
    assert "original_name" not in _files_log(agent_home)[0]


def test_quoting_her_file_names_it_exactly(agent_home, tmp_path):
    import tools.python_sandbox_tool as sandbox

    document = tmp_path / BRIDGE_STORED
    document.write_bytes(b"list")
    sandbox.record_workspace_file(
        document, direction="received", folder="received",
        details={"chat_id": MANAGEMENT_CHAT, "message_id": "3EB0KRISDOC",
                 "original_name": HER_NAME},
    )

    note = sandbox.describe_quoted_workspace_files("3EB0KRISDOC", chat_id=MANAGEMENT_CHAT)

    assert f"that sent you a file named '{HER_NAME}', now saved as " in note


# 2. The display name is sent exactly -----------------------------------------


def _deliver(captured: list[dict], monkeypatch, *, expect_delivered: bool = True) -> list[dict]:
    from gateway.durable_jsonl_consumer import DurableInbox, InboxRecord, deliver_management_replies

    home = Path(__import__("hermes_constants").get_hermes_home())
    constitution = home / "r2d-constitution.yaml"
    constitution.write_text(
        "selectors:\n- job_type: tgg_management\n  match:\n"
        f"    source.platform: whatsapp\n    source.chat_id: '{MANAGEMENT_CHAT}'\n",
        encoding="utf-8",
    )
    config = home / "r2d-config.yaml"
    config.write_text(f"pa:\n  enabled: true\n  constitution_path: {constitution}\n", encoding="utf-8")
    sent: list[dict] = []
    ids = iter(f"3EB0SENT{n}" for n in range(10))

    class Response(_Bridge):
        def __init__(self):
            self.body = json.dumps({"success": True, "messageId": next(ids)}).encode()

        def read(self):
            return self.body

        status = 200

    def fake_urlopen(request, timeout=0):
        sent.append(json.loads(request.data))
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "gateway.durable_jsonl_consumer.threading.Thread",
        lambda target, args=(), kwargs=None, **_: type(
            "Now", (), {"start": lambda self: target(*args, **(kwargs or {}))}
        )(),
    )
    records = [
        InboxRecord(
            seq=index + 1, message_id=f"MSG{index + 1}", chat_id=MANAGEMENT_CHAT,
            start_offset=0, end_offset=1,
            raw={"messageId": f"MSG{index + 1}", "chatId": MANAGEMENT_CHAT,
                 "timestamp": "2026-09-25T04:01:00+00:00"},
        )
        for index in range(len(captured))
    ]
    summary = deliver_management_replies(
        DurableInbox(home / "r2d-inbox.db"),
        config_path=config,
        captured_outbound=captured,
        batch_records=records,
        gate_changed_at="2026-09-25T04:00:00+00:00",
        handled_groups=[{"message_ids": [r.message_id for r in records], "turn_id": "t"}],
    )
    if expect_delivered:
        assert summary["delivered"] == len(captured), summary
    else:
        # The attachment is refused; only the plain failure note may go out.
        assert summary["undelivered"] == len(captured), summary
    return sent


def _send_document(path: str, *, file_name: str | None = None, reply_to: str = "MSG1") -> dict:
    kwargs = {"chat_id": MANAGEMENT_CHAT, "file_path": path, "reply_to": reply_to}
    if file_name is not None:
        kwargs["file_name"] = file_name
    return {"message_id": "replay-1", "kind": "send_document", "args": [],
            "kwargs": kwargs, "delivery_mode": "capture", "workspace_owner": LIVE_KEY}


def test_send_document_display_name_is_sent_exactly(agent_home, monkeypatch):
    _turn(_write("krisleen_update_v5.xlsx", "PK\x03\x04 edited list"))

    [payload] = _deliver([_send_document("work/krisleen_update_v5.xlsx", file_name=HER_NAME)], monkeypatch)

    assert payload["fileName"] == HER_NAME
    stored = Path(payload["filePath"])
    assert stored.name == "krisleen_update_v5.xlsx"
    [entry] = [e for e in _files_log(agent_home) if e["direction"] == "sent"]
    assert entry["display_name"] == HER_NAME


def test_media_directive_with_spaces_and_parentheses_is_sent_exactly(agent_home, monkeypatch):
    _turn(_write(HER_NAME, "PK\x03\x04 edited list"))
    streamed = {
        "message_id": "replay-1", "kind": "send",
        "args": [MANAGEMENT_CHAT, f'Updated.\nMEDIA:"work/{HER_NAME}"'],
        "kwargs": {"reply_to": "MSG1"}, "delivery_mode": "capture",
        "workspace_owner": LIVE_KEY,
    }
    # The ordinary path: the adapter already split the directive into a
    # send_document call with the path as written.
    [streamed_payload, document_payload] = _deliver(
        [streamed, _send_document(f"work/{HER_NAME}", reply_to="MSG2")], monkeypatch
    )

    for payload in (streamed_payload, document_payload):
        assert payload["fileName"] == HER_NAME
        stored = Path(payload["filePath"])
        # Stored under the safe name; shown under hers.
        assert stored.name == "15 Jul 2026 - List Of Outstanding WCs _Savills_.xlsx"
        assert stored.read_text() == "PK\x03\x04 edited list"


def test_scheduled_attachment_display_name_is_sent_exactly(agent_home, monkeypatch):
    _turn(_write(HER_NAME, "PK\x03\x04 scheduled list"))

    sent = _run_and_deliver(_job(), f'Morning list.\nMEDIA:"work/{HER_NAME}"', monkeypatch)

    [payload] = [item for item in sent if "filePath" in item]
    assert payload["fileName"] == HER_NAME
    assert Path(payload["filePath"]).name == "15 Jul 2026 - List Of Outstanding WCs _Savills_.xlsx"
    [entry] = [e for e in _files_log(agent_home) if e["direction"] == "scheduled_delivery"]
    assert entry["display_name"] == HER_NAME


def test_a_path_as_display_name_is_refused_not_sent(agent_home, monkeypatch):
    _turn(_write("x.xlsx", "PK\x03\x04 x"))

    sent = _deliver(
        [_send_document("work/x.xlsx", file_name="../escape.xlsx")],
        monkeypatch,
        expect_delivered=False,
    )

    assert not [payload for payload in sent if "filePath" in payload]


# 3. Two sends under one display name both go out ------------------------------


def test_two_sends_with_the_same_display_name_both_work(agent_home, monkeypatch):
    _turn(_write("v4.xlsx", "PK\x03\x04 version four"))
    _turn(_write("v5.xlsx", "PK\x03\x04 version five"))

    first, second = _deliver(
        [
            _send_document("work/v4.xlsx", file_name=HER_NAME, reply_to="MSG1"),
            _send_document("work/v5.xlsx", file_name=HER_NAME, reply_to="MSG2"),
        ],
        monkeypatch,
    )

    assert first["fileName"] == second["fileName"] == HER_NAME
    assert Path(first["filePath"]).parent != Path(second["filePath"]).parent
    assert Path(first["filePath"]).read_text() == "PK\x03\x04 version four"
    assert Path(second["filePath"]).read_text() == "PK\x03\x04 version five"
