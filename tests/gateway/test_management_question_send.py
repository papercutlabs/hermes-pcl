"""PA-396: one approved Management question, sent by Christopher himself.

Every outbound is Christopher acting on a trigger (Teren, Telegram 109).  The
contract under test:
- the operator command reads the record, runs every refusal, stages the
  photos and enqueues one typed mailbox row; it posts nothing and runs no turn;
- the live consumer claims the row in that chat's lane and runs one internal
  turn in the chat's persistent Management conversation, business writes off;
- his captured sends are checked (one body equal to the draft, photos equal
  to the staged set, nothing else) before the shared send-and-register code
  posts the first photo with the question as its caption and registers the
  notice and photo rows; a failed check posts nothing and leaves an internal
  not-delivered note in his conversation;
- a quoted reply to the notice correlates to the record; the outbox cursor
  never moves.

The model turn itself is stood in for by ``_Christopher``: it uses the real
SessionStore and the real replay session-key mapping, and returns scripted
captured sends instead of a model's.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote

import pytest
import yaml

from gateway import durable_jsonl_consumer as consumer
from gateway import management_question_trigger as trigger
from gateway.durable_jsonl_consumer import (
    ConsumerError,
    DurableInbox,
    InboxRecord,
    ManagementQuestionSource,
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
    config.write_text(yaml.safe_dump({
        # As on tgg-app-1: one shared conversation per Management group.
        "group_sessions_per_user": False,
        "session_reset": {"mode": "none"},
        "pa": {
            "enabled": True,
            "constitution_path": str(constitution),
            "media_retention": {
                "enabled": True,
                "media_root": str(media_root),
                "source_roots": [str(media_root)],
                "operation": "tgg_media_retention",
                "media_ref_prefix": "/media/tgg/hermes",
            },
        },
    }), encoding="utf-8")
    return config


@pytest.fixture()
def inbox(tmp_path: Path) -> DurableInbox:
    return DurableInbox(tmp_path / "inbox.db")


def _trigger_parts(event: dict) -> tuple[str, list[str]]:
    body = event["body"]
    question = body.split("Question:\n", 1)[1].split("\n\nPhotos:\n", 1)[0]
    media = [line for line in body.splitlines() if line.startswith("MEDIA:")]
    return question, media


def _verbatim_reply(event: dict) -> list[dict]:
    """Christopher does as asked: the draft, then one MEDIA line per photo."""
    question, media = _trigger_parts(event)
    content = question + ("\n" + "\n".join(media) if media else "")
    return [{
        "message_id": "replay-1", "kind": "send", "args": [event["chatId"], content],
        "kwargs": {"reply_to": event["messageId"]}, "delivery_mode": "capture",
    }]


class _Christopher:
    """Stands in for the live GatewayRunner (see the module docstring)."""

    def __init__(self, tmp_path: Path):
        from gateway.config import GatewayConfig, SessionResetPolicy
        from gateway.session import SessionStore

        self.config = GatewayConfig(
            group_sessions_per_user=False,
            default_reset_policy=SessionResetPolicy(mode="none"),
        )
        self.session_store = SessionStore(tmp_path / "sessions", self.config)
        self.plans: list = []
        self.histories: list[list[dict]] = []
        self.session_ids: list[str] = []
        self.respond = _verbatim_reply
        self.turn_rows: dict[str, list[dict]] = {}

    async def replay(self, plan):
        from gateway.config import Platform
        from gateway.replay import replay_context
        from gateway.session import SessionSource

        self.plans.append(plan)
        event = dict(plan.messages[-1])
        source = SessionSource(
            platform=Platform.WHATSAPP, chat_id=event["chatId"],
            chat_type="group" if event.get("isGroup", True) else "dm",
            user_id=event.get("senderId"),
        )
        with replay_context(plan):
            entry = self.session_store.get_or_create_session(source)
        self.session_ids.append(entry.session_id)
        self.histories.append(self.session_store.load_transcript(entry.session_id))
        self.session_store.append_to_transcript(
            entry.session_id, {"role": "user", "content": str(event.get("body") or "")},
        )
        outbound = self.respond(event)
        reply = "\n".join(
            str(item["args"][1]) for item in outbound if item.get("kind") == "send"
        )
        if reply:
            self.session_store.append_to_transcript(
                entry.session_id, {"role": "assistant", "content": reply},
            )
        self.turn_rows[plan.run_id] = [{
            "turn_id": f"turn-{len(self.plans)}", "turn_status": "completed",
            "provider": "fixture", "model": "fixture", "error_json": None,
            "message_refs_json": json.dumps([m["messageId"] for m in plan.messages]),
        }]
        return SimpleNamespace(processed=1, outbound=outbound, captured_business_mutations=[])

    def history(self, chat_id: str) -> list[dict]:
        from gateway.config import Platform
        from gateway.replay import ReplayPlan, replay_context
        from gateway.session import SessionSource

        with replay_context(ReplayPlan(replay_namespace=consumer.persistent_chat_namespace())):
            entry = self.session_store.get_or_create_session(SessionSource(
                platform=Platform.WHATSAPP, chat_id=chat_id, chat_type="group",
            ))
        return self.session_store.load_transcript(entry.session_id)


_CHRISTOPHER: list[_Christopher] = []
# Captured at import: a helper in test_attachment_from_capture.py swaps this
# module attribute and can leave its wrapper installed for later tests.
_REAL_PROCESS_LIVE_RECORDS = consumer.process_live_records


@pytest.fixture(autouse=True)
def _live_consumer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Christopher:
    monkeypatch.setenv("CHRISTOPHER_TGG_PS_SERVICE_TOKEN", "test-token")
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    monkeypatch.delenv("TGG_MANAGEMENT_DOCUMENT_API_URL", raising=False)
    monkeypatch.delenv("TGG_MANAGEMENT_DOCUMENT_CHAT_ID", raising=False)

    def no_cursor(*_args, **_kwargs):
        raise AssertionError("the outbox cursor must never move")

    async def no_document_turn(*_args, **_kwargs):
        raise AssertionError("the document-outbox turn must not run")

    monkeypatch.setattr(DurableInbox, "advance_management_document_cursor", no_cursor)
    monkeypatch.setattr(consumer, "_run_management_document_turn", no_document_turn)
    monkeypatch.setattr(consumer, "process_live_records", _REAL_PROCESS_LIVE_RECORDS)
    # The stand-in runner's session store gets its own database: the default
    # path is fixed at import and could be shared between test workers.
    import hermes_state

    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", tmp_path / "runner-state.db")
    christopher = _Christopher(tmp_path)
    monkeypatch.setattr(consumer, "configured_engine", lambda _path: ("fixture", "fixture"))
    monkeypatch.setattr(consumer, "configured_fallback_engines", lambda _path: frozenset())
    monkeypatch.setattr(
        consumer, "_turn_rows",
        lambda _state_db, *, replay_run_id: christopher.turn_rows.get(replay_run_id, []),
    )
    _CHRISTOPHER[:] = [christopher]
    return christopher


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


def _run(coro):
    """Run a coroutine whether or not the test already has a running loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    box: dict = {}

    def worker() -> None:
        try:
            box["value"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - re-raised below
            box["error"] = exc

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box["value"]


def _session_db(config_path: Path):
    from hermes_state import SessionDB

    return SessionDB(db_path=config_path.parent / "state.db")


def _enqueue(inbox: DurableInbox, config_path: Path, *, chat_id: str = MGMT_CHAT,
             check_only: bool = False, resolver_command=None, retry: bool = False) -> dict:
    return trigger.enqueue_management_question_trigger(
        inbox,
        _session_db(config_path),
        config_path=config_path,
        record_id=RECORD_ID,
        chat_id=chat_id,
        question_text=QUESTION + "\n",
        source=ManagementQuestionSource(api_url=SYSTEMS, token_env="CHRISTOPHER_TGG_PS_SERVICE_TOKEN", tenant="tgg"),
        resolver_command=resolver_command,
        retry_after_failed=retry,
        check_only=check_only,
    )


def _process_next(inbox: DurableInbox, config_path: Path) -> dict:
    """What the live loop's trigger task does for the oldest pending row."""
    db = _session_db(config_path)
    row = trigger.next_pending_trigger(db)
    assert row is not None
    assert db.claim_session_mailbox(row["id"], include_body_contract=trigger.CONTRACT)
    receipt = _run(trigger.process_claimed_question_trigger(
        row=row, inbox=inbox, config_path=config_path,
        state_db=config_path.parent / "state.db", runner=_CHRISTOPHER[0],
    ))
    trigger.finish_trigger(db, row, receipt)
    return receipt


def _send(inbox: DurableInbox, config_path: Path, *, chat_id: str = MGMT_CHAT,
          check_only: bool = False, resolver_command=None, retry: bool = False) -> dict:
    """Operator command, then the live consumer's trigger turn and delivery."""
    result = _enqueue(inbox, config_path, chat_id=chat_id, check_only=check_only,
                      resolver_command=resolver_command, retry=retry)
    if check_only:
        result["check_only"] = True
        return result
    return _process_next(inbox, config_path)


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


def test_cli_only_enqueues_and_the_live_consumer_sends(
    inbox: DurableInbox, config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls = _install(monkeypatch, _document())
    question = tmp_path / "question.txt"
    question.write_text(QUESTION + "\n", encoding="utf-8")
    argv = [
        "management-question-send", "--record-id", RECORD_ID, "--chat-id", MGMT_CHAT,
        "--question-file", str(question), "--inbox", str(inbox.db_path),
        "--config", str(config_path), "--systems-url", SYSTEMS, "--tenant", "tgg",
        "--media-resolver-command", "",
    ]
    assert consumer.main(argv + ["--check"]) == 0
    assert trigger.next_pending_trigger(_session_db(config_path)) is None
    assert consumer.main(argv) == 0
    printed = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert printed["trigger"]["status"] == "pending" and printed["check_only"] is False
    # The command posted nothing and ran no turn.
    assert _bridge(calls) == [] and _CHRISTOPHER[0].plans == []
    receipt = _process_next(inbox, config_path)
    assert receipt["outcome"] == "delivered"
    assert _bridge(calls) == [(f"{BRIDGE}/send", {"chatId": MGMT_CHAT, "message": QUESTION})]
    written = json.loads(Path(printed["trigger"]["receipt_path"]).read_text())
    assert written["outcome"] == "delivered" and written["notice"]["message_id"] == "WA-1"
    assert consumer.main(argv) == 2  # already delivered to this chat
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


def _batch_record(message_id: str, sender: str, *, quoted: str | None = None, seq: int = 1) -> InboxRecord:
    raw = {"messageId": message_id, "chatId": MGMT_CHAT, "timestamp": FRESH_TS,
           "senderId": sender, "body": "message", "fromMe": False}
    if quoted:
        raw["quotedMessageId"] = quoted
    return InboxRecord(seq=seq, message_id=message_id, chat_id=MGMT_CHAT,
                       start_offset=0, end_offset=1, raw=raw)


def _christopher_send(content: str, reply_to: str | None) -> dict:
    return {
        "message_id": "replay-1", "kind": "send", "args": [MGMT_CHAT, content],
        "kwargs": {"reply_to": reply_to} if reply_to else {}, "delivery_mode": "capture",
    }


def _deliver_turn(inbox: DurableInbox, config_path: Path, records: list[InboxRecord],
                  sends: list[dict], correlations: dict) -> dict:
    return consumer.deliver_management_replies(
        inbox,
        config_path=config_path,
        captured_outbound=sends,
        batch_records=records,
        gate_changed_at=GATE_CHANGED_AT,
        handled_groups=[{"message_ids": [record.message_id for record in records], "turn_id": "turn-1"}],
        document_correlations=correlations,
    )


def _follow_up_rows(inbox: DurableInbox) -> list[dict]:
    return [row for row in _rows(inbox) if ":follow-up:" in row["delivery_key"]]


def test_mixed_batch_registers_only_the_reply_anchored_to_the_correlated_message(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _document())
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    _send(inbox, config_path)  # question WA-1
    kris = _batch_record("KRIS-1", "krisleen@lid", quoted="WA-1", seq=1)
    ops = _batch_record("OPS-7", "ops@lid", seq=2)
    correlations = {"KRIS-1": inbox.management_document_correlation(kris)}
    summary = _deliver_turn(inbox, config_path, [kris, ops], [
        _christopher_send("Which job number is it, Krisleen?", "KRIS-1"),  # WA-2
        _christopher_send("Noted, the lift is fixed.", "OPS-7"),           # WA-3
    ], correlations)
    assert summary["delivered"] == 2
    [row] = _follow_up_rows(inbox)
    assert row["delivery_key"].endswith(":follow-up:WA-2") and row["reply_to_message_id"] == "KRIS-1"
    assert inbox.management_document_correlation(_quoting("kris-2", "WA-2"))["document_id"] == RECORD_ID
    assert inbox.management_document_correlation(_quoting("ops-2", "WA-3")) is None


def test_unanchored_reply_in_a_correlated_turn_is_not_registered(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _document())
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    _send(inbox, config_path)
    kris = _batch_record("KRIS-1", "krisleen@lid", quoted="WA-1")
    correlations = {"KRIS-1": inbox.management_document_correlation(kris)}
    # Delivery still anchors the bubble to the newest message (KRIS-1), but
    # the reply itself named no anchor, so it is not a follow-up.
    summary = _deliver_turn(inbox, config_path, [kris], [
        _christopher_send("Thanks, filing it now.", None),  # WA-2
    ], correlations)
    assert summary["delivered"] == 1
    assert _follow_up_rows(inbox) == []
    assert inbox.management_document_correlation(_quoting("kris-2", "WA-2")) is None


def test_bundle_composite_anchor_resolved_to_the_correlated_message_is_not_registered(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _document())
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    _send(inbox, config_path)
    ops = _batch_record("OPS-7", "ops@lid", seq=1)
    kris = _batch_record("KRIS-1", "krisleen@lid", quoted="WA-1", seq=2)
    correlations = {"KRIS-1": inbox.management_document_correlation(kris)}
    # A bundled turn replies to "OPS-7+KRIS-1"; delivery resolves it to the
    # newest handled component, KRIS-1, but Christopher did not choose it.
    summary = _deliver_turn(inbox, config_path, [ops, kris], [
        _christopher_send("Ok, I'll check the Sengkang job.", "OPS-7+KRIS-1"),
    ], correlations)
    assert summary["delivered"] == 1
    assert _follow_up_rows(inbox) == []


def test_reviewer_probe_p4_other_persons_swipe_reply_no_longer_correlates(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _document())
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", BRIDGE)
    _send(inbox, config_path)
    kris = _batch_record("KRIS-1", "krisleen@lid", quoted="WA-1", seq=1)
    ops = _batch_record("OPS-7", "ops@lid", seq=2)
    correlations = {"KRIS-1": inbox.management_document_correlation(kris)}
    # Christopher answers only the Ops message in that turn.
    _deliver_turn(inbox, config_path, [kris, ops], [
        _christopher_send("Ok, I'll check the Sengkang job.", "OPS-7"),  # WA-2
    ], correlations)
    assert _follow_up_rows(inbox) == []
    # Days later Ops swipe-replies to Christopher's answer.
    later = _batch_record("OPS-8", "ops@lid", quoted="WA-2", seq=3)
    later.raw["body"] = "AM/JOB/2611/0005 done"
    assert inbox.management_document_correlation(later) is None


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


APPROVED_QUESTION = (
    "Hi Krisleen, one quick check so I can file this correctly. On 4 Oct at 7:45pm in MM1A Maintenance "
    "(AMK, SRG), Muthu reported a burst pipe at Blk 641 Ang Mo Kio Ave 4 #10-822 and said the toilet "
    "water-supply pipe was replaced (his photos attached). The master sheet shows it as completed on 4 Oct, "
    "but with no job number. Which job number should it go under?"
)
F1_FOLLOW_UP = (
    "For the pipe repair at Blk 641 Ang Mo Kio Ave 4 #10-822, I couldn't match \"the plumbing one\" to a "
    "job number. Could you send me the exact job number from the master sheet, in full, so I can file "
    "Muthu's 4 Oct completion report and photos under it? If it doesn't have a job yet, tell me and I'll check."
)


@pytest.mark.asyncio
async def test_f1_follow_up_quote_renders_record_question_and_follow_up_in_full() -> None:
    assert len(APPROVED_QUESTION) == 357 and 280 <= len(F1_FOLLOW_UP) <= 320
    correlation = dict(_correlation("reply-f1", "WA-FU"), question_text=APPROVED_QUESTION)
    text = await _rendered_model_input(
        _inbound("reply-f1", "WA-FU", quoted_text=F1_FOLLOW_UP, correlation=correlation)
    )
    assert text == (
        f'[Replying to: "{RECORD_LINE}\nYour question was: {APPROVED_QUESTION}\n{F1_FOLLOW_UP}"]'
        "\n\nAM/JOB/2610/0418"
    )


@pytest.mark.asyncio
async def test_over_cap_shortens_question_first_and_quoted_text_last_keeping_the_id_line() -> None:
    adapter = _whatsapp_adapter()
    long_question = "Which job is this? " + "detail " * 300
    quoted = "Follow-up " + "x" * 990
    correlation = dict(_correlation("reply-9", "WA-9"), question_text=long_question)
    data = _inbound("reply-9", "WA-9", quoted_text=quoted, correlation=correlation)
    context = adapter._reply_context_text(data)
    assert len(context) <= 2000
    record_line, question_line, quoted_line = context.split("\n")
    assert record_line == RECORD_LINE
    assert question_line.startswith("Your question was: Which job is this?") and question_line.endswith("…")
    assert quoted_line == quoted  # quoted text kept whole while the question absorbs the cut
    text = await _rendered_model_input(data)
    assert text.startswith(f'[Replying to: "{context}"]')

    huge_quote = "y" * 2500
    data = _inbound("reply-9", "WA-9", quoted_text=huge_quote, correlation=correlation)
    context = adapter._reply_context_text(data)
    assert len(context) <= 2000
    record_line, quoted_line = context.split("\n")
    assert record_line == RECORD_LINE and quoted_line.endswith("…")
    text = await _rendered_model_input(data)
    assert text.startswith(f'[Replying to: "{RECORD_LINE}\n')


@pytest.mark.asyncio
async def test_uncorrelated_long_quote_keeps_the_500_cap() -> None:
    quoted = "z" * 800
    text = await _rendered_model_input(_inbound("reply-u", "WA-u", quoted_text=quoted, correlation=None))
    assert text == f'[Replying to: "{"z" * 500}"]\n\nAM/JOB/2610/0418'


def test_turn_bundle_keeps_the_larger_context_only_when_correlated() -> None:
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    adapter = _whatsapp_adapter()
    source = SessionSource(
        platform=Platform.WHATSAPP, chat_id=MGMT_CHAT, chat_name="TGG Management",
        chat_type="group", user_id="6591234567@s.whatsapp.net", user_name="Krisleen",
    )
    correlation = dict(_correlation("reply-f1", "WA-FU"), question_text=APPROVED_QUESTION)
    correlated = _inbound("reply-f1", "WA-FU", quoted_text=F1_FOLLOW_UP, correlation=correlation)
    plain = _inbound("m2", "WA-x", quoted_text="q" * 800, correlation=None)
    events = [
        MessageEvent(text=item["body"], message_type=MessageType.TEXT, source=source,
                     raw_message=item, message_id=item["messageId"])
        for item in (correlated, plain)
    ]
    bundled = adapter._build_turn_event(events)
    assert f"{RECORD_LINE}\nYour question was: {APPROVED_QUESTION}\n{F1_FOLLOW_UP}" in bundled.text
    assert f'[Replying to: "{"q" * 500}"]' in bundled.text and "q" * 501 not in bundled.text


# ---------------------------------------------------------------------------
# r8: the question is Christopher's own message, sent on a trigger.
# ---------------------------------------------------------------------------


def _two_photos(media_root: Path) -> tuple[list[dict], list[str]]:
    digest_a = _jpeg(media_root / "a.jpg", 1)
    digest_b = _jpeg(media_root / "b.jpg", 2)
    return [
        _media(digest_a, ref="/media/tgg/hermes/a.jpg"),
        _media(digest_b, ref="/media/tgg/hermes/b.jpg"),
    ], [digest_a, digest_b]


def test_mailbox_id_matches_the_session_store(config_path: Path) -> None:
    db = _session_db(config_path)
    row = db.create_session_mailbox_message(
        agent_id=trigger.AGENT_ID, from_session_name=trigger.FROM_SESSION,
        to_session_name=trigger.TO_SESSION, body="{}",
        idempotency_key=trigger.idempotency_key(RECORD_ID, MGMT_CHAT, 1),
    )
    assert row["id"] == trigger.mailbox_id(RECORD_ID, MGMT_CHAT, 1)


def test_enqueue_posts_nothing_and_runs_no_turn(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    media, digests = _two_photos(media_root)
    calls = _install(monkeypatch, _document(media))
    result = _enqueue(inbox, config_path)
    assert result["trigger"]["status"] == "pending"
    assert result["trigger"]["photo_refs"] == ["/media/tgg/hermes/a.jpg", "/media/tgg/hermes/b.jpg"]
    assert _bridge(calls) == [] and _live_consumer.plans == [] and _rows(inbox) == []
    row = trigger.next_pending_trigger(_session_db(config_path))
    body = trigger.parse_trigger_body(row)
    assert body["question_text"] == QUESTION and body["chat_id"] == MGMT_CHAT
    assert [photo["digest"] for photo in body["photos"]] == digests


def test_duplicate_trigger_is_refused_while_queued(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, _document())
    _enqueue(inbox, config_path)
    with pytest.raises(ConsumerError, match="QUESTION_TRIGGER_ALREADY_QUEUED"):
        _enqueue(inbox, config_path)
    assert len(_session_db(config_path).list_pending_session_mailbox(agent_id=trigger.AGENT_ID)) == 1


def test_trigger_turn_is_in_the_chats_persistent_conversation_with_writes_off(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    media, _ = _two_photos(media_root)
    _install(monkeypatch, _document(media))
    receipt = _send(inbox, config_path)
    assert receipt["outcome"] == "delivered"
    [plan] = _live_consumer.plans
    assert plan.live_business_writes is False
    assert plan.replay_namespace == consumer.persistent_chat_namespace()
    assert plan.session_key_override == f"agent:live-drain:conversation:whatsapp:group:{MGMT_CHAT}"
    assert receipt["session_key"] == plan.session_key_override
    assert plan.internal_message_ids == (plan.messages[0]["messageId"],)
    assert plan.messages[0]["senderId"] == "system@internal"
    history = _live_consumer.history(MGMT_CHAT)
    assert RECORD_ID in history[0]["content"]  # the trigger, as an internal event
    assert history[-1]["role"] == "assistant" and history[-1]["content"].startswith(QUESTION)


def test_later_unquoted_message_is_delivered_with_the_question_in_history(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    _install(monkeypatch, _document())
    _send(inbox, config_path)
    typed = InboxRecord(
        seq=1, message_id="kris-typed", chat_id=MGMT_CHAT, start_offset=0, end_offset=1,
        raw={"messageId": "kris-typed", "chatId": MGMT_CHAT, "senderId": "krisleen@lid",
             "isGroup": True, "body": "0418", "timestamp": 1791200000, "fromMe": False},
    )
    _live_consumer.respond = lambda event: []
    _run(consumer.process_live_records(
        [typed], config_path=config_path, state_db=config_path.parent / "state.db",
        persistent_session=True, runner=_live_consumer,
    ))
    later_plan = _live_consumer.plans[-1]
    assert later_plan.live_business_writes is True  # ordinary turns unchanged
    assert _live_consumer.session_ids[-1] == _live_consumer.session_ids[0]
    seen = _live_consumer.histories[-1]
    assert any(RECORD_ID in str(message["content"]) for message in seen)
    assert any(message["role"] == "assistant" and QUESTION in message["content"] for message in seen)


def test_native_image_sends_become_caption_and_photo_rows(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    media, digests = _two_photos(media_root)
    calls = _install(monkeypatch, _document(media))

    def native(event: dict) -> list[dict]:
        # The adapter's non-streamed path: text, then one image batch with
        # empty captions, both anchored to the internal event.
        question, refs = _trigger_parts(event)
        paths = [str(media_root / ref.rsplit("/", 1)[1]) for ref in refs]
        return [
            {"kind": "send", "args": [event["chatId"], question],
             "kwargs": {"reply_to": event["messageId"]}},
            {"kind": "send_multiple_images", "args": [],
             "kwargs": {"chat_id": event["chatId"], "reply_to": event["messageId"],
                        "images": [(f"file://{quote(path)}", "") for path in reversed(paths)]}},
        ]

    _live_consumer.respond = native
    receipt = _send(inbox, config_path)
    assert receipt["outcome"] == "delivered"
    # Posted in the staged (record) order whatever order he attached them in.
    assert _bridge(calls) == [
        (f"{BRIDGE}/send-media", {"chatId": MGMT_CHAT, "filePath": str((media_root / "a.jpg").resolve()),
                                  "mediaType": "image", "caption": QUESTION}),
        (f"{BRIDGE}/send-media", {"chatId": MGMT_CHAT, "filePath": str((media_root / "b.jpg").resolve()),
                                  "mediaType": "image"}),
    ]
    assert [row["delivery_key"] for row in _rows(inbox)] == [
        f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}",
        f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}:photo:1",
    ]


def test_media_anchor_is_stripped_and_another_anchor_is_refused(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    media, _ = _two_photos(media_root)
    calls = _install(monkeypatch, _document(media))

    def positional(anchor_of):
        def respond(event: dict) -> list[dict]:
            question, refs = _trigger_parts(event)
            entries = [{"kind": "send", "args": [event["chatId"], question, event["messageId"]], "kwargs": {}}]
            for ref in refs:
                path = str(media_root / ref.rsplit("/", 1)[1])
                # send_image_file(chat_id, image_path, caption, reply_to)
                entries.append({"kind": "send_image_file",
                                "args": [event["chatId"], path, None, anchor_of(event)], "kwargs": {}})
            return entries
        return respond

    _live_consumer.respond = positional(lambda event: "kris-old-message")
    refused = _send(inbox, config_path)
    assert refused["outcome"] == "check_failed" and "anchored to another message" in refused["reason"]
    assert _bridge(calls) == [] and _rows(inbox) == []
    _live_consumer.respond = positional(lambda event: event["messageId"])
    accepted = _send(inbox, config_path, retry=True)
    assert accepted["outcome"] == "delivered"
    assert [url for url, _ in _bridge(calls)] == [f"{BRIDGE}/send-media", f"{BRIDGE}/send-media"]


@pytest.mark.parametrize("case", [
    "missing_photo", "extra_photo", "two_bodies", "wording", "other_kind", "other_chat",
])
def test_each_check_failure_posts_nothing_and_notes_it_in_his_conversation(
    case: str, inbox: DurableInbox, config_path: Path, media_root: Path,
    monkeypatch: pytest.MonkeyPatch, _live_consumer: _Christopher,
) -> None:
    media, _ = _two_photos(media_root)
    _jpeg(media_root / "stranger.jpg", 9)
    calls = _install(monkeypatch, _document(media))

    def respond(event: dict) -> list[dict]:
        question, refs = _trigger_parts(event)
        if case == "missing_photo":
            refs = refs[:1]
        if case == "extra_photo":
            refs = refs + ["MEDIA:/media/tgg/hermes/stranger.jpg"]
        if case == "wording":
            question = "Krisleen, which job is this one on? Also, is it the same fault?"
        content = question + "\n" + "\n".join(refs)
        entries = [{"kind": "send", "args": [event["chatId"], content],
                    "kwargs": {"reply_to": event["messageId"]}}]
        if case == "two_bodies":
            entries.append({"kind": "send", "args": [event["chatId"], "And one more thing."],
                            "kwargs": {"reply_to": event["messageId"]}})
        if case == "other_kind":
            entries.append({"kind": "send_document", "args": [event["chatId"], "/tmp/x.xlsx"], "kwargs": {}})
        if case == "other_chat":
            entries.append({"kind": "send", "args": [SITE_CHAT, "hello site"], "kwargs": {}})
        return entries

    _live_consumer.respond = respond
    receipt = _send(inbox, config_path)
    assert receipt["outcome"] == "check_failed"
    expected = {
        "missing_photo": "staged photo(s) missing",
        "extra_photo": "not the record's",
        "two_bodies": "2 question bodies",
        "wording": "not the approved draft",
        "other_kind": "unexpected send_document",
        "other_chat": "another chat",
    }[case]
    assert expected in receipt["reason"]
    assert _bridge(calls) == [] and _rows(inbox) == []
    assert receipt["not_delivered_note"]["status"] == "appended"
    note = _live_consumer.history(MGMT_CHAT)[-1]
    assert note["role"] == "user" and "was NOT delivered" in note["content"] and RECORD_ID in note["content"]
    row = _session_db(config_path).get_session_mailbox_message(trigger.mailbox_id(RECORD_ID, MGMT_CHAT, 1))
    assert row["status"] == "failed" and row["last_error"].startswith("check_failed")
    if case == "wording":
        assert receipt["drift"]["equal_normalized"] is False
        assert any(line.startswith("+Krisleen, which job is this one on?") for line in receipt["drift"]["diff"])


def test_failed_trigger_needs_the_explicit_flag_to_retry(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    calls = _install(monkeypatch, _document())
    _live_consumer.respond = lambda event: []  # he sends nothing
    first = _send(inbox, config_path)
    assert first["outcome"] == "check_failed", first
    with pytest.raises(ConsumerError, match="QUESTION_TRIGGER_EARLIER_ATTEMPT_FAILED"):
        _enqueue(inbox, config_path)
    _live_consumer.respond = _verbatim_reply
    retried = _send(inbox, config_path, retry=True)
    assert retried["outcome"] == "delivered" and retried["attempt"] == 2
    assert len(_bridge(calls)) == 1


def test_whitespace_drift_is_recorded_and_still_posts(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    calls = _install(monkeypatch, _document())

    def reflowed(event: dict) -> list[dict]:
        question, _ = _trigger_parts(event)
        content = question.replace(" which job", "\nwhich  job")
        return [{"kind": "send", "args": [event["chatId"], content],
                 "kwargs": {"reply_to": event["messageId"]}}]

    _live_consumer.respond = reflowed
    receipt = _send(inbox, config_path)
    assert receipt["outcome"] == "delivered"
    assert receipt["drift"]["equal_normalized"] is True and receipt["drift"]["diff"]
    assert _bridge(calls)[0][1]["message"] == QUESTION.replace(" which job", "\nwhich  job")


def test_turn_that_does_not_complete_posts_nothing(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    calls = _install(monkeypatch, _document())
    monkeypatch.setattr(consumer, "_turn_rows", lambda _db, *, replay_run_id: [])
    receipt = _send(inbox, config_path)
    assert receipt["outcome"] == "turn_failed"
    assert _bridge(calls) == [] and _rows(inbox) == []
    assert receipt["not_delivered_note"]["status"] == "appended"


def test_destination_and_photos_are_rechecked_when_the_trigger_runs(
    inbox: DurableInbox, config_path: Path, media_root: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    media, _ = _two_photos(media_root)
    calls = _install(monkeypatch, _document(media))
    _enqueue(inbox, config_path)
    (media_root / "b.jpg").write_bytes(b"\xff\xd8\xff\xe0changed")
    receipt = _process_next(inbox, config_path)
    assert receipt["outcome"] == "refused" and "missing or changed" in receipt["reason"]
    assert _live_consumer.plans == [] and _bridge(calls) == []


def test_interrupted_trigger_is_failed_and_never_replayed(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, _document())
    _enqueue(inbox, config_path)
    db = _session_db(config_path)
    row = trigger.next_pending_trigger(db)
    assert db.claim_session_mailbox(row["id"], include_body_contract=trigger.CONTRACT)
    # The process stops mid-turn; the next one starts.
    assert trigger.terminalize_prior_process_triggers(_session_db(config_path)) == 1
    after = _session_db(config_path).get_session_mailbox_message(row["id"])
    assert after["status"] == "failed" and after["last_error"] == trigger.PRIOR_PROCESS_OUTCOME_UNKNOWN
    assert trigger.next_pending_trigger(_session_db(config_path)) is None


def _consumer_args(tmp_path: Path, messages: list[dict]):
    from tests.gateway.test_durable_jsonl_consumer import _enabled_consumer_args

    return _enabled_consumer_args(tmp_path, messages)


def _queue_trigger_row(state_db: Path, chat_id: str) -> str:
    from hermes_state import SessionDB

    db = SessionDB(db_path=state_db)
    row = db.create_session_mailbox_message(
        agent_id=trigger.AGENT_ID, from_session_name=trigger.FROM_SESSION,
        to_session_name=trigger.TO_SESSION,
        body=json.dumps({"contract": trigger.CONTRACT, "chat_id": chat_id}),
        idempotency_key=trigger.idempotency_key(RECORD_ID, chat_id, 1),
    )
    return row["id"]


@pytest.mark.asyncio
async def test_live_consumer_runs_the_trigger_in_the_chats_lane_in_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _consumer_args(tmp_path, [])
    mailbox = _queue_trigger_row(Path(args.state_db), "management@g.us")
    runner = object()
    seen: list[dict] = []

    async def fake_trigger(**kwargs):
        seen.append(kwargs)

    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: runner)
    monkeypatch.setattr(consumer, "_process_claimed_management_question_trigger", fake_trigger)
    assert await consumer.run_consumer(args) == 0
    [call] = seen
    assert call["row"]["id"] == mailbox and call["runner"] is runner
    assert call["state_db"] == Path(args.state_db)


@pytest.mark.asyncio
async def test_trigger_waits_while_the_chat_lane_is_busy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.gateway.test_durable_jsonl_consumer import _message

    args = _consumer_args(tmp_path, [_message("ORDINARY-1", "management@g.us")])
    _queue_trigger_row(Path(args.state_db), "management@g.us")
    seen: list[dict] = []

    async def fake_trigger(**kwargs):
        seen.append(kwargs)

    async def fake_process(records, **kwargs):
        return {
            "submitted_message_ids": [record.message_id for record in records],
            "handled": [{"message_ids": [r.message_id for r in records], "turn_id": "turn-1"}],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_process_claimed_management_question_trigger", fake_trigger)
    assert await consumer.run_consumer(args) == 0
    # The ordinary message took the lane this cycle; the trigger stays pending.
    assert DurableInbox(Path(args.inbox)).counts().get("pending", 0) == 0
    assert sum(DurableInbox(Path(args.inbox)).counts().values()) == 1
    assert seen == []
    assert trigger.next_pending_trigger(_session_db(Path(args.config))) is not None


# ---------------------------------------------------------------------------
# r8: open-question candidates, so a typed answer that quotes nothing can be
# bound to Christopher's question by his capability tool.
# ---------------------------------------------------------------------------

OTHER_MGMT_CHAT = "120363407903158826@g.us"


def _typed(message_id: str, *, chat_id: str = MGMT_CHAT, at: float, **extra) -> InboxRecord:
    raw = {"messageId": message_id, "chatId": chat_id, "senderId": "krisleen@lid",
           "isGroup": True, "body": "0418", "timestamp": at, "fromMe": False, **extra}
    return InboxRecord(seq=1, message_id=message_id, chat_id=chat_id,
                       start_offset=0, end_offset=1, raw=raw)


def _seed_notice(inbox: DurableInbox, monkeypatch: pytest.MonkeyPatch, media_root: Path, *,
                 chat_id: str = MGMT_CHAT, record_id: str = RECORD_ID, entry_id: str = ENTRY_ID,
                 photos: int = 0) -> None:
    """The shared send-and-register code, as the trigger path calls it."""
    _install(monkeypatch, _document())
    staged = []
    for index in range(photos):
        path = media_root / f"{record_id[:4]}-{index}.jpg"
        digest = _jpeg(path, 40 + index)
        staged.append(consumer.ManagementQuestionPhoto(path=path, digest=digest, mime="image/jpeg"))
    consumer.deliver_management_question(
        inbox, chat_id=chat_id, record_id=record_id, entry_id=entry_id,
        text=QUESTION, photos=staged,
    )


def test_typed_message_after_the_notice_gets_one_candidate(
    inbox: DurableInbox, monkeypatch: pytest.MonkeyPatch, media_root: Path,
) -> None:
    import time as _time

    _seed_notice(inbox, monkeypatch, media_root, photos=2)
    # Christopher's follow-up in a correlated turn is registered too.
    first = inbox.management_document_correlation(_quoting("kris-1", "WA-1"))
    consumer._register_management_document_follow_up(
        inbox, {"kris-1": first}, chat_id=MGMT_CHAT, anchor="kris-1",
        explicitly_anchored=True, delivery_key=f"{MGMT_CHAT}::kris-1", bridge_message_id="WA-FU",
    )
    candidates = inbox.management_open_question_candidates(_typed("typed-1", at=_time.time() + 60))
    assert candidates == [{
        "document_id": RECORD_ID,
        "entry_id": ENTRY_ID,
        "notice_delivery_key": f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}",
        "notice_bridge_message_id": "WA-1",
        "notice_delivered_at": candidates[0]["notice_delivered_at"],
    }]


def test_message_before_the_notice_gets_none(
    inbox: DurableInbox, monkeypatch: pytest.MonkeyPatch, media_root: Path,
) -> None:
    import time as _time

    _seed_notice(inbox, monkeypatch, media_root)
    assert inbox.management_open_question_candidates(_typed("early", at=_time.time() - 3600)) == []
    # Milliseconds, as WhatsApp exports send them, compare the same way.
    assert inbox.management_open_question_candidates(_typed("early-ms", at=(_time.time() - 3600) * 1000)) == []
    assert len(inbox.management_open_question_candidates(_typed("late-ms", at=(_time.time() + 60) * 1000))) == 1


def test_another_chats_notice_is_not_a_candidate(
    inbox: DurableInbox, monkeypatch: pytest.MonkeyPatch, media_root: Path,
) -> None:
    import time as _time

    _seed_notice(inbox, monkeypatch, media_root, chat_id=OTHER_MGMT_CHAT)
    assert inbox.management_open_question_candidates(_typed("typed", at=_time.time() + 60)) == []
    assert len(inbox.management_open_question_candidates(
        _typed("typed-other", chat_id=OTHER_MGMT_CHAT, at=_time.time() + 60))) == 1


def test_photo_and_follow_up_rows_are_not_candidates(
    inbox: DurableInbox, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import time as _time

    for key, correlation in (
        (f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}:photo:1",
         {"document_id": RECORD_ID, "entry_id": ENTRY_ID, "entry_kind": "initial_default",
          "notice_role": "initial_notice", "notice_part": "photo"}),
        (f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}:follow-up:WA-FU",
         {"document_id": RECORD_ID, "entry_id": ENTRY_ID, "notice_part": "follow_up"}),
        # A later lifecycle notice is not the question either.
        (f"human-resolution:entry-2:destination:{MGMT_CHAT}",
         {"document_id": "record-2", "entry_id": "entry-2", "entry_kind": "amendment",
          "notice_role": "lifecycle_notice", "notice_body": "update"}),
    ):
        inbox.claim_reply_delivery(key, chat_id=MGMT_CHAT, reply_to_message_id=None, correlation=correlation)
        inbox.record_reply_delivery(key, status="delivered", bridge_message_id=key[-6:])
    assert inbox.management_open_question_candidates(_typed("typed", at=_time.time() + 60)) == []


def test_undelivered_notice_is_not_a_candidate(
    inbox: DurableInbox, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import time as _time

    key = f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}"
    inbox.claim_reply_delivery(key, chat_id=MGMT_CHAT, reply_to_message_id=None, correlation={
        "document_id": RECORD_ID, "entry_id": ENTRY_ID, "entry_kind": "initial_default",
        "notice_role": "initial_notice", "notice_body": QUESTION,
    })
    inbox.record_reply_delivery(key, status="undelivered", error="http-202-unconfirmed")
    assert inbox.management_open_question_candidates(_typed("typed", at=_time.time() + 60)) == []


def test_christophers_own_and_internal_messages_get_none(
    inbox: DurableInbox, monkeypatch: pytest.MonkeyPatch, media_root: Path,
) -> None:
    import time as _time

    _seed_notice(inbox, monkeypatch, media_root)
    later = _time.time() + 60
    assert inbox.management_open_question_candidates(_typed("mine", at=later, fromMe=True)) is None
    assert inbox.management_open_question_candidates(
        _typed("internal", at=later, senderId="system@internal")) is None


def test_candidates_reach_the_turn_beside_an_explicit_correlation(
    config_path: Path,
) -> None:
    candidate = {
        "document_id": RECORD_ID, "entry_id": ENTRY_ID,
        "notice_delivery_key": f"human-resolution:{ENTRY_ID}:destination:{MGMT_CHAT}",
        "notice_bridge_message_id": "WA-1", "notice_delivered_at": "2026-10-21T01:00:00+00:00",
    }
    correlation = _correlation("quoted", "WA-1")
    typed = _typed("typed", at=1791200000.0)
    quoted = _typed("quoted", at=1791200001.0, quotedMessageId="WA-1")
    mine = _typed("mine", at=1791200002.0, fromMe=True)
    rendered = consumer._replay_messages_with_retained_documents(
        [typed, quoted, mine], config_path=config_path,
        management_document_correlations={"quoted": correlation},
        management_open_question_candidates={"typed": [candidate], "quoted": [candidate]},
    )
    by_id = {message["messageId"]: message for message in rendered}
    assert by_id["typed"]["_hermes_pa_context"] == {"management_open_question_candidates": [candidate]}
    assert by_id["quoted"]["_hermes_pa_context"] == {
        "management_document_correlation": correlation,
        "management_open_question_candidates": [candidate],
    }
    assert "_hermes_pa_context" not in by_id["mine"]


@pytest.mark.asyncio
async def test_live_batch_passes_candidates_into_the_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import time as _time

    from tests.gateway.test_durable_jsonl_consumer import _message

    typed = dict(_message("TYPED-1", "management@g.us"), body="0418", timestamp=int(_time.time()) + 60)
    args = _consumer_args(tmp_path, [typed])
    # As on tgg-app-1: every Management message is a turn, mention or not.
    constitution = Path(yaml.safe_load(Path(args.config).read_text())["pa"]["constitution_path"])
    data = yaml.safe_load(constitution.read_text())
    data["job_briefs"] = {"tgg_management": {"require_mention": False}}
    constitution.write_text(yaml.safe_dump(data), encoding="utf-8")
    seeded = DurableInbox(Path(args.inbox))
    key = f"human-resolution:{ENTRY_ID}:destination:management@g.us"
    seeded.claim_reply_delivery(key, chat_id="management@g.us", reply_to_message_id=None, correlation={
        "document_id": RECORD_ID, "entry_id": ENTRY_ID, "entry_kind": "initial_default",
        "notice_role": "initial_notice", "notice_body": QUESTION,
    })
    seeded.record_reply_delivery(key, status="delivered", bridge_message_id="WA-1")
    seen: list[dict] = []

    async def fake_process(records, **kwargs):
        seen.append(kwargs)
        return {
            "submitted_message_ids": [record.message_id for record in records],
            "handled": [{"message_ids": [r.message_id for r in records], "turn_id": "turn-1"}],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    assert await consumer.run_consumer(args) == 0
    [call] = seen
    [candidate] = call["management_open_question_candidates"]["TYPED-1"]
    assert candidate["document_id"] == RECORD_ID and candidate["notice_bridge_message_id"] == "WA-1"
    assert call["management_document_correlations"] == {}


def test_old_key_without_destination_is_not_a_candidate(inbox: DurableInbox) -> None:
    import time as _time

    key = f"human-resolution:{ENTRY_ID}"  # pre-PA-83 shape, no destination
    inbox.claim_reply_delivery(key, chat_id=MGMT_CHAT, reply_to_message_id=None, correlation={
        "document_id": RECORD_ID, "entry_id": ENTRY_ID, "entry_kind": "initial_default",
        "notice_body": QUESTION,
    })
    inbox.record_reply_delivery(key, status="delivered", bridge_message_id="WA-old")
    assert inbox.management_open_question_candidates(_typed("typed", at=_time.time() + 60)) == []


def test_trigger_event_names_the_record_in_plain_text(
    inbox: DurableInbox, config_path: Path, monkeypatch: pytest.MonkeyPatch,
    _live_consumer: _Christopher,
) -> None:
    _install(monkeypatch, _document())
    _send(inbox, config_path)
    text = _live_consumer.plans[0].messages[0]["body"]
    assert f"This is your question on human-resolution record {RECORD_ID}." in text
