"""PA-396: one approved Management question, sent by Christopher in his own turn.

Every outbound is Christopher acting on a trigger (Teren, Telegram 109).  The
operator command only stages the record's photos and enqueues one typed row in
the existing session mailbox (the same carrier as the management-list
continuation).  The live consumer claims the row in that chat's lane and runs
one internal turn in the chat's persistent Management conversation, with
business writes off.  His captured sends are checked (one body equal to the
approved draft, photos equal to the staged set, no other send) and only then
delivered and registered as the record's notice and photo rows.  A failed
check posts nothing and leaves an internal not-delivered note in his
conversation.

The turn has no original inbound message: it is driven by the internal event
alone (``replay_messages``) and bound to the chat's persistent session key.
"""
from __future__ import annotations

import difflib
import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from gateway import durable_jsonl_consumer as durable

CONTRACT = "tgg-management-question-trigger/v1"
AGENT_ID = "tgg-management-question"
FROM_SESSION = "operator"
TO_SESSION = "christopher-management"
INTERNAL_MESSAGE_PREFIX = "management-question-trigger:"
PRIOR_PROCESS_OUTCOME_UNKNOWN = "QUESTION_TRIGGER_PRIOR_PROCESS_OUTCOME_UNKNOWN"
_BODY_KEYS = frozenset({
    "contract", "record_id", "entry_id", "chat_id", "question_text",
    "question_sha256", "photos", "receipt_path", "attempt",
})
_PHOTO_KEYS = frozenset({
    "ref", "digest", "mime", "message_id", "media_ordinal", "sender_id", "sent_at",
})


class QuestionTriggerError(durable.ConsumerError):
    """A known trigger refusal; never replayed automatically."""


# --------------------------------------------------------------------------
# Enqueue side (operator command)
# --------------------------------------------------------------------------


def idempotency_key(record_id: str, chat_id: str, attempt: int) -> str:
    """One mailbox row per record, destination and explicit attempt."""
    return f"{record_id}|{chat_id}|attempt-{int(attempt)}"


def mailbox_id(record_id: str, chat_id: str, attempt: int) -> str:
    """The row id ``create_session_mailbox_message`` gives this key.

    Mirrors hermes_state's idempotent id so prior attempts can be read
    without creating a row; a test pins the two together.
    """
    key = idempotency_key(record_id, chat_id, attempt)
    return "idem-" + hashlib.sha256(json.dumps(
        [AGENT_ID, FROM_SESSION, key], separators=(",", ":"), ensure_ascii=False,
    ).encode()).hexdigest()


def attempt_history(session_db: Any, *, record_id: str, chat_id: str) -> list[dict[str, Any]]:
    """Every earlier trigger row for this record and destination, in order."""
    rows: list[dict[str, Any]] = []
    attempt = 1
    while True:
        row = session_db.get_session_mailbox_message(mailbox_id(record_id, chat_id, attempt))
        if row is None:
            return rows
        rows.append(dict(row))
        attempt += 1


def _require_shared_conversation(config_path: Path, chat_id: str) -> str:
    with durable._runtime_config_context(config_path):
        key = durable.management_conversation_session_key(
            config_path, platform="whatsapp", chat_id=chat_id,
        )
    if key is None:
        raise QuestionTriggerError(
            "QUESTION_TRIGGER_NO_SHARED_CONVERSATION: this chat has no single persistent "
            "Management conversation for Christopher to send from"
        )
    return key


def _photo_refs(photos: Sequence[Any], config_path: Path) -> list[dict[str, Any]]:
    retention = durable._retention_config(config_path)
    if photos and retention is None:
        raise QuestionTriggerError("retained media is not configured; photos cannot be referenced")
    refs: list[dict[str, Any]] = []
    for photo in photos:
        path = Path(photo.path)
        if path.parent != Path(retention["root"]):
            raise QuestionTriggerError(f"staged photo {photo.digest[:12]} is not directly in the retained root")
        refs.append({
            "ref": f"{retention['ref_prefix']}/{path.name}",
            "digest": photo.digest,
            "mime": photo.mime,
            "message_id": photo.message_id or None,
            "media_ordinal": photo.media_ordinal,
            "sender_id": photo.sender_id,
            "sent_at": photo.sent_at,
        })
    return refs


def enqueue_management_question_trigger(
    inbox: Any,
    session_db: Any,
    *,
    config_path: Path,
    record_id: str,
    chat_id: str,
    question_text: str,
    source: Any,
    resolver_command: Sequence[str] | None = None,
    receipt_path: Path | None = None,
    retry_after_failed: bool = False,
    check_only: bool = False,
) -> dict[str, Any]:
    """Stage photos, run every refusal, and enqueue one trigger row.

    Nothing is sent here and no model turn runs in this process.  ``--check``
    runs the same reads, staging and refusals and writes no row.
    """
    plan = durable.prepare_management_question(
        inbox,
        config_path=config_path,
        record_id=record_id,
        chat_id=chat_id,
        question_text=question_text,
        source=source,
        resolver_command=resolver_command,
    )
    session_key = _require_shared_conversation(config_path, plan["chat_id"])
    history = attempt_history(session_db, record_id=plan["record_id"], chat_id=plan["chat_id"])
    for row in history:
        status = str(row.get("status") or "")
        if status in {"pending", "running"}:
            raise QuestionTriggerError(
                f"QUESTION_TRIGGER_ALREADY_QUEUED: attempt {row['id']} is {status}"
            )
        if status == "delivered":
            raise QuestionTriggerError(
                f"QUESTION_TRIGGER_ALREADY_DELIVERED: attempt {row['id']} delivered"
            )
        if status == "failed" and not retry_after_failed:
            raise QuestionTriggerError(
                "QUESTION_TRIGGER_EARLIER_ATTEMPT_FAILED: an earlier trigger failed "
                f"({str(row.get('last_error') or '')[:200]}); re-trigger only with "
                "--retry-after-failed-trigger"
            )
    attempt = len(history) + 1
    if receipt_path is None:
        receipt_path = (
            config_path.parent / "runtime" / "management-question-receipts"
            / f"{plan['record_id']}--{plan['chat_id']}--attempt-{attempt}.json"
        )
    receipt_path = Path(receipt_path).expanduser().resolve()
    if receipt_path.exists():
        raise QuestionTriggerError(f"trigger receipt already exists: {receipt_path}")
    body = {
        "contract": CONTRACT,
        "record_id": plan["record_id"],
        "entry_id": plan["entry_id"],
        "chat_id": plan["chat_id"],
        "question_text": plan["text"],
        "question_sha256": hashlib.sha256(plan["text"].encode()).hexdigest(),
        "photos": _photo_refs(plan["photos"], config_path),
        "receipt_path": str(receipt_path),
        "attempt": attempt,
    }
    result = dict(plan["receipt"])
    result["trigger"] = {
        "contract": CONTRACT,
        "attempt": attempt,
        "session_key": session_key,
        "receipt_path": str(receipt_path),
        "photo_refs": [photo["ref"] for photo in body["photos"]],
        "mailbox_id": mailbox_id(plan["record_id"], plan["chat_id"], attempt),
    }
    if check_only:
        return result
    row = session_db.create_session_mailbox_message(
        agent_id=AGENT_ID,
        from_session_name=FROM_SESSION,
        to_session_name=TO_SESSION,
        body=json.dumps(body, ensure_ascii=False, sort_keys=True),
        correlation_id=plan["record_id"],
        idempotency_key=idempotency_key(plan["record_id"], plan["chat_id"], attempt),
    )
    result["trigger"]["status"] = str(row.get("status") or "")
    return result


# --------------------------------------------------------------------------
# Consumer side (live process)
# --------------------------------------------------------------------------


def next_pending_trigger(session_db: Any) -> Mapping[str, Any] | None:
    rows = session_db.list_pending_session_mailbox(
        agent_id=AGENT_ID, include_body_contract=CONTRACT, limit=1,
    )
    return rows[0] if rows else None


def trigger_chat_id(row: Mapping[str, Any]) -> str | None:
    """The lane a pending row would occupy; None for an unreadable body."""
    try:
        body = json.loads(str(row.get("body") or ""))
    except (TypeError, ValueError):
        return None
    chat = str(body.get("chat_id") or "").strip() if isinstance(body, Mapping) else ""
    return chat or None


def terminalize_prior_process_triggers(session_db: Any) -> int:
    """A row running when the process stopped is failed, never replayed."""
    return int(session_db.fail_running_session_mailbox(
        agent_id=AGENT_ID,
        from_session_name=FROM_SESSION,
        to_session_name=TO_SESSION,
        body_contract=CONTRACT,
        error=PRIOR_PROCESS_OUTCOME_UNKNOWN,
    ))


def parse_trigger_body(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        body = json.loads(str(row.get("body") or ""))
    except (TypeError, ValueError) as exc:
        raise QuestionTriggerError("QUESTION_TRIGGER_BODY_INVALID") from exc
    if not isinstance(body, Mapping) or set(body) != _BODY_KEYS or body.get("contract") != CONTRACT:
        raise QuestionTriggerError("QUESTION_TRIGGER_BODY_INVALID")
    for field in ("record_id", "entry_id", "chat_id", "question_text", "receipt_path"):
        if not isinstance(body.get(field), str) or not body[field].strip():
            raise QuestionTriggerError("QUESTION_TRIGGER_BODY_INVALID")
    if hashlib.sha256(body["question_text"].encode()).hexdigest() != body.get("question_sha256"):
        raise QuestionTriggerError("QUESTION_TRIGGER_BODY_INVALID")
    photos = body.get("photos")
    if not isinstance(photos, list) or any(
        not isinstance(photo, Mapping) or set(photo) != _PHOTO_KEYS
        or not re.fullmatch(r"[0-9a-f]{64}", str(photo.get("digest") or ""))
        for photo in photos
    ):
        raise QuestionTriggerError("QUESTION_TRIGGER_BODY_INVALID")
    if str(row.get("id") or "") != mailbox_id(body["record_id"], body["chat_id"], int(body["attempt"])):
        raise QuestionTriggerError("QUESTION_TRIGGER_MAILBOX_IDENTITY_INVALID")
    return dict(body)


def conversation_session_key(runner: Any, chat_id: str) -> str:
    """The persistent session key live Management turns use for this chat."""
    from gateway.config import Platform
    from gateway.replay import namespace_session_key
    from gateway.session import SessionSource, build_session_key

    config = getattr(runner, "config", None)
    per_user = bool(getattr(config, "group_sessions_per_user", True))
    is_group = chat_id.endswith("@g.us")
    if is_group and per_user:
        raise QuestionTriggerError("QUESTION_TRIGGER_NO_SHARED_CONVERSATION")
    base_key = build_session_key(
        SessionSource(
            platform=Platform.WHATSAPP, chat_id=chat_id,
            chat_type="group" if is_group else "dm",
        ),
        group_sessions_per_user=per_user,
        thread_sessions_per_user=bool(getattr(config, "thread_sessions_per_user", False)),
    )
    return namespace_session_key(base_key, durable.persistent_chat_namespace())


def internal_trigger_event(body: Mapping[str, Any], mailbox: str) -> tuple[str, dict[str, Any]]:
    message_id = f"{INTERNAL_MESSAGE_PREFIX}{mailbox}"
    media_lines = "\n".join(f"MEDIA:{photo['ref']}" for photo in body["photos"])
    photo_instruction = (
        f" Attach these {len(body['photos'])} photos of the record, one MEDIA line each, "
        "exactly as written below."
        if body["photos"] else " There are no photos to attach."
    )
    text = (
        "[Internal trigger. This is not a client message.] Send the question below to this "
        "Management chat now, as your own message, in exactly these words."
        f"{photo_instruction} This is your question on human-resolution record "
        f"{body['record_id']}. Send nothing else in this turn and make no case, record "
        "or document change.\n\n"
        f"Question:\n{body['question_text']}"
        + (f"\n\nPhotos:\n{media_lines}" if media_lines else "")
    )
    event = {
        "messageId": message_id,
        "chatId": body["chat_id"],
        "isGroup": str(body["chat_id"]).endswith("@g.us"),
        "senderId": "system@internal",
        "senderName": "Christopher internal trigger",
        "body": text,
        "timestamp": int(time.time()),
        "fromMe": False,
        "_hermes_pa_context": {
            "management_question_trigger": {
                "contract": CONTRACT,
                "mailbox_id": mailbox,
                "record_id": body["record_id"],
                "internal": True,
            }
        },
    }
    return message_id, event


def _normalized(text: str) -> str:
    return " ".join(str(text or "").split())


def collect_question_sends(
    captured_outbound: Sequence[Mapping[str, Any]],
    *,
    chat_id: str,
    internal_message_id: str,
    retention: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Expand his captured sends into question bodies and photo files.

    Text ``MEDIA:`` lines and native image sends are handled alike; the
    internal event anchor is stripped from both.  Anything else (another
    chat, another anchor, another kind of send) is a problem.
    """
    bodies: list[str] = []
    photos: list[Path] = []
    problems: list[str] = []

    def anchor_ok(reply_to: Any) -> bool:
        return not reply_to or str(reply_to) == internal_message_id

    def add_photo(item: Mapping[str, Any]) -> None:
        try:
            path = durable._resolve_captured_media_path(
                item.get("path"), retention,
                workspace_owner=item.get("workspace_owner"), for_delivery=True,
            )
        except (OSError, durable.MediaRetentionError) as exc:
            problems.append(f"an attachment that is not a retained file: {exc}")
            return
        photos.append(Path(path))

    for entry in captured_outbound:
        kind = str(entry.get("kind") or "")
        if kind == "send":
            parsed = durable._parse_captured_send(entry)
            if parsed is None:
                problems.append("an empty text send")
                continue
            if parsed["chat_id"] != chat_id:
                problems.append("a send to another chat")
                continue
            if not anchor_ok(parsed.get("reply_to")):
                problems.append("a send anchored to another message")
                continue
            expanded = durable._expand_captured_send(parsed)
            media = [item for item in expanded if item.get("send_kind") == "media"]
            text = (
                str(expanded[0].get("response_text") or "") if media else str(parsed["content"])
            ).strip()
            if text:
                bodies.append(text)
            for item in media:
                add_photo(item)
        elif kind in {"send_image_file", "send_multiple_images"}:
            items = durable._parse_captured_media(entry)
            if not items:
                problems.append(f"an unreadable {kind}")
            for item in items:
                if item["chat_id"] != chat_id:
                    problems.append("a send to another chat")
                    continue
                if not anchor_ok(item.get("reply_to")):
                    problems.append("a send anchored to another message")
                    continue
                if str(item.get("caption") or "").strip():
                    bodies.append(str(item["caption"]).strip())
                add_photo(item)
        else:
            problems.append(f"an unexpected {kind or 'unknown'} send")
    return {"bodies": bodies, "photos": photos, "problems": problems}


def check_question_sends(
    collected: Mapping[str, Any], *, draft: str, staged: Sequence[Mapping[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    """Return (reasons, drift).  No reasons means the turn may be posted."""
    reasons = list(collected["problems"])
    bodies = list(collected["bodies"])
    drift: dict[str, Any] = {"equal_normalized": None, "diff": []}
    if len(bodies) != 1:
        reasons.append(f"{len(bodies)} question bodies instead of one")
    else:
        drift["equal_normalized"] = _normalized(bodies[0]) == _normalized(draft)
        drift["diff"] = list(difflib.unified_diff(
            draft.strip().splitlines(), bodies[0].strip().splitlines(),
            fromfile="approved_draft", tofile="christopher", lineterm="",
        ))
        if not drift["equal_normalized"]:
            reasons.append("the question is not the approved draft")
    sent = [hashlib.sha256(path.read_bytes()).hexdigest() for path in collected["photos"] if path.is_file()]
    expected = [str(photo["digest"]) for photo in staged]
    if len(sent) != len(collected["photos"]):
        reasons.append("an attached photo is unreadable")
    if len(set(sent)) != len(sent):
        reasons.append("a photo is attached twice")
    missing = set(expected) - set(sent)
    extra = set(sent) - set(expected)
    if missing:
        reasons.append(f"{len(missing)} staged photo(s) missing")
    if extra:
        reasons.append(f"{len(extra)} photo(s) that are not the record's")
    return reasons, drift


def _append_not_delivered_note(runner: Any, session_key: str, record_id: str, reason: str) -> dict[str, Any]:
    """Keep his history honest: the question he wrote was not sent."""
    store = getattr(runner, "session_store", None)
    if store is None:
        return {"status": "failed", "error": "no live session store"}
    try:
        entry = next((item for item in store.list_sessions() if item.session_key == session_key), None)
        if entry is None:
            return {"status": "failed", "error": "no persistent conversation for this chat"}
        store.append_to_transcript(entry.session_id, {
            "role": "user",
            "content": (
                "[Internal runtime note, not a client message] Your question on "
                f"human-resolution record {record_id} was NOT delivered to this chat: {reason}. "
                "Management has not seen it. Do not treat it as asked."
            ),
            "timestamp": datetime.now().isoformat(),
        })
        return {"status": "appended", "session_id": entry.session_id}
    except Exception as exc:
        return {"status": "failed", "error": f"{type(exc).__name__}: {exc}"[:300]}


async def process_claimed_question_trigger(
    *,
    row: Mapping[str, Any],
    inbox: Any,
    config_path: Path,
    state_db: Path,
    runner: Any,
    bridge_url: str | None = None,
) -> dict[str, Any]:
    """Run one claimed trigger: his turn, the check, then the shared send."""
    mailbox = str(row.get("id") or "")
    body = parse_trigger_body(row)
    receipt: dict[str, Any] = {
        "contract": CONTRACT,
        "mailbox_id": mailbox,
        "record_id": body["record_id"],
        "entry_id": body["entry_id"],
        "destination_chat_id": body["chat_id"],
        "attempt": body["attempt"],
        "question_sha256": body["question_sha256"],
    }
    if body["chat_id"] not in durable._management_selector_chats(config_path):
        receipt.update(outcome="refused", reason="destination is not a WhatsApp management selector")
        return receipt
    prior = durable._management_question_prior_delivery(
        inbox, chat_id=body["chat_id"], record_id=body["record_id"], entry_id=body["entry_id"],
    )
    if prior is not None:
        receipt.update(outcome="refused", reason=f"record already has a delivery in this chat ({prior})")
        return receipt
    retention = durable._retention_config(config_path)
    for photo in body["photos"]:
        try:
            path = durable._resolve_captured_media_path(photo["ref"], retention)
            ok = hashlib.sha256(path.read_bytes()).hexdigest() == photo["digest"]
        except (OSError, durable.MediaRetentionError):
            ok = False
        if not ok:
            receipt.update(outcome="refused", reason=f"staged photo {photo['ref']} is missing or changed")
            return receipt
    session_key = conversation_session_key(runner, body["chat_id"])
    receipt["session_key"] = session_key
    message_id, event = internal_trigger_event(body, mailbox)
    result = await durable.process_live_records(
        [],
        config_path=config_path,
        state_db=state_db,
        persistent_session=True,
        runner=runner,
        defer_provider_errors=True,
        replay_messages=(event,),
        replay_session_key_override=session_key,
        replay_internal_message_ids=(message_id,),
        live_business_writes=False,
    )
    handled = [
        dict(group) for group in result.get("handled") or []
        if message_id in {str(value) for value in group.get("message_ids") or []}
    ]
    receipt["turn_ids"] = [group.get("turn_id") for group in handled]
    receipt["business_mutations_captured"] = len(result.get("captured_business_mutations") or [])
    if result.get("provider_errors") or len(handled) != 1:
        reason = "his turn did not complete" + (
            f": {result['provider_errors'][0]}" if result.get("provider_errors") else ""
        )
        receipt.update(outcome="turn_failed", reason=reason[:500])
        receipt["not_delivered_note"] = _append_not_delivered_note(
            runner, session_key, body["record_id"], "the turn did not complete",
        )
        return receipt
    collected = collect_question_sends(
        result.get("captured_outbound") or [],
        chat_id=body["chat_id"], internal_message_id=message_id, retention=retention,
    )
    reasons, drift = check_question_sends(
        collected, draft=body["question_text"], staged=body["photos"],
    )
    receipt["drift"] = drift
    receipt["captured_bodies"] = len(collected["bodies"])
    receipt["captured_photos"] = len(collected["photos"])
    if reasons:
        receipt.update(outcome="check_failed", reason="; ".join(reasons)[:500])
        receipt["not_delivered_note"] = _append_not_delivered_note(
            runner, session_key, body["record_id"], "; ".join(reasons),
        )
        return receipt
    # Post in the staged (record) order, which --check reported, with his
    # question as the first photo's caption.
    by_digest = {hashlib.sha256(path.read_bytes()).hexdigest(): path for path in collected["photos"]}
    photos = [
        durable.ManagementQuestionPhoto(
            path=by_digest[photo["digest"]], digest=photo["digest"], mime=photo["mime"],
            message_id=str(photo.get("message_id") or ""), media_ordinal=photo.get("media_ordinal"),
            sender_id=photo.get("sender_id"), sent_at=photo.get("sent_at"),
        )
        for photo in body["photos"]
    ]
    delivery = durable.deliver_management_question(
        inbox,
        chat_id=body["chat_id"],
        record_id=body["record_id"],
        entry_id=body["entry_id"],
        text=collected["bodies"][0],
        photos=photos,
        bridge_url=bridge_url,
    )
    receipt.update(delivery)
    notice_ok = (delivery.get("notice") or {}).get("status") == "delivered"
    photos_ok = all(item.get("status") == "delivered" for item in delivery.get("photos") or [])
    receipt["outcome"] = "delivered" if notice_ok and photos_ok else (
        "partially_delivered" if notice_ok else "undelivered"
    )
    return receipt


def finish_trigger(session_db: Any, row: Mapping[str, Any], receipt: Mapping[str, Any]) -> None:
    """Terminal the mailbox row and write the immutable receipt."""
    mailbox = str(row.get("id") or "")
    if receipt.get("outcome") == "delivered":
        session_db.complete_session_mailbox(mailbox, to_session_key=receipt.get("session_key"))
    else:
        session_db.fail_session_mailbox(
            mailbox, f"{receipt.get('outcome')}: {receipt.get('reason') or ''}".strip()[:1000],
        )
    write_receipt(row, receipt)


def write_receipt(row: Mapping[str, Any], receipt: Mapping[str, Any]) -> None:
    try:
        target = json.loads(str(row.get("body") or "")).get("receipt_path")
    except (TypeError, ValueError, AttributeError):
        target = None
    if not target:
        return
    try:
        durable._write_immutable_json_receipt(Path(str(target)), receipt, label="management question trigger")
    except Exception as exc:
        print(f"management question trigger receipt NOT written: {exc}", flush=True)
