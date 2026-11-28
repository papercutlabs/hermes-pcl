"""Bounded delivery of authenticated management-list continuation mailbox rows.

The command boundary owns authentication and mailbox creation.  This module is
only the durable consumer side: it accepts the one typed row shape, re-reads
the immutable input it names, and runs one internal turn in Christopher's
ordinary persistent management session.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


_CONTRACT = "management-list-continuation/v1"
_PRIOR_PROCESS_OUTCOME_UNKNOWN = "CONTINUATION_PRIOR_PROCESS_OUTCOME_UNKNOWN"
_LIST_ID = re.compile(r"case-list-[0-9]{14}-[a-f0-9]{10}")
_INTERNAL_MESSAGE_ID = re.compile(r"management-continuation:[A-Za-z0-9-]{1,128}")


class ManagementContinuationError(RuntimeError):
    """A known continuation refusal that must not be replayed automatically."""


@dataclass(frozen=True)
class ManagementContinuationConfig:
    agent_id: str
    from_peer: str
    to_peer: str
    management_chat_id: str
    token_env: str
    coordinator_receipt_root: Path


@dataclass(frozen=True)
class ContinuationEnvelope:
    original_message_id: str
    management_request_id: str
    list_id: str
    chat_id: str
    jobs: tuple[str, ...]
    manifest_sha256: str


def _nonempty_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 512:
        raise ManagementContinuationError(f"CONTINUATION_CONFIG_{field.upper()}_INVALID")
    return text


def load_management_continuation_config(
    config_path: Path,
    *,
    inbox_db: Path,
    state_db: Path,
) -> ManagementContinuationConfig | None:
    """Read the one opt-in PA configuration and verify runtime-store binding."""
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ManagementContinuationError("CONTINUATION_CONFIG_UNREADABLE") from exc
    pa = raw.get("pa") if isinstance(raw, Mapping) else None
    block = pa.get("management_continuation") if isinstance(pa, Mapping) else None
    if block is None:
        return None
    if not isinstance(block, Mapping):
        raise ManagementContinuationError("CONTINUATION_CONFIG_INVALID")
    if block.get("enabled") is not True:
        return None
    for key, actual in (("inbox_db", inbox_db), ("state_db", state_db)):
        declared = block.get(key)
        if declared is not None and Path(str(declared)).expanduser().resolve() != actual.resolve():
            raise ManagementContinuationError(f"CONTINUATION_CONFIG_{key.upper()}_MISMATCH")
    token_env = _nonempty_text(block.get("token_env"), "token_env")
    # The daemon receives its service secret through its environment.  It must
    # never discover a local dotenv file or proceed with an unbound identity.
    if not os.environ.get(token_env, "").strip():
        raise ManagementContinuationError("CONTINUATION_SERVICE_TOKEN_UNAVAILABLE")
    return ManagementContinuationConfig(
        agent_id=_nonempty_text(block.get("agent_id"), "agent_id"),
        from_peer=_nonempty_text(block.get("from_peer"), "from_peer"),
        to_peer=_nonempty_text(block.get("to_peer"), "to_peer"),
        management_chat_id=_nonempty_text(block.get("management_chat_id"), "management_chat_id"),
        token_env=token_env,
        coordinator_receipt_root=Path(
            _nonempty_text(block.get("coordinator_receipt_root"), "coordinator_receipt_root")
        ).expanduser().resolve(),
    )


def _typed_envelope(row: Mapping[str, Any]) -> ContinuationEnvelope | None:
    """Return the envelope only for rows this consumer is allowed to own."""
    try:
        body = json.loads(str(row.get("body") or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(body, Mapping) or body.get("contract") != _CONTRACT:
        return None
    required = {
        "contract", "original_message_id", "management_request_id", "list_id",
        "chat_id", "jobs", "manifest_sha256",
    }
    if set(body) != required:
        raise ManagementContinuationError("CONTINUATION_ENVELOPE_INVALID")
    original_message_id = _nonempty_text(body.get("original_message_id"), "original_message_id")
    request_id = _nonempty_text(body.get("management_request_id"), "management_request_id")
    list_id = _nonempty_text(body.get("list_id"), "list_id")
    chat_id = _nonempty_text(body.get("chat_id"), "chat_id")
    manifest_sha256 = str(body.get("manifest_sha256") or "")
    jobs_raw = body.get("jobs")
    if not _LIST_ID.fullmatch(list_id) or not re.fullmatch(r"[a-f0-9]{64}", manifest_sha256):
        raise ManagementContinuationError("CONTINUATION_ENVELOPE_INVALID")
    if not isinstance(jobs_raw, list) or not 1 <= len(jobs_raw) <= 100:
        raise ManagementContinuationError("CONTINUATION_ENVELOPE_INVALID")
    jobs = tuple(str(job) for job in jobs_raw)
    if any(not re.fullmatch(r"(?:AM|SK|PG)/JOB/\d{4}/\d{1,5}", job) for job in jobs) or len(set(jobs)) != len(jobs):
        raise ManagementContinuationError("CONTINUATION_ENVELOPE_INVALID")
    return ContinuationEnvelope(
        original_message_id=original_message_id,
        management_request_id=request_id,
        list_id=list_id,
        chat_id=chat_id,
        jobs=jobs,
        manifest_sha256=manifest_sha256,
    )


def next_pending_continuation(
    session_db: Any, config: ManagementContinuationConfig
) -> tuple[Mapping[str, Any], ContinuationEnvelope] | None:
    """Find the oldest typed row without taking generic mailbox traffic."""
    for row in session_db.list_pending_session_mailbox(
        agent_id=config.agent_id,
        include_body_contract=_CONTRACT,
        limit=25,
    ):
        envelope = _typed_envelope(row)
        if envelope is not None:
            return row, envelope
    return None


def terminalize_prior_process_continuations(
    session_db: Any, config: ManagementContinuationConfig
) -> int:
    """Mark only this consumer's prior-process claims failed without replay."""
    return int(session_db.fail_running_session_mailbox(
        agent_id=config.agent_id,
        from_session_name=config.from_peer,
        to_session_name=config.to_peer,
        body_contract=_CONTRACT,
        error=_PRIOR_PROCESS_OUTCOME_UNKNOWN,
    ))


def failed_continuation_status(
    session_db: Any, config: ManagementContinuationConfig
) -> Mapping[str, Any]:
    """Expose durable typed failures through the daemon status surface."""
    return session_db.failed_session_mailbox_status(
        agent_id=config.agent_id,
        from_session_name=config.from_peer,
        to_session_name=config.to_peer,
        body_contract=_CONTRACT,
    )


def _validate_row_identity(
    row: Mapping[str, Any], envelope: ContinuationEnvelope, config: ManagementContinuationConfig
) -> None:
    expected = {
        "agent_id": config.agent_id,
        "from_session_name": config.from_peer,
        "to_session_name": config.to_peer,
        "source_message_id": envelope.original_message_id,
    }
    if any(str(row.get(key) or "") != value for key, value in expected.items()):
        raise ManagementContinuationError("CONTINUATION_MAILBOX_IDENTITY_INVALID")
    if envelope.chat_id != config.management_chat_id:
        raise ManagementContinuationError("CONTINUATION_CHAT_BINDING_INVALID")


def _validate_retained_input(
    *, envelope: ContinuationEnvelope, config: ManagementContinuationConfig, inbox: Any
) -> Any:
    records = inbox.message_id_selection([envelope.original_message_id])
    if len(records) != 1 or records[0].chat_id != config.management_chat_id:
        raise ManagementContinuationError("CONTINUATION_ORIGINAL_REQUEST_INVALID")
    manifest_path = config.coordinator_receipt_root / "lists" / envelope.list_id / "manifest.json"
    try:
        raw = manifest_path.read_bytes()
        manifest = json.loads(raw)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ManagementContinuationError("CONTINUATION_MANIFEST_UNREADABLE") from exc
    if hashlib.sha256(raw).hexdigest() != envelope.manifest_sha256:
        raise ManagementContinuationError("CONTINUATION_MANIFEST_HASH_INVALID")
    items = manifest.get("items") if isinstance(manifest, Mapping) else None
    jobs = [item.get("job_no") if isinstance(item, Mapping) else None for item in items or []]
    if (
        not isinstance(manifest, Mapping)
        or manifest.get("contract") != "tgg-per-case-whatsapp-list/v1"
        or manifest.get("list_id") != envelope.list_id
        or manifest.get("management_request_id") != envelope.management_request_id
        or tuple(jobs) != envelope.jobs
    ):
        raise ManagementContinuationError("CONTINUATION_LIST_BINDING_INVALID")
    return records[0]


def _internal_bridge_event(
    *, row: Mapping[str, Any], envelope: ContinuationEnvelope, original: Any
) -> tuple[str, dict[str, Any]]:
    mailbox_id = _nonempty_text(row.get("id"), "mailbox_id")
    internal_message_id = f"management-continuation:{mailbox_id}"
    if not _INTERNAL_MESSAGE_ID.fullmatch(internal_message_id):
        raise ManagementContinuationError("CONTINUATION_MAILBOX_ID_INVALID")
    from gateway.durable_jsonl_consumer import _bridge_item

    original_item = _bridge_item(original.raw)
    body = (
        "[Internal management-list continuation. This is not a new client message. "
        "Continue only the retained request below. Use the registered coordinator. "
        "Do not record the original inbound as handled by this turn. Before drafting the "
        "final management reply, the exact retained list must be substantively complete.]\n\n"
        "In the reply, explain the case findings in ordinary workplace English. "
        "Do not mention retained lists, internal events, coordinators, receipts, "
        "tools or recovery. Do not claim new investigation when reusing existing results.\n\n"
        f"original_message_id={envelope.original_message_id}\n"
        f"management_request_id={envelope.management_request_id}\n"
        f"list_id={envelope.list_id}\n"
        f"jobs={json.dumps(list(envelope.jobs), separators=(',', ':'))}"
    )
    event = {
        "messageId": internal_message_id,
        "chatId": envelope.chat_id,
        "chatName": original_item.get("chatName"),
        "isGroup": bool(original_item.get("isGroup", True)),
        "senderId": "system@internal",
        "senderName": "Christopher internal continuation",
        "body": body,
        "timestamp": int(time.time()),
        "fromMe": False,
        "quotedMessageId": envelope.original_message_id,
        "_hermes_pa_context": {
            "management_continuation": {
                "contract": _CONTRACT,
                "mailbox_id": mailbox_id,
                "original_message_id": envelope.original_message_id,
                "management_request_id": envelope.management_request_id,
                "list_id": envelope.list_id,
                "jobs": list(envelope.jobs),
                "internal": True,
            }
        },
    }
    return internal_message_id, event


def _retained_live_management_session(
    *, runner: Any, original: Any, config_path: Path
) -> tuple[str, str]:
    """Read the session entry the replay path actually selected.

    The configured persistent namespace is not enough by itself: compaction
    lineage lives on the concrete SessionStore entry.  This verifies the
    replay used that entry before the mailbox becomes delivered.
    """
    from gateway.config import Platform
    from gateway.durable_jsonl_consumer import persistent_chat_namespace
    from gateway.replay import namespace_session_key
    from gateway.session import SessionSource, build_session_key

    from gateway.durable_jsonl_consumer import _bridge_item

    item = _bridge_item(original.raw)
    source = SessionSource(
        platform=Platform.WHATSAPP,
        chat_id=str(item["chatId"]),
        chat_name=str(item.get("chatName") or "") or None,
        chat_type="group" if item.get("isGroup") else "dm",
        user_id=str(item.get("senderId") or "") or None,
        user_name=str(item.get("senderName") or "") or None,
    )
    runtime_config = getattr(runner, "config", None)
    base_key = build_session_key(
        source,
        group_sessions_per_user=bool(getattr(runtime_config, "group_sessions_per_user", True)),
        thread_sessions_per_user=bool(getattr(runtime_config, "thread_sessions_per_user", False)),
    )
    namespace = persistent_chat_namespace()
    expected_key = namespace_session_key(base_key, namespace)
    store = getattr(runner, "session_store", None)
    if store is None:
        raise ManagementContinuationError("CONTINUATION_LIVE_SESSION_UNAVAILABLE")
    store._ensure_loaded()
    entry = store._entries.get(expected_key)
    if entry is None or str(getattr(entry, "session_id", "") or "") == "":
        raise ManagementContinuationError("CONTINUATION_RETAINED_SESSION_MISSING")
    origin = getattr(entry, "origin", None)
    if origin is None or str(getattr(origin, "chat_id", "")) != str(item["chatId"]):
        raise ManagementContinuationError("CONTINUATION_RETAINED_SESSION_MISSING")
    return expected_key, str(entry.session_id)


def _same_retained_lineage(
    *, runner: Any, session_key: str, before_session_id: str, after_session_id: str
) -> bool:
    """Accept a normal compression child, but not a different conversation."""
    if before_session_id == after_session_id:
        return True
    store = getattr(runner, "session_store", None)
    if store is None:
        return False
    try:
        store._ensure_loaded()
        entry = store._entries.get(session_key)
        if entry is None or str(getattr(entry, "session_id", "")) != after_session_id:
            return False
        db = getattr(store, "_db", None)
        return bool(
            db is not None
            and db.get_compression_tip(before_session_id) == after_session_id
        )
    except Exception:
        return False


async def process_claimed_management_continuation(
    *,
    row: Mapping[str, Any],
    envelope: ContinuationEnvelope,
    session_db: Any,
    inbox: Any,
    config: ManagementContinuationConfig,
    config_path: Path,
    state_db: Path,
    gate_changed_at: str,
    runner: Any,
) -> dict[str, Any]:
    """Run exactly one claimed continuation and terminal its mailbox row.

    Any failure after a mailbox claim is terminal.  A provider outcome can be
    unknown after a process interruption, so returning it to pending would be
    a duplicate model turn and potentially a duplicate business effect.
    """
    from gateway import durable_jsonl_consumer as durable
    from gateway.management_continuation import require_complete_list

    mailbox_id = str(row.get("id") or "")
    try:
        _validate_row_identity(row, envelope, config)
        original = _validate_retained_input(envelope=envelope, config=config, inbox=inbox)
        internal_message_id, event = _internal_bridge_event(
            row=row, envelope=envelope, original=original
        )
        session_key, session_id = _retained_live_management_session(
            runner=runner, original=original, config_path=config_path
        )
        result = await durable.process_live_records(
            [original],
            config_path=config_path,
            state_db=state_db,
            persistent_session=True,
            runner=runner,
            replay_messages=(event,),
            replay_session_key_override=session_key,
            replay_internal_message_ids=(internal_message_id,),
        )
        handled = [
            dict(group) for group in result.get("handled") or []
            if internal_message_id in {str(value) for value in group.get("message_ids") or []}
        ]
        if result.get("provider_errors") or len(handled) != 1:
            raise ManagementContinuationError("CONTINUATION_MODEL_OUTCOME_UNKNOWN")
        bound_key, bound_id = _retained_live_management_session(
            runner=runner, original=original, config_path=config_path
        )
        if bound_key != session_key or not _same_retained_lineage(
            runner=runner,
            session_key=session_key,
            before_session_id=session_id,
            after_session_id=bound_id,
        ):
            raise ManagementContinuationError("CONTINUATION_RETAINED_SESSION_CHANGED")
        require_complete_list({
            "list_id": envelope.list_id,
            "management_request_id": envelope.management_request_id,
            "jobs": list(envelope.jobs),
        })
        delivery = durable.deliver_management_replies(
            inbox,
            config_path=config_path,
            captured_outbound=result.get("captured_outbound") or [],
            batch_records=[original],
            gate_changed_at=gate_changed_at,
            handled_groups=handled,
            continuation={
                "list_id": envelope.list_id,
                "original_message_id": envelope.original_message_id,
                "internal_message_id": internal_message_id,
            },
        )
        if delivery.get("delivered", 0) < 1 or any(
            delivery.get(key, 0) for key in ("undelivered", "suppressed", "duplicate")
        ):
            raise ManagementContinuationError("CONTINUATION_DELIVERY_UNCONFIRMED")
        session_db.complete_session_mailbox(
            mailbox_id, to_session_key=bound_key, to_session_id=bound_id
        )
        return {"mailbox_id": mailbox_id, "delivery": delivery}
    except BaseException as exc:
        # Cancellation may cut the provider call after it accepted the turn.
        # Preserve the claimed row as failed so a restart never replays it.
        session_db.fail_session_mailbox(mailbox_id, str(exc) or type(exc).__name__)
        raise
