"""Native gateway replay primitives.

Replay runs bridge-message corpora through the real gateway/adapter message path
without connecting live adapters or delivering live outbound messages.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import subprocess
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional


_REPLAY_CONTEXT: ContextVar["ReplayExecutionContext | None"] = ContextVar(
    "HERMES_REPLAY_CONTEXT",
    default=None,
)
_REPLAY_TURN_HISTORY_BEFORE_TS: ContextVar[int | None] = ContextVar(
    "HERMES_REPLAY_TURN_HISTORY_BEFORE_TS",
    default=None,
)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return str(value)


def canonical_json(value: Any) -> str:
    """Return the canonical JSON representation used for replay digests."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def canonical_digest(value: Any) -> str:
    """Return a stable sha256 digest for a manifest-like Python value."""
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _manifest_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _load_json_or_jsonl(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix in {".jsonl", ".ndjson"}:
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return json.loads(text)


def _coerce_messages(raw: Any) -> list[dict[str, Any]]:
    """Return a bridge-message list from common corpus shapes."""
    if isinstance(raw, list):
        return [m for m in raw if isinstance(m, dict)]
    if not isinstance(raw, dict):
        raise ValueError("replay corpus must be a JSON object or list")
    for key in ("messages", "bridge_messages", "bridgeMessages", "events"):
        value = raw.get(key)
        if isinstance(value, list):
            return [m for m in value if isinstance(m, dict)]
    # Some exports wrap the interesting corpus under a chat/group key.
    corpus = raw.get("corpus")
    if isinstance(corpus, dict):
        return _coerce_messages(corpus)
    raise ValueError("replay corpus object must include messages/bridge_messages/events")


def _message_id(message: Mapping[str, Any]) -> str | None:
    value = message.get("messageId") or message.get("message_id") or message.get("id")
    return str(value) if value is not None and str(value) else None


def _message_timestamp(message: Mapping[str, Any]) -> int | None:
    return _timestamp_to_epoch(message.get("timestamp") or message.get("ts"))



def _media_refs(value: Any) -> list[dict[str, Any]]:
    if not value:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []
    if not isinstance(value, list):
        return []
    return [ref for ref in value if isinstance(ref, dict)]


def _raw_json(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


_REACTION_TEXT_RE = re.compile(r"^\s*\[reaction:[^\]]*\]\s*$", re.IGNORECASE)


def _is_bare_reaction_row(*, message_kind: str = "", text: str = "", has_media: bool = False) -> bool:
    """Return True for reaction-only records that must not trigger replay turns."""
    if has_media:
        return False
    kind = str(message_kind or "").strip().lower()
    body = str(text or "").strip()
    if kind == "reaction":
        return not body or bool(_REACTION_TEXT_RE.match(body))
    return bool(_REACTION_TEXT_RE.match(body))


def _source_ref_from_bridge_message(message: Mapping[str, Any]) -> str:
    for key in ("_pa_source_ref", "_tgg_source_ref", "source_ref", "sourceRef", "messageId", "message_id", "id"):
        value = message.get(key)
        if value is not None and str(value):
            return str(value)
    return ""


def _dedup_key_for_message(message: Mapping[str, Any]) -> str:
    for key in ("messageId", "message_id", "id", "_pa_source_ref", "_tgg_source_ref", "source_ref", "sourceRef"):
        value = message.get(key)
        if value is not None and str(value):
            return str(value)
    ts = _message_timestamp(message) or 0
    return f"{message.get('chatId') or message.get('chat_id') or ''}:{ts}:{message.get('body') or ''}"


def _bridge_sort_key(message: Mapping[str, Any]) -> tuple[int, str]:
    ts = _message_timestamp(message)
    return (ts if ts is not None else 0, _dedup_key_for_message(message))


@dataclass(frozen=True)
class ReplayCorpusPolicy:
    """Determinism controls applied before a corpus becomes a ReplayPlan."""

    ordering: tuple[str, ...] = ("timestamp", "source_ref")
    dedup: str = "first_by_message_id_or_source_ref"
    reaction_policy: str = "skip_bare_reactions"
    turn_bundling: Mapping[str, Any] = field(default_factory=lambda: {
        "owner": "WhatsAppAdapter.replay_bridge_messages",
        "window": "adapter turn_policy debounce_seconds / config turn_debounce_ms",
        "split": "original timestamp gap >= debounce_seconds",
        "hard_cap_messages": 10,
    })
    quote_policy: str = "preserve quotedText and quotedMessageId on bridge messages"
    media_policy: str = "preserve local paths; remap missing basename via media_root when provided; report unresolved missing media"
    future_read_fence: str = "per_turn_latest_message_timestamp_plus_one"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ordering": list(self.ordering),
            "dedup": self.dedup,
            "reaction_policy": self.reaction_policy,
            "turn_bundling": dict(self.turn_bundling or {}),
            "quote_policy": self.quote_policy,
            "media_policy": self.media_policy,
            "future_read_fence": self.future_read_fence,
        }


@dataclass(frozen=True)
class ReplayCorpus:
    """Deterministic replay corpus that feeds ReplayPlan.

    The first source is SQLite ``bridge_message_log``. The interface also accepts
    already-shaped JSON/JSONL bridge messages so later exports can plug into the
    same plan surface without changing ``GatewayRunner.replay``.
    """

    messages: tuple[dict[str, Any], ...]
    source_type: str
    source_path: Optional[str] = None
    policy: ReplayCorpusPolicy = field(default_factory=ReplayCorpusPolicy)
    report: Mapping[str, Any] = field(default_factory=dict)
    source_manifest: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_source(cls, source: Mapping[str, Any] | str | Path) -> "ReplayCorpus":
        if isinstance(source, (str, Path)):
            return cls.from_json_path(source)
        if not isinstance(source, Mapping):
            raise ValueError("replay corpus source must be a path or mapping")
        source_type = str(source.get("source") or source.get("type") or source.get("kind") or "json").strip().lower()
        if source_type in {"bridge_message_log", "bridge-message-log", "sqlite"}:
            db_path = source.get("db_path") or source.get("path") or source.get("file")
            if not db_path:
                raise ValueError("bridge_message_log corpus requires db_path/path")
            return cls.from_bridge_message_log(
                db_path,
                chat_id=source.get("chat_id") or source.get("chatId"),
                since=source.get("since") or source.get("since_sgt") or source.get("sinceSgt"),
                until=source.get("until") or source.get("until_sgt") or source.get("untilSgt"),
                tenant=source.get("tenant"),
                agent_id=source.get("agent_id") or source.get("agentId") or source.get("agent"),
                job_type=source.get("job_type") or source.get("jobType"),
                limit=source.get("limit") or source.get("limit_messages") or source.get("limitMessages"),
                skip_messages=source.get("skip_messages") or source.get("skipMessages") or 0,
                media_root=source.get("media_root") or source.get("mediaRoot"),
            )
        if "path" in source or "file" in source:
            return cls.from_json_path(source.get("path") or source.get("file"))
        return cls.from_messages(_coerce_messages(source), source_type=source_type, source_manifest={"inline": True})

    @classmethod
    def from_json_path(cls, path: str | Path) -> "ReplayCorpus":
        corpus_path = Path(path).expanduser()
        messages = _coerce_messages(_load_json_or_jsonl(corpus_path))
        return cls.from_messages(messages, source_type="json", source_path=str(corpus_path))

    @classmethod
    def from_messages(
        cls,
        messages: Iterable[Mapping[str, Any]],
        *,
        source_type: str = "bridge_messages",
        source_path: str | None = None,
        source_manifest: Mapping[str, Any] | None = None,
    ) -> "ReplayCorpus":
        sorted_messages = sorted((dict(m) for m in messages if isinstance(m, Mapping)), key=_bridge_sort_key)
        deduped, duplicates = _dedup_bridge_messages(sorted_messages)
        report = _empty_corpus_report()
        report["duplicates_skipped"] = duplicates
        policy = ReplayCorpusPolicy()
        return cls(
            messages=tuple(deduped),
            source_type=source_type,
            source_path=source_path,
            policy=policy,
            report=_prune_empty_report(report),
            source_manifest=dict(source_manifest or {}),
        )

    @classmethod
    def from_bridge_message_log(
        cls,
        db_path: str | Path,
        *,
        chat_id: Any,
        since: Any,
        tenant: Any,
        agent_id: Any,
        job_type: Any,
        until: Any = None,
        limit: Any = None,
        skip_messages: Any = 0,
        media_root: str | Path | None = None,
    ) -> "ReplayCorpus":
        if not chat_id:
            raise ValueError("bridge_message_log corpus requires chat_id")
        if not since:
            raise ValueError("bridge_message_log corpus requires since")
        if not tenant:
            raise ValueError("bridge_message_log corpus requires tenant")
        if not agent_id:
            raise ValueError("bridge_message_log corpus requires agent_id")
        if not job_type:
            raise ValueError("bridge_message_log corpus requires job_type")
        path = Path(db_path).expanduser()
        skip_count = int(skip_messages or 0)
        limit_count = int(limit) if limit is not None and str(limit) != "" else None
        rows, skipped_offset = _load_bridge_message_log_rows(
            path,
            chat_id=str(chat_id),
            since_sgt=str(since),
            until_sgt=str(until) if until else None,
            limit=limit_count,
            skip_messages=skip_count,
        )
        report = _empty_corpus_report()
        if skipped_offset:
            report["messages_skipped"].append({"reason": "offset", "count": skipped_offset})
        messages: list[dict[str, Any]] = []
        media_root_path = Path(media_root).expanduser().resolve() if media_root else None
        for row in rows:
            if _is_bare_reaction_row(
                message_kind=str(row.get("message_kind") or ""),
                text=str(row.get("text") or ""),
                has_media=bool(row.get("has_media")),
            ):
                report["messages_skipped"].append({
                    "reason": "bare_reaction",
                    "source_ref": row.get("source_ref"),
                    "message_kind": row.get("message_kind"),
                })
                continue
            messages.append(
                _bridge_message_from_log_row(
                    row,
                    media_root=media_root_path,
                    report=report,
                    tenant=str(tenant),
                    agent_id=str(agent_id),
                    job_type=str(job_type),
                )
            )
        messages.sort(key=_bridge_sort_key)
        messages, duplicates = _dedup_bridge_messages(messages)
        report["duplicates_skipped"] = duplicates
        manifest = {
            "source_type": "bridge_message_log",
            "db_path": str(path),
            "chat_id": str(chat_id),
            "since": str(since),
            "until": str(until) if until else None,
            "tenant": str(tenant),
            "agent_id": str(agent_id),
            "job_type": str(job_type),
            "limit_messages": limit_count,
            "skip_messages": skip_count,
        }
        if media_root_path:
            manifest["media_root"] = str(media_root_path)
        return cls(
            messages=tuple(messages),
            source_type="bridge_message_log",
            source_path=str(path),
            policy=ReplayCorpusPolicy(),
            report=_prune_empty_report(report),
            source_manifest=manifest,
        )

    def manifest(self) -> dict[str, Any]:
        timestamps = [ts for ts in (_message_timestamp(m) for m in self.messages) if ts is not None]
        ids = [mid for mid in (_message_id(m) for m in self.messages) if mid is not None]
        manifest = dict(self.source_manifest or {})
        manifest.update({
            "source_type": self.source_type,
            "source_path": self.source_path,
            "message_count": len(self.messages),
            "message_ids": ids[:50],
            "message_ids_truncated": len(ids) > 50,
            "first_timestamp": min(timestamps) if timestamps else None,
            "last_timestamp": max(timestamps) if timestamps else None,
            "messages_digest": canonical_digest(list(self.messages)),
            "policy": self.policy.to_dict(),
            "report": dict(self.report or {}),
        })
        return manifest

    def replay_policy_manifest(self) -> dict[str, Any]:
        return {
            "corpus_policy": self.policy.to_dict(),
            "history_before_ts": None,
            "future_read_fence": {
                "mode": self.policy.future_read_fence,
                "enforced_by": "gateway.replay.history_before_ts_for_event",
            },
        }

    def to_plan_kwargs(self) -> dict[str, Any]:
        return {
            "messages": self.messages,
            "source_path": self.source_path,
            "corpus_manifest": self.manifest(),
            "replay_policy": self.replay_policy_manifest(),
        }


def _empty_corpus_report() -> dict[str, Any]:
    return {
        "messages_skipped": [],
        "duplicates_skipped": [],
        "missing_media": [],
        "warnings": [],
    }


def _prune_empty_report(report: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in dict(report).items() if value}


def _dedup_bridge_messages(messages: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    for message in messages:
        key = _dedup_key_for_message(message)
        if key in seen:
            duplicates.append({
                "reason": "duplicate_message",
                "dedup_key": key,
                "source_ref": _source_ref_from_bridge_message(message),
            })
            continue
        seen.add(key)
        out.append(message)
    return out, duplicates


def _load_bridge_message_log_rows(
    db_path: Path,
    *,
    chat_id: str,
    since_sgt: str,
    until_sgt: str | None,
    limit: int | None,
    skip_messages: int,
) -> tuple[list[dict[str, Any]], int]:
    clauses = ["chat_jid = ?", "sgt >= ?"]
    params: list[Any] = [chat_id, since_sgt]
    if until_sgt:
        clauses.append("sgt < ?")
        params.append(until_sgt)
    sql = f"""
        SELECT source_ref, chat_jid, chat_name, sender_id, ts, sgt, text,
               message_kind, has_media, media_refs, quoted_text,
               reply_to_source_ref, raw_json
        FROM bridge_message_log
        WHERE {' AND '.join(clauses)}
        ORDER BY ts, source_ref
    """
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    elif skip_messages:
        sql += " LIMIT -1"
    if skip_messages:
        sql += " OFFSET ?"
        params.append(int(skip_messages))
    rows: list[dict[str, Any]] = []
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(sql, params):
            rows.append({key: row[key] for key in row.keys()})
    return rows, skip_messages


def _resolve_media_path(candidate: str, *, source_ref: str, media_root: Path | None, report: dict[str, Any]) -> str:
    if not candidate:
        return candidate
    if candidate.startswith(("http://", "https://")):
        return candidate
    try:
        if Path(candidate).exists():
            return candidate
    except Exception:
        pass
    if media_root is not None:
        try:
            remapped = media_root / Path(candidate).name
            if remapped.exists():
                return str(remapped)
        except Exception:
            pass
    report["missing_media"].append({
        "source_ref": source_ref,
        "path": candidate,
        "basename": Path(candidate).name,
        "reason": "media_path_missing",
    })
    return candidate


def _media_paths_from_log_refs(refs: list[dict[str, Any]], *, source_ref: str, media_root: Path | None, report: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for ref in refs:
        candidate = ref.get("local_path") or ref.get("path") or ref.get("file_path")
        if candidate:
            paths.append(_resolve_media_path(str(candidate), source_ref=source_ref, media_root=media_root, report=report))
    return paths


def _bridge_message_from_log_row(
    row: Mapping[str, Any],
    *,
    media_root: Path | None,
    report: dict[str, Any],
    tenant: str,
    agent_id: str,
    job_type: str,
) -> dict[str, Any]:
    raw = _raw_json(row.get("raw_json"))
    source_ref = str(row.get("source_ref") or "")
    message_id = raw.get("id") or source_ref.rsplit("::", 1)[-1] or source_ref
    has_media = bool(row.get("has_media"))
    media_refs = _media_refs(row.get("media_refs"))
    if has_media and not media_refs:
        report["missing_media"].append({
            "source_ref": source_ref,
            "path": None,
            "basename": None,
            "reason": "has_media_no_media_refs",
        })
    media_paths = _media_paths_from_log_refs(media_refs, source_ref=source_ref, media_root=media_root, report=report)
    chat_jid = str(row.get("chat_jid") or "")
    sender_id = str(row.get("sender_id") or "")
    message_kind = str(row.get("message_kind") or "")
    bridge = {
        **raw,
        "messageId": message_id,
        "chatId": chat_jid,
        "chatName": str(row.get("chat_name") or chat_jid or ""),
        "senderId": sender_id,
        "senderName": sender_id.split("@", 1)[0] if sender_id else "",
        "isGroup": chat_jid.endswith("@g.us"),
        "timestamp": int(row.get("ts") or 0),
        "sgt": str(row.get("sgt") or ""),
        "body": str(row.get("text") or ""),
        "hasMedia": has_media,
        "mediaType": message_kind,
        "mediaUrls": media_paths,
        "quotedText": str(row.get("quoted_text") or ""),
        "quotedMessageId": str(row.get("reply_to_source_ref") or ""),
        "fromMe": bool(raw.get("fromMe", False)),
        "_pa_source_ref": source_ref,
        "_pa_local_time": str(row.get("sgt") or ""),
        # D1-23 (the live WhatsApp renderer) is intentionally outside this
        # phase. Dual-write its legacy replay keys until that consumer moves.
        "_tgg_source_ref": source_ref,
        "_tgg_sgt": str(row.get("sgt") or ""),
        "_hermes_pa_job_type": job_type,
        "_hermes_pa_context": {
            "tenant": tenant,
            "agent_id": agent_id,
            "job_type": job_type,
        },
    }
    return bridge


def _default_code_manifest() -> dict[str, Any]:
    """Best-effort readable code/artifact manifest for a replay attempt."""
    repo = Path(__file__).resolve().parents[1]
    manifest: dict[str, Any] = {
        "repo": str(repo),
        "runtime": "hermes",
        "replay_module": "gateway.replay",
        "replay_cli": "hermes replay",
    }
    try:
        commit = subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=2,
        ).strip()
        if commit:
            manifest["git_commit"] = commit
    except Exception:
        pass
    try:
        dirty = subprocess.call(
            ["git", "-C", str(repo), "diff", "--quiet"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        staged_dirty = subprocess.call(
            ["git", "-C", str(repo), "diff", "--cached", "--quiet"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        manifest["git_dirty"] = bool(dirty or staged_dirty)
    except Exception:
        pass
    if os.environ.get("HERMES_HOME"):
        manifest["hermes_home"] = os.environ.get("HERMES_HOME")
    return manifest


@dataclass(frozen=True)
class ReplayPlan:
    """Typed input for ``GatewayRunner.replay`` and ``hermes replay``."""

    platform: str = "whatsapp"
    messages: tuple[dict[str, Any], ...] = ()
    run_id: str = field(default_factory=lambda: f"replay-{uuid.uuid4().hex[:12]}")
    attempt_id: str = field(default_factory=lambda: f"attempt-{uuid.uuid4().hex[:12]}")
    delivery_mode: str = "capture"  # capture | drop
    bypass_require_mention: bool = True
    bypass_auth: bool = True
    # True ONLY for the durable live consumer's drain: business-bridge ops run
    # as ordinary LIVE writes (no X-Replay-* tagging), because the drain IS
    # live processing that happens to reuse the replay machinery for bundling
    # and debounce parity. Evals/replays keep the default False so the
    # server-side replay-target protection continues to refuse prod writes.
    live_business_writes: bool = False
    replay_safe_commands: tuple[str, ...] = ()
    history_before_ts: Optional[int] = None
    source_path: Optional[str] = None
    replay_namespace: Optional[str] = None
    replay_policy: Mapping[str, Any] = field(default_factory=dict)
    corpus_manifest: Mapping[str, Any] = field(default_factory=dict)
    config_overlay_manifest: Mapping[str, Any] = field(default_factory=dict)
    target_descriptor_manifest: Mapping[str, Any] = field(default_factory=dict)
    target_baseline_manifest: Mapping[str, Any] = field(default_factory=dict)
    code_manifest: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        namespace = str(self.replay_namespace or f"agent:replay:{self.run_id}").strip(":")
        if not namespace:
            raise ValueError("replay_namespace cannot be empty")
        if namespace.startswith("agent:main"):
            raise ValueError("replay_namespace must not use the live agent:main namespace")
        object.__setattr__(self, "replay_namespace", namespace)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any], *, base_dir: Path | None = None) -> "ReplayPlan":
        if not isinstance(data, Mapping):
            raise ValueError("replay plan must be a JSON object")
        platform = str(data.get("platform") or "whatsapp").strip().lower()
        delivery_mode = str(data.get("delivery_mode") or data.get("deliveryMode") or "capture").strip().lower()
        if delivery_mode not in {"capture", "drop"}:
            raise ValueError("delivery_mode must be 'capture' or 'drop'")

        messages_value = data.get("messages")
        source_path: Optional[str] = None
        corpus_manifest = _manifest_mapping(data.get("corpus_manifest") or data.get("corpusManifest"))
        corpus_policy: dict[str, Any] = {}
        if messages_value is None:
            corpus = data.get("corpus") or data.get("input")
            if isinstance(corpus, Mapping):
                corpus_path = corpus.get("path") or corpus.get("file")
                corpus_kind = str(
                    corpus.get("source") or corpus.get("type") or corpus.get("kind") or ""
                ).strip().lower()
                if corpus_kind in {"bridge_message_log", "bridge-message-log", "sqlite"}:
                    corpus_spec = dict(corpus)
                    db_path = corpus_spec.get("db_path") or corpus_spec.get("path") or corpus_spec.get("file")
                    if db_path:
                        db = Path(str(db_path)).expanduser()
                        if not db.is_absolute() and base_dir is not None:
                            db = base_dir / db
                        corpus_spec["db_path"] = str(db)
                    media_root = corpus_spec.get("media_root") or corpus_spec.get("mediaRoot")
                    if media_root:
                        root = Path(str(media_root)).expanduser()
                        if not root.is_absolute() and base_dir is not None:
                            root = base_dir / root
                        corpus_spec["media_root"] = str(root)
                    replay_corpus = ReplayCorpus.from_source(corpus_spec)
                    messages_value = list(replay_corpus.messages)
                    source_path = replay_corpus.source_path
                    corpus_manifest.update(replay_corpus.manifest())
                    corpus_policy.update(replay_corpus.replay_policy_manifest())
                elif corpus_path:
                    path = Path(str(corpus_path)).expanduser()
                    if not path.is_absolute() and base_dir is not None:
                        path = base_dir / path
                    replay_corpus = ReplayCorpus.from_json_path(path)
                    source_path = replay_corpus.source_path
                    messages_value = list(replay_corpus.messages)
                    corpus_manifest.update(replay_corpus.manifest())
                    corpus_policy.update(replay_corpus.replay_policy_manifest())
                else:
                    replay_corpus = ReplayCorpus.from_source(corpus)
                    messages_value = list(replay_corpus.messages)
                    source_path = replay_corpus.source_path
                    corpus_manifest.update(replay_corpus.manifest())
                    corpus_policy.update(replay_corpus.replay_policy_manifest())
            elif isinstance(corpus, (str, Path)):
                path = Path(str(corpus)).expanduser()
                if not path.is_absolute() and base_dir is not None:
                    path = base_dir / path
                replay_corpus = ReplayCorpus.from_json_path(path)
                source_path = replay_corpus.source_path
                messages_value = list(replay_corpus.messages)
                corpus_manifest.update(replay_corpus.manifest())
                corpus_policy.update(replay_corpus.replay_policy_manifest())
        messages = tuple(_coerce_messages(messages_value or []))

        safe = data.get("replay_safe_commands") or data.get("replaySafeCommands") or ()
        if isinstance(safe, str):
            safe_commands = tuple(part.strip().lstrip("/").lower() for part in safe.split(",") if part.strip())
        elif isinstance(safe, Iterable):
            safe_commands = tuple(str(part).strip().lstrip("/").lower() for part in safe if str(part).strip())
        else:
            safe_commands = ()

        before = data.get("history_before_ts", data.get("historyBeforeTs"))
        if before is not None:
            try:
                before = int(float(before))
            except (TypeError, ValueError):
                raise ValueError("history_before_ts must be an epoch second") from None

        replay_namespace = data.get("replay_namespace") or data.get("replayNamespace")
        replay_policy = _manifest_mapping(data.get("replay_policy") or data.get("replayPolicy") or {})
        if corpus_policy:
            replay_policy = {**corpus_policy, **replay_policy}
        return cls(
            platform=platform,
            messages=messages,
            run_id=str(data.get("run_id") or data.get("runId") or f"replay-{uuid.uuid4().hex[:12]}"),
            attempt_id=str(data.get("attempt_id") or data.get("attemptId") or f"attempt-{uuid.uuid4().hex[:12]}"),
            delivery_mode=delivery_mode,
            bypass_require_mention=bool(data.get("bypass_require_mention", data.get("bypassRequireMention", True))),
            bypass_auth=bool(data.get("bypass_auth", data.get("bypassAuth", True))),
            replay_safe_commands=safe_commands,
            history_before_ts=before,
            source_path=source_path or (str(data.get("source_path") or data.get("sourcePath") or "") or None),
            replay_namespace=str(replay_namespace) if replay_namespace else None,
            replay_policy=replay_policy,
            corpus_manifest=corpus_manifest,
            config_overlay_manifest=_manifest_mapping(
                data.get("config_overlay_manifest") or data.get("configOverlayManifest") or data.get("config_overlay") or data.get("configOverlay")
            ),
            target_descriptor_manifest=_manifest_mapping(
                data.get("target_descriptor_manifest") or data.get("targetDescriptorManifest") or data.get("target_descriptor") or data.get("targetDescriptor")
            ),
            target_baseline_manifest=_manifest_mapping(
                data.get("target_baseline_manifest") or data.get("targetBaselineManifest") or data.get("target_baseline") or data.get("targetBaseline")
            ),
            code_manifest=_manifest_mapping(data.get("code_manifest") or data.get("codeManifest")),
        )

    @classmethod
    def from_path(cls, path: str | Path) -> "ReplayPlan":
        plan_path = Path(path).expanduser()
        data = _load_json_or_jsonl(plan_path)
        if isinstance(data, list):
            return cls.from_corpus_path(plan_path)
        plan = cls.from_mapping(data, base_dir=plan_path.parent)
        if plan.source_path is None:
            object.__setattr__(plan, "source_path", str(plan_path))
        return plan

    @classmethod
    def from_corpus_path(
        cls,
        path: str | Path,
        *,
        platform: str = "whatsapp",
        delivery_mode: str = "capture",
        bypass_require_mention: bool = True,
        bypass_auth: bool = True,
        ) -> "ReplayPlan":
        replay_corpus = ReplayCorpus.from_json_path(path)
        return cls(
            platform=platform,
            messages=replay_corpus.messages,
            delivery_mode=delivery_mode,
            bypass_require_mention=bypass_require_mention,
            bypass_auth=bypass_auth,
            source_path=replay_corpus.source_path,
            replay_policy=replay_corpus.replay_policy_manifest(),
            corpus_manifest=replay_corpus.manifest(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "messages": list(self.messages),
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "replay_namespace": self.replay_namespace,
            "delivery_mode": self.delivery_mode,
            "bypass_require_mention": self.bypass_require_mention,
            "bypass_auth": self.bypass_auth,
            "replay_safe_commands": list(self.replay_safe_commands),
            "history_before_ts": self.history_before_ts,
            "source_path": self.source_path,
            "replay_policy": dict(self.replay_policy or {}),
            "corpus_manifest": dict(self.corpus_manifest or {}),
            "config_overlay_manifest": dict(self.config_overlay_manifest or {}),
            "target_descriptor_manifest": dict(self.target_descriptor_manifest or {}),
            "target_baseline_manifest": dict(self.target_baseline_manifest or {}),
            "code_manifest": dict(self.code_manifest or {}),
        }

    def policy_manifest(self) -> dict[str, Any]:
        manifest = dict(self.replay_policy or {})
        manifest.update({
            "execution_mode": "replay",
            "delivery_mode": self.delivery_mode,
            "bypass_require_mention": self.bypass_require_mention,
            "bypass_auth": self.bypass_auth,
            "history_before_ts": self.history_before_ts,
            "replay_safe_commands": list(self.replay_safe_commands),
            "replay_namespace": self.replay_namespace,
            "session_namespace_strategy": "replace agent:main prefix",
        })
        return manifest

    def readable_corpus_manifest(self) -> dict[str, Any]:
        manifest = dict(self.corpus_manifest or {})
        messages = list(self.messages)
        timestamps = [
            ts for ts in (_message_timestamp(m) for m in messages if isinstance(m, Mapping))
            if ts is not None
        ]
        ids = [
            mid for mid in (_message_id(m) for m in messages if isinstance(m, Mapping))
            if mid is not None
        ]
        manifest.update({
            "source_path": self.source_path,
            "message_count": len(messages),
            "message_ids": ids[:50],
            "message_ids_truncated": len(ids) > 50,
            "first_timestamp": min(timestamps) if timestamps else None,
            "last_timestamp": max(timestamps) if timestamps else None,
            "messages_digest": canonical_digest(messages),
        })
        return manifest

    def provenance_manifests(self) -> dict[str, dict[str, Any]]:
        target_descriptor = dict(self.target_descriptor_manifest or {})
        if target_descriptor and "run_id" not in target_descriptor:
            target_descriptor["run_id"] = self.run_id
        code = dict(self.code_manifest or {}) or _default_code_manifest()
        return {
            "corpus": self.readable_corpus_manifest(),
            "config_overlay": dict(self.config_overlay_manifest or {}),
            "target_descriptor": target_descriptor,
            "target_baseline": dict(self.target_baseline_manifest or {}),
            "code": code,
            "replay_policy": self.policy_manifest(),
        }

    def manifest_digest(self) -> str:
        return canonical_digest(self.provenance_manifests())


def namespace_session_key(session_key: str, replay_namespace: str) -> str:
    """Map a live session key into the replay namespace."""
    namespace = str(replay_namespace).strip(":")
    if not namespace:
        raise ValueError("replay namespace cannot be empty")
    if session_key.startswith(namespace + ":"):
        return session_key
    live_prefix = "agent:main:"
    if session_key.startswith(live_prefix):
        return f"{namespace}:{session_key[len(live_prefix):]}"
    return f"{namespace}:{session_key}"


@dataclass(frozen=True)
class ReplayAttempt:
    """Persisted replay provenance card.

    This intentionally stores manifests + digests only. Execution reports are
    derived from normal Hermes/PA rows tagged by run/attempt id.
    """

    attempt_id: str
    run_id: str
    replay_namespace: str
    platform: str
    delivery_mode: str
    status: str
    started_at: float
    completed_at: Optional[float] = None
    corpus_manifest: Mapping[str, Any] = field(default_factory=dict)
    corpus_digest: str = ""
    config_overlay_manifest: Mapping[str, Any] = field(default_factory=dict)
    config_overlay_digest: str = ""
    target_descriptor_manifest: Mapping[str, Any] = field(default_factory=dict)
    target_descriptor_digest: str = ""
    target_baseline_manifest: Mapping[str, Any] = field(default_factory=dict)
    target_baseline_digest: str = ""
    code_manifest: Mapping[str, Any] = field(default_factory=dict)
    code_digest: str = ""
    replay_policy_manifest: Mapping[str, Any] = field(default_factory=dict)
    replay_policy_digest: str = ""
    plan_manifest: Mapping[str, Any] = field(default_factory=dict)
    plan_digest: str = ""
    error: Optional[Any] = None

    @classmethod
    def from_plan(cls, plan: ReplayPlan, *, status: str = "running") -> "ReplayAttempt":
        manifests = plan.provenance_manifests()
        plan_manifest = {
            "run_id": plan.run_id,
            "attempt_id": plan.attempt_id,
            "platform": plan.platform,
            "replay_namespace": plan.replay_namespace,
            "manifest_digests": {
                name: canonical_digest(value)
                for name, value in manifests.items()
            },
        }
        return cls(
            attempt_id=plan.attempt_id,
            run_id=plan.run_id,
            replay_namespace=str(plan.replay_namespace),
            platform=plan.platform,
            delivery_mode=plan.delivery_mode,
            status=status,
            started_at=time.time(),
            corpus_manifest=manifests["corpus"],
            corpus_digest=canonical_digest(manifests["corpus"]),
            config_overlay_manifest=manifests["config_overlay"],
            config_overlay_digest=canonical_digest(manifests["config_overlay"]),
            target_descriptor_manifest=manifests["target_descriptor"],
            target_descriptor_digest=canonical_digest(manifests["target_descriptor"]),
            target_baseline_manifest=manifests["target_baseline"],
            target_baseline_digest=canonical_digest(manifests["target_baseline"]),
            code_manifest=manifests["code"],
            code_digest=canonical_digest(manifests["code"]),
            replay_policy_manifest=manifests["replay_policy"],
            replay_policy_digest=canonical_digest(manifests["replay_policy"]),
            plan_manifest=plan_manifest,
            plan_digest=canonical_digest(plan_manifest),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "run_id": self.run_id,
            "replay_namespace": self.replay_namespace,
            "platform": self.platform,
            "delivery_mode": self.delivery_mode,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "corpus_manifest": dict(self.corpus_manifest or {}),
            "corpus_digest": self.corpus_digest,
            "config_overlay_manifest": dict(self.config_overlay_manifest or {}),
            "config_overlay_digest": self.config_overlay_digest,
            "target_descriptor_manifest": dict(self.target_descriptor_manifest or {}),
            "target_descriptor_digest": self.target_descriptor_digest,
            "target_baseline_manifest": dict(self.target_baseline_manifest or {}),
            "target_baseline_digest": self.target_baseline_digest,
            "code_manifest": dict(self.code_manifest or {}),
            "code_digest": self.code_digest,
            "replay_policy_manifest": dict(self.replay_policy_manifest or {}),
            "replay_policy_digest": self.replay_policy_digest,
            "plan_manifest": dict(self.plan_manifest or {}),
            "plan_digest": self.plan_digest,
            "error": self.error,
        }

    def to_db_kwargs(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass
class ReplayExecutionContext:
    plan: ReplayPlan
    started_at: float = field(default_factory=time.time)
    outbound: list[dict[str, Any]] = field(default_factory=list)
    blocked_commands: list[dict[str, Any]] = field(default_factory=list)

    @property
    def execution_mode(self) -> str:
        return "replay"

    @property
    def run_id(self) -> str:
        return self.plan.run_id

    @property
    def attempt_id(self) -> str:
        return self.plan.attempt_id

    @property
    def replay_namespace(self) -> str:
        return str(self.plan.replay_namespace)

    @property
    def delivery_mode(self) -> str:
        return self.plan.delivery_mode

    @property
    def bypass_auth(self) -> bool:
        return self.plan.bypass_auth

    @property
    def replay_safe_commands(self) -> set[str]:
        return {cmd.lstrip("/").lower() for cmd in self.plan.replay_safe_commands}

    def namespace_session_key(self, session_key: str) -> str:
        return namespace_session_key(session_key, self.replay_namespace)

    def bridge_headers(self) -> dict[str, str]:
        if self.plan.live_business_writes:
            # Live-drain semantics: business ops are ordinary live writes.
            return {}
        return {
            "X-Replay-Run-Id": self.run_id,
            "X-Replay-Attempt-Id": self.attempt_id,
            "X-Replay-Namespace": self.replay_namespace,
        }

    def command_allowed(self, command: str | None) -> bool:
        if not command:
            return True
        return command.lstrip("/").lower() in self.replay_safe_commands

    def record_blocked_command(self, *, command: str, platform: str = "", chat_id: str = "") -> None:
        self.blocked_commands.append({
            "command": command.lstrip("/"),
            "platform": platform,
            "chat_id": chat_id,
            "reason": "replay_command_side_effect_blocked",
        })

    def record_outbound(self, *, kind: str, args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> str:
        message_id = f"replay-{len(self.outbound) + 1}"
        self.outbound.append({
            "message_id": message_id,
            "kind": kind,
            "args": list(args),
            "kwargs": dict(kwargs),
            "delivery_mode": self.delivery_mode,
            "replay_run_id": self.run_id,
            "replay_attempt_id": self.attempt_id,
            "replay_namespace": self.replay_namespace,
            "headers": self.bridge_headers(),
        })
        return message_id


def current_replay_context() -> ReplayExecutionContext | None:
    return _REPLAY_CONTEXT.get()


def current_history_before_ts() -> int | None:
    ctx = current_replay_context()
    if ctx is None:
        return None
    turn_value = _REPLAY_TURN_HISTORY_BEFORE_TS.get()
    if turn_value is not None:
        return turn_value
    return ctx.plan.history_before_ts


@contextmanager
def replay_context(plan: ReplayPlan) -> Iterator[ReplayExecutionContext]:
    ctx = ReplayExecutionContext(plan=plan)
    ctx_token = _REPLAY_CONTEXT.set(ctx)
    turn_token = _REPLAY_TURN_HISTORY_BEFORE_TS.set(None)
    try:
        yield ctx
    finally:
        _REPLAY_TURN_HISTORY_BEFORE_TS.reset(turn_token)
        _REPLAY_CONTEXT.reset(ctx_token)


def set_replay_turn_history_before_ts(value: int | None):
    return _REPLAY_TURN_HISTORY_BEFORE_TS.set(value)


def reset_replay_turn_history_before_ts(token) -> None:
    _REPLAY_TURN_HISTORY_BEFORE_TS.reset(token)


def _timestamp_to_epoch(value: Any) -> int | None:
    if isinstance(value, Mapping):
        value = value.get("low") or value.get("value") or value.get("seconds")
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        pass
    try:
        # Accept ISO-ish timestamps from tests/fixtures.
        from datetime import datetime, timezone

        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def history_before_ts_for_event(event: Any) -> int | None:
    raw = getattr(event, "raw_message", None)
    if not isinstance(raw, Mapping):
        return None
    timestamps: list[int] = []
    if isinstance(raw.get("messages"), list):
        for msg in raw.get("messages") or []:
            if isinstance(msg, Mapping):
                ts = _timestamp_to_epoch(msg.get("timestamp"))
                if ts is not None:
                    timestamps.append(ts)
    ts = _timestamp_to_epoch(raw.get("timestamp"))
    if ts is not None:
        timestamps.append(ts)
    if not timestamps:
        return None
    return max(timestamps) + 1


@dataclass
class ReplayResult:
    run_id: str
    attempt_id: str
    platform: str
    processed: int
    outbound: list[dict[str, Any]]
    blocked_commands: list[dict[str, Any]]
    delivery_mode: str
    corpus_report: Optional[dict[str, Any]] = None
    attempt: Optional[dict[str, Any]] = None
    execution_report: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "platform": self.platform,
            "processed": self.processed,
            "outbound": self.outbound,
            "blocked_commands": self.blocked_commands,
            "delivery_mode": self.delivery_mode,
            "corpus_report": self.corpus_report,
            "attempt": self.attempt,
            "execution_report": self.execution_report,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, default=_json_default)
