"""Carry the conversation word for word through PA compaction (PA-648).

Christopher's Management chats compact with the PA keep/drop policy
``preserve-recent``: the head and the latest ``window_size`` messages are kept
and everything older is dropped.  A person's instruction older than the window
was lost, and so were the agent's own replies to it.  The
``preserve-recent-and-conversation`` strategy keeps the same head and window
and, in between, one message carrying the conversation from the dropped
region that is still recent:

* every message a person wrote (a real user-role chat message, not a
  synthetic injection) and every text reply the agent sent (an
  assistant-role message with text and no tool calls) from the last
  ``conversation_days`` is carried word for word, oldest first;
* tool calls and tool output are dropped, as before;
* the carried text is capped at ``conversation_max_tokens`` (oldest dropped
  first); a message longer than ``conversation_message_max_chars`` keeps its
  first and last halves around a note naming the stored row with the full
  text (``read_full_message`` returns it); an exact repeat is carried once.

Each carried message has its own marker line recording who wrote it, when,
and its state.db row, so a later compaction reads the block back, applies the
age window again and carries it on.  The compressor owns the policy; this
module holds the pure parts.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

STRATEGY = "preserve-recent-and-conversation"
# Toolset holding read_full_message; the gateway enables it for a chat whose
# compression policy is STRATEGY, so the cut notes can be followed.
RECALL_TOOLSET = "pa-message-recall"

DEFAULT_MAX_AGE_DAYS = 7.0
DEFAULT_MAX_TOKENS = 30_000
DEFAULT_MAX_MESSAGE_CHARS = 2_000

PERSON = "person"
AGENT = "agent"
_ROLE_OF = {PERSON: "user", AGENT: "assistant"}

BLOCK_HEADER = "[EARLIER CONVERSATION — CARRIED WORD FOR WORD THROUGH COMPACTION]"
BLOCK_PREAMBLE = (
    "Earlier messages from this chat, copied exactly, oldest first: what people "
    "wrote and the replies you sent them. Your tool calls and tool results from "
    "that time were dropped to save space. These messages were already "
    "received and answered, so do not reply to them again; but any instruction, "
    "correction or constraint people gave still applies to the work in "
    "progress. Where a long message was cut, the note names the stored message; "
    "read_full_message returns its full text."
)
BLOCK_END = "[[end of earlier conversation]]"
_MARKER_LABEL = {PERSON: "person message", AGENT: "your reply"}
_ENTRY_RE = re.compile(
    r"^\[\[(?P<label>person message|your reply) sent (?P<when>\S+)"
    r"(?: session=(?P<session>\S+) message=(?P<message>\d+))?\]\]$",
    re.MULTILINE,
)
_CUT_NOTE_RE = re.compile(r"^\[cut [\d,]+ characters, [\d,]+ lines from the middle of ", re.MULTILINE)
_WHATSAPP_ID_RE = re.compile(r"\b(?:source_message_id|original_message_id)=([A-Za-z0-9_.:@\-]+)")

# User-role content that is not a person's chat message, matched
# case-insensitively at the start of the message, or right after the one
# "[sender name] " label the gateway adds in shared chats.  Leading gateway
# notes (_LEADING_NOTE_RE) are stripped first.
_SYNTHETIC_PREFIXES = (
    "[system]",                                   # durable-consumer jobs ("[system] scheduled weekly report run")
    "[system:",                                   # run_agent continuation nudges
    "continue the complete whatsapp investigation for",  # internal investigation continuation prompt
    "[internal ",                                 # management-list continuation (management_continuation_consumer)
    "[important:",                                # watch / background-process / skill / MCP notices
    "[background process",                       # background-process completion notices
    "[session was just handed off",               # CLI-to-channel handoff turn
    "[message from ",                             # inter-session mailbox delivery
    "[scheduled job ",                            # cron delivery
    "[context compaction",                        # a compaction summary landing as a user message
    "[context summary]",                          # legacy compaction summary
    "[pa context compaction]",                    # PA keep/drop policy marker
)
_SYNTHETIC_SENDERS = ("system", "christopher internal continuation")
_SENDER_LABEL_RE = re.compile(r"^\[([^\]\n]{1,80})\]\s+")
# Gateway notes prepended to a real person's message; the person's words
# follow them.  Stripped before judging and before carrying.
_LEADING_NOTE_RE = re.compile(
    r"^\s*\[(?:System note|Note: model was just switched)[^\]]*\]\s*",
    re.IGNORECASE,
)
# Assistant-role text that is compaction machinery, not a reply.
_AGENT_MACHINERY_PREFIXES = (
    "[pa context compaction]",
    "[context compaction",
    "[context summary]",
)


@dataclass(frozen=True)
class CarrySettings:
    max_age_days: float = DEFAULT_MAX_AGE_DAYS
    max_tokens: int = DEFAULT_MAX_TOKENS
    max_message_chars: int = DEFAULT_MAX_MESSAGE_CHARS

    @classmethod
    def from_policy(cls, policy: Mapping[str, Any] | None) -> "CarrySettings":
        """Read the strategy's parameters from its compression policy mapping."""
        policy = policy or {}

        def _num(key: str, default, cast):
            value = policy.get(key, default)
            if isinstance(value, bool):
                return default
            try:
                value = cast(value)
            except (TypeError, ValueError):
                return default
            return value if value > 0 else default

        return cls(
            max_age_days=_num("conversation_days", DEFAULT_MAX_AGE_DAYS, float),
            max_tokens=_num("conversation_max_tokens", DEFAULT_MAX_TOKENS, int),
            max_message_chars=_num("conversation_message_max_chars", DEFAULT_MAX_MESSAGE_CHARS, int),
        )


@dataclass(frozen=True)
class StoredMessage:
    """Where a message is stored: its send time and state.db row."""
    sent_at: float
    session_id: Optional[str] = None
    message_id: Optional[int] = None


@dataclass(frozen=True)
class CarriedEntry:
    author: str                    # PERSON or AGENT
    text: str                      # as carried: exact, or start + cut note + end
    sent_at: float                 # epoch seconds
    session_id: Optional[str] = None
    message_id: Optional[int] = None


def _text_of(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                parts.append(str(part.get("text", "")))
            elif isinstance(part, str):
                parts.append(part)
        return "\n".join(parts)
    return "" if content is None else str(content)


def _is_synthetic(text: str) -> bool:
    lowered = text.lower()
    if any(lowered.startswith(prefix) for prefix in _SYNTHETIC_PREFIXES):
        return True
    label = _SENDER_LABEL_RE.match(text)
    if label:
        if label.group(1).strip().lower() in _SYNTHETIC_SENDERS:
            return True
        rest = text[label.end():].lower()
        return any(rest.startswith(prefix) for prefix in _SYNTHETIC_PREFIXES)
    return False


def person_text(message: Mapping[str, Any]) -> Optional[str]:
    """The person's words in a user-role message, or None if it is synthetic."""
    if message.get("role") != "user":
        return None
    text = _text_of(message.get("content"))
    while True:
        stripped = _LEADING_NOTE_RE.sub("", text, count=1)
        if stripped == text:
            break
        text = stripped
    text = text.strip()
    if not text or _is_synthetic(text):
        return None
    return text


def reply_text(message: Mapping[str, Any]) -> Optional[str]:
    """The agent's text reply in an assistant-role message, or None.

    A reply is assistant text with no tool calls.  Messages that made tool
    calls are the agent's working and are dropped, as is compaction
    machinery.
    """
    if message.get("role") != "assistant" or message.get("tool_calls"):
        return None
    text = _text_of(message.get("content")).strip()
    if not text:
        return None
    if any(text.lower().startswith(prefix) for prefix in _AGENT_MACHINERY_PREFIXES):
        return None
    return text


def conversation_text(message: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    """(author, text) when a message is carried conversation, else None."""
    text = person_text(message)
    if text is not None:
        return PERSON, text
    text = reply_text(message)
    if text is not None:
        return AGENT, text
    return None


def shorten(text: str, max_chars: int, stored: Optional[StoredMessage] = None) -> str:
    """Keep the first and last halves of an over-long message around a cut note.

    Long person messages are usually pasted lists, and the question often
    comes after the list, so both ends are kept.  The note names the stored
    row so the agent can read the full text with ``read_full_message``.
    """
    if len(text) <= max_chars:
        return text
    half = max(1, max_chars // 2)
    middle = text[half:len(text) - half]
    cut_lines = middle.count("\n")
    if stored is not None and stored.session_id and stored.message_id is not None:
        where = (
            f"full text: read_full_message session={stored.session_id} "
            f"message={stored.message_id}"
        )
    else:
        where = "full text: not yet in the message store"
    wa_id = _WHATSAPP_ID_RE.search(text)
    if wa_id:
        where += f"; whatsapp message {wa_id.group(1)}"
    note = (
        f"\n[cut {len(middle):,} characters, {cut_lines:,} lines from the middle of "
        f"this {len(text):,}-character message; {where}]\n"
    )
    return text[:half] + note + text[-half:]


def _format_time(epoch: float, tz) -> str:
    return datetime.fromtimestamp(epoch, tz or timezone.utc).isoformat(timespec="seconds")


def _marker(entry: CarriedEntry, tz) -> str:
    ref = ""
    if entry.session_id and entry.message_id is not None:
        ref = f" session={entry.session_id} message={entry.message_id}"
    return f"[[{_MARKER_LABEL[entry.author]} sent {_format_time(entry.sent_at, tz)}{ref}]]"


def render_block(entries: List[CarriedEntry], tz=None) -> str:
    """The carried block placed between the compaction marker and the window."""
    lines = [BLOCK_HEADER, BLOCK_PREAMBLE, ""]
    for entry in entries:
        lines.append(_marker(entry, tz))
        lines.append(entry.text)
        lines.append("")
    lines.append(BLOCK_END)
    return "\n".join(lines)


def is_block(content: Any) -> bool:
    """True when a message is a carried block written by ``render_block``."""
    return _text_of(content).lstrip().startswith(BLOCK_HEADER)


def parse_block(content: Any) -> List[CarriedEntry]:
    """The carried entries in a block message, with authors, send times and rows."""
    block = _text_of(content)
    stop = block.find(BLOCK_END)
    if stop < 0:
        stop = len(block)
    markers = list(_ENTRY_RE.finditer(block, 0, stop))
    entries: List[CarriedEntry] = []
    for idx, match in enumerate(markers):
        try:
            sent_at = datetime.fromisoformat(match.group("when")).timestamp()
        except ValueError:
            continue
        body_end = markers[idx + 1].start() if idx + 1 < len(markers) else stop
        text = block[match.end():body_end].strip("\n")
        if not text:
            continue
        message = match.group("message")
        entries.append(CarriedEntry(
            author=PERSON if match.group("label") == "person message" else AGENT,
            text=text,
            sent_at=sent_at,
            session_id=match.group("session"),
            message_id=int(message) if message is not None else None,
        ))
    return entries


@dataclass
class Selection:
    carried: List[CarriedEntry]    # oldest first, within the cap
    carried_messages: int          # dropped messages (not earlier blocks) now carried
    carried_tokens: int
    person_messages: int           # carried entries a person wrote
    agent_replies: int             # carried entries the agent sent
    shortened: int                 # carried messages cut to start and end
    excluded_synthetic: int        # synthetic user-role injections dropped
    aged_out: int
    capped_out: int
    duplicates: int


Locate = Callable[[Tuple[str, str]], Optional[StoredMessage]]


def select(
    dropped: List[Dict[str, Any]],
    settings: CarrySettings,
    *,
    now: float,
    locate: Locate,
    estimate: Callable[[str], int],
    window: Iterable[Mapping[str, Any]] = (),
    tz=None,
) -> Selection:
    """Choose the conversation to carry from the messages being dropped.

    ``dropped`` is the region older than the kept window, in order; a carried
    block found there (from an earlier compaction) is read back.  ``locate``
    maps ``(role, content)`` to where the message is stored; a message it
    cannot find arrived in the running turn and counts as sent ``now``.
    ``estimate`` is the compressor's token estimate for a piece of text.
    ``window`` is the kept window: a message still in it is not carried.
    """
    def _stored(msg: Mapping[str, Any], text: str) -> Optional[StoredMessage]:
        role = msg.get("role")
        return locate((role, _text_of(msg.get("content")).strip())) or locate((role, text))

    candidates: List[CarriedEntry] = []
    from_messages: set = set()     # id() of entries read from a message, not a block
    synthetic = 0
    for msg in dropped:
        if msg.get("role") == "user" and is_block(msg.get("content")):
            candidates.extend(parse_block(msg.get("content")))
            continue
        found = conversation_text(msg)
        if found is None:
            if msg.get("role") == "user":
                synthetic += 1
            continue
        author, text = found
        stored = _stored(msg, text)
        entry = CarriedEntry(
            author=author,
            text=shorten(text, settings.max_message_chars, stored),
            sent_at=stored.sent_at if stored is not None else now,
            session_id=stored.session_id if stored is not None else None,
            message_id=stored.message_id if stored is not None else None,
        )
        from_messages.add(id(entry))
        candidates.append(entry)

    cutoff = now - settings.max_age_days * 86_400
    fresh = [e for e in candidates if e.sent_at >= cutoff]
    aged_out = len(candidates) - len(fresh)

    # A message still in the kept window is not carried; an exact repeat is
    # carried once, at its latest occurrence.
    window_texts: set = set()
    window_rows: set = set()
    for msg in window:
        found = conversation_text(msg)
        if found is None:
            continue
        author, text = found
        window_texts.add((author, text))
        stored = _stored(msg, text)
        if stored is not None and stored.message_id is not None:
            window_rows.add((stored.session_id, stored.message_id))

    def _in_window(entry: CarriedEntry) -> bool:
        if (entry.author, entry.text) in window_texts:
            return True
        return entry.message_id is not None and (entry.session_id, entry.message_id) in window_rows

    latest: Dict[Tuple[str, str], int] = {}
    for pos, entry in enumerate(fresh):
        latest[(entry.author, entry.text)] = pos
    unique = [
        e for pos, e in enumerate(fresh)
        if latest[(e.author, e.text)] == pos and not _in_window(e)
    ]
    duplicates = len(fresh) - len(unique)
    unique.sort(key=lambda entry: entry.sent_at)  # stable: same-turn order holds

    costs = [estimate(_marker(e, tz)) + estimate(e.text) for e in unique]
    total = sum(costs)
    first_kept = 0
    while total > settings.max_tokens and first_kept < len(unique):
        total -= costs[first_kept]
        first_kept += 1
    kept = unique[first_kept:]

    return Selection(
        carried=kept,
        carried_messages=sum(1 for e in kept if id(e) in from_messages),
        carried_tokens=total if kept else 0,
        person_messages=sum(1 for e in kept if e.author == PERSON),
        agent_replies=sum(1 for e in kept if e.author == AGENT),
        shortened=sum(1 for e in kept if _CUT_NOTE_RE.search(e.text)),
        excluded_synthetic=synthetic,
        aged_out=aged_out,
        capped_out=first_kept,
        duplicates=duplicates,
    )
