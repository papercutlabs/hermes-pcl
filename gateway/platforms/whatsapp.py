"""
WhatsApp platform adapter.

WhatsApp integration is more complex than Telegram/Discord because:
- No official bot API for personal accounts
- Business API requires Meta Business verification
- Most solutions use web-based automation

This adapter supports multiple backends:
1. WhatsApp Business API (requires Meta verification)
2. whatsapp-web.js (via Node.js subprocess) - for personal accounts
3. Baileys (via Node.js subprocess) - alternative for personal accounts

For simplicity, we'll implement a generic interface that can work
with different backends via a bridge pattern.
"""

import asyncio
import json
import logging
import os
import platform
import re
import shutil
import signal
import subprocess

_IS_WINDOWS = platform.system() == "Windows"
from pathlib import Path
from typing import Dict, Optional, Any, List

from hermes_constants import get_hermes_dir

logger = logging.getLogger(__name__)


def _kill_port_process(port: int) -> None:
    """Kill any process listening on the given TCP port."""
    try:
        if _IS_WINDOWS:
            # Use netstat to find the PID bound to this port, then taskkill
            result = subprocess.run(
                ["netstat", "-ano", "-p", "TCP"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.splitlines():
                parts = line.split()
                if len(parts) >= 5 and parts[3] == "LISTENING":
                    local_addr = parts[1]
                    if local_addr.endswith(f":{port}"):
                        try:
                            subprocess.run(
                                ["taskkill", "/PID", parts[4], "/F"],
                                capture_output=True, timeout=5,
                            )
                        except subprocess.SubprocessError:
                            pass
        else:
            # Try fuser first (Linux), fall back to lsof (macOS / WSL2)
            killed = False
            try:
                result = subprocess.run(
                    ["fuser", f"{port}/tcp"],
                    capture_output=True, timeout=5,
                )
                if result.returncode == 0:
                    subprocess.run(
                        ["fuser", "-k", f"{port}/tcp"],
                        capture_output=True, timeout=5,
                    )
                    killed = True
            except FileNotFoundError:
                pass  # fuser not installed

            if not killed:
                try:
                    result = subprocess.run(
                        ["lsof", "-ti", f":{port}"],
                        capture_output=True, text=True, timeout=5,
                    )
                    for pid_str in result.stdout.strip().splitlines():
                        try:
                            os.kill(int(pid_str), signal.SIGTERM)
                        except (ValueError, ProcessLookupError, PermissionError):
                            pass
                except FileNotFoundError:
                    pass  # lsof not installed either
    except Exception:
        pass


def _kill_stale_bridge_by_pidfile(session_path: Path) -> None:
    """Kill a bridge process recorded in a PID file from a previous run.

    The bridge writes ``bridge.pid`` into the session directory when it
    starts.  If the gateway crashed without a clean shutdown the old bridge
    process becomes orphaned — this helper finds and kills it.
    """
    pid_file = session_path / "bridge.pid"
    if not pid_file.exists():
        return
    try:
        pid = int(pid_file.read_text().strip())
    except (ValueError, OSError, TypeError):
        try:
            pid_file.unlink()
        except OSError:
            pass
        return
    # ``os.kill(pid, 0)`` is NOT a no-op on Windows (bpo-14484) — use the
    # cross-platform existence check before sending a real signal.
    from gateway.status import _pid_exists
    if _pid_exists(pid):
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info("[whatsapp] Killed stale bridge PID %d from pidfile", pid)
        except (ProcessLookupError, PermissionError, OSError):
            pass
    try:
        pid_file.unlink()
    except OSError:
        pass


def _write_bridge_pidfile(session_path: Path, pid: int) -> None:
    """Write the bridge PID to a file for later cleanup."""
    try:
        (session_path / "bridge.pid").write_text(str(pid))
    except OSError:
        pass


def _terminate_bridge_process(proc, *, force: bool = False) -> None:
    """Terminate the bridge process using process-tree semantics where possible."""
    if _IS_WINDOWS:
        cmd = ["taskkill", "/PID", str(proc.pid), "/T"]
        if force:
            cmd.append("/F")
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except FileNotFoundError:
            if force:
                proc.kill()
            else:
                proc.terminate()
            return

        if result.returncode != 0:
            details = (result.stderr or result.stdout or "").strip()
            raise OSError(details or f"taskkill failed for PID {proc.pid}")
        return

    import psutil
    try:
        parent = psutil.Process(proc.pid)
        children = parent.children(recursive=True)
        if force:
            for child in children:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            parent.kill()
        else:
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass
            parent.terminate()
    except psutil.NoSuchProcess:
        return

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    SUPPORTED_DOCUMENT_TYPES,
    cache_image_from_url,
    cache_audio_from_url,
)


def check_whatsapp_requirements() -> bool:
    """
    Check if WhatsApp dependencies are available.
    
    WhatsApp requires a Node.js bridge for most implementations.
    """
    # Check for Node.js.  Resolve via shutil.which so we respect PATHEXT
    # (node.exe vs node) and get a meaningful "not installed" signal
    # instead of spawning a cmd flash on Windows.
    _node = shutil.which("node")
    if not _node:
        return False
    try:
        result = subprocess.run(
            [_node, "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


class WhatsAppAdapter(BasePlatformAdapter):
    """
    WhatsApp adapter.
    
    This implementation uses a simple HTTP bridge pattern where:
    1. A Node.js process runs the WhatsApp Web client
    2. Messages are forwarded via HTTP/IPC to this Python adapter
    3. Responses are sent back through the bridge
    
    The actual Node.js bridge implementation can vary:
    - whatsapp-web.js based
    - Baileys based
    - Business API based
    
    Configuration:
    - bridge_script: Path to the Node.js bridge script
    - bridge_port: Port for HTTP communication (default: 3000)
    - session_path: Path to store WhatsApp session data
    - dm_policy: "open" | "allowlist" | "disabled" — how DMs are handled (default: "open")
    - allow_from: List of sender IDs allowed in DMs (when dm_policy="allowlist")
    - group_policy: "open" | "allowlist" | "disabled" — which groups are processed (default: "open")
    - group_allow_from: List of group JIDs allowed (when group_policy="allowlist")
    """
    
    # WhatsApp message limits — practical UX limit, not protocol max.
    # WhatsApp allows ~65K but long messages are unreadable on mobile.
    MAX_MESSAGE_LENGTH = 4096
    DEFAULT_REPLY_PREFIX = "⚕ *Hermes Agent*\n────────────\n"
    
    # Default bridge location relative to the hermes-agent install
    _DEFAULT_BRIDGE_DIR = Path(__file__).resolve().parents[2] / "scripts" / "whatsapp-bridge-v2"

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.WHATSAPP)
        self._bridge_process: Optional[subprocess.Popen] = None
        self._bridge_port: int = config.extra.get("bridge_port", 3000)
        self._bridge_script: Optional[str] = config.extra.get(
            "bridge_script",
            str(self._DEFAULT_BRIDGE_DIR / "bridge.js"),
        )
        self._session_path: Path = Path(config.extra.get(
            "session_path",
            get_hermes_dir("platforms/whatsapp/session", "whatsapp/session")
        ))
        self._media_root: Path = Path(
            config.extra.get("media_root")
            or os.getenv("WHATSAPP_MEDIA_ROOT", "/home/pclaw/.systems-pcl/data/media")
        )
        self._reply_prefix: Optional[str] = config.extra.get("reply_prefix")
        self._dm_policy = str(config.extra.get("dm_policy") or os.getenv("WHATSAPP_DM_POLICY", "open")).strip().lower()
        self._allow_from = self._coerce_allow_list(config.extra.get("allow_from") or config.extra.get("allowFrom"))
        self._group_policy = str(config.extra.get("group_policy") or os.getenv("WHATSAPP_GROUP_POLICY", "open")).strip().lower()
        self._group_allow_from = self._coerce_allow_list(config.extra.get("group_allow_from") or config.extra.get("groupAllowFrom"))
        self._mention_patterns = self._compile_mention_patterns()
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._turn_debounce_ms: int = int(
            config.extra.get("turn_debounce_ms")
            or os.getenv("WHATSAPP_TURN_DEBOUNCE_MS", "1500")
            or 0
        )
        # Conditional-window defaults (used when neither brief nor config sets a
        # value). Addressed = short (respond promptly); passive = long (patient
        # batch while only observing). The passive value is the testing default;
        # the live value is a one-line config / brief flip.
        self._debounce_addressed_ms_default: int = 1500
        self._debounce_passive_ms_default: int = 10000
        # Optional resolver injected by the gateway: ``(chat_id) -> dict | None``
        # returning the chat's brief settings (require_mention / debounce
        # windows). ``None`` everywhere falls back to config / defaults so
        # today's behaviour is preserved. Results are cached per chat_id.
        self._pa_brief_resolver = config.extra.get("pa_brief_resolver")
        self._pa_brief_settings_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        self._turn_policy = self._config_json_object(
            "turn_policy",
            "WHATSAPP_TURN_POLICY_JSON",
        )
        self._ingest_chats = self._coerce_allow_list(
            config.extra.get("ingest_chats")
            or config.extra.get("capture_all_chats")
            or os.getenv("WHATSAPP_INGEST_CHATS")
            or os.getenv("WHATSAPP_CAPTURE_ALL_CHATS")
        )
        self._direct_mention_immediate = self._config_bool(
            "direct_mention_immediate",
            "WHATSAPP_DIRECT_MENTION_IMMEDIATE",
            False,
        )
        self._turn_buffers: Dict[str, List[MessageEvent]] = {}
        self._turn_tasks: Dict[str, asyncio.Task] = {}
        self._bridge_log_fh = None
        self._bridge_log: Optional[Path] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._http_session: Optional["aiohttp.ClientSession"] = None
        # Set to True by disconnect() before we SIGTERM our child bridge so
        # _check_managed_bridge_exit() can distinguish an intentional
        # shutdown-time exit (returncode -15 / -2 / 0) from a real crash.
        # Without this, every graceful gateway shutdown/restart would log
        # "Fatal whatsapp adapter error" plus dispatch a fatal-error
        # notification before the normal "✓ whatsapp disconnected" fires.
        self._shutting_down: bool = False

    def _config_extra(self) -> Dict[str, Any]:
        config = getattr(self, "config", None)
        extra = getattr(config, "extra", None)
        return extra if isinstance(extra, dict) else {}

    def _config_bool(self, key: str, env_key: str, default: bool) -> bool:
        configured = self._config_extra().get(key)
        if configured is None:
            configured = os.getenv(env_key)
        if configured is None:
            return default
        if isinstance(configured, str):
            return configured.strip().lower() in {"true", "1", "yes", "on"}
        return bool(configured)

    def _config_json_object(self, key: str, env_key: str) -> Dict[str, Any]:
        configured = self._config_extra().get(key)
        if configured is None:
            configured = os.getenv(env_key)
        if not configured:
            return {}
        if isinstance(configured, dict):
            return configured
        try:
            parsed = json.loads(str(configured))
        except Exception:
            logger.warning("[%s] Invalid %s JSON; ignoring", self.name, key)
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _effective_reply_prefix(self) -> str:
        """Return the prefix the Node bridge will add in self-chat mode."""
        whatsapp_mode = os.getenv("WHATSAPP_MODE", "self-chat")
        if whatsapp_mode != "self-chat":
            return ""
        if self._reply_prefix is not None:
            return self._reply_prefix.replace("\\n", "\n")
        env_prefix = os.getenv("WHATSAPP_REPLY_PREFIX")
        if env_prefix is not None:
            return env_prefix.replace("\\n", "\n")
        return self.DEFAULT_REPLY_PREFIX

    def _outgoing_chunk_limit(self) -> int:
        """Reserve room for the bridge-side prefix so final WhatsApp text fits."""
        prefix_len = len(self._effective_reply_prefix())
        # Keep enough space for truncate_message's pagination indicator and
        # code-fence repair even if a user configures a very long prefix.
        return max(1024, self.MAX_MESSAGE_LENGTH - prefix_len)

    def _brief_settings_for_chat(self, chat_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Resolve (and cache) the chat's brief settings, or None.

        Uses the gateway-injected ``pa_brief_resolver`` to fetch the per-chat
        brief gate / debounce settings. None when no resolver is wired, no brief
        matches, or resolution fails — callers then fall back to config /
        defaults. Never raises into the message path.
        """
        if not chat_id:
            return None
        resolver = getattr(self, "_pa_brief_resolver", None)
        if not callable(resolver):
            return None
        cache = self._pa_brief_settings_cache
        if chat_id in cache:
            return cache[chat_id]
        settings: Optional[Dict[str, Any]] = None
        try:
            result = resolver(chat_id)
            if isinstance(result, dict):
                settings = result
        except Exception as exc:
            logger.debug("[%s] WhatsApp brief resolve failed for %s: %s", self.name, chat_id, exc)
            settings = None
        cache[chat_id] = settings
        return settings

    def _whatsapp_require_mention(self, chat_id: Optional[str] = None) -> bool:
        # Brief wins when it sets require_mention (not None); else fall back to
        # config.extra, then env — today's behaviour.
        brief = self._brief_settings_for_chat(chat_id)
        if brief is not None:
            brief_value = brief.get("require_mention")
            if brief_value is not None:
                if isinstance(brief_value, str):
                    return brief_value.lower() in {"true", "1", "yes", "on"}
                return bool(brief_value)
        configured = self.config.extra.get("require_mention")
        if configured is not None:
            if isinstance(configured, str):
                return configured.lower() in {"true", "1", "yes", "on"}
            return bool(configured)
        return os.getenv("WHATSAPP_REQUIRE_MENTION", "false").lower() in {"true", "1", "yes", "on"}

    def _whatsapp_free_response_chats(self) -> set[str]:
        raw = self.config.extra.get("free_response_chats")
        if raw is None:
            raw = os.getenv("WHATSAPP_FREE_RESPONSE_CHATS", "")
        if isinstance(raw, list):
            return {str(part).strip() for part in raw if str(part).strip()}
        return {part.strip() for part in str(raw).split(",") if part.strip()}

    @staticmethod
    def _coerce_allow_list(raw) -> set[str]:
        """Parse allow_from / group_allow_from from config or env var."""
        if raw is None:
            return set()
        if isinstance(raw, list):
            return {str(part).strip() for part in raw if str(part).strip()}
        return {part.strip() for part in str(raw).split(",") if part.strip()}

    def _resolve_outbound_media_path(self, file_path: str) -> str:
        """Resolve portal-style media aliases before sending native WhatsApp media."""
        raw_path = os.path.expanduser(str(file_path or "").strip())
        if not raw_path or os.path.exists(raw_path):
            return raw_path

        rel_path: Optional[str] = None
        if raw_path.startswith("/media/"):
            rel_path = raw_path.removeprefix("/media/")
        elif raw_path.startswith("media/"):
            rel_path = raw_path.removeprefix("media/")

        if not rel_path:
            return raw_path

        media_root = getattr(
            self,
            "_media_root",
            Path(os.getenv("WHATSAPP_MEDIA_ROOT", "/home/pclaw/.systems-pcl/data/media")),
        )
        resolved = Path(media_root) / rel_path
        if resolved.exists():
            return str(resolved)
        return raw_path

    @staticmethod
    def _is_broadcast_chat(chat_id: str) -> bool:
        """True for WhatsApp pseudo-chats that aren't real conversations.

        Covers Status updates (Stories) and Channel/Newsletter broadcasts.
        These show up as inbound messages on Baileys but the agent should
        never reply — answering a Story update spams the contact's status
        feed, and Channel posts aren't addressable in the first place.
        """
        if not chat_id:
            return False
        cid = chat_id.strip().lower()
        if cid == "status@broadcast":
            return True
        # @broadcast suffix covers status@broadcast plus any future
        # broadcast-list variants. @newsletter is the Channel JID suffix.
        if cid.endswith("@broadcast") or cid.endswith("@newsletter"):
            return True
        return False

    def _is_dm_allowed(self, sender_id: str) -> bool:
        """Check whether a DM from the given sender should be processed."""
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "allowlist":
            return sender_id in self._allow_from
        # "open" — all DMs allowed
        return True

    def _is_group_allowed(self, chat_id: str) -> bool:
        """Check whether a group chat should be processed."""
        if self._group_policy == "disabled":
            return False
        if self._group_policy == "allowlist":
            return chat_id in self._group_allow_from
        # "open" — all groups allowed
        return True

    def _compile_mention_patterns(self):
        patterns = self.config.extra.get("mention_patterns")
        if patterns is None:
            raw = os.getenv("WHATSAPP_MENTION_PATTERNS", "").strip()
            if raw:
                try:
                    patterns = json.loads(raw)
                except Exception:
                    patterns = [part.strip() for part in raw.splitlines() if part.strip()]
                    if not patterns:
                        patterns = [part.strip() for part in raw.split(",") if part.strip()]
        if patterns is None:
            return []
        if isinstance(patterns, str):
            patterns = [patterns]
        if not isinstance(patterns, list):
            logger.warning("[%s] whatsapp mention_patterns must be a list or string; got %s", self.name, type(patterns).__name__)
            return []

        compiled = []
        for pattern in patterns:
            if not isinstance(pattern, str) or not pattern.strip():
                continue
            try:
                compiled.append(re.compile(pattern, re.IGNORECASE))
            except re.error as exc:
                logger.warning("[%s] Invalid WhatsApp mention pattern %r: %s", self.name, pattern, exc)
        if compiled:
            logger.info("[%s] Loaded %d WhatsApp mention pattern(s)", self.name, len(compiled))
        return compiled

    @staticmethod
    def _normalize_whatsapp_id(value: Optional[str]) -> str:
        if not value:
            return ""
        normalized = str(value).strip()
        if ":" in normalized and "@" in normalized:
            normalized = normalized.replace(":", "@", 1)
        return normalized

    def _bot_ids_from_message(self, data: Dict[str, Any]) -> set[str]:
        bot_ids = set()
        for candidate in data.get("botIds") or []:
            normalized = self._normalize_whatsapp_id(candidate)
            if normalized:
                bot_ids.add(normalized)
        return bot_ids

    def _message_is_reply_to_bot(self, data: Dict[str, Any]) -> bool:
        if data.get("quotedFromBot"):
            return True
        quoted_participant = self._normalize_whatsapp_id(data.get("quotedParticipant"))
        if not quoted_participant:
            return False
        return quoted_participant in self._bot_ids_from_message(data)

    def _message_mentions_bot(self, data: Dict[str, Any]) -> bool:
        bot_ids = self._bot_ids_from_message(data)
        if not bot_ids:
            return False
        mentioned_ids = {
            nid
            for candidate in (data.get("mentionedIds") or [])
            if (nid := self._normalize_whatsapp_id(candidate))
        }
        if mentioned_ids & bot_ids:
            return True

        body = str(data.get("body") or "")
        lower_body = body.lower()
        for bot_id in bot_ids:
            bare_id = bot_id.split("@", 1)[0].lower()
            if bare_id and (f"@{bare_id}" in lower_body or bare_id in lower_body):
                return True
        return False

    def _message_matches_mention_patterns(self, data: Dict[str, Any]) -> bool:
        if not self._mention_patterns:
            return False
        body = str(data.get("body") or "")
        return any(pattern.search(body) for pattern in self._mention_patterns)

    def _clean_bot_mention_text(self, text: str, data: Dict[str, Any]) -> str:
        if not text:
            return text
        bot_ids = self._bot_ids_from_message(data)
        cleaned = text
        for bot_id in bot_ids:
            bare_id = bot_id.split("@", 1)[0]
            if bare_id:
                cleaned = re.sub(rf"@{re.escape(bare_id)}\b[,:\-]*\s*", "", cleaned)
        return cleaned.strip() or text

    @staticmethod
    def _extract_tgg_refs(text: str) -> list[str]:
        if not text:
            return []
        refs = re.findall(
            r"\b(?:[A-Z]{2}/(?:JOB|WC)/\d{4}/\d{4}|WA-ONLY-[A-Z0-9]+)\b",
            text,
            flags=re.IGNORECASE,
        )
        seen: set[str] = set()
        out: list[str] = []
        for ref in refs:
            normalized = ref.upper()
            if normalized not in seen:
                seen.add(normalized)
                out.append(normalized)
        return out

    def _reply_context_text(self, data: Dict[str, Any]) -> str | None:
        quoted_text = str(data.get("quotedText") or "").strip()
        if not quoted_text:
            return None
        refs = self._extract_tgg_refs(quoted_text)
        if refs:
            return f"Quoted WhatsApp message case refs: {', '.join(refs)}\n{quoted_text}"
        return quoted_text

    def _turn_policy_for_chat(self, chat_id: str) -> Dict[str, Any]:
        raw_policy = getattr(self, "_turn_policy", {}) or {}
        if not isinstance(raw_policy, dict):
            return {}
        candidates = [
            chat_id,
            str(chat_id or "").split("@", 1)[0],
            "*",
        ]
        for candidate in candidates:
            value = raw_policy.get(candidate)
            if isinstance(value, dict):
                return value
        return {}

    def _chat_processes_without_mention(self, chat_id: str) -> bool:
        if chat_id in getattr(self, "_ingest_chats", set()):
            return True
        policy = self._turn_policy_for_chat(chat_id)
        for key in ("process_all", "ingest", "capture_all"):
            if key in policy:
                value = policy.get(key)
                if isinstance(value, str):
                    return value.strip().lower() in {"true", "1", "yes", "on"}
                return bool(value)
        return False

    def _message_is_direct_trigger(self, data: Dict[str, Any]) -> bool:
        body = str(data.get("body") or "").strip()
        if body.startswith("/"):
            return True
        if self._message_is_reply_to_bot(data):
            return True
        if self._message_mentions_bot(data):
            return True
        return self._message_matches_mention_patterns(data)

    def _should_process_message(self, data: Dict[str, Any], *, bypass_require_mention: bool = False) -> bool:
        chat_id_raw = str(data.get("chatId") or "")
        # WhatsApp uses pseudo-chats for Status updates (Stories) and
        # Channel/Newsletter broadcasts. These are not real conversations
        # and the agent should never reply to them — even in self-chat mode
        # where the bridge may surface them as "fromMe" events.
        if self._is_broadcast_chat(chat_id_raw):
            return False
        is_group = data.get("isGroup", False)
        if is_group:
            chat_id = chat_id_raw
            if not self._is_group_allowed(chat_id):
                return False
        else:
            sender_id = str(data.get("senderId") or data.get("from") or "")
            if not self._is_dm_allowed(sender_id):
                return False
            # DMs that pass the policy gate are always processed
            return True
        # Group messages: check mention / free-response settings
        chat_id = str(data.get("chatId") or "")
        if chat_id in self._whatsapp_free_response_chats():
            return True
        if bypass_require_mention or self._chat_processes_without_mention(chat_id):
            return True
        if not self._whatsapp_require_mention(chat_id):
            return True
        return self._message_is_direct_trigger(data)

    @staticmethod
    def _bridge_chat_type(data: Dict[str, Any]) -> str:
        return "group" if data.get("isGroup", False) else "dm"

    @staticmethod
    def _is_numeric_chat_label(chat_id: str, chat_name: Optional[str]) -> bool:
        if not chat_name:
            return True
        label = str(chat_name).strip()
        if not label:
            return True
        numeric_part = str(chat_id or "").split("@", 1)[0]
        return label == numeric_part or bool(re.fullmatch(r"\d{8,}", label))

    def _persist_channel_directory_entry(self, data: Dict[str, Any]) -> None:
        """Persist bridge-supplied chat metadata immediately for target listing."""
        chat_id = str(data.get("chatId") or "")
        if not chat_id:
            return
        chat_name = data.get("chatName")
        # Avoid overwriting a good directory name with the old numeric fallback.
        if self._bridge_chat_type(data) == "group" and self._is_numeric_chat_label(chat_id, chat_name):
            return
        try:
            from gateway.channel_directory import upsert_channel_entry
            upsert_channel_entry("whatsapp", {
                "id": chat_id,
                "name": chat_name or chat_id,
                "type": self._bridge_chat_type(data),
            })
        except Exception as e:
            logger.debug("[%s] Failed to persist WhatsApp channel metadata: %s", self.name, e)

    def _coerce_seconds(self, value: Any, *, default: Optional[float] = None) -> Optional[float]:
        if value is None:
            return default
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        return max(0.0, parsed)

    def _debounce_seconds(self) -> float:
        raw_ms = getattr(self, "_turn_debounce_ms", 0) or 0
        try:
            ms = int(raw_ms)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, ms / 1000.0)

    @staticmethod
    def _coerce_window_ms(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            ms = int(value)
        except (TypeError, ValueError):
            return None
        return ms if ms >= 0 else None

    def _debounce_window_ms(self, chat_id: Optional[str], addressed: bool) -> int:
        """Resolve the debounce window (ms) for a burst in this chat.

        addressed -> short (addressed) window; otherwise -> long (passive)
        window. Resolution per window: brief value (if set) -> config.extra ->
        env -> universal default. Preserves today's single-window behaviour when
        a brief/config sets nothing (passive default = testing 10s; addressed
        default = 1500ms).
        """
        brief = self._brief_settings_for_chat(chat_id)
        if addressed:
            ms = self._coerce_window_ms(brief.get("debounce_addressed_ms")) if brief else None
            if ms is None:
                ms = self._coerce_window_ms(self._config_extra().get("debounce_addressed_ms"))
            if ms is None:
                ms = self._coerce_window_ms(os.getenv("WHATSAPP_DEBOUNCE_ADDRESSED_MS"))
            if ms is None:
                ms = getattr(self, "_debounce_addressed_ms_default", int(getattr(self, "_turn_debounce_ms", 0) or 0))
        else:
            ms = self._coerce_window_ms(brief.get("debounce_passive_ms")) if brief else None
            if ms is None:
                ms = self._coerce_window_ms(self._config_extra().get("debounce_passive_ms"))
            if ms is None:
                ms = self._coerce_window_ms(os.getenv("WHATSAPP_DEBOUNCE_PASSIVE_MS"))
            if ms is None:
                ms = getattr(self, "_debounce_passive_ms_default", int(getattr(self, "_turn_debounce_ms", 0) or 0))
        return ms

    def _debounce_disabled(self) -> bool:
        """True only when debounce is explicitly turned off.

        Off = the single-window switch ``turn_debounce_ms`` is explicitly 0 AND
        no conditional-window override is configured. Conditional windows
        (brief or config) keep debounce on regardless of the legacy switch.
        """
        if self._debounce_seconds() > 0:
            return False
        # Legacy switch is 0; conditional windows can still enable debounce.
        if self._config_extra().get("debounce_passive_ms") is not None:
            return False
        if self._config_extra().get("debounce_addressed_ms") is not None:
            return False
        if os.getenv("WHATSAPP_DEBOUNCE_PASSIVE_MS") is not None:
            return False
        if os.getenv("WHATSAPP_DEBOUNCE_ADDRESSED_MS") is not None:
            return False
        return True

    def _debounce_seconds_for_event(self, event: MessageEvent) -> float:
        source = event.source
        chat_id = source.chat_id if source else ""
        policy = self._turn_policy_for_chat(chat_id)
        if "debounce_seconds" in policy:
            return self._coerce_seconds(policy.get("debounce_seconds"), default=self._debounce_seconds()) or 0.0
        if "quiet_seconds" in policy:
            return self._coerce_seconds(policy.get("quiet_seconds"), default=self._debounce_seconds()) or 0.0
        if "debounce_ms" in policy:
            ms = self._coerce_seconds(policy.get("debounce_ms"), default=None)
            if ms is not None:
                return max(0.0, ms / 1000.0)
        return self._debounce_seconds()

    def _direct_mention_immediate_for_event(self, event: MessageEvent) -> bool:
        source = event.source
        chat_id = source.chat_id if source else ""
        policy = self._turn_policy_for_chat(chat_id)
        if "direct_mention_immediate" in policy:
            value = policy.get("direct_mention_immediate")
            if isinstance(value, str):
                return value.strip().lower() in {"true", "1", "yes", "on"}
            return bool(value)
        return bool(getattr(self, "_direct_mention_immediate", False))

    def _event_epoch_seconds(self, event: MessageEvent) -> Optional[float]:
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        timestamp = raw.get("timestamp")
        if isinstance(timestamp, dict):
            timestamp = timestamp.get("low") or timestamp.get("value")
        try:
            return float(timestamp)
        except (TypeError, ValueError):
            return None

    def _should_flush_turn_immediately(self, event: MessageEvent) -> bool:
        if not self._direct_mention_immediate_for_event(event):
            return False
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        if raw.get("bundle"):
            messages = raw.get("messages") or []
            return any(isinstance(message, dict) and self._message_is_direct_trigger(message) for message in messages)
        return self._message_is_direct_trigger(raw)

    async def _queue_or_handle_event(self, event: MessageEvent) -> None:
        """Debounce WhatsApp events into one turn per chat window.

        Window resolution priority:
        1. turn_policy (per-chat, from _debounce_seconds_for_event / recon path)
        2. brief-based addressed/passive windows (_debounce_window_ms / main PA Phase 1 path)
        3. Global turn_debounce_ms default

        When neither turn_policy nor brief sets a window, falls through to _debounce_disabled.
        """
        source = event.source
        chat_id = source.chat_id if source else ""

        # Check debounce via recon's per-event path (turn_policy).
        debounce_s = self._debounce_seconds_for_event(event)
        if debounce_s <= 0:
            # No turn_policy debounce; also check the legacy switch.
            if self._debounce_disabled():
                await self.handle_message(event)
                return

        if not chat_id:
            await self.handle_message(event)
            return

        buffers = getattr(self, "_turn_buffers", None)
        tasks = getattr(self, "_turn_tasks", None)
        if buffers is None or tasks is None:
            self._turn_buffers = {}
            self._turn_tasks = {}
            buffers = self._turn_buffers
            tasks = self._turn_tasks

        buffers.setdefault(chat_id, []).append(event)
        # Addressed over the WHOLE buffered burst: a single mention/reply pulls
        # the chat out of passive patience. Used by the brief-based window resolution.
        burst_addressed = any(
            getattr(buffered, "addressed", False) for buffered in buffers[chat_id]
        )
        # Main PA Phase 1: brief-based addressed/passive window (_debounce_window_ms).
        # This reads brief settings (debounce_addressed_ms / debounce_passive_ms) and
        # overrides the turn_policy debounce when the brief provides a window.
        window_ms = self._debounce_window_ms(chat_id, burst_addressed)
        window_s = max(0.0, window_ms / 1000.0)
        # Use the addressed/passive window when it's meaningful; otherwise fall back
        # to the per-event turn_policy debounce.
        effective_s = window_s if window_s > 0 else debounce_s

        existing = tasks.get(chat_id)
        if existing and not existing.done():
            # Cancel + reschedule. When a new addressed event collapses a
            # passive burst, this reschedules on the (shorter) addressed window.
            existing.cancel()
        if self._should_flush_turn_immediately(event):
            tasks.pop(chat_id, None)
            await self._flush_turn(chat_id)
            return
        if effective_s <= 0:
            await self._flush_turn(chat_id)
            return
        tasks[chat_id] = asyncio.create_task(self._flush_turn_after(chat_id, effective_s))

    # Replay-only hard cap on messages per simulated turn bundle. Without it,
    # a busy chat whose consecutive messages each land inside the debounce
    # window accumulates into one mega-bundle spanning hours of real time
    # (observed: a 22-message bundle covering ~2h) — a shape no live turn
    # would ever see. Spillover starts the NEXT turn.
    # Live has NO message cap (trailing-quiet debounce only), so the cap
    # itself is a replay/live divergence: it splits bundles live would never
    # split. Evals that must judge live grouping semantics (e.g. a long
    # passive window bundling an album with its late caption) override via
    # config extra ``replay_bundle_message_cap`` (0 = uncapped); default
    # stays 10 for ordinary replay hygiene.
    REPLAY_BUNDLE_MESSAGE_CAP = 10

    def _replay_bundle_cap(self) -> int:
        raw = self._config_extra().get("replay_bundle_message_cap")
        if raw is None:
            return self.REPLAY_BUNDLE_MESSAGE_CAP
        try:
            cap = int(raw)
        except (TypeError, ValueError):
            return self.REPLAY_BUNDLE_MESSAGE_CAP
        return max(0, cap)

    def _explicit_debounce_window_ms(self, chat_id: Optional[str], addressed: bool) -> Optional[int]:
        """Like ``_debounce_window_ms`` but WITHOUT the universal-default
        fallback: returns a window only when the brief, config extra, or env
        explicitly sets one. None = nothing explicitly configured."""
        brief = self._brief_settings_for_chat(chat_id)
        key = "debounce_addressed_ms" if addressed else "debounce_passive_ms"
        env_key = "WHATSAPP_DEBOUNCE_ADDRESSED_MS" if addressed else "WHATSAPP_DEBOUNCE_PASSIVE_MS"
        ms = self._coerce_window_ms(brief.get(key)) if brief else None
        if ms is None:
            ms = self._coerce_window_ms(self._config_extra().get(key))
        if ms is None:
            ms = self._coerce_window_ms(os.getenv(env_key))
        return ms

    def _replay_debounce_seconds(self, chat_id: str, event: MessageEvent) -> float:
        """Resolve the replay gap-split window like live resolves its turn window.

        Explicit per-chat ``turn_policy`` wins (replay test rigs set it);
        otherwise an EXPLICIT brief/config/env addressed-passive window — the
        window live turns actually run on (``_debounce_window_ms``) — governs
        the timestamp-gap split; the legacy ``turn_debounce_ms`` switch comes
        last. Without the brief-window layer, replay grouped at the 1.5s
        legacy default while live grouped at the constitution's
        ``debounce_passive_ms``, so evals judged bundle shapes live would
        never produce (2026-07-20: 76-msg TGG corpus replayed as 38 turns —
        exactly the 1.5s split — under a 10s constitution window). Known
        residual divergence: when NOTHING is explicitly configured, replay
        keeps the legacy 1.5s fallback while live falls to the universal 10s
        passive default — preserved so existing turn_debounce_ms replay rigs
        keep their golden groupings.
        """
        policy = self._turn_policy_for_chat(chat_id) if chat_id else {}
        if any(key in policy for key in ("debounce_seconds", "quiet_seconds", "debounce_ms")):
            return self._debounce_seconds_for_event(event)
        burst = (getattr(self, "_turn_buffers", None) or {}).get(chat_id) or []
        if burst:
            burst_addressed = any(getattr(buffered, "addressed", False) for buffered in burst)
        else:
            burst_addressed = bool(getattr(event, "addressed", False))
        window_ms = self._explicit_debounce_window_ms(chat_id, burst_addressed)
        if window_ms is not None and window_ms > 0:
            return window_ms / 1000.0
        return self._debounce_seconds_for_event(event)

    async def _queue_or_handle_replay_event(self, event: MessageEvent) -> None:
        """Apply WhatsApp turn policy using message timestamps, not wall-clock sleeps.

        Turn bundles split on ORIGINAL message timestamp gaps (gap >=
        the resolved turn window => new turn) and are hard-capped at
        ``_replay_bundle_cap()`` messages (spillover => next turn; 0 = uncapped).
        """
        source = event.source
        chat_id = source.chat_id if source else ""
        debounce_s = self._replay_debounce_seconds(chat_id, event)
        if debounce_s <= 0:
            await self.handle_message(event)
            return

        if not chat_id:
            await self.handle_message(event)
            return

        buffers = getattr(self, "_turn_buffers", None)
        if buffers is None:
            self._turn_buffers = {}
            buffers = self._turn_buffers

        current = buffers.get(chat_id) or []
        if current:
            last_ts = self._event_epoch_seconds(current[-1])
            this_ts = self._event_epoch_seconds(event)
            if last_ts is not None and this_ts is not None and this_ts - last_ts >= debounce_s:
                await self._flush_turn(chat_id)
                current = []

        buffers.setdefault(chat_id, []).append(event)
        cap = self._replay_bundle_cap()
        if cap > 0 and len(buffers.get(chat_id) or []) >= cap:
            await self._flush_turn(chat_id)
            return
        if self._should_flush_turn_immediately(event):
            await self._flush_turn(chat_id)

    async def _flush_turn_after(self, chat_id: str, debounce_s: float) -> None:
        try:
            await asyncio.sleep(debounce_s)
            await self._flush_turn(chat_id)
        except asyncio.CancelledError:
            return

    async def _flush_turn(self, chat_id: str) -> None:
        buffers = getattr(self, "_turn_buffers", {})
        tasks = getattr(self, "_turn_tasks", {})
        events = buffers.pop(chat_id, [])
        tasks.pop(chat_id, None)
        if not events:
            return
        bundled = self._build_turn_event(events)
        await self.handle_message(bundled)

    def _build_turn_event(self, events: List[MessageEvent]) -> MessageEvent:
        if len(events) == 1:
            return events[0]

        first = events[0]
        source = first.source
        lines = [
            f"WhatsApp turn bundle ({len(events)} messages)",
        ]
        if source:
            chat_label = source.chat_name or source.chat_id
            lines.append(f"Chat: {chat_label} ({source.chat_id})")
        lines.append("")

        media_urls: List[str] = []
        media_types: List[str] = []
        raw_messages: List[Any] = []
        ids: List[str] = []
        seen_quote_keys: set[str] = set()
        for index, event in enumerate(events, start=1):
            raw = event.raw_message or {}
            raw_messages.append(raw)
            if event.message_id:
                ids.append(str(event.message_id))
            sender = source.user_name if index == 1 and source else None
            if event.source:
                sender = event.source.user_name or event.source.user_id or sender
            message_id = event.message_id or raw.get("messageId") or f"message-{index}"
            timestamp = raw.get("_tgg_sgt") or raw.get("sgt") or raw.get("timestamp")
            prefix = f"{index}. "
            if timestamp:
                prefix += f"[{timestamp}] "
            if sender:
                prefix += f"{sender}: "
            text = event.text or ""
            if event.media_urls and not text:
                text = "[media received]"
            lines.append(f"{prefix}{text}".rstrip())
            # Media/album messages frequently carry the load-bearing context in
            # their quote (e.g. a photo album replying to an HDB job sheet).
            # The single-message path injects this via reply_to_text in the
            # gateway; the bundle path must render it here or the quote never
            # reaches the model. A quote shared by several messages in one
            # album is rendered once per bundle, not once per image.
            quoted_context = (
                self._reply_context_text(raw) if isinstance(raw, dict) else None
            )
            if quoted_context:
                quote_key = str(
                    raw.get("quotedMessageId") or quoted_context.strip()
                )
                if quote_key not in seen_quote_keys:
                    seen_quote_keys.add(quote_key)
                    lines.append(f'   [Replying to: "{quoted_context[:500]}"]')
            if event.media_urls:
                lines.append(f"   attachments: {', '.join(event.media_urls)}")
            lines.append(f"   source_message_id: {message_id}")
            media_urls.extend(event.media_urls or [])
            media_types.extend(event.media_types or [])

        message_type = MessageType.TEXT
        if media_urls:
            non_text_types = [event.message_type for event in events if event.media_urls]
            if non_text_types and len(set(non_text_types)) == 1:
                message_type = non_text_types[0]
            else:
                message_type = MessageType.DOCUMENT

        return MessageEvent(
            text="\n".join(lines),
            message_type=message_type,
            source=source,
            raw_message={
                "bundle": True,
                "messages": raw_messages,
                "sourceMessageIds": ids,
            },
            message_id="+".join(ids) if ids else first.message_id,
            media_urls=media_urls,
            media_types=media_types,
            # Bundle is addressed if any constituent event addressed the bot —
            # the synthetic bundle raw_message loses per-event mention/reply
            # detection, so carry the flag through explicitly.
            addressed=any(getattr(event, "addressed", False) for event in events),
        )
    
    async def connect(self) -> bool:
        """
        Start the WhatsApp bridge.
        
        This launches the Node.js bridge process and waits for it to be ready.
        """
        if not check_whatsapp_requirements():
            logger.warning("[%s] Node.js not found. WhatsApp requires Node.js.", self.name)
            self._set_fatal_error(
                "whatsapp_node_missing",
                "Node.js is not installed — install Node.js and re-run `hermes gateway`.",
                retryable=False,
            )
            return False
        
        bridge_path = Path(self._bridge_script)
        if not bridge_path.exists():
            logger.warning("[%s] Bridge script not found: %s", self.name, bridge_path)
            self._set_fatal_error(
                "whatsapp_bridge_missing",
                f"WhatsApp bridge script missing at {bridge_path}.",
                retryable=False,
            )
            return False

        # Pre-flight: skip the 30s bridge bootstrap entirely if the user
        # never finished pairing.  Without creds.json the bridge prints
        # QR codes to its log file and never reaches status:connected,
        # so every gateway restart paid the 30s timeout + queued WhatsApp
        # for indefinite retries.  Mark non-retryable so the user gets a
        # clear "run hermes whatsapp" message instead of the watcher
        # silently hammering an unconfigured platform.
        creds_path = self._session_path / "creds.json"
        if not creds_path.exists():
            logger.warning(
                "[%s] WhatsApp is enabled but not paired (no creds.json at %s). "
                "Run `hermes whatsapp` to pair, or remove WHATSAPP_ENABLED from "
                "your .env to disable.",
                self.name, creds_path,
            )
            self._set_fatal_error(
                "whatsapp_not_paired",
                "WhatsApp enabled but not paired — run `hermes whatsapp` to pair.",
                retryable=False,
            )
            return False

        logger.info("[%s] Bridge found at %s", self.name, bridge_path)
        
        # Acquire scoped lock to prevent duplicate sessions
        lock_acquired = False
        try:
            if not self._acquire_platform_lock('whatsapp-session', str(self._session_path), 'WhatsApp session'):
                return False
            lock_acquired = True
        except Exception as e:
            logger.warning("[%s] Could not acquire session lock (non-fatal): %s", self.name, e)

        try:
            # Auto-install npm dependencies if node_modules doesn't exist
            bridge_dir = bridge_path.parent
            if not (bridge_dir / "node_modules").exists():
                print(f"[{self.name}] Installing WhatsApp bridge dependencies...")
                # Resolve npm path so Windows can execute the .cmd shim.
                # shutil.which honours PATHEXT; on POSIX it returns the
                # plain executable path.
                _npm_bin = shutil.which("npm") or "npm"
                try:
                    # Read timeout from environment variable, default to 300 seconds (5 minutes)
                    # to accommodate slower systems like Unraid NAS
                    npm_install_timeout = int(os.environ.get("WHATSAPP_NPM_INSTALL_TIMEOUT", "300"))
                    install_result = subprocess.run(
                        [_npm_bin, "install", "--silent"],
                        cwd=str(bridge_dir),
                        capture_output=True,
                        text=True,
                        timeout=npm_install_timeout,
                    )
                    if install_result.returncode != 0:
                        print(f"[{self.name}] npm install failed: {install_result.stderr}")
                        return False
                    print(f"[{self.name}] Dependencies installed")
                except Exception as e:
                    print(f"[{self.name}] Failed to install dependencies: {e}")
                    return False

            # Ensure session directory exists
            self._session_path.mkdir(parents=True, exist_ok=True)
            
            # Check if bridge is already running and connected
            import aiohttp
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"http://127.0.0.1:{self._bridge_port}/health",
                        timeout=aiohttp.ClientTimeout(total=2)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            bridge_status = data.get("status", "unknown")
                            if bridge_status == "connected":
                                print(f"[{self.name}] Using existing bridge (status: {bridge_status})")
                                self._mark_connected()
                                self._bridge_process = None  # Not managed by us
                                self._http_session = aiohttp.ClientSession()
                                self._poll_task = asyncio.create_task(self._poll_messages())
                                return True
                            else:
                                print(f"[{self.name}] Bridge found but not connected (status: {bridge_status}), restarting")
            except Exception:
                pass  # Bridge not running, start a new one
            
            # Kill any orphaned bridge from a previous gateway run
            _kill_stale_bridge_by_pidfile(self._session_path)
            _kill_port_process(self._bridge_port)
            await asyncio.sleep(1)
            
            # Start the bridge process in its own process group.
            # Route output to a log file so QR codes, errors, and reconnection
            # messages are preserved for troubleshooting.
            whatsapp_mode = os.getenv("WHATSAPP_MODE", "self-chat")
            self._bridge_log = self._session_path.parent / "bridge.log"
            bridge_log_fh = open(self._bridge_log, "a", encoding="utf-8")
            self._bridge_log_fh = bridge_log_fh

            # Build bridge subprocess environment.
            # Pass WHATSAPP_REPLY_PREFIX from config.yaml so the Node bridge
            # can use it without the user needing to set a separate env var.
            bridge_env = os.environ.copy()
            if self._reply_prefix is not None:
                bridge_env["WHATSAPP_REPLY_PREFIX"] = self._reply_prefix

            self._bridge_process = subprocess.Popen(
                [
                    "node",
                    str(bridge_path),
                    "--port", str(self._bridge_port),
                    "--session", str(self._session_path),
                    "--mode", whatsapp_mode,
                ],
                stdout=bridge_log_fh,
                stderr=bridge_log_fh,
                preexec_fn=None if _IS_WINDOWS else os.setsid,
                env=bridge_env,
            )
            _write_bridge_pidfile(self._session_path, self._bridge_process.pid)
            
            # Wait for the bridge to connect to WhatsApp.
            # Phase 1: wait for the HTTP server to come up (up to 15s).
            # Phase 2: wait for WhatsApp status: connected (up to 15s more).
            import aiohttp
            http_ready = False
            data = {}
            for attempt in range(15):
                await asyncio.sleep(1)
                if self._bridge_process.poll() is not None:
                    print(f"[{self.name}] Bridge process died (exit code {self._bridge_process.returncode})")
                    print(f"[{self.name}] Check log: {self._bridge_log}")
                    self._close_bridge_log()
                    return False
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"http://127.0.0.1:{self._bridge_port}/health",
                            timeout=aiohttp.ClientTimeout(total=2)
                        ) as resp:
                            if resp.status == 200:
                                http_ready = True
                                data = await resp.json()
                                if data.get("status") == "connected":
                                    print(f"[{self.name}] Bridge ready (status: connected)")
                                    break
                except Exception:
                    continue

            if not http_ready:
                print(f"[{self.name}] Bridge HTTP server did not start in 15s")
                print(f"[{self.name}] Check log: {self._bridge_log}")
                self._close_bridge_log()
                return False
            
            # Phase 2: HTTP is up but WhatsApp may still be connecting.
            # Give it more time to authenticate with saved credentials.
            if data.get("status") != "connected":
                print(f"[{self.name}] Bridge HTTP ready, waiting for WhatsApp connection...")
                for attempt in range(15):
                    await asyncio.sleep(1)
                    if self._bridge_process.poll() is not None:
                        print(f"[{self.name}] Bridge process died during connection")
                        print(f"[{self.name}] Check log: {self._bridge_log}")
                        self._close_bridge_log()
                        return False
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(
                                f"http://127.0.0.1:{self._bridge_port}/health",
                                timeout=aiohttp.ClientTimeout(total=2)
                            ) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    if data.get("status") == "connected":
                                        print(f"[{self.name}] Bridge ready (status: connected)")
                                        break
                    except Exception:
                        continue
                else:
                    # Still not connected — warn but proceed (bridge may
                    # auto-reconnect later, e.g. after a code 515 restart).
                    print(f"[{self.name}] ⚠ WhatsApp not connected after 30s")
                    print(f"[{self.name}]   Bridge log: {self._bridge_log}")
                    print(f"[{self.name}]   If session expired, re-pair: hermes whatsapp")
            
            # Create a persistent HTTP session for all bridge communication
            self._http_session = aiohttp.ClientSession()

            # Start message polling task
            self._poll_task = asyncio.create_task(self._poll_messages())
            
            self._mark_connected()
            print(f"[{self.name}] Bridge started on port {self._bridge_port}")
            return True
            
        except Exception as e:
            logger.error("[%s] Failed to start bridge: %s", self.name, e, exc_info=True)
            return False
        finally:
            if not self._running:
                if lock_acquired:
                    self._release_platform_lock()
                self._close_bridge_log()
    
    def _close_bridge_log(self) -> None:
        """Close the bridge log file handle if open."""
        if self._bridge_log_fh:
            try:
                self._bridge_log_fh.close()
            except Exception:
                pass
            self._bridge_log_fh = None

    async def _check_managed_bridge_exit(self) -> Optional[str]:
        """Return a fatal error message if the managed bridge child exited."""
        if self._bridge_process is None:
            return None

        returncode = self._bridge_process.poll()
        if returncode is None:
            return None

        # Planned shutdown: disconnect() sets _shutting_down before it sends
        # SIGTERM to the bridge, so a returncode of -15 (SIGTERM), -2 (SIGINT),
        # or 0 (clean exit) at that point is expected, not a crash. Treat it
        # as informational and skip the fatal-error path.
        # getattr-with-default keeps tests that construct the adapter via
        # ``WhatsAppAdapter.__new__`` (bypassing __init__) working without
        # every _make_adapter() helper having to seed the attribute.
        if getattr(self, "_shutting_down", False) and returncode in {0, -2, -15}:
            logger.info(
                "[%s] Bridge exited during shutdown (code %d).",
                self.name,
                returncode,
            )
            return None

        message = f"WhatsApp bridge process exited unexpectedly (code {returncode})."
        if not self.has_fatal_error:
            logger.error("[%s] %s", self.name, message)
            self._set_fatal_error("whatsapp_bridge_exited", message, retryable=True)
            self._close_bridge_log()
            await self._notify_fatal_error()
        return self.fatal_error_message or message

    async def disconnect(self) -> None:
        """Stop the WhatsApp bridge and clean up any orphaned processes."""
        # Flip the shutdown flag BEFORE signalling the child so the exit-check
        # path (which runs from other tasks like send() and the poll loop)
        # doesn't race us and report the intentional termination as fatal.
        self._shutting_down = True
        if self._bridge_process:
            try:
                try:
                    _terminate_bridge_process(self._bridge_process, force=False)
                except (ProcessLookupError, PermissionError):
                    self._bridge_process.terminate()
                await asyncio.sleep(1)
                if self._bridge_process.poll() is None:
                    try:
                        _terminate_bridge_process(self._bridge_process, force=True)
                    except (ProcessLookupError, PermissionError):
                        self._bridge_process.kill()
            except Exception as e:
                print(f"[{self.name}] Error stopping bridge: {e}")
        else:
            # Bridge was not started by us, don't kill it
            print(f"[{self.name}] Disconnecting (external bridge left running)")

        # Clean up PID file
        try:
            (self._session_path / "bridge.pid").unlink(missing_ok=True)
        except OSError:
            pass

        # Cancel the poll task explicitly
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
        self._poll_task = None

        # Flush any buffered WhatsApp turn before shutdown; if flushing fails,
        # cancel the timers so no task keeps the loop alive.
        for chat_id in list(getattr(self, "_turn_buffers", {}).keys()):
            try:
                await self._flush_turn(chat_id)
            except Exception:
                pass
        for task in list(getattr(self, "_turn_tasks", {}).values()):
            if not task.done():
                task.cancel()
        if hasattr(self, "_turn_tasks"):
            self._turn_tasks.clear()

        # Close the persistent HTTP session
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        self._http_session = None

        self._release_platform_lock()

        self._mark_disconnected()
        self._bridge_process = None
        self._close_bridge_log()
        print(f"[{self.name}] Disconnected")
    
    def format_message(self, content: str) -> str:
        """Convert standard markdown to WhatsApp-compatible formatting.

        WhatsApp supports: *bold*, _italic_, ~strikethrough~, ```code```,
        and monospaced `inline`. Standard markdown uses different syntax
        for bold/italic/strikethrough, so we convert here.

        Code blocks (``` fenced) and inline code (`) are protected from
        conversion via placeholder substitution.
        """
        if not content:
            return content

        # --- 1. Protect fenced code blocks from formatting changes ---
        _FENCE_PH = "\x00FENCE"
        fences: list[str] = []

        def _save_fence(m: re.Match) -> str:
            fences.append(m.group(0))
            return f"{_FENCE_PH}{len(fences) - 1}\x00"

        result = re.sub(r"```[\s\S]*?```", _save_fence, content)

        # --- 2. Protect inline code ---
        _CODE_PH = "\x00CODE"
        codes: list[str] = []

        def _save_code(m: re.Match) -> str:
            codes.append(m.group(0))
            return f"{_CODE_PH}{len(codes) - 1}\x00"

        result = re.sub(r"`[^`\n]+`", _save_code, result)

        # --- 3. Convert markdown formatting to WhatsApp syntax ---
        # Bold: **text** or __text__ → *text*
        result = re.sub(r"\*\*(.+?)\*\*", r"*\1*", result)
        result = re.sub(r"__(.+?)__", r"*\1*", result)
        # Strikethrough: ~~text~~ → ~text~
        result = re.sub(r"~~(.+?)~~", r"~\1~", result)
        # Italic: *text* is already WhatsApp italic — leave as-is
        # _text_ is already WhatsApp italic — leave as-is

        # --- 4. Convert markdown headers to bold text ---
        # # Header → *Header*
        result = re.sub(r"^#{1,6}\s+(.+)$", r"*\1*", result, flags=re.MULTILINE)

        # --- 5. Convert markdown links: [text](url) → text (url) ---
        result = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", result)

        # --- 6. Restore protected sections ---
        for i, fence in enumerate(fences):
            result = result.replace(f"{_FENCE_PH}{i}\x00", fence)
        for i, code in enumerate(codes):
            result = result.replace(f"{_CODE_PH}{i}\x00", code)

        return result

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> SendResult:
        """Send a message via the WhatsApp bridge.

        Formats markdown for WhatsApp, splits long messages into chunks
        that preserve code block boundaries, and sends each chunk sequentially.
        """
        if not self._running or not self._http_session:
            return SendResult(success=False, error="Not connected")
        bridge_exit = await self._check_managed_bridge_exit()
        if bridge_exit:
            return SendResult(success=False, error=bridge_exit)

        if not content or not content.strip():
            return SendResult(success=True, message_id=None)

        try:
            import aiohttp

            # Format and chunk the message
            formatted = self.format_message(content)
            chunks = self.truncate_message(formatted, self._outgoing_chunk_limit())

            last_message_id = None
            for chunk in chunks:
                payload: Dict[str, Any] = {
                    "chatId": chat_id,
                    "message": chunk,
                }
                if reply_to and last_message_id is None:
                    # Only reply-to on the first chunk
                    payload["replyTo"] = reply_to

                async with self._http_session.post(
                    f"http://127.0.0.1:{self._bridge_port}/send",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        last_message_id = data.get("messageId")
                    else:
                        body = await resp.text()
                        try:
                            data = json.loads(body)
                        except (TypeError, json.JSONDecodeError):
                            data = {"error": body}
                        if resp.status == 202 and data.get("outcome") == "unknown":
                            return SendResult(
                                success=False,
                                error=data.get("error") or "WhatsApp send outcome unknown",
                                raw_response=data,
                                retryable=False,
                            )
                        return SendResult(success=False, error=body, raw_response=data)

                # Small delay between chunks to avoid rate limiting
                if len(chunks) > 1:
                    await asyncio.sleep(0.3)

            return SendResult(
                success=True,
                message_id=last_message_id,
            )
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a previously sent message via the WhatsApp bridge."""
        if not self._running or not self._http_session:
            return SendResult(success=False, error="Not connected")
        bridge_exit = await self._check_managed_bridge_exit()
        if bridge_exit:
            return SendResult(success=False, error=bridge_exit)
        try:
            import aiohttp
            async with self._http_session.post(
                f"http://127.0.0.1:{self._bridge_port}/edit",
                json={
                    "chatId": chat_id,
                    "messageId": message_id,
                    "message": content,
                },
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 200:
                    return SendResult(success=True, message_id=message_id)
                else:
                    body = await resp.text()
                    try:
                        data = json.loads(body)
                    except (TypeError, json.JSONDecodeError):
                        data = {"error": body}
                    if resp.status == 202 and data.get("outcome") == "unknown":
                        return SendResult(
                            success=False,
                            error=data.get("error") or "WhatsApp media outcome unknown",
                            raw_response=data,
                            retryable=False,
                        )
                    return SendResult(success=False, error=body, raw_response=data)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def _send_media_to_bridge(
        self,
        chat_id: str,
        file_path: str,
        media_type: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
    ) -> SendResult:
        """Send any media file via bridge /send-media endpoint."""
        if not self._running or not self._http_session:
            return SendResult(success=False, error="Not connected")
        bridge_exit = await self._check_managed_bridge_exit()
        if bridge_exit:
            return SendResult(success=False, error=bridge_exit)
        try:
            import aiohttp

            file_path = self._resolve_outbound_media_path(file_path)
            if not os.path.exists(file_path):
                return SendResult(success=False, error=f"File not found: {file_path}")

            payload: Dict[str, Any] = {
                "chatId": chat_id,
                "filePath": file_path,
                "mediaType": media_type,
            }
            if caption:
                payload["caption"] = caption
            if file_name:
                payload["fileName"] = file_name
            if reply_to:
                payload["replyTo"] = reply_to

            async with self._http_session.post(
                f"http://127.0.0.1:{self._bridge_port}/send-media",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return SendResult(
                        success=True,
                        message_id=data.get("messageId"),
                        raw_response=data,
                    )
                else:
                    error = await resp.text()
                    return SendResult(success=False, error=error)

        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
    ) -> SendResult:
        """Download image URL to cache, send natively via bridge."""
        try:
            local_path = await cache_image_from_url(image_url)
            return await self._send_media_to_bridge(chat_id, local_path, "image", caption, reply_to=reply_to)
        except Exception:
            return await super().send_image(chat_id, image_url, caption, reply_to)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a local image file natively via bridge."""
        return await self._send_media_to_bridge(chat_id, image_path, "image", caption, reply_to=reply_to)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a video natively via bridge — plays inline in WhatsApp."""
        return await self._send_media_to_bridge(chat_id, video_path, "video", caption, reply_to=reply_to)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send an audio file as a WhatsApp voice message via bridge."""
        return await self._send_media_to_bridge(chat_id, audio_path, "audio", caption)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a document/file as a downloadable attachment via bridge."""
        return await self._send_media_to_bridge(
            chat_id, file_path, "document", caption,
            file_name or os.path.basename(file_path),
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send typing indicator via bridge."""
        if not self._running or not self._http_session:
            return
        if await self._check_managed_bridge_exit():
            return
        
        try:
            import aiohttp

            # Must wrap in `async with` — a bare `await session.post(...)`
            # leaves the response object alive until GC, holding its TCP
            # socket in CLOSE_WAIT. See #18451.
            async with self._http_session.post(
                f"http://127.0.0.1:{self._bridge_port}/typing",
                json={"chatId": chat_id},
                timeout=aiohttp.ClientTimeout(total=5)
            ):
                pass
        except Exception:
            pass  # Ignore typing indicator failures
    
    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Get information about a WhatsApp chat."""
        if not self._running or not self._http_session:
            return {"name": "Unknown", "type": "dm"}
        if await self._check_managed_bridge_exit():
            return {"name": chat_id, "type": "dm"}
        
        try:
            import aiohttp

            async with self._http_session.get(
                f"http://127.0.0.1:{self._bridge_port}/chat/{chat_id}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "name": data.get("name", chat_id),
                        "type": "group" if data.get("isGroup") else "dm",
                        "participants": data.get("participants", []),
                    }
        except Exception as e:
            logger.debug("Could not get WhatsApp chat info for %s: %s", chat_id, e)
        
        return {"name": chat_id, "type": "dm"}
    
    async def _poll_messages(self) -> None:
        """Poll the bridge for incoming messages."""
        import aiohttp

        while self._running:
            if not self._http_session:
                break
            bridge_exit = await self._check_managed_bridge_exit()
            if bridge_exit:
                print(f"[{self.name}] {bridge_exit}")
                break
            try:
                async with self._http_session.get(
                    f"http://127.0.0.1:{self._bridge_port}/messages",
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        messages = await resp.json()
                        for msg_data in messages:
                            event = await self._build_message_event(msg_data)
                            if event:
                                await self._queue_or_handle_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                bridge_exit = await self._check_managed_bridge_exit()
                if bridge_exit:
                    print(f"[{self.name}] {bridge_exit}")
                    break
                print(f"[{self.name}] Poll error: {e}")
                await asyncio.sleep(5)
            
            await asyncio.sleep(1)  # Poll interval
    
    async def replay_bridge_messages(
        self,
        messages: List[Dict[str, Any]],
        *,
        bypass_require_mention: bool = True,
    ) -> int:
        """Replay raw bridge messages through the same MessageEvent/session path as live WA."""
        processed = 0
        touched_chats: set[str] = set()

        def _sort_key(message: Dict[str, Any]) -> tuple:
            ts = message.get("timestamp")
            if isinstance(ts, dict):
                ts = ts.get("low") or ts.get("value")
            try:
                ts_num = int(ts)
            except Exception:
                ts_num = 0
            return (ts_num, str(message.get("messageId") or ""))

        for msg_data in sorted((m for m in messages if isinstance(m, dict)), key=_sort_key):
            event = await self._build_message_event(
                msg_data,
                bypass_require_mention=bypass_require_mention,
            )
            if event:
                if event.source and event.source.chat_id:
                    touched_chats.add(event.source.chat_id)
                await self._queue_or_handle_replay_event(event)
                processed += 1
        for chat_id in list(touched_chats):
            await self._flush_turn(chat_id)
        return processed

    async def _build_message_event(self, data: Dict[str, Any], *, bypass_require_mention: bool = False) -> Optional[MessageEvent]:
        """Build a MessageEvent from bridge message data, downloading images to cache."""
        try:
            if not self._should_process_message(data, bypass_require_mention=bypass_require_mention):
                return None

            # Determine message type
            msg_type = MessageType.TEXT
            if data.get("hasMedia"):
                media_type = data.get("mediaType", "")
                if "image" in media_type:
                    msg_type = MessageType.PHOTO
                elif "video" in media_type:
                    msg_type = MessageType.VIDEO
                elif "audio" in media_type or "ptt" in media_type:  # ptt = voice note
                    msg_type = MessageType.VOICE
                else:
                    msg_type = MessageType.DOCUMENT
            
            # Determine chat type
            is_group = data.get("isGroup", False)
            chat_type = "group" if is_group else "dm"
            
            # Build source
            source = self.build_source(
                chat_id=data.get("chatId", ""),
                chat_name=data.get("chatName"),
                chat_type=chat_type,
                user_id=data.get("senderId"),
                user_name=data.get("senderName"),
            )
            self._persist_channel_directory_entry(data)
            
            # Download media URLs to the local cache so agent tools
            # can access them reliably regardless of URL expiration.
            raw_urls = data.get("mediaUrls", [])
            declared_media_mimes = data.get("mediaMimes") or []
            if isinstance(declared_media_mimes, str):
                declared_media_mimes = [declared_media_mimes]
            cached_urls = []
            media_types = []
            refused_documents = []
            for media_index, url in enumerate(raw_urls):
                if msg_type == MessageType.PHOTO and url.startswith(("http://", "https://")):
                    try:
                        cached_path = await cache_image_from_url(url, ext=".jpg")
                        cached_urls.append(cached_path)
                        media_types.append("image/jpeg")
                        print(f"[{self.name}] Cached user image: {cached_path}", flush=True)
                    except Exception as e:
                        print(f"[{self.name}] Failed to cache image: {e}", flush=True)
                        cached_urls.append(url)
                        media_types.append("image/jpeg")
                elif msg_type == MessageType.PHOTO and os.path.isabs(url):
                    # Local file path — bridge already downloaded the image
                    cached_urls.append(url)
                    media_types.append("image/jpeg")
                    print(f"[{self.name}] Using bridge-cached image: {url}", flush=True)
                elif msg_type == MessageType.VOICE and url.startswith(("http://", "https://")):
                    try:
                        cached_path = await cache_audio_from_url(url, ext=".ogg")
                        cached_urls.append(cached_path)
                        media_types.append("audio/ogg")
                        print(f"[{self.name}] Cached user voice: {cached_path}", flush=True)
                    except Exception as e:
                        print(f"[{self.name}] Failed to cache voice: {e}", flush=True)
                        cached_urls.append(url)
                        media_types.append("audio/ogg")
                elif msg_type == MessageType.VOICE and os.path.isabs(url):
                    # Local file path — bridge already downloaded the audio
                    cached_urls.append(url)
                    media_types.append("audio/ogg")
                    print(f"[{self.name}] Using bridge-cached audio: {url}", flush=True)
                elif msg_type == MessageType.DOCUMENT and os.path.isabs(url):
                    # Local file path — bridge already downloaded the document
                    ext = Path(url).suffix.lower()
                    mime = (
                        str(declared_media_mimes[media_index])
                        if media_index < len(declared_media_mimes)
                        else ""
                    )
                    if ext in {".xlsx", ".csv", ".xlsm", ".xltm"}:
                        try:
                            from tools.pa_business_tools import validate_tgg_spreadsheet

                            mime = validate_tgg_spreadsheet(
                                url,
                                declared_mime=mime,
                            )
                        except (OSError, ValueError) as exc:
                            refusal_code = str(exc).split(":", 1)[0]
                            refused_documents.append(refusal_code)
                            print(
                                f"[{self.name}] Refused spreadsheet document: {exc}",
                                flush=True,
                            )
                            continue
                    elif not mime:
                        mime = SUPPORTED_DOCUMENT_TYPES.get(
                            ext, "application/octet-stream"
                        )
                    cached_urls.append(url)
                    media_types.append(mime)
                    print(f"[{self.name}] Using bridge-cached document: {url}", flush=True)
                elif msg_type == MessageType.VIDEO and os.path.isabs(url):
                    cached_urls.append(url)
                    media_types.append("video/mp4")
                    print(f"[{self.name}] Using bridge-cached video: {url}", flush=True)
                else:
                    cached_urls.append(url)
                    media_types.append("unknown")

            # For text-readable documents, inject file content directly into
            # the message text so the agent can read it inline.
            # Cap at 100KB to match Telegram/Discord/Slack behaviour.
            body = data.get("body", "")
            if data.get("isGroup"):
                body = self._clean_bot_mention_text(body, data)
            if refused_documents:
                refusal_note = (
                    "[A document attachment was refused by the media safety gate: "
                    + "; ".join(refused_documents)
                    + "]"
                )
                body = f"{refusal_note}\n\n{body}" if body else refusal_note
            MAX_TEXT_INJECT_BYTES = 100 * 1024
            if msg_type == MessageType.DOCUMENT and cached_urls:
                for doc_path in cached_urls:
                    ext = Path(doc_path).suffix.lower()
                    if ext in {".txt", ".md", ".csv", ".json", ".xml", ".yaml", ".yml", ".log", ".py", ".js", ".ts", ".html", ".css"}:
                        try:
                            file_size = Path(doc_path).stat().st_size
                            if file_size > MAX_TEXT_INJECT_BYTES:
                                print(f"[{self.name}] Skipping text injection for {doc_path} ({file_size} bytes > {MAX_TEXT_INJECT_BYTES})", flush=True)
                                continue
                            content = Path(doc_path).read_text(encoding="utf-8", errors="replace")
                            fname = Path(doc_path).name
                            # Remove the doc_<hex>_ prefix for display
                            display_name = fname
                            if "_" in fname:
                                parts = fname.split("_", 2)
                                if len(parts) >= 3:
                                    display_name = parts[2]
                            injection = f"[Content of {display_name}]:\n{content}"
                            if body:
                                body = f"{injection}\n\n{body}"
                            else:
                                body = injection
                            print(f"[{self.name}] Injected text content from: {doc_path}", flush=True)
                        except Exception as e:
                            print(f"[{self.name}] Failed to read document text: {e}", flush=True)

            # Addressed-ness for the conditional turn-debounce: a reply to one
            # of the bot's messages OR an @-mention of the bot. Reuses the same
            # detection the require_mention gate uses.
            addressed = bool(
                self._message_is_reply_to_bot(data)
                or self._message_mentions_bot(data)
            )

            return MessageEvent(
                text=body,
                message_type=msg_type,
                source=source,
                raw_message=data,
                message_id=data.get("messageId"),
                reply_to_message_id=data.get("quotedMessageId"),
                reply_to_text=self._reply_context_text(data),
                media_urls=cached_urls,
                media_types=media_types,
                addressed=addressed,                channel_prompt=data.get("_hermes_channel_prompt"),
                pa_job_type=data.get("_hermes_pa_job_type"),
                pa_context=data.get("_hermes_pa_context") if isinstance(data.get("_hermes_pa_context"), dict) else None,
            )
        except Exception as e:
            print(f"[{self.name}] Error building event: {e}")
            return None
