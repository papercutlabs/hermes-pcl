#!/usr/bin/env bash
# Christopher CAPTURE-FRESHNESS check.
#
# Row age is evidence, not a verdict: routine overnight/weekend quiet can be
# older than CAPTURE_MAX_STALE_HOURS without any capture failure. The verdict
# comes from the bridge's inbound boundary plus capture/inbox lockstep.
set -uo pipefail

HOST="${TGG_HOST:-tgg-app-1}"
EVENTS="/var/lib/tgg-capture/whatsapp/capture/events.jsonl"
MAX_STALE_HOURS="${CAPTURE_MAX_STALE_HOURS:-12}"
FRESHNESS_SLA_SECONDS=7200
HERMES_HOME="${HERMES_HOME:-/home/pclaw/.hermes-christopher-tgg}"
GATE_FILE="$HERMES_HOME/runtime/processing-gate.json"
INBOX_DB="$HERMES_HOME/runtime/capture-inbox.db"
SATURATION_PCT="${CAPTURE_QUEUE_SATURATION_PCT:-90}"
PROBE_FILE=""

usage() {
  echo "usage: $0 [--probe-file <snapshot.json>]" >&2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --probe-file)
      [ "$#" -ge 2 ] || { usage; exit 2; }
      PROBE_FILE="$2"
      shift 2
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

collect_live_probe() {
  ssh -o ConnectTimeout=15 -o BatchMode=yes "$HOST" \
    sudo python3 - "$EVENTS" "$GATE_FILE" "$INBOX_DB" <<'PY'
import datetime
import json
import pathlib
import sqlite3
import sys
import time
import urllib.request


def event_item(record):
    if isinstance(record, dict) and isinstance(record.get("normalized"), dict):
        return record["normalized"]
    return record if isinstance(record, dict) else {}


def capture_epoch(record):
    value = record.get("capturedAt") if isinstance(record, dict) else None
    try:
        value = float(value)
        return value / 1000 if value > 10_000_000_000 else value
    except (TypeError, ValueError):
        if not isinstance(value, str):
            return None
        try:
            return datetime.datetime.fromisoformat(
                value.replace("Z", "+00:00")
            ).timestamp()
        except ValueError:
            return None


def capture_progress(path, inbox_marker, now):
    records = []
    with path.open("rb") as handle:
        for line in handle:
            if line.strip():
                record = json.loads(line)
                item = event_item(record)
                records.append((item.get("messageId"), capture_epoch(record)))
    if not records:
        return None, False, None, inbox_marker is None
    last_id = records[-1][0]
    lockstep = bool(last_id and inbox_marker and last_id == inbox_marker)
    if lockstep:
        return last_id, True, None, True

    marker_index = None
    if inbox_marker is not None:
        for index in range(len(records) - 1, -1, -1):
            if records[index][0] == inbox_marker:
                marker_index = index
                break
    candidate_index = 0 if marker_index is None else marker_index + 1
    if candidate_index >= len(records):
        return last_id, False, None, marker_index is not None
    candidate_epoch = records[candidate_index][1]
    age = None if candidate_epoch is None else max(0, int(now - candidate_epoch))
    return last_id, False, age, inbox_marker is None or marker_index is not None


def read_health():
    error = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(
                "http://127.0.0.1:3011/health", timeout=6
            ) as response:
                return json.load(response)
        except Exception as exc:
            error = exc
            if attempt == 0:
                time.sleep(0.5)
    raise error


def process_environment():
    for proc in pathlib.Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            command = (proc / "cmdline").read_bytes().replace(b"\0", b" ").decode()
            if "whatsapp-bridge/bridge.js" not in command:
                continue
            values = {}
            for entry in (proc / "environ").read_bytes().split(b"\0"):
                if b"=" in entry:
                    key, value = entry.split(b"=", 1)
                    values[key.decode()] = value.decode()
            return values
        except (OSError, UnicodeDecodeError):
            continue
    return {}


events = pathlib.Path(sys.argv[1])
gate_file = pathlib.Path(sys.argv[2])
inbox_db = pathlib.Path(sys.argv[3])
now = time.time()
health = read_health()
env = process_environment()
queue_cap = env.get("WHATSAPP_MAX_QUEUE_SIZE")
if not queue_cap:
    queue_cap = 5000 if env.get("WHATSAPP_SYNC_FULL_HISTORY") == "true" else 100

gate_enabled = None
try:
    gate_enabled = bool(json.loads(gate_file.read_text())["enabled"])
except (OSError, ValueError, KeyError, TypeError):
    pass

inbox_last_message_id = None
inbox_last_created_at = None
probe_error_condition = None
probe_error_reason = None
if inbox_db.exists():
    try:
        conn = sqlite3.connect(f"file:{inbox_db}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT message_id,created_at FROM ingress_events "
                "ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            if row:
                inbox_last_message_id, inbox_last_created_at = row
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as exc:
        probe_error_condition = "capture-inbox-evidence-missing"
        probe_error_reason = f"could not read capture inbox: {exc}"

capture_last_message_id = None
capture_inbox_lockstep = False
capture_inbox_divergence_age_seconds = None
capture_inbox_marker_found = inbox_last_message_id is None
capture_mtime_epoch = 0
try:
    if events.exists():
        capture_mtime_epoch = events.stat().st_mtime
        (
            capture_last_message_id,
            capture_inbox_lockstep,
            capture_inbox_divergence_age_seconds,
            capture_inbox_marker_found,
        ) = capture_progress(events, inbox_last_message_id, now)
except (OSError, ValueError, json.JSONDecodeError) as exc:
    probe_error_condition = "capture-evidence-missing"
    probe_error_reason = f"could not read capture evidence: {exc}"

payload = {
    "probe_epoch": now,
    "capture_mtime_epoch": capture_mtime_epoch,
    "capture_last_message_id": capture_last_message_id,
    "inbox_last_message_id": inbox_last_message_id,
    "inbox_last_created_at": inbox_last_created_at,
    "capture_inbox_lockstep": capture_inbox_lockstep,
    "capture_inbox_divergence_age_seconds": (
        capture_inbox_divergence_age_seconds
    ),
    "capture_inbox_marker_found": capture_inbox_marker_found,
    "queue_length": health.get("queueLength"),
    "socket_status": health.get("status"),
    "queue_cap": int(queue_cap),
    "processing_gate_enabled": gate_enabled,
    "inboundProgress": health.get("inboundProgress"),
    "bridge_health_probe": "loopback-/health",
    "probe_error_condition": probe_error_condition,
    "probe_error_reason": probe_error_reason,
}
print(json.dumps(payload, separators=(",", ":")))
PY
}

if [ -n "$PROBE_FILE" ]; then
  [ -r "$PROBE_FILE" ] || {
    echo "probe file is not readable: $PROBE_FILE" >&2
    exit 2
  }
  PROBE_JSON=$(cat "$PROBE_FILE")
else
  PROBE_JSON=$(collect_live_probe 2>/dev/null)
  probe_rc=$?
  if [ "$probe_rc" -ne 0 ] || [ -z "$PROBE_JSON" ]; then
    printf '{"ok":false,"condition":"bridge-health-unavailable","escalation_target":"edna-central","reason":"live bridge health probe failed"}\n'
    echo "FAIL bridge-health-unavailable: live bridge health probe failed" >&2
    exit 1
  fi
fi

PROBE_JSON="$PROBE_JSON" python3 - "$MAX_STALE_HOURS" \
  "$FRESHNESS_SLA_SECONDS" "$SATURATION_PCT" <<'PY'
import datetime
import json
import os
import sys
from zoneinfo import ZoneInfo

try:
    max_stale_hours = int(sys.argv[1])
    freshness_sla_seconds = int(sys.argv[2])
    saturation_pct = int(sys.argv[3])
    if max_stale_hours < 1:
        raise ValueError("CAPTURE_MAX_STALE_HOURS must be positive")
    if freshness_sla_seconds < 1:
        raise ValueError("freshness SLA must be positive")
    if not 1 <= saturation_pct <= 100:
        raise ValueError("CAPTURE_QUEUE_SATURATION_PCT must be between 1 and 100")
except (TypeError, ValueError) as exc:
    reason = f"capture-freshness configuration is invalid: {exc}"
    print(json.dumps({
        "ok": False,
        "condition": "configuration-invalid",
        "escalation_target": "edna-central",
        "reason": reason,
    }, separators=(",", ":")))
    print(f"FAIL configuration-invalid: {reason}", file=sys.stderr)
    raise SystemExit(1)

try:
    probe = json.loads(os.environ["PROBE_JSON"])
except (KeyError, TypeError, ValueError) as exc:
    reason = f"probe snapshot is not valid JSON: {exc}"
    print(json.dumps({
        "ok": False,
        "condition": "probe-invalid",
        "escalation_target": "edna-central",
        "reason": reason,
    }, separators=(",", ":")))
    print(f"FAIL probe-invalid: {reason}", file=sys.stderr)
    raise SystemExit(1)

probe_error_condition = probe.get("probe_error_condition")
if probe_error_condition in {
    "capture-evidence-missing",
    "capture-inbox-evidence-missing",
}:
    reason = str(probe.get("probe_error_reason") or "probe evidence read failed")
    print(json.dumps({
        "ok": False,
        "condition": probe_error_condition,
        "escalation_target": "edna-central",
        "reason": reason,
    }, separators=(",", ":")))
    print(f"FAIL {probe_error_condition}: {reason}", file=sys.stderr)
    raise SystemExit(1)

now = probe.get("probe_epoch")
mtime = probe.get("capture_mtime_epoch")
try:
    now = float(now)
    mtime = float(mtime)
    if mtime <= 0 or now < mtime:
        raise ValueError("invalid capture clock")
except (TypeError, ValueError):
    reason = "capture timestamp is missing or invalid"
    print(json.dumps({
        "ok": False,
        "condition": "capture-evidence-missing",
        "escalation_target": "edna-central",
        "reason": reason,
    }, separators=(",", ":")))
    print(f"FAIL capture-evidence-missing: {reason}", file=sys.stderr)
    raise SystemExit(1)

stale_seconds = int(now - mtime)
stale_hours = stale_seconds // 3600
progress = probe.get("inboundProgress")
required_ints = (
    "receivedBatches", "completedBatches", "pendingBatches",
    "receivedMessages", "completedMessages", "pendingMessages",
    "failedBatches", "failedMessages",
    "failedBatchesSinceLastCompletion", "failedMessagesSinceLastCompletion",
    "unresolvedPersistedFailures",
)
telemetry_error = None
if not isinstance(progress, dict):
    telemetry_error = "inboundProgress is absent"
else:
    for key in required_ints:
        value = progress.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            telemetry_error = f"inboundProgress.{key} is missing or invalid"
            break
    if telemetry_error is None:
        pending_batches = progress["pendingBatches"]
        pending_messages = progress["pendingMessages"]
        oldest = progress.get("oldestPendingAgeSeconds")
        if pending_batches == 0 and oldest is not None:
            telemetry_error = "oldestPendingAgeSeconds must be null with no pending batches"
        elif pending_batches > 0 and (
            not isinstance(oldest, (int, float))
            or isinstance(oldest, bool)
            or oldest < 0
        ):
            telemetry_error = "oldestPendingAgeSeconds is required for pending batches"
        elif progress["completedBatches"] > progress["receivedBatches"]:
            telemetry_error = "completedBatches exceeds receivedBatches"
        elif progress["completedMessages"] > progress["receivedMessages"]:
            telemetry_error = "completedMessages exceeds receivedMessages"
        elif (
            progress["receivedBatches"]
            != progress["completedBatches"]
            + progress["failedBatches"]
            + pending_batches
        ):
            telemetry_error = (
                "receivedBatches does not match completed/failed/pending counters"
            )
        elif (
            progress["receivedMessages"]
            != progress["completedMessages"]
            + progress["failedMessages"]
            + pending_messages
        ):
            telemetry_error = (
                "receivedMessages does not match completed/failed/pending counters"
            )
        elif (
            progress["failedBatchesSinceLastCompletion"]
            > progress["failedBatches"]
        ):
            telemetry_error = "failedBatchesSinceLastCompletion exceeds failedBatches"
        elif (
            progress["failedMessagesSinceLastCompletion"]
            > progress["failedMessages"]
        ):
            telemetry_error = "failedMessagesSinceLastCompletion exceeds failedMessages"
        else:
            for count_key, timestamp_key in (
                ("receivedBatches", "lastReceivedAt"),
                ("completedBatches", "lastCompletedAt"),
            ):
                timestamp = progress.get(timestamp_key)
                if progress[count_key] == 0 and timestamp is not None:
                    telemetry_error = (
                        f"inboundProgress.{timestamp_key} must be null "
                        f"when {count_key} is zero"
                    )
                    break
                if progress[count_key] > 0:
                    if not isinstance(timestamp, str):
                        telemetry_error = (
                            f"inboundProgress.{timestamp_key} is missing or invalid"
                        )
                        break
                    try:
                        datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    except ValueError:
                        telemetry_error = (
                            f"inboundProgress.{timestamp_key} is missing or invalid"
                        )
                        break
            failed_at = progress.get("lastFailedAt")
            if telemetry_error is None:
                if progress["failedBatches"] == 0 and failed_at is not None:
                    telemetry_error = (
                        "inboundProgress.lastFailedAt must be null "
                        "when failedBatches is zero"
                    )
                elif progress["failedBatches"] > 0:
                    if not isinstance(failed_at, str):
                        telemetry_error = (
                            "inboundProgress.lastFailedAt is missing or invalid"
                        )
                    else:
                        try:
                            datetime.datetime.fromisoformat(
                                failed_at.replace("Z", "+00:00")
                            )
                        except ValueError:
                            telemetry_error = (
                                "inboundProgress.lastFailedAt is missing or invalid"
                            )
            unresolved_at = progress.get("oldestUnresolvedFailureAt")
            if telemetry_error is None:
                if (
                    progress["unresolvedPersistedFailures"] == 0
                    and unresolved_at is not None
                ):
                    telemetry_error = (
                        "inboundProgress.oldestUnresolvedFailureAt must be null "
                        "when unresolvedPersistedFailures is zero"
                    )
                elif progress["unresolvedPersistedFailures"] > 0:
                    if not isinstance(unresolved_at, str):
                        telemetry_error = (
                            "inboundProgress.oldestUnresolvedFailureAt "
                            "is missing or invalid"
                        )
                    else:
                        try:
                            datetime.datetime.fromisoformat(
                                unresolved_at.replace("Z", "+00:00")
                            )
                        except ValueError:
                            telemetry_error = (
                                "inboundProgress.oldestUnresolvedFailureAt "
                                "is missing or invalid"
                            )

queue_length = probe.get("queue_length")
queue_cap = probe.get("queue_cap")
queue_valid = (
    isinstance(queue_length, int)
    and not isinstance(queue_length, bool)
    and queue_length >= 0
    and isinstance(queue_cap, int)
    and not isinstance(queue_cap, bool)
    and queue_cap > 0
)
saturation_threshold = queue_cap * saturation_pct // 100 if queue_valid else None
queue_saturated = bool(queue_valid and queue_length >= saturation_threshold)
gate = probe.get("processing_gate_enabled")
consumer_expected = gate is not False
capture_id = probe.get("capture_last_message_id")
inbox_id = probe.get("inbox_last_message_id")
lockstep_value = probe.get("capture_inbox_lockstep")
lockstep = (
    lockstep_value
    if isinstance(lockstep_value, bool)
    else bool(capture_id and inbox_id and capture_id == inbox_id)
)
divergence_age = probe.get("capture_inbox_divergence_age_seconds")
divergence_age_valid = (
    isinstance(divergence_age, (int, float))
    and not isinstance(divergence_age, bool)
    and divergence_age >= 0
)
marker_found = probe.get("capture_inbox_marker_found")
marker_evidence_valid = marker_found is True
socket_status = probe.get("socket_status")
row_age_exceeds_threshold = stale_hours >= max_stale_hours

condition = None
reason = None
if telemetry_error:
    condition = "inbound-telemetry-missing"
    reason = telemetry_error
elif socket_status != "connected":
    condition = "socket-disconnected"
    reason = f"bridge socket status is {socket_status!r}"
elif not queue_valid:
    condition = "queue-telemetry-missing"
    reason = "queue length or queue cap is missing or invalid"
elif progress["unresolvedPersistedFailures"] > 0:
    condition = "inbound-arrived-not-written"
    reason = (
        f"{progress['unresolvedPersistedFailures']} unresolved persisted "
        "inbound capture failure(s) remain"
    )
elif (
    progress["pendingBatches"] > 0
    and progress["oldestPendingAgeSeconds"] >= freshness_sla_seconds
):
    condition = "inbound-arrived-not-written"
    reason = (
        f"oldest inbound batch has remained pending for "
        f"{int(progress['oldestPendingAgeSeconds'])}s"
    )
elif progress["failedBatchesSinceLastCompletion"] > 0:
    condition = "inbound-arrived-not-written"
    reason = (
        f"{progress['failedBatchesSinceLastCompletion']} inbound batch(es) "
        "failed since the last successful capture completion"
    )
elif (
    consumer_expected
    and not lockstep
    and (not marker_evidence_valid or not divergence_age_valid)
):
    condition = "capture-inbox-evidence-missing"
    reason = (
        "capture/inbox differ but the inbox marker or first unmatched "
        "capture age is unavailable"
    )
elif (
    consumer_expected
    and not lockstep
    and divergence_age >= freshness_sla_seconds
):
    condition = "capture-inbox-diverged"
    reason = (
        f"capture/inbox have diverged for {int(divergence_age)}s "
        f"(SLA {freshness_sla_seconds}s)"
    )
elif consumer_expected and queue_saturated:
    condition = "queue-saturated"
    reason = f"queue is saturated at {queue_length}/{queue_cap}"

failed = condition is not None
weekday = datetime.datetime.fromtimestamp(now, ZoneInfo("Asia/Singapore")).weekday()
# Trailing-14-day maxima measured in the plan. This labels context only; it
# never changes the verdict or either configured threshold.
baseline_limit_hours = 34.59 if weekday >= 5 else 14.60
if not failed:
    if row_age_exceeds_threshold:
        condition = (
            "quiet-within-measured-pattern"
            if stale_seconds <= baseline_limit_hours * 3600
            else "quiet-outside-measured-pattern"
        )
    else:
        condition = "capture-active"

result = {
    "ok": not failed,
    "condition": condition,
    "capture_stale_hours": stale_hours,
    "capture_stale_seconds": stale_seconds,
    "row_age_exceeds_threshold": row_age_exceeds_threshold,
    "threshold_hours": max_stale_hours,
    "freshness_sla_seconds": freshness_sla_seconds,
    "quiet_baseline_limit_hours": baseline_limit_hours,
    "socket_status": socket_status,
    "inbound_progress": progress,
    "capture_inbox_lockstep": lockstep,
    "capture_inbox_divergence_age_seconds": (
        int(divergence_age) if divergence_age_valid else None
    ),
    "queue_length": queue_length,
    "queue_cap": queue_cap,
    "saturation_threshold": saturation_threshold,
    "queue_saturated": queue_saturated,
    "processing_gate_enabled": gate,
    "consumer_expected": consumer_expected,
    "bridge_health_probe": probe.get("bridge_health_probe"),
    "escalation_target": "edna-central" if failed else None,
}
if reason:
    result["reason"] = reason
print(json.dumps(result, separators=(",", ":")))
if failed:
    print(f"FAIL {condition}: {reason}", file=sys.stderr)
raise SystemExit(1 if failed else 0)
PY
