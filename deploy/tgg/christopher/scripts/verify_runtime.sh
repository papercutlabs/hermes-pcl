#!/usr/bin/env bash
set -euo pipefail

# The verifier is routinely run by root during release staging, while this
# script itself is part of the immutable runtime it is checking.  Python's
# normal import cache would otherwise let an ordinary health check create
# unmanifested __pycache__ entries beside release-owned source files.  Keep
# every Python child bytecode-free; an explicit compiler is not a verifier and
# must use its own external cache destination.
export PYTHONDONTWRITEBYTECODE=1

MODE="${1:---quick}"
APP_ROOT="${APP_ROOT:-/home/pclaw/apps/hermes-pcl}"
HERMES_HOME="${HERMES_HOME:-/home/pclaw/.hermes-christopher-tgg}"
TEST_HOME="${TEST_HOME:-/home/pclaw/.hermes-christopher-tgg-test}"
# APP_ROOT owns the long-lived virtualenv and Python package checkout.  The
# release assets being verified must instead come from the script actually
# invoked by systemd: after a standalone release, that is the current runtime
# symlink rather than the retired app checkout.
SCRIPT_PATH="${BASH_SOURCE[0]}"
while [[ -L "$SCRIPT_PATH" ]]; do
  SCRIPT_DIR="$(cd -P "$(dirname "$SCRIPT_PATH")" && pwd)"
  SCRIPT_PATH="$(readlink "$SCRIPT_PATH")"
  [[ "$SCRIPT_PATH" = /* ]] || SCRIPT_PATH="$SCRIPT_DIR/$SCRIPT_PATH"
done
SCRIPT_DIR="$(cd -P "$(dirname "$SCRIPT_PATH")" && pwd)"
DEPLOY_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# The immutable release repository owns the deployment spec and every source
# it names. APP_ROOT remains deliberately separate: it only owns the durable
# Python virtualenv used to execute those release-owned helpers.
RELEASE_ROOT="$(cd "$DEPLOY_ROOT/../../.." && pwd)"
RUNTIME_ROOT="$HERMES_HOME/runtime"

if [[ "$MODE" == "--verify-status-contract" ]]; then
  if [[ "$#" -ne 4 ]]; then
    echo "usage: $0 --verify-status-contract <config> <processing-gate> <consumer-status>" >&2
    exit 2
  fi
  exec "${VERIFY_PYTHON:-$APP_ROOT/.venv/bin/python}" - "$2" "$3" "$4" <<'PY'
import json
import pathlib
import sys

import yaml

config = yaml.safe_load(pathlib.Path(sys.argv[1]).read_text())
gate = json.loads(pathlib.Path(sys.argv[2]).read_text())
status = json.loads(pathlib.Path(sys.argv[3]).read_text())

config_enabled = config["pa"]["enabled"]
gate_enabled = gate["enabled"]
assert isinstance(config_enabled, bool), config_enabled
assert isinstance(gate_enabled, bool), gate_enabled
assert gate_enabled is config_enabled, (gate_enabled, config_enabled)

state = status.get("state")
assert state != "held", "fatal consumer state: held"

for key in ("processing_enabled", "config_enabled", "gate_enabled"):
    assert isinstance(status.get(key), bool), (key, status.get(key))
    assert status[key] is config_enabled, (key, status[key], config_enabled)

retention_held = status.get("retention_held")
retention_quarantined = status.get("retention_quarantined")
retention_quarantine_status = status.get("retention_quarantine_status")
retention_quarantine_message_ids = status.get("retention_quarantine_message_ids")
retention_hold = status.get("retention_hold")
assert config["pa"]["media_retention"]["max_attempts"] == 5
assert config["pa"]["media_retention"]["retry_interval_seconds"] >= 60
assert isinstance(retention_held, int) and not isinstance(retention_held, bool), retention_held
assert retention_held >= 0, retention_held
assert isinstance(retention_quarantined, int) and not isinstance(retention_quarantined, bool), retention_quarantined
assert retention_quarantined >= 0, retention_quarantined
assert isinstance(retention_quarantine_status, dict), retention_quarantine_status
assert set(retention_quarantine_status) <= {"quarantined"}, retention_quarantine_status
assert all(isinstance(value, int) and not isinstance(value, bool) and value >= 0
           for value in retention_quarantine_status.values()), retention_quarantine_status
assert retention_quarantine_status.get("quarantined", 0) == retention_quarantined, (
    retention_quarantine_status, retention_quarantined
)
assert isinstance(retention_quarantine_message_ids, list), retention_quarantine_message_ids
assert all(isinstance(value, str) and value.strip()
           for value in retention_quarantine_message_ids), retention_quarantine_message_ids
assert len(retention_quarantine_message_ids) == retention_quarantined, (
    retention_quarantine_message_ids, retention_quarantined
)
assert len(set(retention_quarantine_message_ids)) == len(retention_quarantine_message_ids), (
    retention_quarantine_message_ids
)
has_retention_hold = isinstance(retention_hold, str) and bool(retention_hold.strip())
if not config_enabled:
    assert state == "standby", (state, "standby")
    # Disabling processing prevents new retention attempts; it does not erase
    # an already-recorded retention hold.  Keep those holds visible in the
    # verifier receipt so they can be reconciled deliberately, but do not let
    # a historical hold prevent deployment of the resolver needed to inspect
    # it.  A disabled consumer must never present the active held-pending
    # state, and a non-zero count must still have its error evidence.
    if retention_held > 0 or has_retention_hold:
        assert retention_held > 0, retention_held
        assert has_retention_hold, retention_hold
elif retention_held > 0 or has_retention_hold:
    assert retention_held > 0, retention_held
    assert has_retention_hold, retention_hold
    assert state == "held-pending", (state, "held-pending")
else:
    assert state == "running", (state, "running")
PY
fi

if [[ "$MODE" == "--verify-standby-inbox-contract" ]]; then
  if [[ "$#" -ne 5 ]]; then
    echo "usage: $0 --verify-standby-inbox-contract <config> <processing-gate> <consumer-status> <capture-inbox-db>" >&2
    exit 2
  fi
  exec "${VERIFY_PYTHON:-$APP_ROOT/.venv/bin/python}" - "$2" "$3" "$4" "$5" <<'PY'
import datetime
import json
import pathlib
import sqlite3
import sys

import yaml

config = yaml.safe_load(pathlib.Path(sys.argv[1]).read_text())
gate = json.loads(pathlib.Path(sys.argv[2]).read_text())
status = json.loads(pathlib.Path(sys.argv[3]).read_text())
inbox_path = pathlib.Path(sys.argv[4])

assert config["pa"]["enabled"] is False
assert gate["enabled"] is False
assert status["state"] == "standby", status["state"]
assert status["processing_enabled"] is False
assert status["config_enabled"] is False
assert status["gate_enabled"] is False
assert status["source_opened"] is False
assert status["cursor_advanced"] is False
assert status["active_management_chats"] == []
assert status["active_site_chats"] == []
boundary = datetime.datetime.fromisoformat(gate["changed_at"])
assert boundary.tzinfo is not None

constitution_path = pathlib.Path(config["pa"]["constitution_path"])
constitution = yaml.safe_load(constitution_path.read_text()) or {}
management = {
    str((selector.get("match") or {}).get("source.chat_id"))
    for selector in constitution.get("selectors", [])
    if selector.get("job_type") == "tgg_management"
    and (selector.get("match") or {}).get("source.platform") == "whatsapp"
    and (selector.get("match") or {}).get("source.chat_id")
}
expected_management = {
    "120363426509183563@g.us",
    "120363407903158826@g.us",
}
assert management == expected_management, (management, expected_management)

actual_counts = {name: 0 for name in ("pending", "processing", "completed", "skipped", "failed")}
if inbox_path.exists():
    conn = sqlite3.connect(inbox_path)
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "ingress_events" in tables:
            actual_counts.update(dict(conn.execute("SELECT status, COUNT(*) FROM ingress_events GROUP BY status")))
            pending_management = conn.execute(
                f"SELECT COUNT(*) FROM ingress_events WHERE status='pending' AND chat_id IN ({','.join('?' for _ in management)})",
                tuple(sorted(management)),
            ).fetchone()[0]
            processing = conn.execute("SELECT COUNT(*) FROM ingress_events WHERE status='processing'").fetchone()[0]
            changed_after_boundary = conn.execute(
                "SELECT COUNT(*) FROM ingress_events WHERE updated_at > ?", (gate["changed_at"],)
            ).fetchone()[0]
        else:
            pending_management = processing = changed_after_boundary = 0
    finally:
        conn.close()
else:
    pending_management = processing = changed_after_boundary = 0

assert processing == 0, processing
assert pending_management == 0, (pending_management, sorted(management))
assert changed_after_boundary == 0, (changed_after_boundary, gate["changed_at"])
reported_counts = status["inbox"]
assert set(reported_counts) <= set(actual_counts), reported_counts
reported_counts = {name: int(reported_counts.get(name, 0)) for name in actual_counts}
assert reported_counts == actual_counts, (reported_counts, actual_counts)
assert status["state_total"] == sum(actual_counts.values()), (status["state_total"], actual_counts)
print(json.dumps({
    "standby_inbox_contract": "pass",
    "historical_inbox_rows": sum(actual_counts.values()),
    "historical_pending_non_management": actual_counts["pending"],
    "management_selector_chats": sorted(management),
    "standby_changed_rows": changed_after_boundary,
}, sort_keys=True))
PY
fi

if [[ "$MODE" == "--verify-disabled-cursor-contract" ]]; then
  if [[ "$#" -ne 3 ]]; then
    echo "usage: $0 --verify-disabled-cursor-contract <processing-gate> <capture-cursor>" >&2
    exit 2
  fi
  exec "${VERIFY_PYTHON:-$APP_ROOT/.venv/bin/python}" - "$2" "$3" <<'PY'
import datetime
import json
import pathlib
import sys

gate = json.loads(pathlib.Path(sys.argv[1]).read_text())
cursor = json.loads(pathlib.Path(sys.argv[2]).read_text())
assert gate["enabled"] is False
initial_offset = int(cursor["initial_offset"])
offset = int(cursor["offset"])
assert offset >= initial_offset, (offset, initial_offset)

# A runtime may have advanced while it was previously enabled.  The disabled
# invariant is that it stopped before the latest gate boundary, not that its
# cursor has remained at its lifetime initial offset.
gate_boundary = datetime.datetime.fromisoformat(gate["changed_at"])
cursor_updated = datetime.datetime.fromisoformat(cursor["updated_at"])
assert gate_boundary.tzinfo is not None
assert cursor_updated.tzinfo is not None
assert cursor_updated <= gate_boundary + datetime.timedelta(seconds=5), (
    cursor_updated,
    gate_boundary,
)
print(json.dumps({
    "disabled_cursor_contract": "pass",
    "initial_offset": initial_offset,
    "offset": offset,
    "cursor_updated_at": cursor["updated_at"],
    "gate_changed_at": gate["changed_at"],
}, sort_keys=True))
PY
fi

if [[ "$MODE" == "--check-mode" ]]; then
  raw="$(pcl service locate --system christopher --domain pa)"
  target="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["data"]["system"]["liveFacts"]["host"]["sshTargetAlias"])' <<<"$raw")"
  exec ssh "$target" /opt/tgg-christopher/runtime/current/deploy/tgg/christopher/scripts/verify_runtime.sh --full
fi
if [[ "$MODE" != "--quick" && "$MODE" != "--full" ]]; then
  echo "usage: $0 --quick|--full|--check-mode" >&2
  exit 2
fi

hostname
systemctl is-active --quiet christopher-tgg-hermes.service
systemctl is-active --quiet christopher-tgg-hermes-health.timer
systemctl is-active --quiet systems-papercut-labs.service
test -x "$APP_ROOT/.venv/bin/python"
test -s "$HERMES_HOME/.env"
test -s "$HERMES_HOME/config.yaml"
test -s "$HERMES_HOME/christopher_tgg_constitution.yaml"

sandbox_enabled="$("$APP_ROOT/.venv/bin/python" - "$HERMES_HOME/config.yaml" <<'PY2'
import sys, yaml
config = yaml.safe_load(open(sys.argv[1])) or {}
print("true" if (config.get("python_sandbox") or {}).get("enabled") is True else "false")
PY2
)"
if [[ "$sandbox_enabled" == "true" ]]; then
  runuser -u pclaw -- unshare --user --map-root-user --net --mount --pid --fork --kill-child \
    /bin/sh -c 'unshare --user --map-user=65534 --map-group=65534 true'
  "$APP_ROOT/.venv/bin/python" - "$HERMES_HOME/config.yaml" <<'PY2'
import pathlib, sys, yaml
config = yaml.safe_load(open(sys.argv[1])) or {}
datasets = (config.get("python_sandbox") or {}).get("datasets") or {}
assert set(datasets) == {"cases", "documents", "media"}, sorted(datasets)
for name in ("cases", "documents", "media"):
    path = pathlib.Path(datasets[name]["path"])
    assert path.exists(), (name, str(path))
PY2
fi
test -s "$RUNTIME_ROOT/engine-slot"
test -s "$RUNTIME_ROOT/processing-gate.json"
test -s "$RUNTIME_ROOT/capture-cursor.json"
for _ in $(seq 1 30); do
  [[ -s "$RUNTIME_ROOT/capture-consumer-status.json" ]] && break
  sleep 1
done
test -s "$RUNTIME_ROOT/capture-consumer-status.json"
grep -qE '^OPENAI_API_KEY=' "$HERMES_HOME/.env"
grep -qE '^CHRISTOPHER_TGG_PS_SERVICE_TOKEN=' "$HERMES_HOME/.env"
test -L "$HERMES_HOME/plugins/report-operations"
if systemctl is-enabled --quiet christopher-tgg-report-weekly.timer; then
  echo "weekly report timer must ship disabled" >&2
  exit 35
fi
if systemctl is-enabled --quiet christopher-tgg-report-monthly.timer; then
  echo "monthly report timer must ship disabled" >&2
  exit 35
fi
"$APP_ROOT/.venv/bin/python" \
  "$DEPLOY_ROOT/scripts/validate_deployment_spec.py" \
  --app-root "$RELEASE_ROOT" \
  --spec "$DEPLOY_ROOT/client-agent-deployment.yaml" >/dev/null

if grep -q '/messages' "$DEPLOY_ROOT/systemd/christopher-tgg-hermes.service"; then
  echo "consumer unit must never reference destructive /messages" >&2
  exit 31
fi
grep -q '/var/lib/tgg-capture/whatsapp/capture/events.jsonl' \
  "$DEPLOY_ROOT/systemd/christopher-tgg-hermes.service"

if python3 - "$RUNTIME_ROOT/capture-consumer-status.json" <<'PY'
import json, pathlib, sys
status = json.loads(pathlib.Path(sys.argv[1]).read_text())
raise SystemExit(0 if status.get("state") == "held" else 1)
PY
then
  echo "fatal consumer state: held" >&2
  exit 34
fi

main_pid="$(systemctl show -p MainPID --value christopher-tgg-hermes.service)"
if [[ ! "$main_pid" =~ ^[1-9][0-9]*$ ]]; then
  echo "Christopher consumer has no live MainPID" >&2
  exit 32
fi
python3 - "$main_pid" <<'PY'
import pathlib
import sys

raw = pathlib.Path(f"/proc/{sys.argv[1]}/environ").read_bytes()
env = {}
for item in raw.split(b"\0"):
    if b"=" in item:
        key, value = item.split(b"=", 1)
        env[key.decode(errors="replace")] = value.decode(errors="replace")
for key in ("HERMES_TIMEZONE", "TZ"):
    assert env.get(key, "") in {"", "Asia/Singapore"}, (key, env.get(key))
PY
for _ in $(seq 1 30); do
  if python3 - "$RUNTIME_ROOT/capture-consumer-status.json" "$main_pid" <<'PY'
import json, pathlib, sys
try:
    status = json.loads(pathlib.Path(sys.argv[1]).read_text())
except (OSError, ValueError):
    raise SystemExit(1)
raise SystemExit(0 if status.get("pid") == int(sys.argv[2]) and status.get("scheduler_mode") == "per-chat-parallel" else 1)
PY
  then
    break
  fi
  sleep 1
done
python3 - "$RUNTIME_ROOT/capture-consumer-status.json" "$main_pid" <<'PY'
import json, pathlib, sys
status = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert status.get("pid") == int(sys.argv[2]), (status.get("pid"), int(sys.argv[2]))
assert status.get("scheduler_mode") == "per-chat-parallel"
PY
APP_ROOT="$APP_ROOT" VERIFY_PYTHON="$APP_ROOT/.venv/bin/python" \
  "$0" --verify-status-contract \
  "$HERMES_HOME/config.yaml" \
  "$RUNTIME_ROOT/processing-gate.json" \
  "$RUNTIME_ROOT/capture-consumer-status.json"
if python3 - "$HERMES_HOME/config.yaml" <<'PY'
import sys, yaml
raise SystemExit(0 if not yaml.safe_load(open(sys.argv[1]))["pa"]["enabled"] else 1)
PY
then
  APP_ROOT="$APP_ROOT" VERIFY_PYTHON="$APP_ROOT/.venv/bin/python" \
    "$0" --verify-disabled-cursor-contract \
    "$RUNTIME_ROOT/processing-gate.json" \
    "$RUNTIME_ROOT/capture-cursor.json"
  APP_ROOT="$APP_ROOT" VERIFY_PYTHON="$APP_ROOT/.venv/bin/python" \
    "$0" --verify-standby-inbox-contract \
    "$HERMES_HOME/config.yaml" \
    "$RUNTIME_ROOT/processing-gate.json" \
    "$RUNTIME_ROOT/capture-consumer-status.json" \
    "$RUNTIME_ROOT/capture-inbox.db"
fi
if python3 - "$RUNTIME_ROOT/capture-consumer-status.json" <<'PY'
import json, pathlib, sys
raise SystemExit(0 if not json.loads(pathlib.Path(sys.argv[1]).read_text()).get("processing_enabled") else 1)
PY
then
  for fd in /proc/"$main_pid"/fd/*; do
    target="$(readlink "$fd" 2>/dev/null || true)"
    if [[ "$target" == /var/lib/tgg-capture/* ]]; then
      echo "disabled consumer unexpectedly opened capture state: $target" >&2
      exit 33
    fi
  done
fi

"$APP_ROOT/.venv/bin/python" - "$APP_ROOT" "$HERMES_HOME" "$DEPLOY_ROOT" <<'PY'
import datetime, hashlib, importlib.util, json, os, pathlib, sqlite3, stat, subprocess, sys, time, urllib.parse, urllib.request, yaml

app = pathlib.Path(sys.argv[1])
home = pathlib.Path(sys.argv[2])
deploy = pathlib.Path(sys.argv[3])
runtime = home / "runtime"
slot = (runtime / "engine-slot").read_text().strip()
profile_path = runtime / "provider-profile.json"
profile = json.loads(profile_path.read_text()) if profile_path.exists() else {
    "version": 1, "provider": "openai-direct-primary", "credential_label": None,
}
assert profile.get("version") == 1
provider = profile.get("provider")
credential_label = profile.get("credential_label")
assert provider in {"openai-direct-primary", "openai-codex"}
if provider == "openai-codex":
    assert isinstance(credential_label, str) and credential_label.strip()
else:
    assert credential_label is None
# slot id -> model. Suffixed slots pin an explicit reasoning effort.
SLOT_MODELS = {
    "gpt-5.4-mini": "gpt-5.4-mini",
    "gpt-5.6-luna": "gpt-5.6-luna",
    "gpt-5.6-luna-low": "gpt-5.6-luna",
    "gpt-5.6-luna-xhigh": "gpt-5.6-luna",
    "gpt-5.6-terra-medium": "gpt-5.6-terra",
    "gpt-5.6-terra-high": "gpt-5.6-terra",
    "gpt-6-sol-medium": "gpt-6-sol",
    "gpt-6.1-sol-medium": "gpt-6.1-sol",
}
SLOT_REASONING_EFFORT = {
    "gpt-5.6-luna-low": "low",
    "gpt-5.6-luna-xhigh": "xhigh",
    "gpt-5.6-terra-medium": "medium",
    "gpt-5.6-terra-high": "high",
    "gpt-6-sol-medium": "medium",
    "gpt-6.1-sol-medium": "medium",
}
assert slot in SLOT_MODELS, slot
slot_model = SLOT_MODELS[slot]
slot_effort = SLOT_REASONING_EFFORT.get(slot)

expected = {}
for line in (deploy / "runtime-slots/SHA256SUMS").read_text().splitlines():
    digest, relative = line.split(None, 1)
    expected[relative.strip()] = digest
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

config = yaml.safe_load((home / "config.yaml").read_text())
constitution = yaml.safe_load((home / "christopher_tgg_constitution.yaml").read_text())
management_selector_ids = [
    str((selector.get("match") or {}).get("source.chat_id"))
    for selector in constitution.get("selectors", [])
    if selector.get("job_type") == "tgg_management"
    and (selector.get("match") or {}).get("source.platform") == "whatsapp"
]
assert management_selector_ids == [
    "120363426509183563@g.us",
    "120363407903158826@g.us",
], management_selector_ids
loader_path = deploy / "scripts/apply_engine_slot.py"
loader_spec = importlib.util.spec_from_file_location("christopher_engine_slot_health", loader_path)
loader = importlib.util.module_from_spec(loader_spec)
loader_spec.loader.exec_module(loader)
capability = loader._external_capability(runtime, home, slot)
if capability:
    for plugin_name, plugin_source in capability["plugin_sources"].items():
        plugin_link = home / "plugins" / plugin_name
        assert plugin_link.is_symlink(), plugin_link
        assert plugin_link.resolve(strict=True) == plugin_source.resolve(strict=True)
config_enabled = config["pa"]["enabled"]
assert isinstance(config_enabled, bool)
# Host config is authoritative. Verify only the runtime and capability
# contracts consumed here; unrelated host values must survive deploys.
assert config["group_sessions_per_user"] is False
assert config["timezone"] == "Asia/Singapore"
assert config["session_reset"] == {"mode": "none"}
assert config["platforms"]["whatsapp"]["enabled"] is False
assert config["model"]["provider"] == provider
if provider == "openai-codex":
    assert config["model"]["credential_label"] == credential_label
else:
    assert "credential_label" not in config["model"]
assert config["model"]["default"] == slot_model
if slot_effort is None:
    assert "reasoning_effort" not in config["agent"]
else:
    assert config["agent"]["reasoning_effort"] == slot_effort
assert constitution["runtime"] == {"provider": provider, "model": slot_model}
for brief in constitution.get("job_briefs", {}).values():
    if isinstance(brief.get("runtime"), dict):
        assert brief["runtime"] == {"provider": provider, "model": slot_model}
report_operations = config["pa"]["report_operations"]
assert report_operations["enabled"] is True
assert report_operations["schedule"]["enabled"] is False
assert report_operations["auth"]["token_env"] == "CHRISTOPHER_TGG_PS_SERVICE_TOKEN"
assert {
    "fetch-sources", "preview-reconcile", "apply-reconcile",
    "generate", "get-reports", "status",
}.issubset(report_operations["operations"])
if capability:
    required_plugins = {
        item["id"] for item in capability["compatibility"]["requirements"]
    }
    assert required_plugins.issubset(set(config["plugins"]["enabled"]))
management = constitution["job_briefs"]["tgg_management"]
assert "report-operations" in management["enabled_toolsets"]
assert "report-operations" not in constitution["job_briefs"]["tgg_ops_ingest"]["enabled_toolsets"]
if capability:
    assert "tgg-whatsapp-evidence" in management["enabled_toolsets"]
    assert "tgg-whatsapp-evidence" not in constitution["job_briefs"]["tgg_ops_ingest"]["enabled_toolsets"]
    receipt = json.loads((runtime / "engine-slot-receipt.json").read_text())
    assert receipt["configuration_source"] == "external-capability"
    assert receipt["host_config_authoritative"] is True
    assert receipt["config_sha256"] == sha(home / "config.yaml")
    assert receipt["capability_release_id"] == capability["release_id"]
    assert receipt["capability_manifest_sha256"] == capability["manifest_sha256"]
    # Capability releases pin Systems source compatibility by commit; the
    # endpoint remains host-owned config just like the report operations that
    # use this authenticated read-only health/query path.
    base_url = str(report_operations.get("base_url") or "").strip().rstrip("/")
    parsed_base_url = urllib.parse.urlparse(base_url)
    assert parsed_base_url.scheme == "https" and parsed_base_url.hostname, base_url
    allowed_hosts = set(report_operations.get("allowed_download_hosts") or [])
    assert parsed_base_url.hostname in allowed_hosts, (parsed_base_url.hostname, allowed_hosts)
    health_request = urllib.request.Request(
        base_url + "/api/health",
        headers={"User-Agent": "Christopher-TGG/1.0"},
    )
    with urllib.request.urlopen(health_request, timeout=10) as response:
        health = json.load(response)
    assert health.get("ok") is True
    env = {}
    for raw in (home / ".env").read_text().splitlines():
        if "=" in raw and not raw.lstrip().startswith("#"):
            key, value = raw.split("=", 1)
            env[key.strip()] = value.strip().strip('"').strip("'")
    token = env.get("CHRISTOPHER_TGG_PS_SERVICE_TOKEN")
    assert token
    openai_key = env.get("OPENAI_API_KEY")
    assert openai_key and openai_key.endswith("XK8A")
    query = urllib.request.Request(
        base_url + "/api/operator/query?tenant=tgg",
        method="POST",
        data=json.dumps({"sql": "SELECT message_id, source_ref, chat_jid, chat_name, sender_id, ts, text, message_kind, has_media, media_refs, reply_to_source_ref, raw_json, in_scope FROM message_ledger ORDER BY ts DESC LIMIT 1"}).encode(),
        # The Systems edge denies urllib's default Python user agent (403/1010),
        # even when the service token is valid.  Name this internal verifier so
        # its authenticated read-only health check is stable and auditable.
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Christopher-TGG/1.0",
        },
    )
    with urllib.request.urlopen(query, timeout=10) as response:
        payload = json.load(response)
    rows = payload["data"]["rows"]
    assert rows, "live message ledger is empty"
    required_message_columns = {
        "message_id", "source_ref", "chat_jid", "chat_name", "sender_id",
        "ts", "text", "message_kind", "has_media", "media_refs",
        "reply_to_source_ref", "raw_json", "in_scope",
    }
    assert required_message_columns.issubset(rows[0]), rows[0]
    retained_root = pathlib.Path(config["pa"]["media_retention"]["media_root"]).resolve(strict=True)
    unreadable = subprocess.run(
        ["runuser", "-u", "tggcapture", "--", "find", str(retained_root), "-type", "f", "!", "-readable", "-print", "-quit"],
        cwd="/",
        check=True,
        text=True,
        capture_output=True,
    )
    assert not unreadable.stdout.strip(), unreadable.stdout.strip()
    acl = subprocess.run(
        ["getfacl", "-cp", str(retained_root)],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.splitlines()
    assert "default:user:tggcapture:r-x" in acl, acl

gate = json.loads((runtime / "processing-gate.json").read_text())
assert isinstance(gate["enabled"], bool)
assert gate["enabled"] is config_enabled, (gate["enabled"], config_enabled)
# Generation counts every APPLIED transition, including fail-closed
# rollbacks of attempted activations (activate writes gen N+1, the
# confirmation window lapses, deactivate writes gen N+2 — the counter is
# history, not drift). The disabled-state invariant is gate CONSISTENCY,
# never gate virginity: a virginity assert fails forever after the first
# attempted transition (2026-07-21 activation round 6 scar, generation 1).
assert isinstance(gate["generation"], int) and gate["generation"] >= 0
if gate["generation"] > 0:
    assert gate.get("change_run_id"), "transitioned gate must carry its change run id"
    assert datetime.datetime.fromisoformat(gate["changed_at"]).tzinfo is not None
boundary_raw = gate.get("disabled_at") or gate["initial_disabled_boundary"]
boundary = datetime.datetime.fromisoformat(boundary_raw)
assert boundary.tzinfo is not None
assert boundary <= datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=5)

cursor = json.loads((runtime / "capture-cursor.json").read_text())
assert cursor["source_path"] == "/var/lib/tgg-capture/whatsapp/capture/events.jsonl"

status = json.loads((runtime / "capture-consumer-status.json").read_text())
if capability:
    process_env = pathlib.Path(f"/proc/{status['pid']}/environ").read_bytes().split(b"\0")
    assert not any(item.startswith(b"GEMINI_API_KEY") and item.split(b"=", 1)[-1] for item in process_env)
assert status["gate_generation"] == gate["generation"]
assert status["scheduler_mode"] == "per-chat-parallel"
assert status["site_concurrency"] == 4
assert status["chat_batch_size"] == 25
assert isinstance(status["state_total"], int) and status["state_total"] >= 0
assert sum(status["inbox"].values()) == status["state_total"]
for key in (
    "retention_total",
    "retention_failures",
    "media_root_count",
    "media_root_bytes",
):
    assert isinstance(status.get(key), int) and status[key] >= 0, (key, status.get(key))
free_percent = status.get("media_volume_free_percent")
free_bytes = status.get("media_volume_free_bytes")
if config_enabled:
    assert isinstance(free_percent, (int, float)), free_percent
    assert isinstance(free_bytes, int) and free_bytes >= 0, free_bytes
    reserve = config["pa"]["media_retention"].get("min_free_bytes")
    if reserve is not None:
        assert free_bytes >= int(reserve), (free_bytes, reserve)
    else:
        assert free_percent >= float(config["pa"]["media_retention"]["min_free_percent"]), (
            free_percent,
            config["pa"]["media_retention"]["min_free_percent"],
        )
else:
    assert free_percent is None or isinstance(free_percent, (int, float)), free_percent
    assert free_bytes is None or isinstance(free_bytes, int), free_bytes
if not config_enabled:
    assert status["source_opened"] is False
    assert status["cursor_advanced"] is False

state_db = home / "state.db"
production_turns = 0
if state_db.exists():
    conn = sqlite3.connect(state_db)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "pa_turns" in tables:
            production_turns = conn.execute(
                "SELECT COUNT(*) FROM pa_turns WHERE replay_run_id IS NULL AND started_at > ?",
                (boundary.timestamp(),),
            ).fetchone()[0]
    finally:
        conn.close()
if not config_enabled:
    assert production_turns == 0, production_turns

inbox_db = runtime / "capture-inbox.db"
inbox_rows = 0
if inbox_db.exists():
    conn = sqlite3.connect(inbox_db)
    conn.row_factory = sqlite3.Row
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "ingress_events" in tables:
            inbox_rows = conn.execute("SELECT COUNT(*) FROM ingress_events").fetchone()[0]
            high_water = conn.execute(
                "SELECT value FROM ingress_meta WHERE key='state_total_high_water'"
            ).fetchone()
            assert high_water is not None
            assert int(high_water[0]) == inbox_rows, (int(high_water[0]), inbox_rows)
            active_chats = set(status["active_management_chats"]) | set(status["active_site_chats"])
            processing = conn.execute(
                "SELECT DISTINCT chat_id FROM ingress_events WHERE status='processing'"
            ).fetchall()
            processing_chats = {str(row["chat_id"]) for row in processing}
            assert processing_chats <= active_chats, (processing_chats, active_chats)
            constitution_path = pathlib.Path(config["pa"]["constitution_path"])
            management = {
                str((selector.get("match") or {}).get("source.chat_id"))
                for selector in (yaml.safe_load(constitution_path.read_text()) or {}).get("selectors", [])
                if selector.get("job_type") == "tgg_management"
                and (selector.get("match") or {}).get("source.platform") == "whatsapp"
                and (selector.get("match") or {}).get("source.chat_id")
            }
            if management:
                placeholders = ",".join("?" for _ in management)
                for _ in range(30):
                    pending_management = conn.execute(
                        f"SELECT COUNT(*) FROM ingress_events WHERE status='pending' AND chat_id IN ({placeholders})",
                        tuple(sorted(management)),
                    ).fetchone()[0]
                    current_status = json.loads(
                        (runtime / "capture-consumer-status.json").read_text()
                    )
                    if pending_management == 0 or current_status["active_management_chats"]:
                        break
                    time.sleep(1)
                assert pending_management == 0 or bool(current_status["active_management_chats"]), (
                    pending_management, current_status["active_management_chats"]
                )
            oldest = conn.execute(
                "SELECT MIN(updated_at) FROM ingress_events WHERE status='processing'"
            ).fetchone()[0]
            if oldest:
                oldest_dt = datetime.datetime.fromisoformat(str(oldest))
                age = (datetime.datetime.now(datetime.timezone.utc) - oldest_dt).total_seconds()
                assert age <= int(status["claim_stale_seconds"]), (age, status["claim_stale_seconds"])
    finally:
        conn.close()
config_mode = stat.S_IMODE((home / "config.yaml").stat().st_mode)
assert config_mode == 0o640, oct(config_mode)
assert (home / "config.yaml").stat().st_uid == 0
print(json.dumps({
    "quick_verify": "pass",
    "slot": slot,
    "provider": provider,
    "model": slot_model,
    "reasoning_effort": slot_effort,
    "config_sha256": sha(home / "config.yaml"),
    "constitution_sha256": sha(home / "christopher_tgg_constitution.yaml"),
    "processing_enabled": config_enabled,
    "production_turns_after_disabled_boundary": production_turns,
    "production_inbox_rows": inbox_rows,
    "disabled_cursor_boundary_verified": (not config_enabled),
    "scheduler_mode": status["scheduler_mode"],
    "state": status["state"],
    "state_total": status["state_total"],
    "retention_held": status["retention_held"],
    "retention_quarantined": status["retention_quarantined"],
    "retention_quarantine_status": status["retention_quarantine_status"],
    "retention_quarantine_message_ids": status["retention_quarantine_message_ids"],
    "retention_hold": status["retention_hold"],
    "configuration_source": "external-capability" if capability else "repo-engine-slot",
    "capability_release_id": capability["release_id"] if capability else None,
    "capability_manifest_sha256": capability["manifest_sha256"] if capability else None,
}, sort_keys=True))
PY

if [[ -s "$RUNTIME_ROOT/provider-profile.json" ]] && [[ "$("$APP_ROOT/.venv/bin/python" - "$RUNTIME_ROOT/provider-profile.json" <<'PY'
import json, pathlib, sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text())["provider"])
PY
)" == "openai-codex" ]]; then
  codex_label="$("$APP_ROOT/.venv/bin/python" - "$RUNTIME_ROOT/provider-profile.json" <<'PY'
import json, pathlib, sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text())["credential_label"])
PY
)"
  runuser -u pclaw -- env HERMES_HOME="$HERMES_HOME" \
    PYTHONPATH="$APP_ROOT${PYTHONPATH:+:$PYTHONPATH}" \
    "$APP_ROOT/.venv/bin/python" "$DEPLOY_ROOT/scripts/verify_codex_auth.py" \
      --hermes-home "$HERMES_HOME" --credential-label "$codex_label" --service-user pclaw
fi

if [[ "$MODE" == "--quick" ]]; then
  exit 0
fi

for scenario in clean corrupt; do
  scenario_report="$TEST_HOME/latest-report-${scenario}-verification.json"
  runuser -u pclaw -- env \
    HERMES_HOME="$HERMES_HOME" \
    PYTHONPATH="$APP_ROOT${PYTHONPATH:+:$PYTHONPATH}" \
      "$APP_ROOT/.venv/bin/python" "$DEPLOY_ROOT/scripts/run_isolated_smoke.py" \
    --app-root "$APP_ROOT" \
    --live-home "$HERMES_HOME" \
    --test-root "$TEST_HOME" \
    --slot-file "$RUNTIME_ROOT/engine-slot" \
    --report "$scenario_report" \
    --chat-id 120363409954029949@g.us \
    --chat-name "Christopher Deployment Verification" \
    --body "run weekly report" \
    --report-ops-scenario "$scenario"
done

"$APP_ROOT/.venv/bin/python" - "$TEST_HOME/latest-report-clean-verification.json" "$TEST_HOME/latest-report-corrupt-verification.json" <<'PY'
import json, pathlib, sys
clean = json.loads(pathlib.Path(sys.argv[1]).read_text())
corrupt = json.loads(pathlib.Path(sys.argv[2]).read_text())
assert clean["external_outbound_sent"] == 0
assert corrupt["external_outbound_sent"] == 0
clean_cycle_paths = [
    path for path in clean["report_ops_request_paths"]
    if path.startswith("/api/operator/report-cycle/")
]
corrupt_cycle_paths = [
    path for path in corrupt["report_ops_request_paths"]
    if path.startswith("/api/operator/report-cycle/")
]
assert clean_cycle_paths == [
    "/api/operator/report-cycle/status?tenant=tgg",
    "/api/operator/report-cycle/fetch-sources?tenant=tgg",
    "/api/operator/report-cycle/preview-reconcile?tenant=tgg",
    "/api/operator/report-cycle/apply-reconcile?tenant=tgg",
    "/api/operator/report-cycle/generate?tenant=tgg",
    "/api/operator/report-cycle/get-reports?tenant=tgg",
]
assert corrupt_cycle_paths == [
    "/api/operator/report-cycle/status?tenant=tgg",
    "/api/operator/report-cycle/fetch-sources?tenant=tgg",
]
print(json.dumps({
    "report_judgment_fixture": "pass",
    "clean_chain": 6,
    "corrupt_chain": 2,
    "attachments": 4,
    "external_outbound_sent": 0,
}, sort_keys=True))
PY

# Verify the sole configured model provider and its credential without
# producing client-visible output or mutating any client system.
runuser -u pclaw -- env \
  HERMES_HOME="$HERMES_HOME" \
  "$APP_ROOT/.venv/bin/python" - <<'PY'
import json, urllib.request
from pathlib import Path
from dotenv import dotenv_values

values = dotenv_values(Path.home() / ".hermes-christopher-tgg/.env")
key = values.get("OPENAI_API_KEY")
assert key
request = urllib.request.Request(
    "https://api.openai.com/v1/models",
    headers={"Authorization": f"Bearer {key}"},
)
with urllib.request.urlopen(request, timeout=30) as response:
    payload = json.load(response)
assert isinstance(payload.get("data"), list) and payload["data"]
print(json.dumps({"provider": "openai-direct-primary", "reachable": True}))
PY

full_report="$TEST_HOME/latest-full-verification.json"
runuser -u pclaw -- env \
  HERMES_HOME="$HERMES_HOME" \
  PYTHONPATH="$APP_ROOT${PYTHONPATH:+:$PYTHONPATH}" \
    "$APP_ROOT/.venv/bin/python" "$DEPLOY_ROOT/scripts/run_isolated_smoke.py" \
  --app-root "$APP_ROOT" \
  --live-home "$HERMES_HOME" \
  --test-root "$TEST_HOME" \
  --slot-file "$RUNTIME_ROOT/engine-slot" \
  --report "$full_report"

"$APP_ROOT/.venv/bin/python" - "$full_report" "$RUNTIME_ROOT/engine-slot" "$RUNTIME_ROOT/provider-profile.json" <<'PY'
import json, pathlib, sys
p = json.loads(pathlib.Path(sys.argv[1]).read_text())
slot = pathlib.Path(sys.argv[2]).read_text().strip()
provider = json.loads(pathlib.Path(sys.argv[3]).read_text())["provider"]
SLOT_MODELS = {
    "gpt-5.4-mini": "gpt-5.4-mini",
    "gpt-5.6-luna": "gpt-5.6-luna",
    "gpt-5.6-luna-low": "gpt-5.6-luna",
    "gpt-5.6-luna-xhigh": "gpt-5.6-luna",
    "gpt-5.6-terra-medium": "gpt-5.6-terra",
    "gpt-5.6-terra-high": "gpt-5.6-terra",
    "gpt-6-sol-medium": "gpt-6-sol",
    "gpt-6.1-sol-medium": "gpt-6.1-sol",
}
SLOT_REASONING_EFFORT = {
    "gpt-5.6-luna-low": "low",
    "gpt-5.6-luna-xhigh": "xhigh",
    "gpt-5.6-terra-medium": "medium",
    "gpt-5.6-terra-high": "high",
    "gpt-6-sol-medium": "medium",
    "gpt-6.1-sol-medium": "medium",
}
assert slot in SLOT_MODELS, slot
assert p["ok"] is True
assert p["mode"] == "fixture-only"
assert p["result"]["processed"] == 1
assert p["result"]["turn_id"]
assert p["result"]["provider"] == provider
assert p["result"]["model"] == SLOT_MODELS[slot], (p["result"]["model"], slot)
assert p["client_mutation_requests"] == 0
assert p["external_outbound_sent"] == 0
print(json.dumps({
    "full_verify": "pass",
    "slot": slot,
    "turn_id": p["result"]["turn_id"],
    "provider": p["result"]["provider"],
    "model": p["result"]["model"],
    "reasoning_effort": SLOT_REASONING_EFFORT.get(slot),
    "client_mutation_requests": 0,
    "external_outbound_sent": 0,
}, sort_keys=True))
PY
