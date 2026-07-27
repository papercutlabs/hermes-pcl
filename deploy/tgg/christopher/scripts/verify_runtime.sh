#!/usr/bin/env bash
set -euo pipefail

MODE="${1:---quick}"
APP_ROOT="${APP_ROOT:-/home/pclaw/apps/hermes-pcl}"
HERMES_HOME="${HERMES_HOME:-/home/pclaw/.hermes-christopher-tgg}"
TEST_HOME="${TEST_HOME:-/home/pclaw/.hermes-christopher-tgg-test}"
DEPLOY_ROOT="$APP_ROOT/deploy/tgg/christopher"
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
retention_hold = status.get("retention_hold")
assert isinstance(retention_held, int) and not isinstance(retention_held, bool), retention_held
assert retention_held >= 0, retention_held
has_retention_hold = isinstance(retention_hold, str) and bool(retention_hold.strip())
if not config_enabled:
    assert state == "standby", (state, "standby")
    assert retention_held == 0, retention_held
    assert not has_retention_hold, retention_hold
elif retention_held > 0 or has_retention_hold:
    assert retention_held > 0, retention_held
    assert has_retention_hold, retention_hold
    assert state == "held-pending", (state, "held-pending")
else:
    assert state == "running", (state, "running")
PY
fi

if [[ "$MODE" == "--check-mode" ]]; then
  raw="$(pcl service locate --system christopher --domain pa)"
  target="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["data"]["system"]["liveFacts"]["host"]["sshTargetAlias"])' <<<"$raw")"
  exec ssh "$target" "$DEPLOY_ROOT/scripts/verify_runtime.sh" --full
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
assert set(datasets) == {"cases", "media"}, sorted(datasets)
for name in ("cases", "media"):
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
grep -qE '^GEMINI_API_KEY=' "$HERMES_HOME/.env"

"$APP_ROOT/.venv/bin/python" \
  "$DEPLOY_ROOT/scripts/validate_deployment_spec.py" \
  --app-root "$APP_ROOT" \
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

"$APP_ROOT/.venv/bin/python" - "$APP_ROOT" "$HERMES_HOME" <<'PY'
import datetime, hashlib, json, os, pathlib, sqlite3, stat, sys, yaml

app = pathlib.Path(sys.argv[1])
home = pathlib.Path(sys.argv[2])
deploy = app / "deploy/tgg/christopher"
runtime = home / "runtime"
slot = (runtime / "engine-slot").read_text().strip()
# slot id -> model. gpt-5.6-luna-low runs gpt-5.6-luna at reasoning_effort low.
SLOT_MODELS = {
    "gpt-5.4-mini": "gpt-5.4-mini",
    "gpt-5.6-luna": "gpt-5.6-luna",
    "gpt-5.6-luna-low": "gpt-5.6-luna",
}
SLOT_REASONING_EFFORT = {"gpt-5.6-luna-low": "low"}
assert slot in SLOT_MODELS, slot
slot_model = SLOT_MODELS[slot]
slot_effort = SLOT_REASONING_EFFORT.get(slot)

expected = {}
for line in (deploy / "runtime-slots/SHA256SUMS").read_text().splitlines():
    digest, relative = line.split(None, 1)
    expected[relative.strip()] = digest
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

assert sha(home / "christopher_tgg_constitution.yaml") == expected[f"{slot}/christopher_tgg_constitution.yaml"]
config = yaml.safe_load((home / "config.yaml").read_text())
slot_config = yaml.safe_load((deploy / "runtime-slots" / slot / "config.yaml").read_text())
constitution = yaml.safe_load((home / "christopher_tgg_constitution.yaml").read_text())
config_enabled = config["pa"]["enabled"]
assert isinstance(config_enabled, bool)
# ExecStartPre preserves the activation-owned live key while every authored
# slot stays disabled by default.  Normalize only that key before comparing;
# every other config field must remain byte-semantically equal to the slot.
normalized_config = json.loads(json.dumps(config))
normalized_config["pa"]["enabled"] = False
assert normalized_config == slot_config
assert config["group_sessions_per_user"] is False
assert config["platforms"]["whatsapp"]["enabled"] is False
assert config["model"]["provider"] == "openai-direct-primary"
assert config["model"]["default"] == slot_model
if slot_effort is None:
    assert "reasoning_effort" not in config["agent"]
else:
    assert config["agent"]["reasoning_effort"] == slot_effort
assert constitution["runtime"] == {"provider": "openai-direct-primary", "model": slot_model}

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
if not config_enabled:
    assert int(cursor["offset"]) == int(cursor["initial_offset"])

status = json.loads((runtime / "capture-consumer-status.json").read_text())
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
if config_enabled:
    assert isinstance(free_percent, (int, float)), free_percent
    assert free_percent >= float(config["pa"]["media_retention"]["min_free_percent"]), (
        free_percent,
        config["pa"]["media_retention"]["min_free_percent"],
    )
else:
    assert free_percent is None or isinstance(free_percent, (int, float)), free_percent
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
                pending_management = conn.execute(
                    f"SELECT COUNT(*) FROM ingress_events WHERE status='pending' AND chat_id IN ({placeholders})",
                    tuple(sorted(management)),
                ).fetchone()[0]
                assert pending_management == 0 or bool(status["active_management_chats"]), (
                    pending_management, status["active_management_chats"]
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
if not config_enabled:
    assert inbox_rows == 0, inbox_rows

config_mode = stat.S_IMODE((home / "config.yaml").stat().st_mode)
assert config_mode == 0o640, oct(config_mode)
assert (home / "config.yaml").stat().st_uid == 0
print(json.dumps({
    "quick_verify": "pass",
    "slot": slot,
    "provider": "openai-direct-primary",
    "model": slot_model,
    "reasoning_effort": slot_effort,
    "config_sha256": sha(home / "config.yaml"),
    "constitution_sha256": sha(home / "christopher_tgg_constitution.yaml"),
    "processing_enabled": config_enabled,
    "production_turns_after_disabled_boundary": production_turns,
    "production_inbox_rows": inbox_rows,
    "cursor_unchanged_while_disabled": (not config_enabled),
    "scheduler_mode": status["scheduler_mode"],
    "state": status["state"],
    "state_total": status["state_total"],
    "retention_held": status["retention_held"],
    "retention_hold": status["retention_hold"],
}, sort_keys=True))
PY

if [[ "$MODE" == "--quick" ]]; then
  exit 0
fi

# Verify the configured auxiliary model and its provider credential without
# producing client-visible output or mutating any client system.
runuser -u pclaw -- env \
  HERMES_HOME="$HERMES_HOME" \
  "$APP_ROOT/.venv/bin/python" - <<'PY'
import json, urllib.request
from pathlib import Path
from dotenv import dotenv_values

values = dotenv_values(Path.home() / ".hermes-christopher-tgg/.env")
key = values.get("GEMINI_API_KEY")
assert key
request = urllib.request.Request(
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite",
    headers={"x-goog-api-key": key},
)
with urllib.request.urlopen(request, timeout=30) as response:
    payload = json.load(response)
assert payload.get("name", "").endswith("gemini-3.1-flash-lite"), payload.get("name")
print(json.dumps({"auxiliary_provider": "gemini", "auxiliary_model": "gemini-3.1-flash-lite", "reachable": True}))
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

"$APP_ROOT/.venv/bin/python" - "$full_report" "$RUNTIME_ROOT/engine-slot" <<'PY'
import json, pathlib, sys
p = json.loads(pathlib.Path(sys.argv[1]).read_text())
slot = pathlib.Path(sys.argv[2]).read_text().strip()
SLOT_MODELS = {
    "gpt-5.4-mini": "gpt-5.4-mini",
    "gpt-5.6-luna": "gpt-5.6-luna",
    "gpt-5.6-luna-low": "gpt-5.6-luna",
}
SLOT_REASONING_EFFORT = {"gpt-5.6-luna-low": "low"}
assert slot in SLOT_MODELS, slot
assert p["ok"] is True
assert p["mode"] == "fixture-only"
assert p["result"]["processed"] == 1
assert p["result"]["turn_id"]
assert p["result"]["provider"] == "openai-direct-primary"
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
