#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <slot> [gpt-6.2-sol-medium|none]; slot is gpt-5.4-mini|gpt-5.6-luna|gpt-5.6-luna-low|gpt-5.6-luna-xhigh|gpt-5.6-terra-medium|gpt-5.6-terra-high|gpt-6-sol-medium|gpt-6.1-sol-medium|gpt-6.1-terra-medium; the optional second argument sets the Management chats' engine (none = the slot's engine); omitted keeps the current one" >&2
  exit 2
fi
case "$1" in
  gpt-5.4-mini|gpt-5.6-luna|gpt-5.6-luna-low|gpt-5.6-luna-xhigh|gpt-5.6-terra-medium|gpt-5.6-terra-high|gpt-6-sol-medium|gpt-6.1-sol-medium|gpt-6.1-terra-medium) ;;
  *) echo "invalid engine slot: $1" >&2; exit 2 ;;
esac

management_args=()
if [[ $# -eq 2 ]]; then
  case "$2" in
    gpt-6.2-sol-medium|none) management_args=(--management-engine "$2") ;;
    *) echo "invalid management engine: $2" >&2; exit 2 ;;
  esac
fi

# The service runs apply_engine_slot.py from the standalone runtime pointer at
# every start, so a switch must use the same copy.  The standalone runtime has
# no virtualenv of its own; the service interpreter is the app checkout's.
APP_ROOT="${APP_ROOT:-/opt/tgg-christopher/runtime/current}"
PYTHON="${PYTHON:-/home/pclaw/apps/hermes-pcl/.venv/bin/python}"
HERMES_HOME="${HERMES_HOME:-/home/pclaw/.hermes-christopher-tgg}"
DEPLOY_ROOT="$APP_ROOT/deploy/tgg/christopher"
old_slot="$(<"$HERMES_HOME/runtime/engine-slot")"
old_management="none"
if [[ -s "$HERMES_HOME/runtime/management-engine" ]]; then
  old_management="$(<"$HERMES_HOME/runtime/management-engine")"
fi

restart_service() {
  systemctl reset-failed christopher-tgg-hermes.service
  systemctl restart christopher-tgg-hermes.service
}
restore_previous_slot() {
  "$PYTHON" "$DEPLOY_ROOT/scripts/apply_engine_slot.py" \
    --app-root "$APP_ROOT" --hermes-home "$HERMES_HOME" --slot "$old_slot" \
    --management-engine "$old_management"
  restart_service
  "$PYTHON" "$DEPLOY_ROOT/scripts/verify_release_minimal.py" \
    --app-root "$APP_ROOT" --hermes-home "$HERMES_HOME" --python "$PYTHON"
  echo "engine switch failed; previous slot restored" >&2
  exit 1
}

"$PYTHON" "$DEPLOY_ROOT/scripts/apply_engine_slot.py" \
  --app-root "$APP_ROOT" \
  --hermes-home "$HERMES_HOME" \
  --slot "$1" \
  ${management_args[@]+"${management_args[@]}"}
if ! restart_service; then
  restore_previous_slot
fi
if ! "$PYTHON" "$DEPLOY_ROOT/scripts/verify_release_minimal.py" \
  --app-root "$APP_ROOT" --hermes-home "$HERMES_HOME" --python "$PYTHON"; then
  restore_previous_slot
fi
