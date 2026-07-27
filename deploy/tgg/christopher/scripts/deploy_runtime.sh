#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="$(git rev-parse --show-toplevel)"
SPEC_REL="deploy/tgg/christopher/client-agent-deployment.yaml"
SPEC_PATH="$APP_ROOT/$SPEC_REL"
VALIDATOR="$APP_ROOT/deploy/tgg/christopher/scripts/validate_deployment_spec.py"
MANIFEST_BUILDER="$APP_ROOT/deploy/tgg/christopher/scripts/build_pa_agent_manifest.py"

read -r client agent system domain expected_target manifest_rel < <(
  python3 - "$SPEC_PATH" <<'PY'
import pathlib, sys, yaml
d = yaml.safe_load(pathlib.Path(sys.argv[1]).read_text())
print(
    d["metadata"]["client"],
    d["metadata"]["agent"],
    "christopher",
    "pa",
    d["spec"]["host"]["resolution"]["expectedSshTargetAlias"],
    d["spec"]["deploy"]["manifestRef"],
)
PY
)
manifest_path="$APP_ROOT/$manifest_rel"

python3 "$MANIFEST_BUILDER" \
  --app-root "$APP_ROOT" \
  --manifest "$manifest_path" \
  --check
python3 "$VALIDATOR" --app-root "$APP_ROOT" --spec "$SPEC_PATH"

if [[ -n "$(git -C "$APP_ROOT" status --porcelain)" ]]; then
  echo "deployment source is dirty; commit and push the complete spec first" >&2
  exit 11
fi
git -C "$APP_ROOT" fetch origin
head_sha="$(git -C "$APP_ROOT" rev-parse HEAD)"
origin_sha="$(git -C "$APP_ROOT" rev-parse origin/main)"
if [[ "$head_sha" != "$origin_sha" ]]; then
  echo "deployment source is not the exact origin/main head: local=$head_sha origin=$origin_sha" >&2
  exit 12
fi

service_raw="$(pcl service locate --system "$system" --domain "$domain")"
target="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["system"]["liveFacts"]["host"]["sshTargetAlias"])' <<<"$service_raw")"
if [[ "$target" != "$expected_target" ]]; then
  echo "deployment target mismatch: spec=$expected_target substrate=$target" >&2
  exit 13
fi
remote_hostname="$(ssh "$target" hostname)"
if [[ "$remote_hostname" != "$expected_target" ]]; then
  echo "remote host identity mismatch: target=$target hostname=$remote_hostname" >&2
  exit 13
fi

receipt_root="${PCL_CLIENT_DATA_ROOT:-$HOME/pcl-client-data}/tgg/deploy/$(date -u +%Y%m%dT%H%M%SZ)-hermes"
mkdir -p "$receipt_root"
chmod 0700 "$receipt_root"
printf '%s\n' "$head_sha" >"$receipt_root/source-commit.txt"
cp "$SPEC_PATH" "$receipt_root/client-agent-deployment.yaml"

"$APP_ROOT/deploy/tgg/christopher/scripts/prepare_host_secrets.sh" \
  >"$receipt_root/prepare-secrets.txt"

bundle_raw="$receipt_root/bundle.json"
pcl pa-agent bundle \
  --client "$client" \
  --agent "$agent" \
  --repo "$APP_ROOT" \
  --manifest "$manifest_path" >"$bundle_raw"
bundle_path="$(python3 - "$bundle_raw" <<'PY'
import json, pathlib, sys
raw = pathlib.Path(sys.argv[1]).read_text().strip()
try:
    payload = json.loads(raw)
except json.JSONDecodeError:
    payload = next(
        json.loads(line)
        for line in reversed(raw.splitlines())
        if line.lstrip().startswith("{")
    )
if not payload.get("ok"):
    raise SystemExit(f"bundle failed: {payload}")
print(payload["data"]["bundlePath"])
PY
)"
test -s "$bundle_path/manifest.json"

# The first deploy command is a non-mutating gate over registry state, remote
# drift, target identity, and the exact immutable bundle.
pcl pa-agent deploy \
  --client "$client" \
  --agent "$agent" \
  --target "$target" \
  --bundle "$bundle_path" \
  --dry-run >"$receipt_root/deploy-dry-run.json"

# No --override path exists in this wrapper. Registry gaps stop the deploy and
# return to the sprint driver rather than becoming a local judgment call.
pcl pa-agent deploy \
  --client "$client" \
  --agent "$agent" \
  --target "$target" \
  --bundle "$bundle_path" >"$receipt_root/deploy.json"

pcl pa-agent verify \
  --client "$client" \
  --agent "$agent" \
  --target "$target" >"$receipt_root/verify.json"

ssh "$target" 'set -euo pipefail
systemctl is-active christopher-tgg-hermes.service
systemctl is-enabled christopher-tgg-hermes.service
systemctl is-active christopher-tgg-hermes-health.timer
systemctl is-enabled christopher-tgg-hermes-health.timer
systemctl show christopher-tgg-hermes.service -p MainPID -p ActiveEnterTimestamp -p FragmentPath --no-pager
cat /home/pclaw/.hermes-christopher-tgg/runtime/engine-slot-receipt.json
cat /home/pclaw/.hermes-christopher-tgg/runtime/capture-consumer-status.json' \
  >"$receipt_root/live-state.txt"

evaluator_stdout="$receipt_root/output-quality-eval.stdout.json"
evaluator_stderr="$receipt_root/output-quality-eval.stderr.txt"
set +e
python3 - \
  "$APP_ROOT/deploy/tgg/christopher/scripts/output_quality_eval.py" \
  "$head_sha" "$evaluator_stdout" "$evaluator_stderr" <<'PY'
import pathlib
import subprocess
import sys

evaluator, head_sha, stdout_name, stderr_name = sys.argv[1:]
try:
    result = subprocess.run(
        [
            evaluator,
            "run",
            "--trigger",
            "deploy",
            "--maker-session-id",
            f"deploy:{head_sha}",
        ],
        text=True,
        capture_output=True,
        timeout=3900,
        check=False,
    )
    pathlib.Path(stdout_name).write_text(result.stdout)
    pathlib.Path(stderr_name).write_text(result.stderr)
    raise SystemExit(result.returncode)
except subprocess.TimeoutExpired as exc:
    stdout = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
    stderr = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
    pathlib.Path(stdout_name).write_text(stdout)
    pathlib.Path(stderr_name).write_text(stderr + "\nevaluator timed out after 3900 seconds\n")
    raise SystemExit(124)
PY
evaluator_rc=$?
set -e
evaluator_ok="$(python3 - "$evaluator_stdout" "$evaluator_stderr" \
  "$receipt_root/output-quality-eval.json" "$evaluator_rc" <<'PY'
import json
import pathlib
import sys

stdout_path, stderr_path, receipt_path = map(pathlib.Path, sys.argv[1:4])
returncode = int(sys.argv[4])
try:
    stdout_text = stdout_path.read_text()
except OSError as exc:
    stdout_text = f"<unreadable evaluator stdout: {exc}>"
try:
    output = json.loads(stdout_text)
except json.JSONDecodeError:
    output = {"ok": False, "raw_stdout": stdout_text}
evaluator_ok = returncode == 0 and output.get("ok") is True
receipt = {
    "evaluator_ok": evaluator_ok,
    "returncode": returncode,
    "output": output,
    "stderr": stderr_path.read_text(errors="replace"),
}
receipt_path.write_text(json.dumps(receipt, sort_keys=True) + "\n")
print("true" if evaluator_ok else "false")
PY
)"

printf '{"ok":true,"target":"%s","source_commit":"%s","bundle_path":"%s","receipt_root":"%s","evaluator_ok":%s}\n' \
  "$target" "$head_sha" "$bundle_path" "$receipt_root" "$evaluator_ok"
