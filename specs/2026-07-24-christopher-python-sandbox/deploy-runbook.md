# Christopher python sandbox — Sunday deploy runbook

This runbook is for the driver. The build worker does not deploy and does not touch `tgg-app-1`.

## Preconditions

- Deploy the reviewed commit from canonical `main`, not a worker tree.
- `deploy/tgg/christopher/pa-agent.hermes.manifest.json` bundle check is green.
- The host-side read-only probe already established unprivileged user namespaces and the 48.4 MB database size. Re-run the runtime invariant after the tree lands; do not rely on the earlier probe as current state.

## Install the sandbox runtime extra

After the reviewed tree is staged at `/home/pclaw/apps/hermes-pcl`, install the exact pinned extra into the deployed venv before restarting Christopher:

```bash
cd /home/pclaw/apps/hermes-pcl
/home/pclaw/apps/hermes-pcl/.venv/bin/pip install -e '.[sandbox]'
/home/pclaw/apps/hermes-pcl/.venv/bin/python - <<'PY'
import numpy, openpyxl, pandas
print(numpy.__version__, pandas.__version__, openpyxl.__version__)
PY
```

Expected versions: numpy 2.4.3, pandas 3.0.5, openpyxl 3.1.5.

## Stage, verify, and flip

Use the existing Christopher bundle/engine-slot path. No systemd unit change is part of this deploy.

```bash
cd /home/pclaw/apps/hermes-pcl
python deploy/tgg/christopher/scripts/validate_deployment_spec.py \
  --app-root /home/pclaw/apps/hermes-pcl \
  --spec deploy/tgg/christopher/client-agent-deployment.yaml
sudo deploy/tgg/christopher/scripts/verify_runtime.sh --quick
```

The quick invariant must prove, from the live config, that:

- `unshare --user --map-root-user --net --mount --pid --fork --kill-child true` succeeds as `pclaw`;
- both configured dataset source paths exist;
- the active config and constitution match the selected engine slot.

Then flip with the existing engine-slot deploy procedure and run:

```bash
sudo deploy/tgg/christopher/scripts/verify_runtime.sh --full
journalctl -u christopher-tgg-hermes.service --since '-10 min' --no-pager
```

## Canned sandbox smoke

Run the repository's jailed E2E fixture as `pclaw` with the deployed venv. A skipped jail test is a failure on this host.

```bash
cd /home/pclaw/apps/hermes-pcl
sudo -u pclaw env HERMES_HOME=/home/pclaw/.hermes-christopher-tgg \
  .venv/bin/pytest -q tests/test_python_sandbox_tool.py -m sandbox_e2e
```

Pass requires the reconciliation counts, no-network proof, read/write escape proofs, timeout cleanup, OOM classification, and WAL snapshot test to pass.

## Kill-switch rollback drill

Exercise this before the client demo:

1. Confirm `python_sandbox.enabled: true`, restart, and verify the tool appears in the management brief schema.
2. Set `python_sandbox.enabled: false`, restart, and verify the tool is absent from the schema and a direct handler call returns `unavailable` rather than running unjailed.
3. Restore `enabled: true`, restart, re-run `verify_runtime.sh --quick`, and repeat one canned reconciliation.

If any step fails, leave the kill switch false and flip the engine slot back to the prior reviewed tree. Do not bypass the probe or run a degraded subprocess.

## XLSX return path

The v1 sandbox can create a valid workbook in the run's retained `/work`
directory, but that does not make the workbook client-deliverable.

- Return the result summary and counts in WhatsApp.
- Do not emit a local path or promise an attachment. WB `65b4eac4` proves an
  image-only `MEDIA:` path on a worker branch; it is not on `main`, and it does
  not cover XLSX documents.
- The existing portal download route serves registered report artifacts only.
  It does not currently expose arbitrary sandbox-run artifacts.

Until one of those consumers is built and verified, retain the XLSX as operator
evidence. The portal fallback, if selected, must be an authenticated
sandbox-artifact route keyed by run ID and file name. It must resolve only
inside `HERMES_HOME/sandbox_runs/<run_id>/work`, allowlist extension and MIME
type, respect run expiry, return `Content-Disposition: attachment`, and return
404 after expiry. The client receives that authenticated portal URL, never the
host path.

## Demo and close evidence

Run the management-chat demo from design §6.1. Record tool-call count, wall time, independently recomputed counts, snapshot wording in the reply, and the journal window. The ship gate is one sandbox run (at most one self-corrected retry), under 60 seconds, with no service disruption.

Measurement window after demo: two weeks. Success means at least one real batch task per week uses the sandbox without per-item fallback and with zero service disruptions attributable to sandbox runs. At window close, record keep / revise / retire.
