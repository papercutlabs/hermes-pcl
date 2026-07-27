# Build brief — `python_sandbox` tool for Hermes PA runtime [WB:8ddae42d]

**Design authority:** `design.md` in this directory — read it fully first; this brief is the execution decomposition, the design doc wins on any conflict.
**Repo:** hermes-pcl (work against a fresh worktree of origin main; the studied tree was `/Users/pcloffice/pcl/_worktrees/hermes-origin-709f9a23-1784905938`).
**Outcome:** Christopher (TGG PA on the client VPS) can run model-authored Python offline in a kernel-jailed sandbox over whitelisted read-only datasets, so batch tasks (the 3,907-job-number reconciliation class) complete in one tool call instead of per-item LLM reasoning. Client data never leaves the box; the sandbox has no network and no write path except its scratch dir; the tool is instantly disableable.

## Hard constraints (inviolable)

- **No client tokens in shared-plane files.** `tools/python_sandbox_tool.py`, `toolsets.py`, `pyproject.toml`, shared tests must pass `scripts/plane_lint.py` with **zero new entries** vs `plane-lint-baseline.json`. All TGG specifics live in `deploy/tgg/christopher/` (client plane).
- **Fail closed.** Isolation probe failure ⇒ tool unavailable. Never fall back to an unjailed subprocess. No config flag may enable a degraded mode.
- **No systemd unit changes.** The jail must work under the existing `christopher-tgg-hermes.service` hardening (`NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=full`, `User=pclaw`).
- **Additive only.** No behavior change to `execute_code`, existing toolsets, or any brief other than the specified constitution patch.

## Files to create

1. **`tools/python_sandbox_tool.py`** (shared plane) — the whole feature:
   - Config reader for top-level `python_sandbox:` (pattern: `code_execution_tool._load_config` via `hermes_cli.config.read_raw_config`; defensive, never raises).
   - `check_sandbox_available()` — enabled-flag + `shutil.which("unshare")` + live probe `unshare --user --map-root-user --net --mount --pid --fork true` (probe result cached; registry TTL-caches on top).
   - Dataset resolution: `type: sqlite` → parent-side `sqlite3` backup-API snapshot into the run dir (`file:...?mode=ro` URI source; `max_snapshot_mb` guard); `type: path` → resolved real path validated with `tools/path_security.validate_within_dir`, recorded for ro bind mount.
   - Run staging under `$HERMES_HOME/sandbox_runs/<run_id>/` (0700): `script.py`, `inputs/`, `work/`, generated `init.sh`, `meta.json`.
   - Jail launch per design §2.2–2.4: `unshare --user --map-root-user --net --mount --pid --fork --kill-child /bin/sh init.sh`; `init.sh` builds tmpfs jail root, ro-binds `/usr /bin /lib* <venv> /etc-subset`, ro-binds datasets at `/inputs/<name>`, rw-binds `work/` at `/work`, mounts fresh `/proc`, `pivot_root`, `exec <venv-python> -I /script.py`.
   - `preexec_fn` rlimits (CPU 60s, AS 1024MB, FSIZE 64MB, NPROC 64, NOFILE 256, CORE 0 — config-overridable), scrubbed child env (reuse `code_execution_tool._scrub_child_env`) + `SANDBOX_INPUTS`, `RESULT_PATH`, `TMPDIR=/work`, `TZ`, `PYTHONIOENCODING/UTF8/DONTWRITEBYTECODE`.
   - Watchdog loop: interrupt check (`tools.interrupt.is_interrupted`), wall-clock kill (SIGTERM→5s→SIGKILL on the process group), `touch_activity_if_due`.
   - Harvest + return per design §4.4/§4.5: head+tail stdout drain (16KB), stderr tail (4KB), `result.json` parse with 8KB cap → `result_invalid` guidance, `files` listing of `work/`, `strip_ansi` + `agent.redact.redact_sensitive_text` on all model-bound text, distinct statuses (`success/error/timeout/oom/dataset_unknown/result_invalid/unavailable`).
   - Post-run: delete `inputs/` snapshots; TTL prune of `sandbox_runs/` (`artifact_ttl_days`, `max_runs_kept`) at the end of every invocation (mise-en-place — no cron).
   - Schema per design §4.1–4.3 with `dynamic_schema_overrides` injecting the config's dataset names/descriptions and probed libraries (importability of numpy/pandas/openpyxl) into the description.
   - Registration: `registry.register(name="python_sandbox", toolset="python-sandbox", ..., check_fn=..., max_result_size_chars=40_000, dynamic_schema_overrides=...)` at module top level (so `discover_builtin_tools` picks it up).
2. **`tests/test_python_sandbox_tool.py`** — see Tests.
3. **`tests/fixtures/`** shared fixture: small generic sqlite DB + xlsx/csv fixture for the E2E (generic tokens only — do NOT put it under `tests/fixtures/clients/`; it exercises the shared mechanism).

## Files to modify

4. **`toolsets.py`** — add the `"python-sandbox"` toolset entry (generic description, tools `["python_sandbox"]`). Do NOT add to `_HERMES_CORE_TOOLS`.
5. **`pyproject.toml`** — optional extra `sandbox = ["pandas==<current-stable-pin>", "openpyxl==<current-stable-pin>"]` (pin exact versions consistent with the file's style).
6. **`deploy/tgg/christopher/config.yaml`** — `python_sandbox:` section exactly as design §3 (datasets `cases` → `/home/pclaw/.systems-pcl/data/tenants/tgg.db`, `media` → `/home/pclaw/.systems-pcl/data/media/tgg/hermes`; limits block; enabled: true).
7. **`deploy/tgg/christopher/christopher_tgg_constitution.yaml`** — management brief only: add `python-sandbox` to `enabled_toolsets` + the WHEN/WHEN-NOT instruction block from design §6 (verbatim intent, wording may be tightened).
8. **`deploy/tgg/christopher/pa-agent.hermes.manifest.json`** (and the root `pa-agent.manifest.json` if it still enumerates tool files at build time) — add `tools/python_sandbox_tool.py` to the shipped-paths list. Then run `pcl pa-agent bundle --check` (or the repo's equivalent validate script, `deploy/tgg/christopher/scripts/validate_deployment_spec.py`) — a manifest path miss bricks the flip (known trap: de-fusion finding (b)2).
9. **`deploy/tgg/christopher/scripts/verify_runtime.sh`** or `checks/runtime-invariants.json` — add an invariant: if config has `python_sandbox.enabled: true`, the unshare probe must pass and the two dataset source paths must exist.

## Tests required

Unit (run everywhere, no namespaces needed — factor the pure logic out of the launch path):

- config parsing: missing section, malformed limits, defaults, per-call clamping of `timeout_seconds`;
- dataset resolution: unknown name → `dataset_unknown` with valid names; sqlite snapshot produced and source untouched (mtime/hash); snapshot-size guard error; `path` traversal/symlink escape rejected (`validate_within_dir`);
- init.sh generation: golden-file assert of the mount plan for a given dataset set (ro flags on every input bind, rw only on `/work`, no `$HOME`/`HERMES_HOME` binds);
- env construction: secrets scrubbed (reuse `_scrub_child_env` tests as a model), `SANDBOX_INPUTS`/`RESULT_PATH` present;
- output shaping: stdout head+tail truncation marker, result.json 8KB cap → `result_invalid` message, `files` listing, redaction applied;
- check_fn: disabled config → False; missing unshare → False; probe failure → handler returns `unavailable` naming the step.

Sandboxed E2E (marked, auto-skip when the userns probe fails — must RUN in the Linux CI/battery environment and on the VPS smoke, skip on macOS):

- **fixture batch task:** attach fixture sqlite (500 rows) + fixture spreadsheet (600 numbers, 50 missing, 20 mismatched); model-shaped script computes reconciliation; assert exact counts in `result`, summary in stdout, detail CSV listed in `files`;
- **no-network proof:** script attempts `socket.create_connection(("1.1.1.1", 53), 2)` and an HTTP request → both fail; status `success` with the failure printed (the jail, not the script, is under test);
- **no-write-escape proof:** script attempts writes to `/inputs/...`, `/etc/x`, `os.path.expanduser("~")` → all fail; only `/work` write succeeds;
- **no-read-escape proof:** script asserts `os.path.exists` is False for a canary file the test creates in the fake `HERMES_HOME` and for the live-path of the sqlite source (only the snapshot is visible);
- **timeout:** `while True: pass` with `timeout_seconds=5` → `timeout` status ≤ ~8s, no orphan processes after (assert via process scan);
- **oom:** allocate > limit → `oom` status with guidance;
- **live-db safety:** snapshot taken while a writer holds a WAL write txn on the fixture DB → snapshot consistent, writer unaffected.

## Verification gates (DoD blockers, in order)

1. Full existing test battery green (`pytest` suite as configured in the repo) — no skips introduced outside the userns-gated E2E marker.
2. `scripts/plane_lint.py` → **zero new entries** vs `plane-lint-baseline.json`; grep-proof: no `tgg|christopher|hdb|ilinked` tokens in any new/modified shared-plane file.
3. Replay battery `scripts/tgg_christopher_hermes_replay.py` green/unchanged (additive tool must not perturb existing brief behavior).
4. Bundle/manifest check passes (item 8).
5. **Cross-provider review** (per `shelf-4-mechanics.md § M2.3`) of the full diff, with explicit attention to: jail escape vectors in `init.sh`, mount-flag correctness, kill-tree completeness, and redaction coverage.
6. On-VPS smoke (driver-run or runbook'd — the build worker does NOT get live-host access if sandboxed; deliver the runbook as `deploy-runbook.md` in this spec dir): venv `pip install '.[sandbox]'`; tree flip; `verify_runtime.sh` invariant green; one canned reconciliation run as pclaw under the service context; journalctl clean.
7. Demo per design §6.1 executed and pass-criteria recorded (this is the ship gate teren asked for).

## DoD checklist

- [ ] `tools/python_sandbox_tool.py` implemented per design §2–§4; registered; discoverable.
- [ ] `toolsets.py` + `pyproject.toml` extra landed; shared plane token-clean.
- [ ] Client config + constitution patch (mgmt brief only) landed in `deploy/tgg/christopher/`.
- [ ] Bundle manifests updated + check green.
- [ ] Unit + E2E suites written and green (E2E proven to actually execute in at least one userns-capable environment — a fully-skipped E2E suite is a DoD failure).
- [ ] Battery + replay + plane-lint gates green; cross-provider review done and findings addressed.
- [ ] `deploy-runbook.md` written (venv extra install, flip, verify, rollback drill: config kill-switch → tool absent from schema, verified).
- [ ] Rollback drill actually exercised once on the VPS (enable → disable → re-enable).
- [ ] Demo (3,907-class reconciliation from WhatsApp) completed; counts independently verified; results + timings recorded in this spec dir.
- [ ] Measurement window named at close (Constitution: measurement-before-close): 2 weeks; success = ≥1 real batch task/week routed through the sandbox with no per-item fallback, no service disruptions attributable to sandbox runs; outcome decision keep/revise/retire recorded.

## Open questions already flagged to the driver (do not silently resolve)

Q1 pandas/openpyxl extra (assumed yes), Q2 media-root scope, Q3 tgg.db size vs snapshot cap, Q4 mgmt-brief-only v1, Q5 kernel userns sysctl fallback, Q6 artifact file delivery out of scope — see design §9. If any answer changes the design, update `design.md` first, then build.
