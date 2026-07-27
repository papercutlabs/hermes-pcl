# Christopher python-sandbox — design

**Date:** 2026-07-25 · **Driver spec** for WB `8ddae42d` (build brief in `build-brief.md`)
**Intent (teren):** "i have a feeling its time to give him that python sandbox." Motivating incident: Christopher was asked to reconcile ~3,907 job numbers across sources and did per-item LLM reasoning where a 10-line script is the right tool.
**Target demo:** Christopher completes that batch reconciliation end-to-end from a WhatsApp ask, inside the sandbox, on the client VPS.

Grounded against the hermes-pcl tree at `/Users/pcloffice/pcl/_worktrees/hermes-origin-709f9a23-1784905938` (referred to below as `<hermes>`): tool registry (`tools/registry.py`), the existing PTC sandbox (`tools/code_execution_tool.py`), toolset composition (`toolsets.py`), PA constitution/job-brief scoping (`agent/pa_constitution.py`), Christopher deploy artifacts (`deploy/tgg/christopher/`), and the plane rules (`plane-manifest.json`, `scripts/plane_lint.py`, shared-core-hygiene, de-fusion second-pass findings).

---

## 1. Summary of the chosen shape

One new **shared-plane, client-agnostic tool** `python_sandbox` in `tools/python_sandbox_tool.py`, registered through the existing `registry.register()` pattern and exposed via a new `"python-sandbox"` toolset. It runs LLM-authored Python **on the client VPS**, in a kernel-enforced jail built from primitives already on a Debian box:

- **Isolation:** `unshare` (util-linux) user + network + mount + PID namespaces. No network exists inside the jail (empty netns — not "filtered", *absent*). The filesystem is default-deny: a tmpfs jail root with read-only bind mounts of the interpreter/runtime and of the explicitly attached datasets, plus exactly one writable scratch dir.
- **Resource limits:** `resource.setrlimit` in `preexec_fn` (CPU seconds, address space, fsize, nproc, nofile) + a parent wall-clock watchdog that SIGKILLs the whole PID namespace. Same poll/kill/drain patterns already proven in `code_execution_tool.py`.
- **Data access:** a `python_sandbox.datasets` **config section** (client plane — `deploy/tgg/christopher/config.yaml`), never client concepts in shared code. SQLite datasets are attached as **parent-side read-only snapshots** (`sqlite3` backup API), so the sandbox never touches the live DB file. Directory/file datasets are read-only bind mounts.
- **Output contract:** capped head+tail stdout, a structured `result.json` channel, persisted run artifacts with source-owned TTL cleanup. The schema description teaches the model to aggregate in code and never dump rows.
- **Fail closed:** if the isolation probe fails (userns disabled, unshare missing), the tool is *unavailable* (registry `check_fn`) — it never degrades to an unjailed subprocess.
- **Rollback:** config kill switch (`python_sandbox.enabled: false`), brief-level toolset removal, or engine-slot tree revert. All three are minutes, none touches other tools.

Why not the existing `execute_code`? See §8 — it is the PTC ("call hermes tools from a script") tool: its child process has full network, the session CWD, and RPC access to `terminal`. That is the wrong trust envelope for a client-data box, and Christopher's briefs deliberately disable shell/terminal. We reuse its proven mechanics (env scrub, head+tail drain, kill-tree, ANSI strip, secret redaction) but not its envelope.

---

## 2. Execution mechanism and isolation model

### 2.1 Runtime reality (what we design against)

- Service: `christopher-tgg-hermes.service`, `User=pclaw`, `WorkingDirectory=/home/pclaw/apps/hermes-pcl` (non-git rsync'd tree), venv at `/home/pclaw/apps/hermes-pcl/.venv`, `HERMES_HOME=/home/pclaw/.hermes-christopher-tgg`. Unit already sets `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectSystem=full` — none of which blocks creating child namespaces.
- Tenant DB: `/home/pclaw/.systems-pcl/data/tenants/tgg.db` (owned by the systems-pcl service, readable by pclaw).
- No docker, no firejail, no bubblewrap/nsjail on the box. `unshare`/`nsenter` (util-linux) and `sqlite3` are stock Debian. Kernel unprivileged user namespaces are default-enabled on Debian 11/12 (`kernel.unprivileged_userns_clone=1`) — **verified at deploy time, not assumed** (see §7 gate and open question Q5).
- Hermes venv deps include `numpy` and `psutil` (pyproject); `pandas`/`openpyxl` are **not** currently installed — the build adds them as an optional extra (§7, Q1) because the demo parses an xlsx.

### 2.2 Invocation pipeline (per call)

All staging happens in the parent (Hermes process); the child sees only the finished jail.

1. **Validate + resolve.** Check `enabled`, resolve requested dataset names against config, enforce per-call limits (clamped to config maxima).
2. **Stage run dir** `$HERMES_HOME/sandbox_runs/<run_id>/` (0700):
   - `script.py` — the LLM's code, written verbatim.
   - `inputs/` — dataset materializations:
     - `sqlite` dataset → parent opens source with `sqlite3.connect(f"file:{path}?mode=ro", uri=True)` and runs the **backup API** into `inputs/<name>.db`. WAL-safe, lock-safe, and the live file is never exposed to the jail. Guarded by `max_snapshot_mb` (default 512): oversize → structured error, no partial snapshot.
     - `path` dataset (file or dir) → no copy; recorded for read-only bind mount at `/inputs/<name>`.
   - `inputs/params.json` — the optional `input_json` argument (small inline data, ≤32 KB).
   - `work/` — the only writable directory in the jail.
   - `init.sh` — generated jail-setup script (below).
3. **Spawn** (reusing `code_execution_tool.py` mechanics — scrubbed env via `_scrub_child_env`, `os.setsid`, pipe drain threads):

   ```
   unshare --user --map-root-user --net --mount --pid --fork --kill-child \
       /bin/sh <run_dir>/init.sh
   ```

   with `preexec_fn` applying rlimits (§2.4) so every descendant inherits them.
4. **`init.sh`** (runs as ns-root inside the namespaces; the point of `--map-root-user` is only to permit mounts — outside the ns it is still uid pclaw):
   - `mount -t tmpfs tmpfs <jailroot>` (size-capped tmpfs, e.g. 64m — jailroot is a subdir of the run dir).
   - Read-only bind mounts into the jailroot: `/usr`, `/bin`, `/lib`, `/lib64` (interpreter + system libs), the hermes venv dir (site-packages: numpy/pandas/openpyxl), and a minimal `/etc` subset (`ld.so.cache`, `ld.so.conf*`, `localtime`, `passwd`, `alternatives`) — **not** all of `/etc` and not `$HOME`, so hermes state, `.env` secrets, WhatsApp session state, and the rsync tree are simply not present in the jail.
   - `mount -o bind,ro` each dataset source → `<jailroot>/inputs/<name>`; bind `params.json` and `script.py` ro; bind `work/` rw → `<jailroot>/work`.
   - Fresh `mount -t proc proc <jailroot>/proc` (valid because of the new PID ns).
   - `pivot_root` into the jailroot (umount the old root), `cd /work`, `exec <venv-python> -I /script.py` (`-I` = isolated mode: no user site, no cwd on `sys.path`, env-var Python hooks ignored).
   - Child env (set by parent, post-scrub): `SANDBOX_INPUTS` (JSON `{name: "/inputs/<name>[...]"}`), `RESULT_PATH=/work/result.json`, `TMPDIR=/work`, `TZ` (from `HERMES_TIMEZONE`), `PYTHONIOENCODING=utf-8`, `PYTHONUTF8=1`, `PYTHONDONTWRITEBYTECODE=1`.
5. **Watchdog loop** (parent): poll child; on interrupt (`tools.interrupt.is_interrupted`) or wall-clock expiry, kill the process group (SIGTERM → 5s → SIGKILL; `--kill-child` additionally guarantees the ns dies with the leader). Periodic `touch_activity_if_due` so the gateway inactivity timeout doesn't reap the agent mid-run (same as `execute_code`, issue #10807).
6. **Harvest:** collect capped stdout/stderr (head+tail drain), read + validate `work/result.json`, run `strip_ansi` + `agent.redact.redact_sensitive_text` over everything model-bound, delete `inputs/` snapshots (they can be large), keep `script.py`, `work/`, and a `meta.json` (limits, datasets, exit status, durations) as the persisted run artifact.
7. **Source-owned cleanup (mise-en-place):** at the end of every invocation, prune `sandbox_runs/` entries older than `artifact_ttl_days` (default 7) and enforce `max_runs_kept` (default 40). No cron, no "remember to tidy".

### 2.3 What enforces each guarantee

| Guarantee | Enforced by | Class |
|---|---|---|
| No network egress | `--net`: new, empty network namespace (only a down `lo`). Not a filter — there is no route off the box. | kernel |
| Client data can't leave | no network + filesystem default-deny; the only writable surface is `work/`, which stays on the box | kernel |
| No reads outside whitelist | tmpfs jail root + pivot_root: paths not bind-mounted **do not exist** in the jail (no `$HOME`, no `.env`, no live tgg.db, no capture tree) | kernel |
| No writes outside scratch | every bind except `/work` is `ro`; tmpfs root is ns-private | kernel |
| Live DB integrity | sandbox only ever sees a parent-made snapshot copy | construction |
| Can't kill/starve the service | rlimits (§2.4) + wall-clock kill + PID-ns `--kill-child` (no orphan escapes) + `Nice`/ionice on the child | kernel + parent |
| Secrets never reach model context | `_scrub_child_env` (child env), jail (no secret files visible), `redact_sensitive_text` on all returned text (belt-and-suspenders) | construction |
| No privilege gain | unprivileged userns (no setuid involved); service already `NoNewPrivileges=true` | kernel |
| Fail closed | `check_fn` probe (§2.5); probe failure ⇒ tool absent from the schema, and the handler re-checks per call | construction |

### 2.4 Resource limits (defaults; config-overridable, per-call clamped)

| Limit | Default | Mechanism |
|---|---|---|
| Wall clock | 120 s (call may request 5–300 s; config `max_wall_seconds` caps) | parent watchdog |
| CPU time | 60 s | `RLIMIT_CPU` |
| Memory | 1024 MB | `RLIMIT_AS` |
| File size | 64 MB per file | `RLIMIT_FSIZE` |
| Processes | 64 | `RLIMIT_NPROC` |
| Open files | 256 | `RLIMIT_NOFILE` |
| Scratch space | tmpfs `size=` cap on jail root + `work/` quota via `RLIMIT_FSIZE`; run dir on disk bounded by `max_snapshot_mb` | mount opts + rlimit |
| Core dumps | 0 | `RLIMIT_CORE` |

`systemd-run` was considered and rejected for the limit layer: system-level `systemd-run` needs root/polkit; `--user` needs a per-user manager the `User=pclaw` **system** service doesn't get by default (no session, no `XDG_RUNTIME_DIR`, would require lingering). rlimits + namespaces need nothing.

### 2.5 Availability probe (`check_fn`)

`check_sandbox_available()`, TTL-cached by the registry (30 s):

1. `python_sandbox.enabled` is true in config;
2. `shutil.which("unshare")`;
3. one cached live probe: `unshare --user --map-root-user --net --mount --pid --fork true` exits 0 (proves kernel userns + all needed ns types under the systemd hardening actually work).

Any failure ⇒ tool absent from the model's schema (same UX as other gated toolsets). The handler re-runs the probe on call and returns `status: "unavailable"` with the failing step named — **never** a degraded unjailed run.

---

## 3. Data-access whitelist mechanism (client config, not shared code)

Shared code knows only the *mechanism*: named datasets of type `sqlite` or `path`. Which datasets exist, their paths, and their descriptions are **client-plane config**. No TGG token appears in `tools/python_sandbox_tool.py` or `toolsets.py` (plane-lint enforced; `clientTokenRegistry.tgg` covers `tgg`, `christopher`, `SK/JOB`, `hdb`, `ilinked`, …).

Top-level config section (read via the same `hermes_cli.config` raw-config path `execute_code` uses — **not** under `pa:` because `pa.enabled: false` in Christopher's live config; the PA overlay gates constitution behavior, not generic tools):

```yaml
# deploy/tgg/christopher/config.yaml  (CLIENT plane — tgg tokens allowed here)
python_sandbox:
  enabled: true
  datasets:
    cases:
      type: sqlite
      path: /home/pclaw/.systems-pcl/data/tenants/tgg.db
      description: "Operational case/work-costing database (read-only snapshot; sqlite)"
    media:
      type: path
      path: /home/pclaw/.systems-pcl/data/media/tgg/hermes
      description: "Retained WhatsApp media/attachments (read-only; spreadsheets land here)"
  limits:            # optional; defaults shown in §2.4
    wall_seconds: 120
    max_wall_seconds: 300
    cpu_seconds: 60
    memory_mb: 1024
    max_snapshot_mb: 512
  artifact_ttl_days: 7
  max_runs_kept: 40
```

Rules enforced by shared code:

- A dataset is visible in a run **only if named in the call's `datasets` argument** — attach-on-request, not attach-everything. Unknown name ⇒ `status: "dataset_unknown"` listing valid names (never a silent skip).
- `sqlite` ⇒ snapshot (never the live file, never `immutable=1` games against a WAL db); `path` ⇒ `ro` bind of the resolved real path; symlinks resolved parent-side and validated (`tools/path_security.validate_within_dir` against the configured root) before mounting.
- Dataset names + descriptions surface to the model via `dynamic_schema_overrides` (existing registry feature), so the schema is honest per client with zero client tokens in shared code — the same trick `delegate_task` uses for runtime limits.
- Per-chat scoping rides the existing job-brief `enabled_toolsets` mechanism (`agent/pa_constitution.py`) — see §6. No new scoping machinery.

**De-fusion note:** this section is a *new instance of the correct pattern* (shared mechanism + client-plane declaration), the same seam shape the Phase-2 client-pack loader will formalize. When the pack loader lands, `python_sandbox.datasets` moves into the tgg pack verbatim — nothing in shared code changes. No new entry may appear in `plane-lint-baseline.json` from this build.

---

## 4. Tool schema and model contract

### 4.1 Registration

```python
registry.register(
    name="python_sandbox",
    toolset="python-sandbox",
    schema=PYTHON_SANDBOX_SCHEMA,
    handler=_handle_python_sandbox,          # (args, **kw) -> JSON str
    check_fn=check_sandbox_available,
    emoji="🧮",
    max_result_size_chars=40_000,
    dynamic_schema_overrides=_schema_overrides,  # datasets + available libs
)
```

`toolsets.py` (shared plane, generic wording):

```python
"python-sandbox": {
    "description": "Offline sandboxed Python for batch computation over whitelisted local datasets",
    "tools": ["python_sandbox"],
    "includes": [],
},
```

### 4.2 Parameters

```json
{
  "name": "python_sandbox",
  "parameters": {
    "type": "object",
    "properties": {
      "code":            {"type": "string",
                          "description": "Python 3 source. Paths to attached datasets are in os.environ['SANDBOX_INPUTS'] (JSON name->path). Write your final structured answer as JSON to os.environ['RESULT_PATH']; print a short human-readable summary to stdout."},
      "datasets":        {"type": "array", "items": {"type": "string"},
                          "description": "Names of datasets to attach (available names are listed above). Only attached datasets exist inside the sandbox."},
      "input_json":      {"type": "object",
                          "description": "Optional small inline input (<=32KB), available at /inputs/params.json."},
      "timeout_seconds": {"type": "integer", "minimum": 5, "maximum": 300,
                          "description": "Wall-clock limit. Default 120."}
    },
    "required": ["code"]
  }
}
```

### 4.3 Description (behavioral contract shown to the model)

Rendered with live dataset list and probed library list via `dynamic_schema_overrides`:

> Run Python offline in a locked sandbox on this machine — no network, no shell, read-only data, one scratch dir (`/work`). Use it for batch computation the chat should not do item-by-item: comparing/reconciling lists across sources, counting or deduplicating more than ~50 items, sums/statistics, parsing spreadsheets or CSVs.
>
> Datasets you can attach (pass names in `datasets`): {name — description, per config}. SQLite datasets are point-in-time read-only snapshots.
>
> Libraries: Python stdlib (sqlite3, csv, json, statistics, re, datetime, collections) plus {probed: numpy, pandas, openpyxl}.
>
> Output rules — your context is small and the data may be huge: do the aggregation IN the code. Print at most a short summary (counts, totals, up to ~20 example rows). Write the full structured answer to `RESULT_PATH` as JSON (it is returned to you, capped at 8KB — keep detail lists in `/work/` files and report their names and row counts instead). Never print thousands of rows.

### 4.4 Return shape (JSON string, like every registry tool)

```json
{
  "status": "success | error | timeout | oom | dataset_unknown | unavailable | result_invalid",
  "stdout": "<= 16KB, head+tail truncated with an explicit '[N chars omitted]' marker",
  "stderr": "<= 4KB tail (present on error; carries the traceback)",
  "result": {"...": "parsed result.json, or null"},
  "files":  [{"path": "work/mismatches.csv", "bytes": 123456, "lines": 3907}],
  "datasets_attached": ["cases", "media"],
  "duration_seconds": 3.4,
  "truncated": {"stdout": false, "result": false},
  "run_id": "r_ab12cd34",
  "error": "only on non-success; one actionable line"
}
```

Output caps (context protection for the 3,907-row class):

- `stdout` capped at 16 KB using the proven head(40%)+tail(60%) drain — the final `print()` summary always survives.
- `result` capped at 8 KB serialized; oversize ⇒ `status: "result_invalid"` with `error: "result.json is NNN KB (cap 8KB) — write detail to /work files and return counts + samples"` and stdout still returned, so the model can self-correct in one retry.
- `files` lists `work/` contents (name, bytes, line count) — evidence that full detail exists without ever loading it; artifacts persist under `$HERMES_HOME/sandbox_runs/<run_id>/` for follow-ups within the TTL.
- Everything model-bound passes `strip_ansi` + `redact_sensitive_text`.

### 4.5 Error/timeout surface (each failure teaches the fix)

| status | trigger | message pattern |
|---|---|---|
| `timeout` | wall clock hit | partial stdout + "killed at {N}s — reduce work or raise timeout_seconds (max {max})" |
| `error` (CPU) | `RLIMIT_CPU` (SIGXCPU / rc≈152) | "CPU limit ({N}s) exhausted — algorithmic issue, not a hang; simplify" |
| `oom` | MemoryError in stderr, or rc 137/-9 without watchdog kill | "memory limit ({N}MB) — stream/chunk instead of loading everything" |
| `error` | nonzero exit | stderr tail with traceback (the model debugs from it) |
| `dataset_unknown` | bad dataset name | valid names listed |
| `result_invalid` | result.json unparseable/oversize | see §4.4 |
| `unavailable` | probe failed / disabled | failing step named; model told to fall back to normal tools and flag it |

---

## 5. Deploy shape

What lands where (all rides the existing rsync + engine-slot flip; **no systemd unit change required** — verified against `christopher-tgg-hermes.service`, whose hardening permits child namespaces):

| artifact | plane | change |
|---|---|---|
| `tools/python_sandbox_tool.py` | shared | new file; auto-discovered by `discover_builtin_tools()` (top-level `registry.register` call) |
| `toolsets.py` | shared | add `"python-sandbox"` toolset (generic wording only) |
| `pyproject.toml` | shared | new optional extra `sandbox = ["pandas==<pin>", "openpyxl==<pin>"]` (Q1) |
| `tests/test_python_sandbox_tool.py` | shared | unit + E2E (see build brief) |
| `deploy/tgg/christopher/config.yaml` | client | `python_sandbox:` section (§3) |
| `deploy/tgg/christopher/christopher_tgg_constitution.yaml` | client | brief toolset + instruction patch (§6) |
| `deploy/tgg/christopher/scripts/verify_runtime.sh` (or `checks/runtime-invariants.json`) | client | add sandbox probe invariant (unshare probe + `python_sandbox.enabled` coherence) |
| VPS venv | ops step | `.venv/bin/pip install -e '.[sandbox]'` (or explicit pins) during the deploy runbook |

Bundle manifests: `deploy/tgg/christopher/pa-agent.hermes.manifest.json` (and the root manifest until D1-35's quick kill moves it) enumerate literal source paths — the new tool file must be added there in the same change, and `pcl pa-agent bundle --check` run pre-flip (this is exactly the de-fusion finding (b)2 trap; do not re-trip it).

Config delivery: `deploy/tgg/christopher/config.yaml` is the source of truth rendered to `/home/pclaw/.hermes-christopher-tgg/config.yaml` by the existing deploy scripts (`build_runtime_slots.py` / `deploy_runtime.sh`); the sandbox section rides that path unchanged.

### Rollback (instant-disable ladder, no code revert needed)

1. **Kill switch:** set `python_sandbox.enabled: false` in the runtime config + `systemctl restart christopher-tgg-hermes` → `check_fn` fails → tool vanishes from the schema. < 2 minutes, touches nothing else. (Config edits require the restart because raw-config is mtime-cached per process; the restart is already the standard config-change procedure on this host.)
2. **Scope-only:** remove `python-sandbox` from a brief's `enabled_toolsets` → gone from that chat only.
3. **Full revert:** engine-slot flip back to the previous tree (`switch_engine_slot.sh` + restart) — the standard whole-tree rollback.

---

## 6. Constitution patch (WHEN to reach for it) — client plane

Applied to `deploy/tgg/christopher/christopher_tgg_constitution.yaml`. Enable on the **management brief first** (the demo's chat; ingest brief stays unchanged in v1 — expansion is a follow-up once mgmt behavior is observed; Constitution #16: the mgmt chat is where a miss produces visible signal).

```yaml
# management brief (the one currently enabling: memory, file, web, custom, pa-observability)
    enabled_toolsets:
    - memory
    - file
    - web
    - custom
    - pa-observability
    - python-sandbox          # ADD
    instructions:
    # ADD:
    - 'python_sandbox is your batch-computation tool. Reach for it — instead of
      reasoning item by item — whenever a task means computing over many records:
      comparing or reconciling lists between sources (spreadsheet vs system),
      counting, deduplicating, or summing more than ~50 items, spreadsheet/CSV
      parsing, totals and statistics. One sandbox run that prints counts plus a
      few examples beats hundreds of per-item lookups. Attach only the datasets
      you need; the cases dataset is a read-only snapshot of the system database,
      so cite it as "as of this run". Do NOT use it for: single-record lookups
      (use your business tools), anything that must WRITE to the system (sandbox
      is read-only — compute first, then apply changes through business
      operations), or judgment calls on individual messages. If the sandbox
      reports unavailable, say so and fall back — never hand-simulate a batch
      computation over more than ~50 items without flagging the degradation.'
```

## 6.1 Demo script — the 3,907-number reconciliation

Actors: teren (or client) in the mgmt WhatsApp chat. Precondition: sandbox deployed + enabled, spreadsheet with ~3,907 job numbers sent into the chat (media retention puts it under the `media` dataset root).

1. **Ask (WhatsApp):** "check this list against the system — which of these job numbers aren't in our cases, and which cases have a different status than the sheet says?" + attached `.xlsx`.
2. **Expected tool call:** `python_sandbox(datasets=["cases","media"], code=...)` where the code (model-authored, ~30 lines): opens the xlsx from the media path via `openpyxl`, normalizes the job-number column; opens `SANDBOX_INPUTS["cases"]` via `sqlite3`; set-compares + status-joins; writes `result.json` (`{"sheet_rows": 3907, "matched": N, "missing_in_system": N, "status_mismatch": N, "samples": {...}}`) and `/work/missing.csv` + `/work/mismatches.csv`; prints a 6-line summary.
3. **Expected reply:** counts + up to ~10 examples + "full lists saved (missing.csv 212 rows, mismatches.csv 89 rows) — want them?" No row-dump in chat, no per-item tool calls.
4. **Pass criteria:** (a) single sandbox run (≤1 self-corrected retry), (b) wall time under 60 s, (c) counts verified against an offline recomputation of the same snapshot by the driver, (d) journalctl shows no service disruption, (e) reply cites snapshot semantics.
5. **Negative probes (same session):** ask for something requiring network from the sandbox ("in the sandbox, fetch the sheet from Google") → must fail closed and Christopher must route around it; ask a single-job lookup → must use `tgg_case_lookup`, not the sandbox.

---

## 7. Verification gates (summarized; operationalized in build-brief.md)

- Unit + E2E suite green (including a fixture-DB batch reconciliation E2E that runs the *real* jail on Linux, skips cleanly on macOS/CI-without-userns).
- Full existing test battery green; `scripts/plane_lint.py` zero new entries vs `plane-lint-baseline.json`.
- Replay battery (`scripts/tgg_christopher_hermes_replay.py`) unchanged-green (tool is additive; no brief regressions).
- Cross-provider review before merge.
- On-VPS smoke before enabling for the model: run the probe + a canned reconciliation against the live snapshot path as pclaw under the systemd context (`systemd-run` not needed — invoke through a one-shot `run_isolated_smoke.py`-style check), then the live demo (§6.1).

---

## 8. Alternatives considered and rejected

- **Reuse/extend `execute_code` (PTC).** Wrong trust envelope by design: its child has full network, runs in the session CWD in `project` mode, and gets RPC stubs to `terminal` (a shell) — on a box holding client data, for briefs that deliberately disable shell/web, each of those is a violation of this feature's ground rules. Retrofitting no-net/jail *into* it would fork its semantics for every other Hermes user of PTC. We reuse its internals (env scrub, drain, kill-tree, redaction) as imports/copies instead. Also keeps rollback independent: disabling the sandbox can't affect PTC users.
- **Docker / Podman.** Not installed on the VPS; a daemon + root-equivalent group + image lifecycle is a huge new attack/ops surface for one tool; violates "prefer primitives already on a debian VPS".
- **firejail / bubblewrap / nsjail.** Not installed (ground rule states firejail absent; no bwrap/nsjail in the deploy). bwrap is the closest fit and remains a clean future swap-in — the tool's jail builder is one function; if we ever apt-install bubblewrap, `init.sh` collapses into a `bwrap` argv. Not worth a new package dependency today when `unshare` (already present) provides the same kernel primitives.
- **systemd-run scopes/units for isolation.** System-level needs root/polkit from a `User=pclaw` service; `--user` needs a user manager the service doesn't have (no lingering/session). Namespaces + rlimits achieve the same properties with zero privilege.
- **Pyodide / WASM.** No POSIX sqlite file story against a host DB, heavyweight runtime shipped into the rsync tree, numpy/pandas wheels pinned to the wasm build — massive complexity for weaker data access, solving a browser problem we don't have.
- **In-process `exec()` in Hermes.** No isolation of any kind (network, fs, memory, GIL/event-loop starvation); a runaway script kills the consumer service. Disqualified on the first ground rule.
- **Restricted-Python / AST allowlisting instead of OS isolation.** Python sandboxing at the language layer is a known-broken security model (escape via dunders/imports is a solved attacker problem); kernel namespaces are strictly stronger and simpler to reason about.
- **Pre-exported CSV snapshots instead of sqlite snapshot.** Loses schema/joins, adds an export pipeline to maintain, and goes stale between exports; the sqlite backup API gives point-in-time consistency for free at call time. (CSV export remains available as a `path` dataset if a client ever needs one.)
- **`mode=ro` URI directly on the live DB inside the jail.** Requires bind-mounting the live systems-pcl data dir into the jail (wider exposure), and a read TX on a hot WAL db from a CPU-starved sandbox can hold the WAL open. Snapshot is safer on both axes.

## 9. Open questions for the driver

- **Q1 — venv additions:** add `pandas` + `openpyxl` via the `sandbox` extra (demo parses xlsx; "10-line pandas script" was the stated bar). Assumed YES in this design; if NO, the demo script switches to stdlib+numpy and xlsx parsing degrades to "ask for CSV".
- **Q2 — `media` dataset scope:** whitelisting the whole retained-media root mirrors access Christopher already has via `tgg_case_media`/photos; confirm no narrower root is wanted (e.g. documents only).
- **Q3 — tgg.db size:** unverifiable without host access. If > ~512 MB, either raise `max_snapshot_mb` or switch the `cases` dataset to a nightly snapshot + `path` mount. Driver should `ls -lh` once during deploy.
- **Q4 — brief coverage:** v1 enables mgmt brief only (design choice, §6). Confirm ingest-brief exclusion is intended for the demo window.
- **Q5 — userns on the actual kernel:** probe is part of the deploy gate; if the VPS has `unprivileged_userns_clone=0`, the enable step is a root sysctl (persisted in `/etc/sysctl.d/`) — needs a deliberate ops decision, not silent.
- **Q6 — WhatsApp file delivery of `/work` artifacts** ("want the full list?") is out of scope for v1; artifacts persist under the run dir for a follow-up mechanism later. Confirm acceptable.

## §10 Driver probe results (2026-07-25 03:01, live tgg-app-1, read-only)

- Q5 RESOLVED: `kernel.unprivileged_userns_clone = 1`; the exact jail incantation `unshare --user --map-root-user --net --mount --pid --fork --kill-child true` SUCCEEDS as pclaw. No sysctl change needed.
- Q3 RESOLVED: tgg.db = 48.4MB (50778112 bytes) — far under the 512MB snapshot cap.
- Q1 APPROVED (driver): pandas/openpyxl/numpy sandbox extra. NOTE: deployed venv at /home/pclaw/apps/hermes-pcl/.venv is MISSING numpy entirely (repo pyproject ≠ deployed reality) — build must install the extra into the deployed venv as a deploy step, and the jail must ro-bind the venv.
- Q2 (driver call): media whitelist = retained media root, read-only, v1.
- Q4 (driver call): mgmt-brief-only v1. Q6 stays deferred.
