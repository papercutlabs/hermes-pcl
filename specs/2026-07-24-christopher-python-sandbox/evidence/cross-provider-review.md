# External Review — claude

**Prompt:** Blocking security and design-conformance review of a kernel-jailed Python execution tool. Review the ratified design and build brief against the complete implementation diff. Focus on namespace/mount ...

---

## Review — `python_sandbox` (WB 8ddae42d)

Scope reviewed: `tools/python_sandbox_tool.py`, `tests/test_python_sandbox_tool.py`, `toolsets.py`, `pyproject.toml`/`uv.lock`, `deploy/tgg/christopher/**`, `.github/workflows/python-sandbox.yml`, spec evidence, against `design.md` §2–§8 and `build-brief.md`.

The shape is right and most of the brief is genuinely delivered: fail-closed probe, snapshot-not-live-DB, recursive redaction, plane cleanliness, both manifests, runtime invariant, runbook, and a real Linux jail battery that actually executed (`evidence/linux-jailed-e2e.txt` line 288: `22 passed`, no skips, on the merge of `d7ec98b8d9`). Three things block.

### BLOCKING

**B1 — The payload runs with full capabilities in the user namespace, so the read-only jail is self-defeating.**
`tools/python_sandbox_tool.py:288` (`exec /venv/bin/python -I /script.py`) execs the model-authored code as ns-root with `CAP_SYS_ADMIN` and `CAP_DAC_OVERRIDE` over every uid mapped by `--map-root-user`. That the mount plan itself works (`init.sh` execs `/bin/mount` repeatedly after `unshare`) is direct proof caps survive exec here. None of the ro binds are `MNT_LOCKED` — they were created inside the namespace by the same userns owner — so the payload can call `mount(NULL, "/venv", NULL, MS_REMOUNT|MS_BIND, NULL)` via `ctypes` (no binary needed) and then write to any host path bound into the jail that is owned by `pclaw`:
- `/venv` → `/home/pclaw/apps/hermes-pcl/.venv` — arbitrary code persistence executed by `christopher-tgg-hermes.service` on next restart;
- `/inputs/media` → the retained-media root — client-data modification/destruction.

`design.md` §2.3 lists "No writes outside scratch" and "No reads outside whitelist" as *kernel*-class guarantees; with retained `CAP_SYS_ADMIN` the write half is construction-class at best and defeatable in ~5 lines of payload. Fix: drop privilege after jail assembly and before `exec` (nested `unshare --user --map-user=…`, which locks the outer mounts against the payload, or an equivalent cap/seccomp drop), and add a jailed E2E that attempts an in-payload `MS_REMOUNT|MS_BIND` rw on `/venv` and on a directory dataset and asserts `EPERM`. Network isolation is unaffected (rejoining the host netns needs CAP_SYS_ADMIN in the *initial* userns) — that guarantee holds.

**B2 — `ro_dir` fails open if `findmnt` is missing.**
`tools/python_sandbox_tool.py:242-243`: `findmnt -Rrn -o TARGET "$dst" | sort -r | while … mount -o remount,bind,ro …`. The pipeline's status is the `while` loop's, which is `0` on empty input; POSIX `sh`/dash has no `pipefail`, so `set -eu` does **not** catch `findmnt: not found` (rc 127). Result: every `--rbind` (`/usr`, `/bin`, `/lib*`, `/venv`, every directory dataset) stays **read-write**, silently, with no error surfaced to the model or the log. The availability probe (`:97`) checks only `unshare` — not `findmnt`, `mount`, or `pivot_root`. The later `mount -o remount,bind,ro "$JAIL"` (`:284`) does not cover child mounts. Fix: capture `findmnt` output to a file, assert non-empty and `findmnt` rc 0, abort otherwise; and add `findmnt` to `check_sandbox_available`.

**B3 — Process-count limit dropped with no substitute; implementation contradicts the ratified design.**
`DEFAULTS` (`:34-42`) has no `nproc`; `_preexec` (`:315-336`) sets CPU/AS/FSIZE/NOFILE/CORE only. `design.md` §2.4 mandates `RLIMIT_NPROC 64` and §2.3 claims "Can't kill/starve the service" as kernel-enforced. Commit `393bbe79f8` correctly identified that `RLIMIT_NPROC` is per-uid and host-wide, but shipped no replacement (no pids cgroup, no delegated slice). Consequences: a forking payload can exhaust `pclaw`'s process/thread budget and destabilise the consumer service, and because `RLIMIT_AS` is per-process, the 1 GB memory bound is also evadable by forking. This is a ratified non-negotiable that is now unmet in both code and doc. Either implement a real bound or amend `design.md` §2.3/§2.4 with the driver's explicit acceptance of the residual risk — and add a fork-bomb E2E either way. Do not leave design and code disagreeing.

### Important (fix before the demo, not necessarily before merge)

- **Interrupt misreported as `oom`.** `:659-661` sets `timed_out=False` on `is_interrupted()`, then `:688-693` maps the resulting `-SIGKILL` to `status="oom"` with a bogus "memory limit exhausted — stream/chunk" message. Needs a distinct interrupted status.
- **Kill ladder is 0.5 s, not the specified 5 s.** `_kill_group(grace=0.5)` (`:514`) vs design §2.2 step 5 (SIGTERM → 5 s → SIGKILL). Separately, `proc.wait(timeout=5)` at `:670` can raise into the outer `except Exception` (`:696`), which overwrites `stderr` with "sandbox launch failed" and drops the collected output while `status` stays `error` despite `timed_out`.
- **`stdin` not closed.** The `Popen` at `:608` omits `stdin`, so the jailed payload inherits the service's stdin; `code_execution_tool.py:1239` passes `subprocess.DEVNULL`. One-line fix.
- **Env scrub honours the passthrough allowlist.** `_build_env` (`:298`) calls `_scrub_child_env`, which lets `tools.env_passthrough.is_env_passthrough` re-admit *any* name including credentials (`code_execution_tool.py:143`). The sandbox should not consult `execute_code`'s passthrough config. Also `HOME` and `HERMES_*` are forwarded verbatim (`code_execution_tool.py:79-82`) — path disclosure only, but `HOME` should be pinned to `/work`.
- **Unbounded scratch on the client disk.** `RLIMIT_FSIZE` caps a single file at 64 MB; total bytes written into `/work` (a host-disk bind under `$HERMES_HOME`) are unbounded, so a loop can fill the VPS volume. Design §2.4's "scratch space" row overstates the control. Also the tmpfs cap reuses `file_size_mb` (`:236`, `:598`), conflating two knobs.
- **`_list_files` is unbounded work on untrusted output** (`:423-438`): recursive walk plus a full line count of every file after every run, before the registry's 40k-char truncation.

### Test gaps (concrete)

- The write-escape E2E (`tests/test_python_sandbox_tool.py:380-414`) only tries plain `open(..., "w")`. It does not exercise B1 (remount-rw), does not test a **directory**-type dataset for writability (only the file-bind `sheet`), and does not test `/venv` writability.
- No test for the `RLIMIT_CPU`/SIGXCPU → "CPU limit exhausted" path, though `design.md` §4.5 defines it as a distinct surface.
- No fork-bomb / nproc test (see B3).
- The workflow asserts nothing about skips; the DoD says a fully-skipped E2E suite is a failure. The recorded run happens to show `22 passed`, but nothing prevents a future silent all-skip (e.g. `_can_run_jail()` returning False after an image change). Add an explicit "must not skip on Linux" guard.

### Verified clean

- Fail-closed: `enabled is not True` → unavailable (`:95`); handler re-probes (`:737`) and never degrades. Probe TTL-cached by `tools/registry.py:121`.
- Snapshot: backup API over `mode=ro` URI, pre- and post-size guard with no partial file (`:148-166`); WAL-writer test asserts the live writer's txn is undisturbed.
- Redaction: `_sanitize_value` recurses keys, values, lists and is applied to the whole payload including `files` (`:358-369`, `:720`).
- Shared plane is client-agnostic; all TGG specifics live under `deploy/tgg/christopher/**` and `build_runtime_slots.py`; both manifests list `tools/python_sandbox_tool.py`; `verify_runtime.sh:29-46` gates on the probe + dataset existence; SHA256SUMS and all three runtime slots regenerated coherently.
- The 600-row comparison class is genuinely exercised in the jail (550 DB rows / 600 sheet rows / 50 missing / 20 mismatched — fixture arithmetic checks out) and passed on Linux.

BLOCKED