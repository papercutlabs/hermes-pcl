# External Review — claude

**Prompt:** Final blocking re-review. The previous v2 verdict left five closure items. Verify from this current diff that: the NPROC E2E now actually forks without /dev/null and asserts EAGAIN near the configured...

---

## Closure verification (v3, against working tree — matches `review-diff-v3.patch`)

**1. NPROC E2E actually forks, no `/dev/null`, asserts EAGAIN near cap — CONFIRMED.**
`tests/test_python_sandbox_tool.py:566` spawns 40 `subprocess.Popen(["/venv/bin/python", "-c", "…sleep(5)"])` with inherited fds (no `DEVNULL`, which matters because the jail has no `/dev`), children stay alive so the cap is actually reached, and asserts `1 <= spawned <= 15` against `max_processes=16` plus `errno.EAGAIN in errors`. The limit is applied inside the payload namespace (`tools/python_sandbox_tool.py:305-314`, `RLIMIT_NPROC` set by the nested-userns wrapper under uid 65534), so it bounds the payload rather than the parent's uid.

**2. `/work` is size-capped tmpfs; export copy is trusted-wrapper, post-exit — CONFIRMED.**
`python_sandbox_tool.py:292` mounts `/work` as `tmpfs size=<scratch_mb>m,nosuid,nodev,noexec`; the host-visible `run/work` is bound at `/export` and immediately remounted `ro` (`:294-295`) before `pivot_root`. The payload runs in a *child* user namespace (`unshare --user --map-user=65534`) that does not own the mount namespace, so it holds no CAP_SYS_ADMIN over these mounts — the E2E asserts exactly this (`venv_remount`/`directory_remount` → `EPERM`, test:441-452). Only after `rc=$?` does the outer ns-root shell `remount,bind,rw /export` and `cp -a /work/. /export/` (`:317-318`). `script.count("remount,bind,rw") == 1` is asserted in the unit mount-plan test. `test_jailed_scratch_total_is_bounded` proves `ENOSPC` at the cap and that the exported host bytes stay ≤ 4 MB.

**3. Deploy verification probes the nested userns — CONFIRMED.**
`verify_runtime.sh` runs `runuser -u pclaw -- unshare --user --map-root-user --net --mount --pid --fork --kill-child /bin/sh -c 'unshare --user --map-user=65534 --map-group=65534 true'` — byte-for-byte the same nesting the runtime `_probe` requires (`python_sandbox_tool.py:110-124`), gated on `python_sandbox.enabled: true` read from the live config, plus existence of both dataset source paths.

**4. Interruption precedes the CPU heuristic and stays in contract — CONFIRMED.**
Status ladder at `:728-741`: `timeout` → `rc==0`/`success` → `elif interrupted_requested: status = "error"` → `elif _cpu_limit_exhausted(...)` → SIGKILL/`oom`. Interrupt therefore can never be misclassified as CPU exhaustion, and `:766` sets `error: "sandbox execution interrupted"` under status `error`, which is inside the design §4.4 status set (no new status introduced).

**5. Payload cwd is `/work` — CONFIRMED.** `:303` `cd /work` after `pivot_root`/`umount -l /.oldroot`, before the payload exec.

## Regression recheck (no reopening found)

- **Mount/write authority:** `ro_dir` uses `--rbind` + `--make-rslave` + recursive `findmnt` deepest-first `remount,bind,ro`, and fails closed if `findmnt` returns nothing. Jail root itself is remounted `ro` before pivot (`:299`); `/proc` is `nosuid,nodev,noexec`; `/export` is `ro` for the payload's whole lifetime. `unshare --mount` defaults to private propagation, so nothing leaks host-ward. Only writable surface reachable by the payload is the `/work` tmpfs — matching the E2E write-escape matrix (`input_write`, `directory_write`, `etc_write`, `venv_write` all blocked; `HOME == /work` writable).
- **Read confinement:** `test_jailed_host_paths_and_home_canary_are_absent` still proves the fake `HERMES_HOME` canary and the live DB path are absent while the snapshot is present.
- **Harvesting:** concurrent drain threads start before the wait loop (no pipe-fill deadlock), head+tail caps preserved, `result.json` 8 KB cap → `result_invalid` with actionable text, recursive `_sanitize_value` over result keys/values and `files` entries.
- **Timeout:** watchdog + `os.setsid`/`killpg` SIGTERM→SIGKILL + `--kill-child`; `test_jailed_timeout_is_bounded_and_leaves_no_process` asserts ≤12 s and no surviving process referencing the run path.
- **Snapshot:** `file:…?mode=ro` URI source, backup API, size guard before *and* after with `unlink` on overflow; source hash/mtime asserted unchanged; WAL-writer test shows an uncommitted `BEGIN IMMEDIATE` txn is neither captured nor disturbed.
- **CI:** `.github/workflows/python-sandbox.yml` disables the AppArmor userns restriction, installs `--extra sandbox`, and requires `^8 passed in ` with zero `skipped`. The file defines exactly 8 `sandbox_e2e` tests (reconciliation, network/write escape, read escape, timeout, oom, cpu, nproc, scratch), so the count assertion is exact and cannot pass with a silent skip.

## Non-blocking notes (do not gate)

- `/dev` is not mounted at all. Model-authored code using `/dev/null` (e.g. `subprocess(stdout=DEVNULL)`) will fail with a confusing `FileNotFoundError`. Consider a tiny `ro` `/dev/null` bind later, or a line in the tool description.
- The grep `^8 passed in ` breaks if pytest emits `8 passed, N warnings in …`. That fails closed (false red), but it will bite someone.
- `_generate_init_script` receives `limits["file_size_mb"]` as `tmpfs_mb` for the jail *root* while `scratch_mb` correctly sizes `/work`. Harmless (root is remounted ro and holds only mountpoints) but the parameter reuse reads as accidental.
- `_probe(force=True)` runs in both `_handle_python_sandbox` and `python_sandbox`, so two probe subprocesses per call. Cheap, but redundant.

CLEAR