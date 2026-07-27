# External Review — claude

**Prompt:** Re-review the jailed Python sandbox after remediation of your prior BLOCKED verdict. Confirm B1 is closed by a nested user namespace that removes payload mount authority and by actual remount/write E2...

---

## Re-review — v2 diff (grounded in the committed tree: `tools/python_sandbox_tool.py`, `tests/test_python_sandbox_tool.py`, `.github/workflows/python-sandbox.yml`, `deploy/tgg/christopher/**`)

### B1 — payload mount authority: CLOSED
`_generate_init_script` freezes the tmpfs root (`mount -o remount,bind,ro "$JAIL"`) before `pivot_root`, lazily unmounts `/.oldroot`, then `exec`s a **nested** `unshare --user --map-user=65534 --map-group=65534` before the payload. That is the right primitive: the payload's capabilities are scoped to the child user namespace, while the mount namespace remains owned by the parent userns, so `mount`/`umount`/`pivot_root` are EPERM. Escaping by nesting a fresh mount namespace does not help either — a mount tree copied into a userns-owned mount ns carries `MNT_LOCK_READONLY`/`MNT_LOCKED`, so ro flags cannot be dropped and locked mounts cannot be detached. E2E `test_jailed_network_and_write_escape_are_blocked` proves it empirically: `MS_REMOUNT|MS_BIND` on `/venv` and on the directory dataset both return EPERM, writes to `/inputs`, the directory dataset, `/etc`, and `/venv` all fail, `/work` is the single writable surface, and `test_jailed_host_paths_and_home_canary_are_absent` proves the HERMES_HOME canary and the live DB path do not exist. No residual write path found.

### B2 — fail-closed mount plan and probing: CLOSED
`ro_dir` now fails closed three ways: `set -eu` aborts if `findmnt` exits nonzero, an explicit `[ -s "$targets" ] || { …; exit 1; }` aborts on empty output, and every submount is remounted ro in reverse-sorted (child-first) order. `_probe` requires `unshare`, `mount`, `findmnt`, `pivot_root` to resolve, and — importantly — now validates the *actual* incantation including the nested `unshare --user --map-user=65534`. `check_sandbox_available` and `_handle_python_sandbox` both force a fresh probe; there is no degraded path.
Residual (non-blocking) nit: the generator hardcodes `/usr/sbin/pivot_root` and `/usr/bin/unshare` while the probe resolves via `PATH`. Drift fails hard rather than silently, but resolve them in the generator so probe and runtime cannot disagree.

### B3 — process cap: **NOT closed by the cited evidence**
The mechanism is placed correctly (`RLIMIT_NPROC` hard==soft inside the nested userns; the payload cannot raise it because `do_prlimit`'s `capable(CAP_SYS_RESOURCE)` resolves against the *initial* userns). But the E2E that is supposed to prove it cannot prove anything:

- The jail creates `usr, bin, lib, lib64, venv, etc, inputs, work, proc, .oldroot` — **there is no `/dev` in the jail at all**.
- `test_jailed_process_count_is_bounded` spawns children with `stdin/stdout/stderr=subprocess.DEVNULL`. `Popen` opens `os.devnull` in the payload process *before* forking, so every one of the 40 iterations raises `FileNotFoundError` (an `OSError`).
- Result: `spawned == 0`, `errors == 40`, which satisfies both assertions (`spawned < 40`, `errors > 0`) **without ever hitting `RLIMIT_NPROC`**. The test is green and vacuous, on CI and on the host smoke.

This also hides a real environment question the test was meant to answer: per-userns NPROC accounting only exists on kernels ≥ 5.14 (Debian 12/6.1 fine; 5.10 counts against `pclaw` host-wide). Fix: drop `DEVNULL` (or mount a minimal `/dev/null`), spawn via `os.fork()`/`Popen` without devnull, and assert specifically on `EAGAIN` plus a spawned count at or just under `max_processes`.

Paired starvation gap: `/work` is a bind of the on-disk run dir with **no total-size bound** — `RLIMIT_FSIZE` caps a single file at 64 MB, the tmpfs `size=` cap applies only to the jail root, and `_prune_runs` is count/age-based, not byte-based. A model-authored loop can write many 64 MB files for the full wall clock on a client VPS, and up to 40 run dirs persist 7 days. Design §2.3's "can't kill/starve the service" is not held on the disk axis.

### Items you asked me to confirm
- **CPU status** — `_cpu_limit_exhausted` covers `-SIGXCPU`, `128+SIGXCPU`, and the util-linux wrapper strings; status `error` with `CPU limit (Ns) exhausted` in stderr and surfaced in `error`, matching design §4.5. ✓ Caveat: the CPU branch is evaluated **before** `interrupted_requested`, and the `"sigprocmask unblock failed"` substring is broad — an interrupt-kill that makes `unshare` emit it would be reported as a CPU-limit error, not `interrupted`. Also `interrupted` is a status not present in the design §4.4 contract or the schema.
- **Kill ladder** — `_kill_group`: `killpg(SIGTERM)` → `wait(5)` → `killpg(SIGKILL)` → `wait(2)`, on a real process group (`os.setsid()` in `preexec_fn`) with `--kill-child` backstopping the PID ns. ✓ Minor: if `proc.wait(timeout=5)` after the break raises, the outer `except` overwrites stderr and status stays `error` while `error` reads "killed at Ns".
- **stdin** — `stdin=subprocess.DEVNULL` on the jail launch. ✓
- **Fixed credential-blind env** — `_build_env` constructs from scratch (no `_scrub_child_env` allowlist inheritance), only reads `HERMES_TIMEZONE`; unit test asserts a secret-shaped var is absent and the contract paths present. ✓ Stronger than the brief required.
- **Workflow no-skip gate** — `-m sandbox_e2e`, `grep -Eq '^7 passed in '`, `! grep -q skipped`, with `set -euo pipefail`. Structurally correct and fails closed. ✓ Caveats: it is `pull_request`-path-filtered only (won't run on main pushes, or when `toolsets.py`/`pyproject.toml` change), and it proves 7 tests pass — one of which is the vacuous flood test, so "no skips" ≠ "cap proven".

### Other important, non-blocking
- `cd /` instead of design §2.2's `cd /work`: model-authored relative writes hit EROFS. Self-correctable from the traceback, but avoidable and unpinned by any test.
- `verify_runtime.sh` probes only the **outer** jail (`unshare --user --map-root-user --net --mount --pid --fork --kill-child true`). It can go green on a host where `_probe` fails (e.g. util-linux < 2.38 lacking `--map-user`, or nested-userns denial), i.e. a false-green invariant against the tool's real requirement. Make it invoke the same nested incantation.
- Absent `/dev` is good hardening, but undocumented in the tool description; `subprocess`/`os.devnull` usage fails opaquely.

### Clean
Plane hygiene (no client tokens in `tools/python_sandbox_tool.py`, `toolsets.py`, `pyproject.toml`, shared tests); slot builder patches the June baseline, so the source-constitution edit is generated output, not a double-application (anchors still match, `MGMT_NEW_INSTRUCTION_COUNT` 13→14 and the `python_sandbox` config assertion are consistent); both manifests carry `tools/python_sandbox_tool.py`; SQLite snapshot path, symlink boundary, redaction recursion, and output caps all hold up.

**Closure list:** (1) make the flood E2E actually exercise `RLIMIT_NPROC` (no `DEVNULL` dependence; assert `EAGAIN` and a bound near `max_processes`); (2) bound total `/work` bytes and prune by size; (3) align `verify_runtime.sh`'s probe with `_probe`; (4) order `interrupted` before the CPU heuristic and declare the status in the contract; (5) `cd /work`.

BLOCKED