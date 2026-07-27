# External Review — claude

**Prompt:** Re-review the current python_sandbox implementation after remediation of your B1-B3 findings. Ground against current tools/python_sandbox_tool.py, tests/test_python_sandbox_tool.py, the prior BLOCK re...

---

Grounded in the current tree (`tools/python_sandbox_tool.py`, `tests/test_python_sandbox_tool.py`, `.github/workflows/python-sandbox.yml`, `deploy/tgg/christopher/**`, `specs/.../evidence/**`) and the committed run log. Note: read-only tools only — I could not hash the workbook or re-run anything.

## B1 — privileged PID-1 sibling: closed

`init.sh` now `exec`s a Python supervisor as namespace PID 1 (`tools/python_sandbox_tool.py:362-363`), and `_SUPERVISOR_SOURCE:58-61` calls `prctl(PR_SET_DUMPABLE, 0)` and hard-fails on error **before** `subprocess.run` spawns the nested `unshare --map-user=65534` payload (`:71-83`). That ordering is the whole point and it is correct.

The mechanism holds by construction, independent of the test: `__ptrace_may_access` reaches the uid-equality path (payload and supervisor share the host kuid), but the `!dumpable` branch then requires `CAP_SYS_PTRACE` in `mm->user_ns` — the *supervisor's* userns. The payload holds capabilities only in its own child userns (descendant), never in an ancestor, so it fails there. The supervisor never `execve`s again, so dumpable stays 0 for its lifetime. This also incidentally closes `/proc/1/environ|mem|root|cwd`. `verify_runtime.sh:84-85` no longer needs a `yama.ptrace_scope` assertion — the guarantee is no longer sysctl-dependent, which is the right fix rather than the one I suggested.

**Important caveat on the proof, not the mechanism:** `tests/test_python_sandbox_tool.py:556-558` asserts `PTRACE_ATTACH(1)` returns `EPERM`, and it passed — but the runner is Ubuntu 24.04 (`linux-jailed-e2e-acceptance.txt:11-16`), where `kernel.yama.ptrace_scope=1` by default and the workflow only clears `apparmor_restrict_unprivileged_userns` (`.github/workflows/python-sandbox.yml:26`). Yama alone rejects attaching to a non-descendant, so this E2E would pass even if the `prctl` were deleted. On the Debian VPS (`ptrace_scope=0`) the test would exercise the real gate, but CI would not catch a regression. Cheap discriminator: add `open("/proc/1/environ","rb").read()` (or `/proc/1/mem`) to the same probe dict — `PTRACE_MODE_READ` does not go through Yama's `ptrace_scope=1` attach check, so it isolates the dumpable gate on both hosts.

## B2 — symlink export: closed, with a bounded residual

`_SUPERVISOR_SOURCE:87-96` walks `/work` with `followlinks=False` and unlinks every symlink (dirs and files, including danglers, which `os.walk` classifies as non-dirs) before the `remount,bind,rw /export` + `cp -a` (`:98-99`). Host-side defence in depth is also in: `_list_files` uses `path.stat(follow_symlinks=False)` + `S_ISREG` (`:500-509`), and `_harvest` lstats `result.json` before reading (`:532-536`). The E2E asserts both the absent listing entry and `not os.path.lexists(run_work/"host-pointer")` (`tests:580-582`).

Residual (non-blocking): the strip is not atomic with respect to a lingering payload *grandchild*. `subprocess.run` waits only on the direct child; a backgrounded process in the same PID ns can recreate a symlink between the strip loop and `cp -a`, or write into `/export` during the rw window. Nothing on the host follows it (both readers lstat), and PID-1 exit SIGKILLs the namespace immediately after, so the only artefact is a dead pointer persisted in the run dir — which becomes live the day Q6 (`/work` delivery) ships. A `find /export -type l -delete` after the copy, or moving the strip host-side post-harvest, closes it permanently.

Second residual in the same block: `cp -a` runs with `check=True`. A payload that leaves a unix socket in `/work` makes GNU `cp` exit non-zero, so the supervisor dies with a `CalledProcessError` traceback — masking a successful payload as `error` and discarding all artifacts. Self-inflicted only, no host exposure, but worth `check=False` plus a status carry-through.

## Signal statuses: preserved and proven

`:100-102` maps a signal-killed payload to `128 - (-N)` = `128+N` and otherwise passes the code through; `init.sh` `exec`s the supervisor so `sh` and the outer `unshare --fork` propagate it unchanged. `137` lands in the OOM branch (`:795-800`) and `152` in `_cpu_limit_exhausted` (`:622`); both jailed E2Es (`tests:642-683`) are green in the run. Pre-existing and unchanged: a payload legitimately exiting `137`/`152` is misreported as `oom`/CPU-exhausted.

## Evidence: green run is real, provenance record needs two corrections

`linux-jailed-e2e-acceptance.txt` is a full log for run `30237584638`: 18 unit passed (`:294-295`), `9 passed in 8.22s` with the `^9 passed in `, no-skip, and `test -s .../open-cases.xlsx` gates all executed (`:299-311`), artifact `8642133344`, 6386 bytes, uploaded and finalized (`:335-337`). Checkout is `fba5109 = merge 4d999fa71d into 895588632a` (`:118`) — i.e. current `HEAD`, so the tested code is the reviewed code. B3 is satisfied.

Two things to fix before close (mechanical, not blocking):
1. `xlsx-acceptance.md:15` says "Branch commit: `693a980889`" — that commit predates the supervisor remediation entirely and its CI run had 8 E2Es. The run it cites checked out `4d999fa`. Correct the attribution.
2. `verification-summary.md:16` still reads "BLOCKED → BLOCKED → `CLEAR`. Final verdict … in `cross-provider-review-v3.md`", omitting the fourth review (`cross-provider-review-acceptance-blocked.md`) and this one. Update the sequence and point at the final verdict.

Also note the evidence is not yet committed: per git status, `linux-jailed-e2e-acceptance.txt` is untracked and `verification-summary.md` / `xlsx-acceptance.md` / `open-cases-acceptance.xlsx` are modified. The workbook SHA-256 in `xlsx-acceptance.md:23` is unverifiable with read-only tools.

## Carried forward from the prior review (still open, still non-blocking)

- **Timeout discards artifacts.** `_kill_group` SIGTERMs the group including the supervisor, so the export never runs — `files: []` and `result: null` on `timeout`/interrupt. Consistent with design §4.5, but undocumented in `_BASE_DESCRIPTION`.
- **CI trigger is `pull_request` only** (`python-sandbox.yml:4-13`); a merge can land without the jail battery running on the merged tree.
- **Probe cache is dead.** `_PROBE`/`_PROBE_TTL` is never read — every entry point forces (`:198`, `:641`, `:846`), so each call spawns two probe subprocesses.
- **No `/dev` in the jail**, still unmentioned in the tool description.

Plane hygiene re-checked on the new code: `_SUPERVISOR_SOURCE` and the acceptance fixture carry no client tokens (`JOB-####`/`ZONE-#` are generic; `job_no` does not match the registered `SK/JOB` token). `supervisor.py` is generated at runtime from shared-plane source, so no manifest entry is required.

VERDICT: CLEAR