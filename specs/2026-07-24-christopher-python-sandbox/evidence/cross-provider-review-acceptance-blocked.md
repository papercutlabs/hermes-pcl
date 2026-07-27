# External Review — claude

**Prompt:** Review the full python_sandbox implementation and acceptance extension against the ratified design and build brief. Maker is Codex; you are the cross-provider reviewer. Pay special attention to jail e...

---

## Review — `python_sandbox` (WB 8ddae42d), post-acceptance extension

Reviewed against `design.md` §2–§8 and `build-brief.md`, grounded in the working tree (`tools/python_sandbox_tool.py`, `tests/test_python_sandbox_tool.py`, `.github/workflows/python-sandbox.yml`, `deploy/tgg/christopher/**`, `specs/.../evidence/**`).

Most of the ratified shape is genuinely delivered: fail-closed probe including the nested incantation (`tools/python_sandbox_tool.py:98-138`), snapshot-not-live-DB with pre/post size guard and unlink (`:154-172`, proven by `tests/test_python_sandbox_tool.py:134-152`), fail-closed `ro_dir` (`:249-256`), recursive redaction over stdout/stderr/result/files (`:388-399`, `:776`), fixed credential-blind env (`:326-346`), tmpfs-capped `/work` with a `ro` `/export` for the payload's whole lifetime (`:292-295`), real `EAGAIN`/`ENOSPC` E2Es (`tests:689-760`), both manifests and the runtime invariant. Three things I can't clear.

### Blocking

**B1 — a privileged ns-root shell stays alive inside the jail for the payload's entire lifetime, and the payload shares its host uid.**
`tools/python_sandbox_tool.py:304-319` deliberately stopped `exec`-ing the nested `unshare` (v2 did `exec`) so the wrapper can run `mount -o remount,bind,rw /export; cp -a /work/. /export/` after the payload exits. That wrapper is PID 1 of the jail's PID namespace, owns the mount namespace, and holds CAP_SYS_ADMIN over exactly the mounts that make the read-only guarantee (`/venv` → the deployed hermes tree, `:264`; every dataset bind, `:283-289`).

The payload cannot drop those RO flags itself — the E2E proves `EPERM` for direct `mount(2)` (`tests:542-551`) — but it does not have to. `unshare --map-user=65534` only remaps presentation; the payload's *kuid/kgid stay the host `pclaw` identity*, identical to the wrapper's. `__ptrace_may_access` passes on the uid/gid equality path, the wrapper's `mm` is `SUID_DUMP_USER`, and the only remaining gate is Yama — which is `ptrace_scope=0` by default on Debian (Ubuntu, not Debian, ships the sysctl that sets 1). Under that configuration a payload can `PTRACE_ATTACH` to PID 1 and inject `mount(NULL, "/venv", NULL, MS_REMOUNT|MS_BIND, NULL)`, then write into `/home/pclaw/apps/hermes-pcl/.venv` (code executed by `christopher-tgg-hermes.service` on next restart) or into the client media root. That is design §2.3's kernel-class "no writes outside scratch" and "no privilege gain" defeated.

The v2 closure argument ("payload's caps are scoped to the child userns") is correct about the payload's *own* credentials and silent about borrowing the parent's; `cross-provider-review-v3.md:11` restates it as closed without considering a live privileged sibling. Nothing in the battery tests this, and `verify_runtime.sh` does not assert `kernel.yama.ptrace_scope`, so the guarantee currently rests on an unverified host sysctl.

Closure options, cheapest first: (1) add an E2E that `PTRACE_ATTACH`es PID 1 and injects a remount, asserting `EPERM` — if it fails on the CI runner and on Debian, the concern is refuted and the test locks it in; (2) replace `/bin/sh` with a tiny wrapper that calls `prctl(PR_SET_DUMPABLE, 0)` before spawning the payload (the dumpable check then demands caps in the *wrapper's* userns, which the payload lacks); (3) restore `exec` of the nested `unshare` and harvest `/work` from the host parent via `/proc/<pid>/root/work` while the namespace lives, so no privileged process coexists with untrusted code.

**B2 — `cp -a` copies payload-controlled symlinks into the host run dir, and the host harvester follows them.**
`:318` copies `/work` verbatim; `cp -a` preserves symlinks as symlinks. A payload can plant `/work/x -> /home/pclaw/.hermes-christopher-tgg/.env`. Host-side `_list_files` (`:455-464`) uses `path.is_file()` and then opens the path to count lines — both follow the link. Today that leaks only size/line-count metadata for any `pclaw`-readable file into model context, but it also makes the persisted run dir a live pointer into `HERMES_HOME`, and it becomes content exfiltration the moment Q6 (`/work` artifact delivery to WhatsApp) ships. Fix: `os.lstat`/`is_file(follow_symlinks=False)` and skip non-regular entries in `_list_files`; optionally `cp -a --no-dereference` plus a symlink strip during export.

**B3 — the acceptance gate has no green-run evidence, and the summary is stale.**
`.github/workflows/python-sandbox.yml:38,40` now require `^9 passed in ` and `test -s /tmp/python-sandbox-acceptance/open-cases.xlsx`, but the only committed CI log (`evidence/linux-jailed-e2e-final.txt:298,308`) is the 8-test run with the `^8 passed` gate, and `evidence/verification-summary.md:12` still claims "exactly eight jailed E2Es" and links that same run. So `evidence/open-cases-acceptance.xlsx` has no verifiable provenance from the repo. Build brief gate 1/DoD ("E2E proven to actually execute") is not evidenced for the new test. Fix is mechanical: attach the green 9-test run log and update the summary.

On the substantive question you asked: the artifact *is* jail-produced by construction — `openpyxl` writes to the in-jail `/work` tmpfs (`tests:463-464`), the trusted wrapper exports it post-exit (`:317-318`), and the test copies from the run dir to `PYTHON_SANDBOX_ACCEPTANCE_DIR` (`tests:500-504`), which the workflow uploads. The mechanism claim is sound; only the evidence is missing. The test's assertions are also internally consistent (80 of 120 rows have `index % 3` truthy; `max_row == 81` with the header).

### Important, not blocking

- **Timeout discards all artifacts.** `_kill_group` SIGTERMs the process group (`:546`), which includes the wrapper, so `mount ... rw /export; cp -a` (`:317-318`) never runs: a timed-out or interrupted run returns empty `files` and `null` result even though the payload may have written detail. Design §4.5 only promises partial stdout, so this is not a contract break, but it is a real behavior change from the pre-export build and worth either fixing (trap in the wrapper, or copy-out from the host via `/proc/<pid>/root`) or documenting in the tool description so the model retries sensibly.
- **CI trigger coverage.** `pull_request` path filter only (`:4-13`); no `push` to main. A merge can land without the jail battery running on the merged tree. Also `grep -Eq '^9 passed in '` false-reds if pytest emits warnings — fails closed, still fragile.
- **Dead probe cache.** `_PROBE`/`_PROBE_TTL` (`:48-49`, `:104-106`) is never consulted because every entry point forces (`:144`, `:589`, `:793`), so each call spawns two probe subprocesses. Registry TTL-caching described in design §2.5 is effectively bypassed.
- **No `/dev`.** Undocumented in `_BASE_DESCRIPTION` (`:804-813`); model-authored `subprocess(stdout=DEVNULL)` fails with an opaque `FileNotFoundError`. The NPROC test now relies on inheriting fds (`tests:695-697`) precisely because of this.
- **`verify_runtime.sh`** matches the runtime probe (good) but does not assert `kernel.yama.ptrace_scope`, which B1 makes load-bearing.

### Clean

Plane hygiene holds — no `tgg|christopher|SK/JOB|hdb|ilinked|huidapcl|sprucing` tokens in `tools/python_sandbox_tool.py`, `toolsets.py`, or the shared test (the acceptance fixture uses generic `JOB-####`/`ZONE-#`, and `job_no` does not match the registered `SK/JOB` token, `plane-manifest.json:23-32`). Output caps, `result_invalid` guidance, concurrent drain (no pipe-fill deadlock), snapshot integrity, symlink boundary on `path` datasets, interrupt-before-CPU status ordering (`:730-749`), and kill-ladder/`--kill-child` all check out.

VERDICT: BLOCK