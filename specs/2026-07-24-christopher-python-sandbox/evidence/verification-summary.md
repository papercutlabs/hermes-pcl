# Python sandbox verification

## Local (macOS)

- Relevant Hermes suites: `91 passed, 8 skipped` (`local-relevant-tests-final.txt`): sandbox tool, toolset exposure/distributions, and registry. The eight skips are the real Linux user-namespace E2Es; macOS has no `unshare`.
- Static/deployment gates (`local-static-gates-clean.txt` plus final rerun): Ruff clean, runtime slots regenerated, deployment spec valid, and `git diff --check` clean.
- Plane fence: zero branch-added violations, zero baseline changes, zero client-token hits in the shared tool (`plane-lint-delta-final.md`). Upstream main itself is globally baseline-red after a concurrent scanner/baseline split; the branch and upstream violation sets are identical.
- Bundle assembly dry-run: success, tool included (`bundle-dry-run.txt`). This was read-only; no deployment was performed.

## Linux jailed E2E

The dedicated `Python sandbox` GitHub Actions job runs unit tests plus exactly nine jailed E2Es on Ubuntu and fails on any skip. Green run: https://github.com/teren-papercutlabs/hermes-pcl/actions/runs/30237584638 (`linux-jailed-e2e-acceptance.txt`). Coverage includes the 600-row reconciliation fixture, empty network namespace, plain-write and mount-remount escape attempts, host-path absence, timeout/orphan cleanup, distinct OOM and CPU-limit statuses, real NPROC/EAGAIN enforcement, and a hard total scratch-space cap.

## Independent review

Claude/Opus cross-provider security review progressed BLOCKED → BLOCKED →
`CLEAR` on the original build, then BLOCKED → `CLEAR` on the XLSX acceptance
extension and supervisor hardening. The final verdict and grounding are in
`cross-provider-review-acceptance-clear.md`; prior verdicts are retained as
remediation evidence.

## Deployment boundary

No deployment or production mutation was performed. `tgg-app-1` was not changed. The driver-owned Sunday procedure is in `../deploy-runbook.md`.

The final acceptance extension additionally proves the non-dumpable PID-1 supervisor rejects `PTRACE_ATTACH`, strips payload-controlled symlinks before export, and produces the validated 80-row XLSX artifact.
