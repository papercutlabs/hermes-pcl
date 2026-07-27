# Acceptance verification

- Local non-E2E suite: 18 passed.
- GitHub Linux jailed suite: 9 passed, 0 skipped.
- The jail run generated and validated the staged XLSX described in
  `xlsx-acceptance.md`.
- Plane-lint comparison used isolated `origin/main` and branch trees:
  79 findings on main, 79 on the branch, 0 added, 0 removed.
- Shared-plane diff scan found zero TGG/Christopher/client dataset-instance
  tokens.
- `plane-lint-baseline.json` is unchanged.

The plane-lint command remains globally red on the repository's existing
79-item baseline. The relevant acceptance claim is the exhaustive branch/main
delta above: this change introduces zero findings and zero baseline entries.
- Final branch-head Linux run `30237839217`: 18 unit passed; 9 jailed E2E passed with 0 skips, including both `PTRACE_ATTACH` and `/proc/1/environ` PID-1 isolation checks. Evidence: `linux-jailed-e2e-final-head.txt`.
