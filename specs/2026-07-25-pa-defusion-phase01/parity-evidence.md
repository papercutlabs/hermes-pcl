# PA de-fusion Phase 0 + Phase 1 parity evidence

**WB:** `e49bfc29-9dfb-4cc8-98a5-cd7a712ca913`  
**Scope:** ratified Phase 0 and Phase 1 only, with the 2026-07-25
second-pass mechanical amendments. D1-23 and every Phase 2+ item remain
excluded. No client deploy or `tgg-app-1` access occurred in this execution.

## Phase 0 evidence carried forward and re-grounded

| Step | Result | Evidence |
|---|---|---|
| 0.1 MTU pin | PASS | Local live process resolves to `/Users/pcloffice/pcl-run/hermes-mtu/.venv/bin/hermes gateway run`; pinned worktree HEAD `99a6b5e3e`, exact tag `release/mtu-pin-2026-07-24`. Original Phase-0 evidence: `~/pcl-biz/_agents/edna/specs/2026-07-24-pa-defusion-phase0/evidence-bundle.md`. |
| 0.2 Hermes plane lint | PASS before amendment | `2f9a481eff`; strict baseline at the original scan surface: 68 total, 68 known, 0 new. Raw output: `evidence/plane-lint-before.txt`. |
| 0.2 Systems plane lint | PASS and pushed | Existing lint change rebased as `153bb0f`; `x/` ignore `c85cd9a`; `origin/main` verified at `c85cd9a`. Strict lint: 26 total, 26 known, 0 new. |
| 0.3 Goldens | PASS, carried | Christopher full verify, MTU battery, portal 376-test run, and production schema snapshot are archived in the original Phase-0 evidence bundle. No client or box was re-touched here. |
| 0.4 Completeness sweep | PASS, carried | `sweep-findings.md` in the original Phase-0 evidence records D1-35..45 and D2-7. The second pass adds the manifest-surface correction executed below. |
| Plan §0.2 sync | PASS | Local edna-workspace commit `a79433fb`: shipped counts corrected to 68 Hermes / 26 systems and widened-surface entries explicitly classified as findings rather than silently absorbed. |

## Phase 0 second-pass amendments

| Amendment | Result | Evidence |
|---|---|---|
| Widen shared-plane surface | PASS | `35196fc867` adds `scripts/`, repo-root manifests, and `tests/`, while treating per-client test directories as client-plane. |
| Widen client-plane surface | PASS | `35196fc867` adds `deploy/finexis/` and the general `deploy/<client>/` classification, with regression tests. |
| Honest re-baseline | PASS, warnings exposed | `93da88bd57` preserves 23 established entries and records all 75 newly exposed entries in `plane-lint-findings.md`; none of the 75 were suppressed. Warn mode exits 0. Strict mode intentionally exits 1 until later phases remove or legitimately classify those findings. |
| D1-35 root-manifest quick kill | PASS in Hermes | Root `pa-agent.manifest.json` moved under `deploy/tgg/christopher/`; all Hermes callers/docs/tests now use explicit manifest paths (`35196fc867`, `5531250691`). |
| D1-35 CLI refusal | PASS on marshal `origin/main` | `531fd40df92cc9db5ae19c1a2242ca7b806667fc` requires an explicit manifest for both Commander and programmatic bundle construction. Independent verification: 7 focused tests pass and `tsc --noEmit` passes; remote ancestry and head were verified after push. |
| Neutral examples | PASS, description-only | Neutralized comment/docstring examples in `gateway/pa_observability.py`. Tool schema examples were deliberately unchanged because the registry-dump parity gate is byte-exact. |

## Phase 1 replay-chain parity

`5a5a6bb2d9768226b670aa7e93f8d51af5a65082` makes replay tenant and
context explicit:

- replay-run requires `--tenant`; bridge-log replay refuses an omitted tenant;
- confirmation tokens derive from configured tenant rather than a TGG constant;
- generic `--since` / `--until` time bounds replace the TGG-named replay
  surface;
- canonical replay metadata is written as `_pa_source_ref` and
  `_pa_local_time`;
- legacy `_tgg_source_ref` and `_tgg_sgt` are temporarily dual-written with
  equal values because D1-23's live renderer is explicitly excluded;
- the committed corpus fixtures still match the current harness turn
  boundaries.

Focused parity verification is 4/4 green in
`evidence/replay-parity-focused.txt`. The broader post-change run adds the
plane-lint tests and is 81 passed / 2 failed. Its two failures are byte-for-byte
the same pre-existing fixture defect reproduced at the Phase-0 baseline (74
passed / 2 failed): `Namespace` lacks `prod_pilot_run_id` in
`test_tgg_christopher_replay_profile.py`.

## Behavior and safety contract

| Gate | Expected | Result | Evidence |
|---|---|---|---|
| Tool registry | byte-identical canonical dump | PASS | Before and after: 79 tools, SHA-256 `806c7b6fb59708047ef5f0f31fcc45d53776ff79be1611f2db24e6fb730f1a4d`; `cmp` empty. Raw: `evidence/registry-before.json`, `evidence/registry-after.json`. |
| Replay fixture parity | explicit tenant path stays golden-equal; omitted tenant refuses | PASS | Focused parity 4/4; no-tenant CLI exits 1 with `--bridge-message-log requires --tenant`. Raw: `evidence/replay-parity-focused.txt`, `evidence/replay-cli-refusal.txt`. |
| Relevant suites | green, except failures reproduced on baseline | PASS | Baseline: 74 passed / 2 failed. After: 81 passed / the same 2 failed with the same missing `prod_pilot_run_id` fixture attribute. Raw: `evidence/replay-baseline-tests.txt`, `evidence/replay-after-tests.txt`. |
| Widened plane lint | green in Phase-0 warn mode, with every new scanned entry named as a finding | PASS with explicit strict-red debt | Warn mode: 98 total, 23 established, 75 new findings, exit 0. Strict mode: exit 1 by design because the new findings were not suppressed. Raw: `evidence/plane-lint-after.txt`, `evidence/plane-lint-strict.txt`, `plane-lint-findings.md`. |
| Cross-provider review | CLEAR | PASS | Claude/Opus returned `CLEAR` after inspecting the Hermes and marshal patches plus parity evidence. Raw: `evidence/cross-provider-review.md`. |
| Live effects | none | PASS | No deploy, no `tgg-app-1`, no MTU repin/restart, no env/flag rename. |

## Commits

| Repository | Step | Commit |
|---|---|---|
| Hermes | Phase-0 plane widening + explicit manifests | `35196fc867` |
| Hermes | Remove ambiguous root manifest | `5531250691` |
| Hermes | Phase-1 replay tenant/context refactor | `5a5a6bb2d9768226b670aa7e93f8d51af5a65082` |
| Hermes | Honest widened-surface findings inventory | `93da88bd57` |
| systems | Existing lint commit, rebased | `153bb0f` |
| systems | Ignore generated `x/` | `c85cd9a706c2858f206f982cf328a734b3b84691` |
| marshal | D1-35 explicit-manifest source gate | `531fd40df92cc9db5ae19c1a2242ca7b806667fc` |
| edna workspace | Plan §0.2 documentation sync | `a79433fbcc891a77ab01187a0e6b9c68718fbafb` |

No release or client deployment is part of these commits. The marshal gate is
merged on source `origin/main` but remains undeployed until a separately
authorized release.

## Review dispositions

The CLEAR review raised seven non-blocking notes. The evidence-only TypeScript
gap was corrected by recapturing the command and exit code. Operator-facing
replay context requirements, digest incompatibility for pre-change in-flight
runs, and tenant-derived promote confirmation are now explicit in
`docs/pa-replay-run-lifecycle.md`. The stale Phase-0 coverage narrative now
labels its scorecard historical and records the amended plane boundary.

The remaining notes are deliberately not expanded into out-of-scope work:
the 42 entries removed by the client-plane reclassification are carried debt,
not proof of cleanliness; the deployed target path remains untouched because
there is no deploy; and no Phase-2 cleanup was attempted.
