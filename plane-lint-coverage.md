# plane-lint coverage vs the agnosticism audit

**Date:** 2026-07-24 · **WB:** 69614cce (PA de-fusion Phase 0.2, L2 warn-mode)
**Audit:** `specs/2026-07-23-tgg-diag-synthesis/agnosticism-audit.md` (worktree edna-675911a8, commit 4ce36d6e) — IDs D1-1..34, D2-1..6, D3-1..2.
**Original Phase-0 baselines:** hermes-pcl = 68 entries; systems-papercut-labs
= 26 entries.

**Phase-1 amendment:** the Hermes scan now includes `scripts/`, repo-root
manifests, and `tests/`, while `deploy/<client>/` and per-client test
directories are client-plane. The current Hermes result is 98 findings:
23 established baseline entries plus 75 newly exposed, unsuppressed findings
enumerated in `plane-lint-findings.md`. The scorecard below is the historical
Phase-0 audit mapping; it is not a claim about the amended scanner's current
counts.

Granularity note: the lint keys violations by **file + check + token/import**, not by audit finding. "Caught" below means the finding's shared-plane site carries at least one baselined lint entry that the finding's fix would remove or that pins the file as contaminated. Several findings share one file (e.g. 8 findings in `tools/pa_business_tools.py`); burning one finding down does not clear the file's entry until all its tokens go.

## Scorecard

| Class | Findings | Caught by lint | Missed |
|---|---|---|---|
| D1 client-into-shared (hermes) | 26 (D1-1..26) | 24 | D1-12, D1-26 |
| D1 client-into-shared (systems) | 8 (D1-27..34) | 6 | D1-27, D1-30 |
| D2 platform-into-client | 6 | 3 (D2-3, D2-5, D2-6) | D2-1, D2-2, D2-4 |
| D3 copy-paste-platform | 2 | 0 | D3-1, D3-2 |
| ilinked modules (uncovered by audit) | 2 | 2 | — |
| **Total** | **44** | **35** | **9** |

The plan's honest-coverage table (§4, plan doc) estimated L2 at ~30/42; the shipped lint catches 35/44 — over the bar. It exceeds the estimate because agent-config.ts (D1-29) and the D2-3 consumer site happen to carry literal `tgg` tokens, so L2 reaches two items the plan had provisionally assigned to L3.

## Caught (35)

- **D1-1..11** — `tools/pa_business_tools.py` (tokens: tgg, SK/JOB, hdb, ilinked, christopher, sprucing), `toolsets.py` (tgg, sprucing), `tools/pa_photo_pair_classifier.py` (sprucing; its `/JOB/` grammar rides the file's sprucing flag).
- **D1-13..23** — `gateway/run.py`, `gateway/durable_jsonl_consumer.py`, `gateway/replay.py`, `gateway/platforms/whatsapp.py` (tgg, hdb, christopher), `gateway/replay_orchestrator.py` (tgg). D1-19's hard import of `validate_tgg_spreadsheet` is caught as a **token** violation, not import-direction — `tools/` is itself shared-plane today, so the arrow is shared→shared until Phase 2 moves the validator into `clients/tgg/`; the token entry pins the site regardless.
- **D1-24, D1-25** — historically caught in `hermes_cli/replay.py`. Phase 1
  removed the literal `SWAP_TGG_TARGET` and tenant default; confirmation is
  now tenant-derived and tenant is explicit.
- **D1-28, D1-29, D1-31..34** — systems: `src/lib/types.ts` (hdb, sprucing, ilinked, tgg — the camelCase boundary rule catches `iLinked*`/`Tgg*` identifiers), `src/spine/agent-config.ts` (tgg), `src/App.tsx` (tgg token + 4 `@/tenants/tgg/*` import-direction entries), `src/server/main.ts` (tgg, huidapcl + 4 import entries), `src/server/seed-tgg-master.ts` (path + content + 2 imports).
- **D2-3** — `gateway/durable_jsonl_consumer.py`: the tenant-DB capture writer sits in an already-token-flagged shared file (file-granular catch).
- **D2-5** — caught on its shared-plane side: `src/server/main.ts :: import-direction :: ../tenants/tgg/media-index.js` is exactly the platform→client dependency inversion the finding names.
- **D2-6** — `src/components/CorrectionReviewView.tsx` (tgg token + `@/tenants/tgg/views/_shared` import).
- **ilinked ×2** — `tools/tgg_ilinked_reads.py`, `tools/tgg_ilinked_lookup.py`: path + content (tgg, ilinked, christopher). These are the two modules the audit missed and the 10-minute token sweep found — the lint reproduces that catch mechanically.

## Missed (9) and why

Every miss is a **by-design L2 boundary**, not matcher weakness — each is assigned to another layer of the anti-drift plan:

- **D1-12** (`agent/prompt_builder.py` noun list), **D1-27** (`grounded-correction-contract.ts` zone/case_type enum), **D1-30** (`src/spine/types.ts` scope vocabulary) — token-clean shape violations. No client token appears; the violation is that the *vocabulary shape* is one client's. This is exactly the photo-classifier class → **L3 diff-fired concept lens**. (Adding `zone`/`case_type`/`cases:*`/`sgt` to the registry would flood shared code with false positives — `zone` alone appears in timezone handling everywhere.)
- **D1-26** (`--since-sgt`/`--until-sgt`) — `sgt` is deliberately NOT in the tgg registry: it is a Singapore-time suffix used platform-wide in hermes (`ZoneInfo("Asia/Singapore")` sites, time helpers), so as a token it is not client-discriminating. File-granular consolation: `hermes_cli/replay.py` is already pinned by its tgg entries, so the file cannot silently regress. Fix rides Phase 1 regardless.
- **D2-1, D2-2, D2-4** (replay-target provider, `bridge_message_log`, turn-provenance columns — all in `src/tenants/tgg/`) — the inverse arrow: platform machinery homed in a **client-plane** file. Both lint checks scan shared-plane files only; a client file containing platform concepts is invisible by construction. Assigned to the weekend message-store build (D2-2/4) and Phase-4 extraction (D2-1) as named acceptance criteria.
- **D3-1, D3-2** (`deploy/tgg/christopher/scripts/` manifest + processing-gate) — same inverse-arrow class on the deploy plane; rides the next `pa-agent` bundle work.

## Matcher notes

- Word-ish boundaries treat `_ - / .` as separators AND recognize camelCase transitions — required for `tggView`, `registerTggRoutes`, `iLinkedReconciliation` (real audit sites). Verified no false positives from `mtu`-in-word (`mtual` etc.): zero mtu-registry hits in hermes shared code, matching the audit's finding that drift is entirely TGG-direction.
- `deploy/finexis/` was shared-plane in the original Phase-0 snapshot. The
  amended manifest now classifies `deploy/<client>/` as client-plane, so those
  42 prior baseline entries are intentionally outside shared-plane lint. This
  is a plane-boundary reclassification, not proof that the carried client
  content is clean.
- Hermes has **zero import-direction violations today** because the fused client code physically lives inside shared dirs — there is no `clients/` package to import yet. The import check becomes load-bearing at Phase 2 (tool-pack extraction), when `clients/tgg/` exists and the loader seam gets its one `loaderSeamExceptions` entry.

## CI

- **hermes-pcl:** `.github/workflows/plane-lint.yml` runs the lint in warn mode on push/PR (always exit 0; annotates new violations). Flip to `--strict` when the suppression list burns down, per plan §4 dial setting.
- **systems-papercut-labs:** no CI exists in the repo (no `.github/`); skipped per WB scope. The lint is runnable locally (`python3 scripts/plane_lint.py [--strict]`) and via the future deploy-time hard-block (plan L2, `pcl pa-agent bundle`).
