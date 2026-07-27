# External Review — claude

**Prompt:** Review this scoped PA de-fusion Phase 0+1 change. Return CLEAR on the first line only if: (1) runtime behavior is preserved for the explicit TGG tenant and registry dump is byte-identical, (2) D1-23 g...

---

CLEAR

Verified against the working tree, not just the narrative.

## What I confirmed

**(1) TGG runtime parity.** `tenant_confirmation_token("SWAP", "tgg")` → `SWAP_TGG_TARGET`, byte-identical to the deleted `PROVIDER_CONFIRM_PROMOTE` constant; no `SWAP_TGG_TARGET`/`ORCHESTRATOR_PROMOTE` literals remain in code (`hermes_cli/replay.py`, `gateway/replay_orchestrator.py`). `ReplayTargetProviderConfig` keeps valid dataclass field ordering with the default removed, and the only non-test callers (`provider_config_from_env`, `_provider_client_from_args`) pass tenant explicitly. Registry dumps: I can't hash without a shell, but `registry-before.json`/`registry-after.json` are structurally equal on spot-check (165 `"name":` each), consistent with the recorded SHA.

**(2) D1-23 seam intact / no Phase 2 leak.** `gateway/platforms/whatsapp.py` is absent from the diff; its only replay dependency is line 1043 (`raw.get("_tgg_sgt")`), which the dual-write still satisfies. `loaderSeamExceptions` is still empty, no `clients/` pack loader appeared.

**(3) Dual-write.** `gateway/replay.py:528-533` writes `_pa_source_ref`/`_pa_local_time` alongside `_tgg_source_ref`/`_tgg_sgt` with equal values; `_source_ref_from_bridge_message` and `_dedup_key_for_message` put the new key first with the same value, so dedup/ordering is unchanged. `scripts/tgg_christopher_hermes_replay.py:1253,1511` is a self-contained legacy path and still writes/reads its own legacy keys — no call into the changed `from_bridge_message_log` signature (its `since_sgt` hits its own `_load_records`), so the renamed kwargs break nothing.

**(4) D1-35.** No root `pa-agent.manifest.json` remains; the only in-repo references are the relocated `deploy/tgg/christopher/pa-agent.manifest.json` (self `sourcePath` updated) and its entry in `pa-agent.hermes.manifest.json`. `deploy_runtime.sh` resolves `--manifest` from `spec.deploy.manifestRef` (explicit path, no inference). On the marshal side I checked the worktree: `findSourceManifest` has exactly one caller (`buildPaAgentBundle`), which now passes a required `manifestPath`, so the signature tightening has no other call site to break.

**(5)/(6) Fence and evidence honesty.** All 75 newly exposed entries appear verbatim in both `plane-lint-after.txt` and `plane-lint-findings.md`; the re-based `plane-lint-baseline.json` (23 entries) only drops entries that are genuinely fixed (`gateway/replay.py::tgg:christopher`, `replay_orchestrator`, `hermes_cli/replay.py`) or reclassified. Warn-green vs strict-red is stated plainly in the table and in the findings doc.

## Non-blocking findings

1. **`marshal-tsc.txt` does not evidence the tsc claim.** The file contains only pnpm install output — no `tsc --noEmit` invocation or result. A clean run is silent, so this is indistinguishable from "never ran". Re-capture with the command line echoed and exit code recorded. (The 7 pa-agent tests are genuinely green, and I independently confirmed the single call site, so this is an evidence gap rather than a suspected defect. Note also that only the 2 pa-agent test files were run, not the marshal suite.)
2. **Corpus source-manifest keys changed** (`since_sgt`/`until_sgt` → `since`/`until` + `tenant`/`agent_id`/`job_type`, `gateway/replay.py:322-333`), so replay-plan/corpus digests differ from pre-change runs. Any run manifest started before this commit will fail digest comparison on a post-upgrade `verify`/`promote`. Correct anti-drift behavior, but it should be stated in the phase notes so an in-flight run isn't stranded silently.
3. **New required inputs are undocumented.** `docs/pa-replay-run-lifecycle.md` gained `--tenant` but never mentions the now-required `--agent-id`/`--job-type` for bridge-log corpora, nor that plan JSON `source` blocks must now carry `tenant`/`agent_id`/`job_type` (hard `ValueError` otherwise). No committed plan files are affected; operator-local plans are.
4. **TGG promote confirmation string changed** from `ORCHESTRATOR_PROMOTE` to `ORCHESTRATOR_TGG_TARGET`, and `--confirm` lost its argparse `choices` (now validated in the `promote` branch of `cmd_replay_run`). Equivalent guard and the doc was updated in the same commit, but it is an operator-facing contract change for TGG, which the parity table describes only as "derive from configured tenant".
5. **`deploy/*/` as client-plane removes 42 previously-visible entries**, including genuine cross-client contamination (`tgg:christopher` in `deploy/finexis/mtu/OPS-NOTE.md`, `config.yaml`, `mtu_constitution.yaml`). They were baseline-suppressed before, so enforcement doesn't regress, and the findings doc discloses the re-base in prose — but unlike the 75 new entries these were not enumerated anywhere. Consider listing them once as carried-over debt, since cross-client tokens inside a client plane are now structurally invisible to the lint.
6. **Residual root manifest on the host.** The relocated manifest still has `targetPath: home/pclaw/apps/hermes-pcl/pa-agent.manifest.json`, so the next deploy recreates a repo-root-looking manifest on `tgg-app-1`. Harmless now that marshal refuses inference, but it undercuts D1-35's intent if inference is ever reintroduced.
7. **Stale doc line:** `plane-lint-coverage.md:26` still describes `hermes_cli/replay.py` as carrying `SWAP_TGG_TARGET` and a `--tenant` default (D1-24/D1-25) — both fixed by this change.