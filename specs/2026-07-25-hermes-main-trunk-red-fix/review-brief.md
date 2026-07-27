# Cross-provider review brief

Review commit `31d517b043` against the attached triage report and patch. This is a blocking pre-merge review of the mechanical/source-test repair for hermes-pcl main trunk red.

Focus on correctness, regression risk, and whether each change repairs the shared fixture/contract rather than weakening the production invariant. Pay particular attention to:
- bounded dry-run still proves logical read-only state and zero sends/writes;
- TUI watcher teardown cannot leak or consume another test's global completion queue and does not corrupt production session cleanup;
- `/root/.hermes` PermissionError handling does not hide required service PATH entries;
- task-local PA source-ref fixtures exercise the concurrent-turn-safe production surface;
- current-contract assertion updates (`custom`, `tgg_case_search`, `inter_session`, 8192 output cap) match source history rather than deleting protection.

Two PA constitution regressions are deliberately NOT in the patch: the worker proved the recovered constitution dropped the THIS-turn state gate and reintroduced preserve-case-state. Their tests remain failing and live constitution restoration is held for Teren's freeze decision. Do not treat those two expected failures as a defect in this mechanical commit; do flag any accidental constitution mutation (there should be none).

Return one of `CLEAR` or `BLOCK`, followed by concrete findings ordered by severity. A CLEAR may include non-blocking notes.
