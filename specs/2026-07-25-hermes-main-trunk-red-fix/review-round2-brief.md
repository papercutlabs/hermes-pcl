# Cross-provider review round 2

Re-review the full branch diff after the round-1 BLOCK. Verify each blocker/important finding is resolved:

1. TUI notification pollers are tracked independent of `server._sessions`; teardown signals, joins, and fails loudly on leaks. Production finalization also joins its poller. A one-worker sequence of the three raw-pop TUI tests followed by all async-delegation tests passes (21/21).
2. Bounded dry-run snapshots `ingress_events`, `ingress_meta`, and `reply_deliveries`, plus downstream state/case/audit tables, and explicitly closes snapshot connections.
3. PA source-ref fixtures reset their ContextVar tokens to restore prior state rather than calling the production explicit-clear helper.
4. xAI key mocks return only for `XAI_API_KEY`, preserving base-URL fallback behavior.

Round-1 concern about `max_output_tokens == 8192` is resolved by source history: commit `b5bf7e9d76` (`feat(tgg): retain and deliver case media`, 2026-07-22) deliberately added the 8192 cap AFTER recovery commit `94b62b29cc` (2026-07-10). It is not recovery residue and updating the masked stale assertion is correct. The two actual recovery regressions remain unapplied and their tests remain failing pending Teren's freeze decision.

Return CLEAR or BLOCK with concrete evidence. Review all branch changes, not only the response patch.
