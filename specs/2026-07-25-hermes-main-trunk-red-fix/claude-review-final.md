# External Review — claude

**Prompt:** Blocking pre-merge review of hermes-pcl trunk-red repair, built by Codex. Review the supplied full diff against these constraints:
- Repair shared fixtures/assertions for the pre-existing 57-test trun...

---

## CLEAR

Verified read-only against source in this worktree. I could not run the suite or inspect git history, so the "21/21 sequenced pass" and the `b5bf7e9d76` provenance for `max_output_tokens: 8192` remain reported-not-verified.

### Constraint-by-constraint verification

**Bounded dry-run logical oracle** — `tests/gateway/test_durable_jsonl_consumer.py:1581-1626` snapshots all three inbox tables (`ingress_events`, `ingress_meta`, `reply_deliveries`), matching the schema, plus `pa_turns`, `cases`, `ps_audit_log`, `inbox.counts()` and `window_statuses(selected)`; `selected` is computed *before* the run (`:1579`) so the window is pinned. All six connections close in `finally`. `zero_real_sends` and `processed_message_ids == []` retained (`:1644-1645`). A stray delivery row now fails the equality assert.

**No constitution / live-state mutation** — the only `deploy/` file touched is `christopher/scripts/attention_digest_watcher.py:62` (the Ruff encoding repair, present ✓). `christopher_tgg_constitution.yaml` is untouched, and the new assertions match it as-is: `tgg_case_search` at `:377`/`:400`, `max_output_tokens: 8192` at `:707`.

**Two regressions remain truthful strict xfails** — `tests/test_pa_case_state_echo.py:307` (THIS-turn state gate) and `tests/test_pa_compaction_guidance.py:222` (ops-ingest guidance-not-policy, i.e. the reintroduced `preserve-recent`). Both `strict=True`, both name WB 7fa805be, and neither assertion body was rewritten — a constitution restore will XPASS and force marker removal. `test_christopher_management_keeps_preserve_recent_policy` (`:243`) still runs unmarked, so the counterpart contract is not lost.

**PA refs on the task-local surface** — `tests/test_pa_business_facts.py:51-64` and `tests/test_pa_case_state_echo.py:35-45` reset each `Token` via `token.var.reset(token)`, restoring the `_UNSET` sentinel so `get_session_env`'s `os.environ` fallback (`gateway/session_context.py:158-166`) still works for the neighbouring env-based tests. This is the correct fix over `clear_session_vars`, which permanently pins `""` (`:129-140`).

**TUI teardown, no production regression** — `tests/test_tui_gateway_server.py:15-48` wraps `server._start_notification_poller` at the module global, so pollers are tracked regardless of `_sessions.pop`; teardown signals, joins (1.0 s ≥ the 0.5 s `get` in `tui_gateway/server.py:3060`), and **asserts** both per-thread and via a global name scan. `threading` is imported at `:4`. Production `_finalize_session` (`tui_gateway/server.py:290-299`) sets `_finalized`, guards self-join (`:296`), and joins *before* touching `history_lock` (`:303`) — no lock-ordering deadlock. `stop._thread = t` is safe (`threading.Event` has no `__slots__`).

**Remaining mechanical items** — `_set_session_env(_context, event=None)` matches `gateway/run.py:15032`; `pa-business` → `custom` at `tests/cron/test_pa_job_brief.py:87` matches the unmodified fixture `tests/fixtures/pa/bobby_tgg_constitution.yaml:38` (stale assertion, no behavior ratified); WhatsApp isolation is explicit with a control assert on `_group_policy` plus autouse `delenv`; TTS/vision fakes and `prod_pilot_run_id` / `_client_cache_key` / `st_mode` all match production surfaces.

### Non-blocking notes

- **Scope:** `scripts/release.py:48-49` (`edna@papercut-labs.com` → `teren-papercutlabs`) is unrelated to the trunk-red inventory and changes who generated release notes credit for agent-authored commits (`resolve_author`, `:1249-1254`). It rides its own commit, so I'm not blocking, but it deserves explicit sign-off from that maintainer rather than landing inside a test-repair merge.
- `tests/test_tui_gateway_server.py:41` — if the per-thread assert fires, `sessions.clear()` and the global scan (`:42-48`) never run, leaving a poisoned registry for the rest of the module. Wrap the cleanup in `try/finally`.
- The autouse teardown calls the real `_finalize_session`, which fires plugin hooks and `db.end_session` for every leftover session. Both paths are `except`-wrapped, so it's safe, but the fixture now has production side effects it didn't before.
- `hermes_cli/gateway.py:2114` — the `PermissionError` swallow makes the two `/root/` unit tests pass because the probe is *inaccessible*, not because PATH is remapped; a root-run image with a readable `/root/.hermes/node/bin` re-reds them. Patching `get_hermes_home` in the fixture (as `tests/hermes_cli/test_whatsapp_setup_ordering.py:32-34` does) would be environment-independent.
- `tests/hermes_cli/test_whatsapp_setup_ordering.py:135` stubbing `Path.exists` True for any `bridge.js` removes a real precondition from the skip-branch test.
- The PA fixtures set the other eight session vars to `""` for the test's duration (`set_session_vars` sets all nine, `gateway/session_context.py:104-115`); harmless for these tests, but worth knowing if one later needs `HERMES_SESSION_PLATFORM` from env.