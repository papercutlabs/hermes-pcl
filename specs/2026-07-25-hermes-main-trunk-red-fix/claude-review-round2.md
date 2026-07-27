# External Review — claude

**Prompt:** Blocking round-2 review. Read the prior verdict, response brief, and full current branch diff. Return CLEAR or BLOCK with evidence.

---

## CLEAR (with non-blocking notes)

All four round-2 items check out against source. Verified read-only; I could not run tests or inspect git history, so the "21/21 sequenced pass" and the `b5bf7e9d76` provenance claim are taken as reported, not independently confirmed.

### Blocker 1 — resolved
- `tests/test_tui_gateway_server.py:15-48` now tracks pollers at the source. Both production call sites (`tui_gateway/server.py:592`, `:1969`) resolve `_start_notification_poller` through the module global, so `monkeypatch.setattr(server, ...)` at line 30 intercepts every start regardless of whether the test later pops `_sessions`. No test patches that symbol itself (grep: only the fixture references it), so nothing escapes tracking.
- Teardown signals, joins with a 1 s budget, and **asserts** per-thread (`:41`) plus a global name scan (`:43-48`) — loud, not silent.
- The join budget is adequate: `_notification_poller_loop` blocks at most 0.5 s per iteration (`tui_gateway/server.py:3060`, `get(timeout=0.5)`) and re-checks `stop_event` each pass (`:3058`).
- `threading` is imported at `tests/test_tui_gateway_server.py:4` ✓. `stop._thread = t` (`server.py:3138`) is valid — `threading.Event` is a plain Python class, no `__slots__`.
- Fixture/monkeypatch finalization order matches the comment at `:18-20`: the fixture depends on `monkeypatch`, so it tears down first and still sees the real registry.
- Production `_finalize_session` (`server.py:290-299`) joins with the correct self-join guard.

### Blocker 2 — resolved on the evidence given
`deploy/tgg/christopher/christopher_tgg_constitution.yaml:707` carries `max_output_tokens: 8192` under `tgg_management`'s `response_policy` (`:328`, `:696`), and the brief attributes it to `b5bf7e9d76` post-dating recovery. That is a git-history claim I can't check with read-only tools; if it holds, updating the masked stale assertion (`tests/test_pa_constitution.py:161`) is correct. The two real recovery regressions remain unapplied per the brief.

### Important 1 — resolved
`tests/gateway/test_durable_jsonl_consumer.py:1581-1626` snapshots all three inbox tables — `ingress_events`, `ingress_meta` (PK `key`), `reply_deliveries` (PK `delivery_key`) — matching the schema at `gateway/durable_jsonl_consumer.py:368-383`, plus `pa_turns`, `cases`, `ps_audit_log`, counts and window statuses. All six connections close in `finally`. A stray `reply_deliveries` insert would now fail the equality assert. Seeded tables all exist (`_seed_bounded_state`, `:1498-1517`), and `sqlite3` is imported at `:5`.

### Important 2 — resolved
Both fixtures now reset tokens (`tests/test_pa_business_facts.py:51-65`, `tests/test_pa_case_state_echo.py:35-45`) instead of calling `clear_session_vars`, which sets `""` permanently (`gateway/session_context.py:118-140`). `set_session_vars` returns one token per var (`:104-115`); resetting restores the `_UNSET` sentinel, so the env fallback at `:158-166` works again — which is what the still-env-based tests at `tests/test_pa_business_facts.py:255,279` need.

### Important 3 — resolved
`tools.xai_http.get_env_value` is patched with a `side_effect` that returns `default` for everything but `XAI_API_KEY`, so `XAI_BASE_URL` resolution is exercised again. The provider gate reaches that seam via `resolve_xai_http_credentials()` (`tools/transcription_tools.py:269-271`, `:295-297`, `:714-716`) ✓.

### New-since-round-1 changes, both fine
- `tests/cron/test_pa_job_brief.py:87` `pa-business` → `custom`: matches `christopher_tgg_constitution.yaml:657-662`, and `toolsets.py:181-207` shows `custom` and `pa-business` expose an **identical** tool list. No capability loss ratified.
- `tests/hermes_cli/test_whatsapp_setup_ordering.py:32-34`: `get_hermes_home` exists in both patched namespaces (`hermes_cli/main.py:209`, `hermes_cli/config.py:325`), so `setattr` won't AttributeError.

### Non-blocking
- `tests/test_tui_gateway_server.py:41` — if the per-thread assert fires, `sessions.clear()` and the global scan never run, leaving a live poller and a poisoned registry for the rest of the worker. Wrap `sessions.clear()` in `try/finally` so cleanup precedes the assertions.
- `_finalize_session`'s new `join(timeout=1.0)` can add up to 1 s to session close in production if the poller is mid-`_run_prompt_submit`. The post-stop drain loop (`server.py:3095-3126`) also still dispatches turns on a session that is being finalized — pre-existing, but the join makes the window observable.
- `tests/hermes_cli/test_whatsapp_setup_ordering.py:135` stubbing `Path.exists` True for any `bridge.js` removes a real precondition from the skip-branch test; narrow scope, but it weakens what that test proves.
- Patching `get_hermes_home` in only `hermes_cli.config` and `hermes_cli.main` leaves other importers (e.g. `hermes_cli/gateway.py`) on the real home; fine for this test's surface, worth knowing if the setup path grows.