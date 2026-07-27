# External Review — claude

**Prompt:** Perform the blocking cross-provider review described in review-brief.md. Read all three context files. Return CLEAR or BLOCK with evidence.

---

## BLOCK

The commit is directionally right — most clusters repair the fixture rather than the invariant, and no constitution/spec YAML is touched (diff is tests + `attention_digest_watcher.py`, `hermes_cli/gateway.py`, `tui_gateway/server.py` only). Four findings must be resolved before merge; the first two are blockers.

---

### Blocker 1 — TUI watcher teardown misses the pollers that actually leak (async-delegation cluster not repaired)

`tests/test_tui_gateway_server.py:15-37` only finalizes sessions **still present in `server._sessions` at teardown**. The tests that start a *real* poller thread all remove their own entry first:

- `test_init_session_fires_reset_hook` (`tests/test_tui_gateway_server.py:786`) calls `server._init_session(...)` → `tui_gateway/server.py:1962` starts `hermes-tui-notification-sid`, then the test does `server._sessions.pop(sid, None)` at line 821.
- `session.create` tests at lines 3218 and 3293 → `tui_gateway/server.py:585` starts the poller inside `_build` (`threading.Thread` is *not* patched in these tests; both wait on `agent_ready`), then pop at lines 3244 / 3304.

The poller loop exits only on `stop_event` or `session["_finalized"]` (`tui_gateway/server.py:3051`). Popping the registry sets neither, and the fixture never sees those sessions, so the daemon threads survive for the life of the xdist worker and keep draining the global `process_registry.completion_queue` — the exact mechanism the triage attributes to failures #48–#55. Two consequences:

1. The async-delegation cluster is not reliably fixed; it will pass or fail on worker scheduling.
2. The wait loop at lines 28-37 has no assertion. With a permanently-live thread it burns the full 1.0 s on **every subsequent test in the module** and reports nothing. That is a silent multi-minute suite regression plus a silently-defeated isolation guarantee.

Fix shape: track pollers at the source rather than via the registry — an autouse fixture that wraps `server._start_notification_poller`, records every returned stop event plus thread, sets/joins them all in teardown, and **asserts** no `hermes-tui-notification-*` thread remains (fail loudly). The three tests above should also finalize instead of raw-popping.

### Blocker 2 — `max_output_tokens == 8192` ratifies an unreviewed constitution change inside a "mechanical" commit

`tests/test_pa_constitution.py` flips `assert "max_output_tokens" not in brief.response_policy` to `== 8192`. The current source does carry it (`deploy/tgg/christopher/christopher_tgg_constitution.yaml:696` `response_policy` for `tgg_management` at :328, value at :707), but the June live baseline has `max_output_tokens` **only** on ops-ingest (`baselines/june-2026/christopher_tgg_constitution.live-2026-06-19.yaml:87`) and none under `tgg_management` (:279). So this is a management-brief behavior contract that a prior test explicitly asserted must be absent, and it appears to arrive with the same recovered constitution the worker has already flagged as regression-bearing.

It is also **not in the triage inventory** — node #43 fails at the earlier `operation case_search` assert, so this one was masked. Under the brief's own carve-out (recovered-constitution regressions held for Teren's freeze decision), this cannot be silently accepted as drift. Either cite the source-history commit showing 8192 was a deliberate, reviewed addition, or route it to the same freeze decision and leave the assertion failing with the other two. `operation tgg_case_search` / `Run separate tgg_case_search calls` (constitution :377, :399-400) are genuinely obsolete-assertion fixes and are fine.

---

### Important — bounded dry-run oracle no longer covers the send-adjacent tables

`logical_state()` (`tests/gateway/test_durable_jsonl_consumer.py:1581`) snapshots only `ingress_events` from the inbox DB. That DB has three tables (`gateway/durable_jsonl_consumer.py:368-383`): `ingress_meta`, **`reply_deliveries`**, `ingress_events`. The replaced byte oracle covered all of them. A dry run that inserted a `reply_deliveries` row — i.e. recorded a delivery — now passes undetected; the only remaining send proof is the production-authored `audit["zero_real_sends"]` flag (line 1624). Add `reply_deliveries` and `ingress_meta` to the snapshot. Otherwise the logical rewrite is a good call and keeps counts/statuses/state/case/audit coverage.

Minor within the same helper: `with inbox.connect()` / `with sqlite3.connect(...)` commit but do not close; six connections leak per invocation.

### Important — `source_refs_context` teardown poisons session context for the rest of the worker

Both new fixtures (`tests/test_pa_business_facts.py:51`, `tests/test_pa_case_state_echo.py:35`) tear down with `clear_session_vars`, which by design sets **all nine** contextvars to `""` and marks them "explicitly cleared", suppressing the `os.environ` fallback (`gateway/session_context.py:118-166`). Contextvars are not reset between pytest tests, so after the first such test every later test in that worker that relies on `HERMES_SESSION_*` env fallback silently reads `""` — including `tests/test_pa_business_facts.py:254` (same file, still uses `monkeypatch.setenv`), `tests/tools/test_yolo_mode.py:42`, `tests/agent/test_skill_commands.py:526`, `tests/test_pa_turn_recording.py:739`. That is a new order-dependent contamination of the same class the commit is fixing. Restore the prior values instead — reset the individual token(s), or run the body in `contextvars.copy_context()`, or set/restore only `_SESSION_SOURCE_MESSAGE_REFS`. Binding via the task-local surface is the right call; only the teardown is wrong.

---

### Non-blocking notes

- **`_is_dir` PermissionError swallow** (`hermes_cli/gateway.py:2114`): `path_entries` *are* remapped to the target user (`hermes_cli/gateway.py:2178`), so an unreadable calling-user `~/.hermes/node/bin` silently prunes a PATH entry the service would have needed. Real-world exposure is narrow (system install runs as root, which can read `/root`), so this is acceptable, but consider logging at warning level rather than failing silent. Note also the two tests assert `'/root/' not in unit`; they now pass because the probe is *inaccessible*, not because PATH is remapped — a root-run container image where `/root/.hermes/node/bin` exists and is readable would re-red them. The triage's test-side option (patch `get_hermes_home` in the fixture) would have been environment-independent.
- **xAI patches** (`tests/tools/test_transcription_dotenv_fallback.py`): `tools.xai_http.get_env_value` is the documented test seam (`tools/xai_http.py:14-25`) ✓, but `return_value="dotenv-secret"` also feeds `XAI_BASE_URL` (`tools/xai_http.py:78`), so base-URL resolution is no longer exercised. Prefer `side_effect=lambda name, default=None: "dotenv-secret" if name == "XAI_API_KEY" else default`.
- **TTS fakes**: `extract_attachment_images` is a staticmethod on `BasePlatformAdapter` (`gateway/platforms/base.py:2064`), so the fake matches the production surface ✓. `send_multiple_images` is broader than the reported drift; fine as long as the routing assertions still fail if media is misrouted to the image sender. The triage's spec-backed shared fake remains the durable fix.
- **`inter_session`** is emergent from `_get_platform_tools` recovery, not from `_load_enabled_toolsets` (`tui_gateway/server.py:1013`); the assertion update matches current policy, but it means `send_session_message` is TUI-universal regardless of config — worth a one-line confirmation from whoever landed `2bf1ef2b39`.
- **WhatsApp**: explicit `group_policy: "open"` plus the control assert (`adapter._group_policy`) correctly pins `gateway/platforms/whatsapp.py:269`, and the autouse `delenv` covers the `gateway/config.py:1115-1121` leak source ✓.
- **`_client_cache_key`**, **`prod_pilot_run_id`**, **`st_mode`**, **`_vision_preanalysis_max_concurrency`**, **`_set_session_env(..., event=None)`**, **Ruff encoding** — all verified against source and correct.
- The two expected PA constitution failures (state gate, `preserve-case-state`) are untouched, as instructed ✓.