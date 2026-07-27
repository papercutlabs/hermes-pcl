# External Review — claude

**Prompt:** Re-review WB 56c5c6d3 after fixes in commit 0915bddd1 (base 267d2020e, overall branch against origin/main). Prior review BLOCK is included. Verify all four blockers are closed: tool path containment a...

---

## Verdict: **CLEAR**

All four blockers are closed in `0915bddd1`, and the previously-passing FAIL criteria remain intact. Findings below are read-only static verification of the current tree; I did not run the 169 tests, ruff, the 4.8 MB workbook extraction, or the live `tgg_case_query` reads, so that evidence is accepted as reported.

### Blockers — re-verified closed

**1. Path containment + generic errors — closed.**
`pa_business_tools.py::_handle_tgg_spreadsheet_job_numbers` now resolves the candidate and requires `candidate.is_relative_to(root)` for at least one `pa.media_retention.source_roots` entry; empty/absent roots refuse (`not allowed_roots or not any(...)`). `resolve(strict=True)` follows symlinks, so an in-root symlink to an out-of-root target is refused. All of `OSError | RuntimeError | TypeError | ValueError` collapse to the single generic `INVALID_MEDIA_REF: spreadsheet is unavailable or outside configured roots` — no path, no existence oracle. Covered by `test_tgg_spreadsheet_tool_refuses_path_outside_configured_roots`, which asserts the path is absent from the message.

**2. Bounded CSV read + size ceiling — closed.**
`_sniff_tgg_csv` is now `with path.open("rb") as handle: sample = handle.read(128 * 1024)` — bounded before slice. `validate_tgg_spreadsheet` applies `_TGG_SPREADSHEET_MAX_FILE_BYTES` (64 MB) to **both** branches before any sniff, so the CSV path no longer lacks a cap. `test_tgg_spreadsheet_gate_refuses_oversized_csv_without_full_read` patches `Path.stat` and asserts refusal without reading.

**3. Filename injection removed — closed.**
`whatsapp.py::_build_message_event` now appends only `str(exc).split(":", 1)[0]` — the refusal code — never the basename. Our own `ValueError` codes are a fixed vocabulary; an `OSError` renders as `[Errno N] <reason>` with the quoted path after the first colon, so the split still drops it. No newline or `]` can reach the bracketed note. `test_executable_renamed_xlsx_is_not_exposed_to_agent` uses a filename containing `]`, a newline, and `Ignore prior instructions` and asserts none survive.

**4. Permanent refusal is terminal — closed.**
New `PermanentMediaRefusal(ConsumerError)`. `_event_spreadsheets` raises it for a missing provider MIME; `_retain_record_media_impl` converts `validate_tgg_spreadsheet`'s `ValueError` into it. `ensure_record_media_retained` catches it and calls `record_retention(bypassed=True, refusal=...)` → `retention_state='bypassed'`, which `retention_candidates` (`IN ('pending','held')`) never re-selects, and which counts as retention-satisfied for `pending_chat_batches`. `test_permanent_spreadsheet_refusal_is_durable_and_not_retried` asserts `('bypassed', attempts=1, failures=0)` and an empty candidate queue. `.xlsm`/`.xltm` still enter `_event_spreadsheets` but now terminate as `UNSUPPORTED_MEDIA_TYPE` → bypassed rather than looping as `held`. FIFO is no longer violated indefinitely.

### Other requested checks

| Requirement | Result |
|---|---|
| Mixed image + spreadsheet retains the image | ✅ early return now gated on `if not _event_media(item)`; `test_mixed_spreadsheet_and_image_still_retains_image` asserts `retained == 1` |
| Per-attachment MIME (no fabrication) | ✅ `stage_from_source` only sets `mediaMimes` when `len(mediaUrls) == 1`; multi-attachment messages carry no declared MIME → permanent refusal (fail-closed), never a false "provider-declared" claim |
| Streaming worksheet parse | ✅ `iterparse(..., events=("end",))` with `row.clear()` and stream close in `finally`; plus a 10 k unique-job cap → `SPREADSHEET_TOO_LARGE`. `ParseError` still fails closed |
| Image MIME+ext+magic gate unchanged | ✅ `_validated_image_type` and `_IMAGE_SIGNATURES` byte-identical |
| `PROVENANCE_DIVERGENCE` semantics unchanged | ✅ declared≠detected, changed retained bytes, ordinal MIME change, `_resolve_case_photo` all intact |
| XLSX/CSV allowlist, content-verified | ✅ extension allowlist → MIME-per-extension → byte sniff |
| XLSM/XLTM refused | ✅ `UNSUPPORTED_MEDIA_TYPE` |
| Macro OOXML renamed `.xlsx` refused | ✅ entry-name and content-type macro markers |
| Executable renamed `.xlsx` refused | ✅ `PK` magic + required parts |
| MIME/bytes mismatch refused | ✅ both directions |
| Zip-bomb / traversal guards | ✅ ≤20 k entries, ≤512 MB uncompressed, no `extractall`, `..` rejected in sheet paths |
| Reuses existing read-only `tgg_case_query` | ✅ tool returns job numbers + a `next` pointer; `TGG_CASE_QUERY_SCHEMA` and its single-SELECT server enforcement are untouched — no new cross-check logic |
| Gateway ingress fails closed | ✅ refused documents dropped from `cached_urls`, absolute path not leaked |
| No deployment in this change | ✅ diff is code + tests only; no new bridge operation, no `deploy/tgg/christopher/config.yaml` edit needed (registry-local tool). I cannot observe the host, so "not deployed" rests on your attestation |

### Should-fix (non-blocking, for a follow-up)

- **Tool scoping is still coarse.** `tgg_spreadsheet_job_numbers` lives in the `pa-business`/`custom` toolsets; `_scope_operations_to_job_brief` only scopes *bridge operations*, so any chat holding the toolset can call it. Impact is now bounded (containment-checked, read-only, output regex-filtered to job numbers), but a site chat can still enumerate job numbers from another chat's captured spreadsheet inside the shared source roots. Worth an explicit decision or a brief-level tool allowlist.
- **Late-race error leaks a path.** If the file is deleted between the containment check and `validate_tgg_spreadsheet`'s own `resolve(strict=True)`, the resulting `FileNotFoundError` reaches the blanket `except Exception: tool_error(exc)` with the path in the message. The path is in-root and model-supplied, so this is cosmetic rather than a disclosure primitive — but the generic-refusal treatment should cover it too.
- **`retain_claimed_media` bypasses the refusal handler.** It calls `retain_record_media` directly, so a `PermanentMediaRefusal` would escape as a bare `ConsumerError`. It appears unused on the live path (`_process_claimed_chat_batch` uses `ensure_record_media_retained`); either route it through `ensure_` or delete it.
- **`ElementTree.fromstring` on `xl/sharedStrings.xml`** is still a single in-memory parse bounded only by the 512 MB uncompressed budget. Lower the cap or stream it; `defusedxml` remains optional hardening (ParseError already fails closed).

Re-review complete — **CLEAR** to proceed.