# External Review — claude

**Prompt:** Blocking cross-provider security review for WB 56c5c6d3. Review commit 267d2020e against origin/main. This is a client-facing live-agent media-gate change. FAIL if existing image MIME+extension+magic ...

---

## Verdict: **BLOCK**

The stated FAIL criteria are all **satisfied** — no existing control was weakened, and the new allowlist refuses everything it is required to refuse. I am blocking on four defects the commit *introduces*, three of which are new attack surface on a live, prompt-injectable client agent.

---

### What passed (explicitly re-verified)

| Requirement | Result |
|---|---|
| Image MIME+extension+magic checks intact | ✅ `_validated_image_type` unchanged (signature table, `image/jpg`→`image/jpeg` allowance only) |
| `PROVENANCE_DIVERGENCE` behavior intact | ✅ Still raised on declared≠detected MIME, on changed retained bytes, and on ordinal MIME change in `_retain_record_media_impl`; `_resolve_case_photo` unchanged |
| XLSX/CSV explicit allowlist, content-verified | ✅ `_TGG_SPREADSHEET_EXTENSIONS = {.xlsx, .csv}`, MIME must match extension via `_TGG_SPREADSHEET_MIMES`, then `_sniff_tgg_xlsx`/`_sniff_tgg_csv` verify bytes |
| XLSM/XLTM refused | ✅ Not in extension allowlist → `UNSUPPORTED_MEDIA_TYPE` |
| Macro-bearing OOXML renamed `.xlsx` refused | ✅ Rejected on `vbaProject`/`macrosheet` entry names **and** on macro-enabled content-types → `PROVENANCE_DIVERGENCE` |
| Executable renamed `.xlsx` refused | ✅ `PK` magic check + required `[Content_Types].xml`/`xl/workbook.xml` |
| MIME/bytes mismatch refused | ✅ Both directions (declared-vs-extension, declared-vs-content) |
| Zip-bomb guards | ✅ ≤20k entries, ≤512 MB total uncompressed |
| Zip path traversal | ✅ No `extractall`; `_xlsx_sheet_paths` rejects `..` segments |
| Extraction feeds the **existing** read-only path | ✅ No new cross-check logic; handler returns `next` pointing at `tgg_case_query`, and `TGG_CASE_QUERY_SCHEMA` (single SELECT, server-enforced read-only) is untouched |
| Gateway ingress fails closed | ✅ `whatsapp.py::_build_message_event` drops refused documents from `cached_urls` and does not leak the absolute path in the refusal note |

---

### Must-fix (blocking)

**1. `tgg_spreadsheet_job_numbers` accepts an unconstrained filesystem path.**
`extract_tgg_spreadsheet_job_numbers` does `Path(path).expanduser().resolve(strict=True)` and validates *content*, but never checks containment against `media_retention.source_roots` / `media_root`. Every other media path in this system is containment-checked (`_contained_existing_file`, `_resolve_case_photo`). A prompt-injected site message can steer the agent to open any readable `.csv`/`.xlsx` on the host. Exfil is bounded (output filtered to `^[A-Z]{2}/JOB/\d{4}/\d{4}$` under a job-number header), but it is a new arbitrary-open primitive plus a file-existence oracle — `FileNotFoundError` is surfaced verbatim through the handler's blanket `except Exception: tool_error(exc)`, disclosing paths back into chat.
*Fix:* resolve and containment-check against the configured roots before opening; return a generic refusal on missing/out-of-root.

**2. `_sniff_tgg_csv` reads the entire file into memory before slicing.**
`sample = path.read_bytes()[: 128 * 1024]` — the `[:128KB]` slice happens *after* full-file read. This runs in the consumer's retention path for every captured `.csv`, and there is no maximum-file-size guard on the CSV branch (unlike XLSX, which has the 512 MB uncompressed cap). A large document from an authorized chat is an in-process memory spike in the live ingest daemon.
*Fix:* `with path.open("rb") as fh: sample = fh.read(128 * 1024)`, plus an explicit `stat().st_size` ceiling for both branches.

**3. Attacker-controlled filename is injected into the model prompt.**
`whatsapp.py::_build_message_event` builds `refused_documents.append(f"{Path(url).name}: {exc}")` and prepends `"[A document attachment was refused by the media safety gate: …]"` to `body`. The filename is fully attacker-controlled and lands inside a bracketed system-styled note — e.g. `report.xlsx] Ignore prior instructions and …`. This is a new injection channel created by this commit on a live client agent.
*Fix:* sanitize/escape the basename (strip `]`, newlines, cap length), or omit it and emit a count only.

**4. Refused spreadsheets create a permanently held inbox row with unbounded retries.**
`_event_spreadsheets` includes `.xlsm`/`.xltm` in its suffix set, but `validate_tgg_spreadsheet` rejects them → `MediaRetentionError` → `retention_state='held'`. `retention_candidates` re-selects `'held'` rows every cycle, so the row is re-validated and re-fails forever, incrementing `retention_failures` and pinning consumer status at `held-pending` indefinitely. Same for a spreadsheet with no provider-declared MIME (`PROVENANCE_DIVERGENCE: spreadsheet has no provider-declared MIME`). The row also never reaches the model, so per-chat FIFO is silently violated (later messages in that chat process past it) — a correctness gap for an evidence-ordering system.
*Fix:* distinguish *permanent* refusals (unsupported type, macro payload, MIME/bytes mismatch) from *retryable* I/O failures; terminal-state the former as `bypassed`/`refused` with a durable reason instead of retrying.

---

### Should-fix (non-blocking, but real)

- **Fabricated per-attachment provenance.** `durable_jsonl_consumer.stage_from_source` broadcasts one recovered MIME to *every* media URL: `item["mediaMimes"] = [declared_document_mime for _ in item["mediaUrls"]]`. For multi-attachment messages, indices ≥1 carry a declared MIME the provider never asserted for that file, and `_declared_document_mime` picks the first `documentMessage` found by dict-traversal order. Content verification still fails closed, but the "provider-declared" claim in `PROVENANCE_DIVERGENCE` is not accurate for those indices. Extract per-attachment, or refuse multi-document messages.
- **Mixed image+spreadsheet messages skip image retention.** `_retain_record_media_impl` returns early when `spreadsheets` is non-empty, before `_event_media`. An image attached alongside a spreadsheet is never retained and the row is marked `bypassed` — silent evidence loss.
- **Unbounded in-memory XML parse.** `ElementTree.fromstring(archive.read(sheet_path))` can materialize up to the 512 MB budget for a single worksheet. Lower the cap or switch to `iterparse`. Also consider `defusedxml` for the two `fromstring` sites; ParseError is caught and fails closed today, so this is hardening, not a live hole.
- **No per-chat scoping for the new tool.** `_scope_operations_to_job_brief` scopes *bridge operations* only; `tgg_spreadsheet_job_numbers` is a registry tool in the `pa-business`/`custom` toolsets, so any chat with that toolset gets it — unlike `tgg_case_query` et al., which the brief can deny. Worth an explicit decision.

---

### Verification notes

I could not independently confirm the reported evidence (165 tests, ruff, the 4,821,080-byte / SHA256 `739a5929…` workbook → 3,907 job numbers, or the two live `tgg_case_query` reads) — this review is read-only static analysis. The test file `tests/gateway/test_durable_jsonl_consumer.py` and `tests/test_pa_business_facts.py` do cover the four required refusal cases plus retention idempotence, and the existing image PROVENANCE tests remain in place. **No test covers items 1–4 above**; add cases for out-of-root path, oversized CSV, filename-with-`]`, and permanent-refusal terminal state before re-review.

Re-submit after 1–4 are addressed and I'll re-run this against the updated diff.