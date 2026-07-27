# TGG output-quality evaluator loop

WB: `47c0aa00-51bb-42c4-9f80-f5972c1b79e9`

## Outcome

Every newly processed TGG case batch is inspected as a TGG manager sees it:
the authenticated rendered case page is judged against the raw WhatsApp
messages and retained media that caused the case change. Defects reach Edna's
queue with screenshot and message-id evidence. Human catches permanently
strengthen the registry.

## Process sheet

### Feed

- Read-only snapshot of `capture-inbox.db.ingress_events`, ordered by `seq`.
- Read-only snapshot of the TGG tenant database for cases and WhatsApp
  observations whose `source_refs` intersect the newly completed messages.
- Read-only copies of retained media referenced by the source messages or
  case observations.
- Authenticated public portal pages at
  `https://systems.papercut-labs.com/tgg`.

### Trigger and idempotency

One Studio runner owns a durable local cursor and run ledger.

- Evaluate when at least 25 completed inbox rows exist after the committed
  cursor.
- Evaluate after four hours when the delta is non-zero.
- Evaluate on `pa_agent_deployed:tgg/christopher`.
- Evaluate at the daily backstop when the delta is non-zero.
- A run commits its cursor only after bundles, screenshots, judge output, and
  defect filing are durably recorded.
- A stable defect key prevents duplicate WB rows across retries.

### Steps

1. Pull source rows read-only over SSH into a Studio-owned run directory.
2. Resolve touched cases by intersecting message ids with observation
   `source_refs`; keep unmapped completed messages visible in coverage.
3. For each case, save a source bundle containing the raw ingress messages,
   case record, matching observations, and retained media paths.
4. Log into the public portal through the `tgg-pa-admin` agent-browser auth
   profile, open the case through the real Cases UI, and capture a full-page
   screenshot plus accessibility snapshot.
5. Give screenshot + source bundle + versioned check registry to a
   vision-capable judge. The judge must check both directions:
   every page claim traces to source; every source fact is represented; the
   page reads sensibly to a TGG manager. Each named check returns
   `pass|fail|unsure`; uncertainty can never become a pass.
6. Persist the judgment. For each `fail` or `unsure`, create or reuse one Edna
   WB defect row with screenshot path, message ids, case/job number, and check
   class.
7. Record batch coverage and defect-origin metrics, then atomically advance
   the cursor.

### Verification

- Deterministic tests cover cursor gating, retry idempotency, check-registry
  validation, `unsure` preservation, and defect deduplication.
- A golden fixture includes one pass, one fail, and one unsure result. Any
  registry hash change re-runs it before live judgment.
- Tonight's real completed batch is the canary. The build stays open until a
  real authenticated page screenshot is judged and defect WBs are filed.
- Checker != maker: the runner requires a judge session id and refuses it when
  it equals the supplied maker session id. Fix authors cannot judge their own
  fix from the same session.

### Health

`loop.pa.tgg.output_eval` is a fail-closed Studio shell check, owned by Edna,
with `max_silence=PT26H`. Its JSON result carries:

- `batches_evaluated` and `batches_occurred`;
- `coverage_ratio`;
- defects by recent batch (trend);
- `human_caught` and `loop_caught`;
- last successful run and cursor position.

### Kaizen

Human catches are appended to the check registry immediately as named checks.
Registry changes change its content hash and force the golden regression
before the next real case can be judged. The registry remains principles plus
named recurring defect classes, not one-off case answers.

## Machinery binding

- Cursor, bundle assembly, thresholds, metrics, and dedupe: deterministic
  Python.
- Rendered interaction and screenshots: `agent-browser`, isolated named
  session, saved auth profile.
- Page/source interpretation: vision-capable model worker.
- Defect intake: existing whiteboard CLI; no parallel queue or `dev.*` schema.
- Health: existing `pcl check` substrate.
- Deploy event + interval: the check definition's activation events and
  interval; no new daemon.

## Shape audit

### Gate 0 — live premise

Live portal inspection shows a `New` badge with no matching status filter and
real case pages whose displayed sender/status/media can diverge from the
WhatsApp observation. The implicated gap is absence of a consumer-layer
output evaluator, not absence of row/runtime checks.

### Gate 1 — mechanism and class

- Symptom: Teren is discovering client-facing defects manually.
- Immediate cause: deployment verification stops at process/data mechanics.
- Root mechanism: no event-fired rendered-output comparison against source.
- Recurrence: category; every client-facing generated artifact has the class.
- Audit: this WB installs the TGG instance; fleet generalization is outside
  this client-specific build.

### Gate 2 — layer

Deterministic collection and accounting stay in code. Judgment stays in a
vision-capable model. Browser state stays in agent-browser. Human involvement
is exception/ruling only.

### Gate 3 — primitives

Extend the capture inbox cursor model, `agent-browser`, Whiteboard, and
`pcl check`. Build only the client-specific runner and registry because no
existing primitive composes those four surfaces into a TGG case judgment.

### Gate 4 — proof and persistence

- Result: fresh TGG rendered cases are judged against their source.
- Event proof: tonight's real run directory, screenshots, judgments, and WB
  defect ids.
- State proof: `loop.pa.tgg.output_eval`, max silence PT26H.
- Attachment: TGG Christopher deployment.
- Activation: four-hour interval, deploy events, daily runner backstop.
- Red owner: Edna.
- Mutation/consumer: local evaluator state + Edna WB defects; Edna consumes.
- Rollback: retire the check and revert this commit; local evaluator artifacts
  are additive and deletable. No client data is mutated.

### Gate 5 — target conflicts

Read the current Christopher deploy spec, capture consumer schema, runtime
invariant check, and public portal route/auth implementation. The evaluator
must run on Studio and may only read the VPS. No existing deploy manifest or
runtime check is replaced.

## Design passport

- passport: `SS-PASSPORT-2026-06-22-F3A9D1`
- classification: `design-bearing`
- obligation: true
- checks: `loop.pa.tgg.output_eval`
- Ruth: pass — removes a recurring principal QA dependency.
- Cowboy: pass — additive, read-only client access, bounded rollback.
- Codex: pass — code/model/browser layers separated.
- rollback blast: Studio evaluator files and check registration only; data loss
  `none`.

## Scaffolding co-deliverable

- New state: Studio evaluator cursor/run ledger and versioned TGG check
  registry.
- Scaffolding artifact: this design plus the runner's `--help` and check
  registry schema validation.
- Writer: `deploy/tgg/christopher/scripts/output_quality_eval.py`.
- Landing paths: `deploy/tgg/christopher/quality-checks.yaml`,
  `deploy/tgg/christopher/checks/output-quality-eval.json`,
  `deploy/tgg/christopher/quality_eval/`, and tests under `tests/deploy/`.
- DoD: registry, runner, fixture, real run, and registered health check ship in
  this WB, not a follow-up.

## Rollback mechanics

1. `pcl check retire --logical-key loop.pa.tgg.output_eval --reason ...`
2. Revert the evaluator commit from `origin/main`.
3. Remove the Studio-local evaluator state directory only if its audit history
   is no longer required.

Safe proof: all code is additive; no runtime manifest includes the evaluator,
and the runner's SSH path opens SQLite databases read-only. Abort is terminating
the Studio runner process. Data-loss implication: none for TGG; optional loss
of evaluator-only audit artifacts if step 3 is chosen.
