# Christopher capture-freshness discriminator

WB: `fcdcfced-c6c4-41b5-a673-44aec767ea51`

## Outcome

Replace row-age-only failure with an inbound-side discriminator:

- quiet means no inbound batch is currently stuck, the socket is connected, and
  capture and the Systems inbox remain in lockstep;
- stopped means an inbound batch arrived but did not finish the capture path
  inside the bounded processing window, the socket disconnected, or capture
  advanced while the inbox failed to follow while a consumer is expected.

The live client runtime remains untouched. This slice lands source and
consumer-safe tests only; deployment stays behind Teren's active freeze.

## Exact emitter trace

The red is emitted by the registered `dev.check_definitions` row
`01c63cc3-9206-495e-84c7-812d06f5302a`, not by
`deploy/tgg/christopher/checks/runtime-invariants.json`.

Its live runner is:

```text
/Users/pcloffice/pcl-dev/hermes-pcl/deploy/tgg/christopher/scripts/verify_capture_freshness.sh
```

The check runner treats its non-zero exit as `status=fail`; the open alert
`fae5eefb` routes to `edna-central`. The canonical runner and this worktree's
copy were byte-identical at pickup (`sha256
414d768097de954bceb4f9ca674ea8a4bddb018e293280986e98620a80338b56`).
Running the exact canonical path reproduced the red with
`capture_stale_hours=18`, `socket_status=connected`, and exit 1.

## Measured quiet baseline

Read-only measurement over the trailing 14 days of the live capture file:

- population: 4,565 capture events and every interarrival gap at least 4h;
- 12 gaps: `34.59, 11.77, 9.32, 12.71, 14.28, 14.60, 34.20, 13.67,
  13.83, 10.83, 9.89, 10.34` hours;
- weekday/night gaps: 9.32–14.60h;
- weekend gaps: 34.20–34.59h.

The baseline is emitted as evidence/classification context. It does not replace
the inbound-side discriminator and does not become a larger freshness
threshold. `freshness_sla_seconds=7200` and `CAPTURE_MAX_STALE_HOURS=12` remain
unchanged.

## Shape audit

### Gate 0 — premise and surface

- Live proof: the registered check runner, its latest fail rows, the exact
  canonical script, the active bridge `/health`, the bridge journal, the live
  capture timestamps, and the Systems `message_ledger`.
- Implicated surface: `verify_capture_freshness.sh` treats row age as a stall,
  while the bridge exposes no inbound-work progress signal.

### Gate 1 — mechanism and class

- Symptom: a blocker page during routine client quiet.
- Immediate cause: `STALE_H >= MAX_STALE_HOURS` unconditionally sets `FAIL`.
- Root mechanism: the checker samples output age but has no signal at the
  receive boundary before capture processing begins.
- Recurrence stance: category. Any event-driven capture product has the same
  quiet-versus-stopped ambiguity when output age is the only input.
- Audit: bounded to this deployed capture path; no fleet-general abstraction is
  introduced in this slice.

### Gate 2 — layer

Deterministic source instrumentation belongs at the Baileys
`messages.upsert` boundary. Deterministic classification belongs in the
existing check runner. Human interpretation is removed from the happy path.

### Gate 3 — primitive

Extend the existing bridge `/health` and existing check runner. No parallel
daemon, check, or alert path.

### Gate 4 — proof and persistence

- Event proof: bridge tracker test demonstrates received-without-completed
  stays pending; runner integration test proves that evidence reds with the
  named `inbound-arrived-not-written` condition.
- State proof: the existing scheduled check remains the standing monitor after
  the source is eventually deployed and its registered runner is updated.
- Attachment: client agent `tgg/christopher`; activation remains scheduled;
  red owner remains Edna.
- Rollback: revert the two source commits. No runtime state or client data is
  changed in this slice; data loss is none.

### Gate 5 — target conflicts

- Read: live registered definition, canonical runner, Hermes manifest,
  deployed bridge lineage, bridge source-integrity manifest, TGG runtime door,
  and the 17 July disposition/spec carried on the WB.
- Finding: the live definition points to the canonical Hermes checkout, while
  the inbound signal must originate in the separate `tgg-agent` bridge lineage.
- Plan action: land paired commits and hold them undeployed. A later
  Teren-authorized deployment must deploy bridge instrumentation before
  registering/updating the check runner.

### Reviews

- Ruth: skipped — the shape preserves the no-principal happy path and the
  active client deployment freeze.
- Cowboy: skipped — extending the two existing primitives is the smallest
  complete shape.
- Codex: run through the source/tests in this worker.
- Schema lens: n/a.

## Build phases

1. **Inbound receipt boundary (`tgg-agent`)**
   - Add a small in-memory progress tracker.
   - Mark each live `messages.upsert` batch received synchronously before the
     capture-processing await, then classify the terminal outcome in `finally`
     as completed or failed. A later successful completion clears the active
     failure indicator without erasing cumulative evidence.
   - Expose only counters/timestamps/ages in `/health`; no message bodies,
     sender IDs, or client content.
   - Add tracker and bridge structural tests.
   - Refresh source-integrity metadata if the tracked source bytes require it.

2. **Check classification (`hermes-pcl`)**
   - Parse the new health fields plus capture and inbox timestamps.
   - Keep row age informational.
   - Fail with explicit conditions:
     `socket-disconnected`, `inbound-arrived-not-written`,
     `capture-inbox-diverged`, or existing queue saturation.
   - Pass a connected/no-pending/lockstep stale window as
     `quiet-within-measured-pattern` or `quiet-outside-measured-pattern`.
   - Preserve 12h and 2h values unchanged.
   - Add fixture-safe integration tests covering both required directions.

3. **Verification**
   - Run all affected Node and Python/shell tests.
   - Exercise the modified check against a read-only live snapshot for the
     quiet direction.
   - Induce pending inbound in a local fixture and prove the same runner reds.
   - Confirm no SSH mutation, service restart, config write, check
     registration, waiver, or deployment occurred.

## Rollback mechanics

Reverse order:

1. Revert the Hermes check commit.
2. Revert the TGG bridge instrumentation commit.

Proof: both changes are source-only additions on isolated branches; `git diff`
must show no client-host or database mutation. Live abort action is to stop
before any deploy/register command. Data-loss implication: none.

## Scaffolding co-deliverable

- New state: bridge inbound-progress fields.
- Scaffolding artifact: bridge tracker tests and check-runner integration
  fixtures.
- Writer: this WB's worker.
- Landing paths: `tgg-agent/runtime/tgg-capture-whatsapp-bridge/` and
  `hermes-pcl/tests/deploy/`.
- Co-deliverable: tests land in the same commits as the behavior; no follow-up
  row is required.

## Standing guidance

`knowledge/tgg-systems-runtime.md` already contains the cold-reader
disposition. It will be updated only after deployment changes live behavior;
this source-only slice must not rewrite live-state guidance as though deployed.
