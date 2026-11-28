# hermes-mtu — Ops Note (Telegram pilot)

**Owner:** edna · **Sponsor:** amelia · **WB:** ad8e3abd · **Built:** 2026-07-13

## Deploy specifics (RESOLVED — live pilot 2026-07-13)
| Item | Value |
|---|---|
| **Host** | Studio-local pilot (`HERMES_HOME=~/.hermes-mtu`), runs on the `~/pcl-dev/hermes-pcl` main checkout venv (`.venv/bin/python`). Reversible; Carbon target is client-VPS (Amelia's call — flagged). |
| **Runtime** | hermes `gateway run` (foreground/nohup, interactive two-way). Isolated state at `~/.hermes-mtu/`. Gateway confirmed `running` + telegram `connected`. |
| **Channel** | Telegram (native hermes platform), bot **@pcl_mtu_bor_bot** (id `8869502462`). Thin/swappable — constitution+knowledge are channel-agnostic; WA can be added later by enabling the WA platform, no rebuild. |
| **Model** | `gpt-5.4-mini` via **`provider: custom`** (OpenAI-direct, `OPENAI_API_KEY`). **Provider fix:** the inherited config used `openai-direct-primary`, which is a NAMED runtime-provider that only resolves in a HERMES_HOME carrying a `runtime_providers` registry (christopher's does; a fresh home does NOT → "Unknown provider"). `provider: custom` + base_url + api_key_source is the portable equivalent (matches `~/.hermes-tss-support`) and is what runs here. **FLAGGED model swap** from the constitution's design intent `gemini-3.1-flash-lite`: christopher runs gpt-5.4-mini after moving off gemini for structured-extraction/drafting accuracy. Correction to the prior note: a `GEMINI_API_KEY` **IS present** on this host — so gemini is available; gpt-5.4-mini is a deliberate accuracy choice, not a key-absence fallback. Revert by setting `model.provider: gemini` + `default: gemini-3.1-flash-lite`. |
| **Bot token** | Canonical: `~/.marshal/secrets.env` → `MTU_BOR_BOT_TOKEN` (chmod 600). Runtime copy: `~/.hermes-mtu/.env` → `TELEGRAM_BOT_TOKEN` (chmod 600). NOT committed. |
| **Advisor allowlist** | `~/.hermes-mtu/.env` → `TELEGRAM_ALLOWED_USERS`. Pilot self-test uses the controlled tester account `276672685` (Teren's Telegram, @t4177x). Melody's / Amelia's Telegram user IDs must be added before they can message it (DMs bypass `allowed_chats`; the user allowlist is the gate). |
| **engagement_id** | Ties to the `amelia-finexis` Finexis engagement (deal `179de144-79ac-4caf-a0d5-e2dccc9e9b82`). Literal `client.engagement_id` in the constitution still `TBD` — set at DEBUT (out of build scope). |
| **Escalation contact** | Melody Tan (constitution `escalation.escalation_to`). |

## Controlled-tester pass (DoD 5)
- **2026-07-13** — READY-state controlled-tester pass run through the **production stack** (Hermes gateway + native Telegram ingress), synthetic cases only, tester `276672685`. Transcript: `test-transcripts/2026-07-13-synthetic-e2e.md`. Case A (ROP Term→Term) drafted a full BOR with ROP disadvantages disclosure + `[[MISSING: ECIM]]` placeholder; Case B (non-ROP new purchase) correctly did NOT fire ROP checks. Agent behaviour is constitution/knowledge-driven, not generic-LLM.

## What remains (before a real advisor can use it)
1. ~~**[BLOCKER] A free Telegram bot.**~~ **RESOLVED 2026-07-13.** Teren authorised freeing one slot from the 20-bot cap. Deleted exactly one retired throwaway test bot (interlock-verified: BotFather's confirmation prompt was asserted to name that test bot and NOT `pcl_moltbot`=Kleya's before confirming). Minted **@pcl_mtu_bor_bot** (id 8869502462). Deployed + live + synthetic-E2E tested.
2. **[Amelia gate]** DEBUT go (first real advisor) — WB 16f2b3ae. **Still open — nothing reaches a real advisor before this.**
3. **[Melody gate]** Confirm/edit the BOR table (`knowledge/bor-required-checks.yaml`) + approved disclosure wording. **Still open.**
4. Set literal `engagement_id`, add Melody's/advisors' Telegram IDs to the allowlist (currently tester-only `276672685`).
5. **Persistence:** currently a foreground/nohup pilot run (dies on host reboot / process kill). Before real-advisor use, add a launchd unit `com.pcl.hermes-mtu` (or move to the Carbon client-VPS target — Amelia's call).
6. Register hermes-mtu in the PA runtime registry so `pcl service locate --system mtu` resolves — do once it's persistently running.
7. **WhatsApp divergence:** the P0 design assumed WhatsApp (Finexis advisors use WA); this pilot ships on **Telegram** per Amelia's stated intent + faster path to "working". The constitution + knowledge are channel-agnostic — WA can be added later by enabling the WA platform, no rebuild. Flag for Amelia: confirm whether the real advisor DEBUT is Telegram or WA.

## Rollback
| Change | Revert |
|---|---|
| `~/.hermes-mtu/` (config + state + .env) | `rm -rf ~/.hermes-mtu` |
| Running gateway | `kill <pid>` / `pkill -f "hermes gateway run"` (with HERMES_HOME=~/.hermes-mtu) |
| Deploy artifacts (this dir) | `git revert <sha>` on the worker branch |
| (If a bot is reused) display-name rename | BotFather `/setname` back to original |
No data-loss risk: synthetic-only pilot; no client data ingested.

## Verification status (2026-07-13 — all deploy DoD verified)
- Stack: gateway `running`, telegram `connected`, loads MTU constitution + SOUL KB, model `gpt-5.4-mini` via `provider: custom` — **validated live**.
- Bot: `@pcl_mtu_bor_bot` getMe OK (id 8869502462), clean webhook, real message → real agent response (not stub) — **DoD #1 verified**.
- E2E BOR flow (ROP synthetic, full draft): classify path → run checks → ask missing → draft copy-pasteable BOR with disclosures + `[[MISSING]]` placeholder — **DoD #2 verified**.
- Config-driving-behaviour (ROP fires ROP checks/disclosures; non-ROP does not): **DoD #3 verified**.
- Controlled-tester pass through production stack, synthetic only, transcript recorded: **DoD #5 verified**.
- Client-data safety: `client-raw/` blanket-gitignored (git check-ignore confirmed), zero PII/secrets committed: **DoD #6 verified**.

## Update 2026-07-15 — persistence DONE + allowlist widened (edna)

**Outage + fix (2026-07-14 ~23:00):** gateway ran as an unsupervised worker-session child; the ~22:44 fleet restart dropped it → silent outage (8 undelivered updates, no alert). **Root cause = missing supervision (this OPS-NOTE's step 5).**

**Step 5 (persistence) — RESOLVED.** hermes-mtu now runs as a `marshal daemon` (launchd `com.teren.hermes-mtu`, `keep_alive: true`). Source: `~/pcl/marshal/daemons/{agents/hermes-mtu.yaml, scripts/hermes-mtu.sh}` (marshal commit 9094ace7). Survives reboot / kill / fleet restart. Restart via `marshal daemon restart --name hermes-mtu`; verify via `marshal daemon verify --name hermes-mtu`. NOTE: the gateway exits 1 on shutdown by design (expects a supervisor to revive) — `exit=1` in verify is normal, `running=true` is what matters. Systemic gap (no liveness pager + fleet-restart-drops-runtimes) filed as infra bug 5388ad5c (rasim).

**Allowlist widened.** `TELEGRAM_ALLOWED_USERS` now = teren(tester) + amelia + Melody(388010100, confirmed by amelia 2026-07-15, id sourced from her rejected inbound at 18:47). **Durability caveat:** the daemon execs hermes directly and never regenerates `.env`, so allowlist edits persist across daemon restarts. BUT `bootstrap_local.sh` takes the allowlist as its `$2` CLI arg — a future manual re-bootstrap with the old arg WOULD clobber the added IDs. Fix-forward (deferred, small): make bootstrap default `$2` to the existing `.env` allowlist when omitted. Until then: pass ALL current IDs to any re-bootstrap.

**Still open:** step 6 (register in PA runtime registry so `pcl service locate --system mtu` resolves) — do next now that it's persistently running.
