"""CLI support for native gateway replay."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from gateway.replay import ReplayCorpus, ReplayPlan


def add_replay_parser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "replay",
        help="Replay bridge-message corpora through the gateway",
        description=(
            "Replay a typed bridge-message corpus through the real Hermes gateway "
            "adapter path without connecting live adapters or sending live outbound messages. "
            "ReplayCorpus applies deterministic ordering, dedup, bare-reaction skipping, "
            "quote/media preservation, missing-media reporting, and the per-turn future-read fence "
            "before messages feed the ReplayPlan."
        ),
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--plan",
        metavar="PATH",
        help="JSON replay plan containing platform, messages/corpus, and replay options.",
    )
    source.add_argument(
        "--corpus",
        metavar="PATH",
        help="JSON/JSONL bridge-message corpus. Defaults to --platform whatsapp.",
    )
    source.add_argument(
        "--bridge-message-log",
        metavar="PATH",
        help="SQLite DB containing bridge_message_log rows (first-class ReplayCorpus source).",
    )
    parser.add_argument(
        "--platform",
        default="whatsapp",
        help="Platform adapter to use with --corpus (default: whatsapp).",
    )
    parser.add_argument(
        "--delivery-mode",
        choices=("capture", "drop"),
        default="capture",
        help="Capture outbound sends in the replay result or silently drop them (default: capture).",
    )
    parser.add_argument(
        "--require-mention",
        dest="bypass_require_mention",
        action="store_false",
        default=True,
        help="Honor platform mention gates during replay (default bypasses them).",
    )
    parser.add_argument(
        "--enforce-auth",
        dest="bypass_auth",
        action="store_false",
        default=True,
        help="Honor live gateway authorization checks during replay (default bypasses them).",
    )
    parser.add_argument("--chat-id", help="bridge_message_log chat_jid to replay.")
    parser.add_argument("--tenant", help="PA tenant for replay context.")
    parser.add_argument("--agent-id", help="PA agent id for replay context.")
    parser.add_argument("--job-type", help="PA job type for replay context.")
    parser.add_argument(
        "--since", help="Inclusive bridge_message_log time lower bound."
    )
    parser.add_argument(
        "--until", help="Exclusive bridge_message_log time upper bound."
    )
    parser.add_argument(
        "--limit-messages",
        type=int,
        help="Limit bridge_message_log rows before policy skips.",
    )
    parser.add_argument(
        "--skip-messages",
        type=int,
        default=0,
        help="Skip the first N ordered bridge_message_log rows; reported in corpus_report.",
    )
    parser.add_argument(
        "--media-root",
        help="Flat directory for remapping missing bridge_message_log media basenames; unresolved media is reported.",
    )
    parser.set_defaults(func=cmd_replay)
    return parser


def _load_plan(args) -> ReplayPlan:
    if getattr(args, "plan", None):
        return ReplayPlan.from_path(Path(args.plan))
    if getattr(args, "corpus", None):
        return ReplayPlan.from_corpus_path(
            Path(args.corpus),
            platform=getattr(args, "platform", "whatsapp"),
            delivery_mode=getattr(args, "delivery_mode", "capture"),
            bypass_require_mention=getattr(args, "bypass_require_mention", True),
            bypass_auth=getattr(args, "bypass_auth", True),
        )
    if getattr(args, "bridge_message_log", None):
        if not getattr(args, "chat_id", None):
            raise SystemExit("--bridge-message-log requires --chat-id")
        for flag, attribute in (
            ("--since", "since"),
            ("--tenant", "tenant"),
            ("--agent-id", "agent_id"),
            ("--job-type", "job_type"),
        ):
            if not getattr(args, attribute, None):
                raise SystemExit(f"--bridge-message-log requires {flag}")
        corpus = ReplayCorpus.from_bridge_message_log(
            Path(args.bridge_message_log),
            chat_id=args.chat_id,
            since=args.since,
            until=getattr(args, "until", None),
            tenant=args.tenant,
            agent_id=args.agent_id,
            job_type=args.job_type,
            limit=getattr(args, "limit_messages", None),
            skip_messages=getattr(args, "skip_messages", 0),
            media_root=getattr(args, "media_root", None),
        )
        return ReplayPlan(
            platform=getattr(args, "platform", "whatsapp"),
            delivery_mode=getattr(args, "delivery_mode", "capture"),
            bypass_require_mention=getattr(args, "bypass_require_mention", True),
            bypass_auth=getattr(args, "bypass_auth", True),
            **corpus.to_plan_kwargs(),
        )
    raise ValueError("provide --plan, --corpus, or --bridge-message-log")


def cmd_replay(args) -> None:
    from gateway.run import GatewayRunner

    plan = _load_plan(args)
    result = asyncio.run(GatewayRunner().replay(plan))
    print(result.to_json())


# ── PA replay run orchestrator ─────────────────────────────────────────────


def _add_plan_source_args(
    parser: argparse.ArgumentParser, *, required: bool = True
) -> None:
    source = parser.add_mutually_exclusive_group(required=required)
    source.add_argument(
        "--plan",
        metavar="PATH",
        help="JSON replay plan containing platform, messages/corpus, and replay options.",
    )
    source.add_argument(
        "--corpus",
        metavar="PATH",
        help="JSON/JSONL bridge-message corpus. Defaults to --platform whatsapp.",
    )
    source.add_argument(
        "--bridge-message-log",
        metavar="PATH",
        help="SQLite DB containing bridge_message_log rows.",
    )
    parser.add_argument(
        "--platform",
        default="whatsapp",
        help="Platform adapter to use with --corpus (default: whatsapp).",
    )
    parser.add_argument(
        "--delivery-mode",
        choices=("capture", "drop"),
        default="capture",
        help="Capture outbound sends in the replay result or silently drop them (default: capture).",
    )
    parser.add_argument(
        "--require-mention",
        dest="bypass_require_mention",
        action="store_false",
        default=True,
        help="Honor platform mention gates during replay (default bypasses them).",
    )
    parser.add_argument(
        "--enforce-auth",
        dest="bypass_auth",
        action="store_false",
        default=True,
        help="Honor live gateway authorization checks during replay (default bypasses them).",
    )
    parser.add_argument("--chat-id", help="bridge_message_log chat_jid to replay.")
    parser.add_argument("--agent-id", help="PA agent id for replay context.")
    parser.add_argument("--job-type", help="PA job type for replay context.")
    parser.add_argument(
        "--since", help="Inclusive bridge_message_log time lower bound."
    )
    parser.add_argument(
        "--until", help="Exclusive bridge_message_log time upper bound."
    )
    parser.add_argument(
        "--limit-messages",
        type=int,
        help="Limit bridge_message_log rows before policy skips.",
    )
    parser.add_argument(
        "--skip-messages",
        type=int,
        default=0,
        help="Skip the first N ordered bridge_message_log rows.",
    )
    parser.add_argument(
        "--media-root",
        help="Flat directory for remapping missing bridge_message_log media basenames.",
    )


def _add_provider_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--provider-url",
        help="systems-pcl replay provider base URL (or PS_REPLAY_PROVIDER_URL).",
    )
    parser.add_argument(
        "--provider-admin-token",
        help="Provider admin bearer token (or PS_REPLAY_PROVIDER_ADMIN_TOKEN).",
    )
    parser.add_argument(
        "--tenant", required=True, help="Provider tenant query value."
    )
    parser.add_argument(
        "--provider-timeout",
        type=float,
        default=30.0,
        help="Provider HTTP timeout seconds (default: 30).",
    )


def _provider_client_from_args(args):
    from gateway.replay_orchestrator import (
        ReplayTargetProviderClient,
        provider_config_from_env,
    )

    config = provider_config_from_env(
        provider_url=getattr(args, "provider_url", None),
        admin_token=getattr(args, "provider_admin_token", None),
        tenant=getattr(args, "tenant", None),
        timeout_seconds=getattr(args, "provider_timeout", 30.0),
    )
    if not config.provider_url:
        raise SystemExit("replay-run requires --provider-url or PS_REPLAY_PROVIDER_URL")
    if not config.admin_token:
        raise SystemExit(
            "replay-run requires --provider-admin-token or PS_REPLAY_PROVIDER_ADMIN_TOKEN"
        )
    return ReplayTargetProviderClient(config)


def _gate_from_args(args):
    from gateway.replay_orchestrator import VerifyGateConfig

    return VerifyGateConfig(
        tool_error_budget=getattr(args, "tool_error_budget", 0) or 0,
        allowed_outbound_kinds=tuple(getattr(args, "allow_outbound_kind", None) or ()),
        expected_turn_count=getattr(args, "expected_turn_count", None),
        min_turn_count=getattr(args, "min_turn_count", None),
        require_provider_invariants=not getattr(args, "no_provider_invariants", False),
    )


def add_replay_run_parser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "replay-run",
        help="Orchestrate PA replay target lifecycle",
        description=(
            "Prepare a provider target, run native Hermes replay, enforce the "
            "mechanical PA verify gate, then gate provider promote/rollback."
        ),
    )
    commands = parser.add_subparsers(dest="replay_run_command", required=True)

    start = commands.add_parser("start", help="prepare target -> run replay -> verify")
    _add_provider_args(start)
    _add_plan_source_args(start)
    start.add_argument(
        "--source-data-dir",
        required=True,
        help="Provider source PS data dir to snapshot.",
    )
    start.add_argument(
        "--target-data-dir",
        required=True,
        help="Fresh isolated target PS data dir to create.",
    )
    start.add_argument(
        "--target-base-url",
        required=True,
        help="Base URL Hermes tools should use for the target.",
    )
    start.add_argument(
        "--out-dir",
        help="Directory for run manifests (default: ~/.hermes/replay-runs).",
    )
    start.add_argument(
        "--run-id",
        help="Optional deterministic run id for tests/recovery; omitted means mint one.",
    )
    start.add_argument(
        "--tool-error-budget",
        type=int,
        default=0,
        help="Allowed tool errors before verify fails (default: 0).",
    )
    start.add_argument(
        "--allow-outbound-kind",
        action="append",
        help="Outbound kind allow-list metadata retained in gate reports; non-capture delivery modes still fail.",
    )
    start.add_argument(
        "--expected-turn-count", type=int, help="Exact PA turn count expected."
    )
    start.add_argument(
        "--min-turn-count", type=int, help="Minimum PA turn count expected."
    )
    start.add_argument(
        "--no-provider-invariants",
        action="store_true",
        help="Skip provider verify call (test harness only).",
    )

    verify = commands.add_parser(
        "verify", help="rerun the mechanical verify gate for a manifest"
    )
    _add_provider_args(verify)
    verify.add_argument("--manifest", required=True, help="Path to run-manifest.json.")
    verify.add_argument(
        "--session-db",
        help="Hermes state.db path to derive PA turn/tool coverage from.",
    )
    verify.add_argument("--tool-error-budget", type=int, default=0)
    verify.add_argument(
        "--allow-outbound-kind",
        action="append",
        help="Outbound kind allow-list metadata retained in gate reports; non-capture delivery modes still fail.",
    )
    verify.add_argument("--expected-turn-count", type=int)
    verify.add_argument("--min-turn-count", type=int)
    verify.add_argument("--no-provider-invariants", action="store_true")

    promote = commands.add_parser(
        "promote", help="call provider promote after a verified fresh run"
    )
    _add_provider_args(promote)
    promote.add_argument("--manifest", required=True, help="Path to run-manifest.json.")
    promote.add_argument(
        "--prod-data-dir",
        required=True,
        help="Provider prod/non-prod data dir to swap into.",
    )
    promote.add_argument(
        "--confirm",
        required=True,
        help="Tenant-derived confirmation: ORCHESTRATOR_<TENANT>_TARGET.",
    )

    rollback = commands.add_parser(
        "rollback", help="call provider rollback for an orchestrated promotion"
    )
    _add_provider_args(rollback)
    rollback.add_argument(
        "--manifest", required=True, help="Path to run-manifest.json."
    )
    rollback.add_argument(
        "--promotion-manifest-path",
        help="Provider promotion manifest path; defaults from run manifest.",
    )

    dirty = commands.add_parser(
        "dirty", help="mark a target dirty so it cannot promote"
    )
    _add_provider_args(dirty)
    dirty.add_argument("--manifest", required=True, help="Path to run-manifest.json.")
    dirty.add_argument(
        "--reason", required=True, help="Dirty reason recorded locally and at provider."
    )

    status = commands.add_parser("status", help="print a run manifest")
    status.add_argument("--manifest", required=True, help="Path to run-manifest.json.")

    parser.set_defaults(func=cmd_replay_run)
    return parser


def cmd_replay_run(args) -> None:
    import json

    from gateway.replay_orchestrator import (
        PAReplayOrchestrator,
        ReplayRunManifest,
        ReplayVerifyError,
    )

    command = getattr(args, "replay_run_command", None)
    if command == "status":
        print(
            json.dumps(
                ReplayRunManifest.load(args.manifest).to_dict(),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return

    client = _provider_client_from_args(args)
    if command == "start":
        orch = PAReplayOrchestrator(
            provider_client=client,
            out_dir=getattr(args, "out_dir", None),
            run_id=getattr(args, "run_id", None),
        )
        orch.prepare_target(
            source_data_dir=args.source_data_dir,
            target_data_dir=args.target_data_dir,
            target_base_url=args.target_base_url,
        )
        plan = _load_plan(args)
        orch.run_agent_replay(plan)
        try:
            orch.verify(_gate_from_args(args))
        except ReplayVerifyError as exc:
            print(
                json.dumps(
                    orch.manifest.to_dict(),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            raise SystemExit(str(exc)) from exc
        print(
            json.dumps(
                orch.manifest.to_dict(), ensure_ascii=False, indent=2, sort_keys=True
            )
        )
        return

    orch = PAReplayOrchestrator.from_manifest(args.manifest, provider_client=client)
    if command == "verify":
        try:
            report = orch.verify(
                _gate_from_args(args), session_db_path=getattr(args, "session_db", None)
            )
        except ReplayVerifyError as exc:
            print(
                json.dumps(
                    orch.manifest.to_dict(),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            raise SystemExit(str(exc)) from exc
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return
    if command == "promote":
        from gateway.replay_orchestrator import tenant_confirmation_token

        expected_confirm = tenant_confirmation_token("ORCHESTRATOR", args.tenant)
        if args.confirm != expected_confirm:
            raise SystemExit(
                f"--confirm must be {expected_confirm!r} for tenant {args.tenant!r}"
            )
        print(
            json.dumps(
                orch.promote(prod_data_dir=args.prod_data_dir),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return
    if command == "rollback":
        print(
            json.dumps(
                orch.rollback(
                    promotion_manifest_path=getattr(
                        args, "promotion_manifest_path", None
                    )
                ),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return
    if command == "dirty":
        print(
            json.dumps(
                orch.mark_dirty(reason=args.reason),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return
    raise SystemExit(f"unknown replay-run command: {command}")
