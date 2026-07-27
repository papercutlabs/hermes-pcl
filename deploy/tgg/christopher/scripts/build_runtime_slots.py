#!/usr/bin/env python3
"""Build deployable Christopher runtime slots from the recovered June baseline.

The June baseline files stay byte-exact (provenance-pinned). Every slot is
baseline + the authored patch layer:

  - processing gate forced closed (pa.enabled false, whatsapp gateway off)
  - memory hard-off (memory_enabled / user_profile_enabled false) — WB b63dd4f0
  - ops-judgment operations (clarification / attention / WC attach) —
    patches/ops-judgment-operations.snippet.yaml
  - ops-ingest judgment rules + structured work_items extraction —
    patches/ops-ingest-judgment.snippet.yaml
  - management-chat behavior (register, grounded answers, proactive attention
    push, write authority + three-flow, provenance, hold-framing, refusals) —
    patches/mgmt-chat-behavior.snippet.yaml
  - management-chat business-operation scope (full case-shaped registry;
    config/routing/purge asks refused by registry absence) —
    patches/mgmt-business-operations.snippet.yaml
  - per-slot engine: model + optional agent.reasoning_effort

Slots are keyed by slot id, not bare model name: gpt-5.6-luna-low runs
gpt-5.6-luna at reasoning_effort low.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import yaml


DEPLOY_ROOT = Path(__file__).resolve().parents[1]
BASELINE_ROOT = DEPLOY_ROOT / "baselines" / "june-2026"
PATCHES_ROOT = DEPLOY_ROOT / "patches"
SLOTS_ROOT = DEPLOY_ROOT / "runtime-slots"
BASELINE_MODEL = "gpt-5.4-mini"
DEFAULT_SLOT = "gpt-5.4-mini"
# slot id -> engine settings. reasoning_effort None = provider default (omit key).
SLOTS: dict[str, dict] = {
    "gpt-5.4-mini": {"model": "gpt-5.4-mini", "reasoning_effort": None},
    "gpt-5.6-luna": {"model": "gpt-5.6-luna", "reasoning_effort": None},
    "gpt-5.6-luna-low": {"model": "gpt-5.6-luna", "reasoning_effort": "low"},
}

MEMORY_OFF_BLOCK = "memory:\n  memory_enabled: false\n  user_profile_enabled: false\n"
PYTHON_SANDBOX_BLOCK = (
    "python_sandbox:\n"
    "  enabled: true\n"
    "  datasets:\n"
    "    cases:\n"
    "      type: sqlite\n"
    "      path: /home/pclaw/.systems-pcl/data/tenants/tgg.db\n"
    '      description: "Operational case/work-costing database (read-only snapshot; sqlite)"\n'
    "    media:\n"
    "      type: path\n"
    "      path: /home/pclaw/.systems-pcl/data/media/tgg/hermes\n"
    '      description: "Retained WhatsApp media/attachments (read-only; spreadsheets land here)"\n'
    "  limits:\n"
    "    wall_seconds: 120\n"
    "    max_wall_seconds: 300\n"
    "    cpu_seconds: 60\n"
    "    memory_mb: 1024\n"
    "    file_size_mb: 64\n"
    "    scratch_mb: 64\n"
    "    max_processes: 64\n"
    "    max_open_files: 256\n"
    "    max_snapshot_mb: 512\n"
    "  artifact_ttl_days: 7\n"
    "  max_runs_kept: 40\n"
)
MEDIA_RETENTION_BLOCK = (
    "  media_retention:\n"
    "    enabled: true\n"
    "    media_root: /home/pclaw/.systems-pcl/data/media/tgg/hermes\n"
    "    media_ref_prefix: /media/tgg/hermes\n"
    "    source_roots:\n"
    "    - /var/lib/tgg-capture/whatsapp/media\n"
    "    operation: tgg_media_retention\n"
    "    min_free_percent: 20\n"
)

# Chat-scoped inbound allowlist (2026-07-20, teren-ratified; WB 0cd5698b).
# STAGED, NOT ACTIVATED: this only NARROWS which chats inbound processing would
# ever consider. Processing itself stays shut by pa.enabled=false +
# platforms.whatsapp.enabled=false + the runtime processing gate; none of those
# are touched here. The gate this feeds — whatsapp.py _is_group_allowed, called
# from _should_process_message as the FIRST statement of _build_message_event —
# can only return False for more chats than before, never True for more.
# Under `open` every group passes; under `allowlist` only this JID passes.
#
# The JID is rung 3a's authorized group, matching the deployed independent
# verifier table (VERIFIED_RUNG_JIDS['3a'] in
# /opt/tgg-capture/whatsapp-bridge/rung-authority.js), so staged inbound scope
# and outbound scope name the SAME chat.
#
# Both blocks must carry the keys. The top-level `whatsapp:` block is bridged
# into platforms.whatsapp.extra and WINS (gateway/config.py: `extra.update(bridged)`),
# and `_group_allow_from` reads config.extra with NO env fallback — so setting
# only one block would leave the allowlist empty and block every group.
#
# DM policy is deliberately untouched (teren: keep DMs on).
# ACTIVATION SET (2026-07-21 go-live, teren-authorized "all maintenance" in-pane
# session f59ecb85 2026-07-21 00:04 SGT; WB 288cdaa2). Enumerated from the WA
# bridge's own store (capture history-metadata.jsonl group names cross-checked
# against tgg-chat-map.md). INCLUDED: the four maintenance site/zone work
# groups + the rung-3a mgmt live-test group + the PcL-internal deployment-
# verification group ("Boss of Christopher" — NOT a client maintenance group;
# included deliberately, coordinator-approved 2026-07-21, so the stock
# verify_runtime deploy smoke, whose fixture chat this is, stays green).
# EXCLUDED (fail-closed, zero pre-model cost): the real client management
# group (120363407903158826, rung 3b only), both mgmt simulators, all
# sprucing-scope groups (teren-ruled out of christopher case-ledger scope
# 2026-06-11), vendor/photos/escalation/general groups not positively
# identifiable as maintenance site groups — full excluded list in the
# 2026-07-21-activation run archive.
# Partition of the inbound allowlist for the outbound-boundary invariant:
# SITE groups are ops-ingest scope (reply-suppressed) and must NEVER carry
# outbound authorization; MGMT entries are the only inbound entries allowed
# to also appear in outbound_allowed_chats. The partition is authored
# explicitly — never positional — because the boundary is the safety claim.
SITE_GROUP_JIDS_TUPLE = (
    "120363421424519051@g.us",  # MM1A Maintenance (AMK, SRG)
    "120363422582425366@g.us",  # MM1B Maintenance (HG, Bedok Resv)
    "120363423568509280@g.us",  # MM2 Maintenance (PG)
    "120363403845802098@g.us",  # MM2 Maintenance (SK)
    # Demo ingest site group (teren 2026-07-21 demo rulings; landed pre-flip
    # with the rest of the demo end-state so the ceremony head moves once).
    # SITE class: ingest-selector scope, reply-suppressed, never outbound.
    "120363429226022766@g.us",  # [TGG] Christopher x PcL Ops Test
)
MGMT_INBOUND_JIDS_TUPLE = (
    "120363426509183563@g.us",  # TGG Christopher Mgmt Live Test 20260528 (rung-3a destination)
    "120363409954029949@g.us",  # Boss of Christopher — PcL deployment-verification (deploy-gate infra)
    # REAL client management group (teren 2026-07-21: the demo runs here;
    # inbound READ opens now — outbound to it stays impossible until the
    # separately-ceremonied 3a->3b arm releases the 3b rung).
    "120363407903158826@g.us",  # Christopher x TGG Management
)
GROUP_ALLOWLIST_JIDS = SITE_GROUP_JIDS_TUPLE + MGMT_INBOUND_JIDS_TUPLE
SITE_GROUP_JIDS = frozenset(SITE_GROUP_JIDS_TUPLE)
MGMT_INBOUND_JIDS = frozenset(MGMT_INBOUND_JIDS_TUPLE)

def _group_allowlist_block(indent: str) -> str:
    lines = [f"{indent}group_policy: allowlist\n", f"{indent}group_allow_from:\n"]
    lines += [f"{indent}- {jid}\n" for jid in GROUP_ALLOWLIST_JIDS]
    return "".join(lines)

# Anchored on the leading newline: the 2-space top-level form is otherwise a
# substring of the 6-space platforms.whatsapp.extra form, which would make the
# exactly-once replacement count 2 and silently patch the wrong block.
GROUP_POLICY_EXTRA_OLD = "\n      group_policy: open\n"
GROUP_POLICY_EXTRA_NEW = "\n" + _group_allowlist_block("      ")
GROUP_POLICY_ROOT_OLD = "\n  group_policy: open\n"
GROUP_POLICY_ROOT_NEW = "\n" + _group_allowlist_block("  ")

# Insertion anchors — each must appear exactly once in its baseline file.
# Ops-test ingest selector (2026-07-21 demo end-state). The anchor is the
# LAST tgg_ops_ingest whatsapp selector in the June baseline (MM2 PG),
# exactly-once; the new selector rides directly after it so the ops-test
# group processes under the same ingest brief as the maintenance sites.
OPS_TEST_SELECTOR_ANCHOR = (
    "- job_type: tgg_ops_ingest\n"
    "  match:\n"
    "    source.platform: whatsapp\n"
    "    source.chat_id: 120363423568509280@g.us\n"
)
OPS_TEST_SELECTOR_SNIPPET = (
    "- job_type: tgg_ops_ingest\n"
    "  match:\n"
    "    source.platform: whatsapp\n"
    "    source.chat_id: 120363429226022766@g.us\n"
)

OPERATIONS_ANCHOR = "          agent_config_read:\n"
AGENT_SECTION = "agent:\n  profile: pa\n  max_turns: 12\n"
OPS_INGEST_OBSERVABILITY_ANCHOR = (
    "    - 'Recording what happened (observability): at the end of each turn, use the record_event\n"
    "      tool to record the meaningful things you did — one event per distinct action\n"
    "      or decision. This is how the team sees your work"
)
EVENT_LABELS_OLD = "case_observation, clarification_requested, routed_out_of_scope. Use a new label"
EVENT_LABELS_NEW = (
    "case_observation, clarification_requested, routed_out_of_scope, attention_raised,\n"
    "      scope_addition_recorded. Use a new label"
)

NEW_OPERATIONS = ("tgg_clarification_raise", "tgg_attention_raise", "tgg_case_wc_attach")
NEW_INSTRUCTION_COUNT = 14
MGMT_NEW_INSTRUCTION_COUNT = 14

# The ingest brief is unscoped (no `business_operations` block at all), which
# grants it the full registry — that is exactly what makes it reachable for
# tgg_attention_list/tgg_attention_read. Annotate is a management-only write:
# ingest's chats never operate on an existing attention item, and unscoped
# would otherwise hand it that write for free. A denied-only block subtracts
# just that one name; with `allowed` empty the allow-filter is skipped
# entirely (`_scope_operations_to_job_brief`), so every other operation ingest
# has today — including the new reads — survives untouched.
INGEST_TOOLSETS_ANCHOR = (
    "    enabled_toolsets:\n"
    "    - memory\n"
    "    - file\n"
    "    - custom\n"
    "    - pa-observability\n"
    "    disabled_toolsets:\n"
    "    - web\n"
    "    - shell\n"
)
INGEST_DENIED_OPERATIONS_SNIPPET = (
    "    business_operations:\n"
    "      denied:\n"
    "      - tgg_attention_annotate\n"
    "      - tgg_case_media\n"
    "      - tgg_media_retention\n"
)

# The management brief is the only brief with a `web` toolset, so this block is
# unique to it in the baseline — the ingest brief disables web.
MGMT_TOOLSETS_ANCHOR = (
    "    enabled_toolsets:\n"
    "    - memory\n"
    "    - file\n"
    "    - web\n"
    "    - custom\n"
    "    - pa-observability\n"
    "    disabled_toolsets:\n"
    "    - terminal\n"
)

# Both briefs carry a "Recording what happened (observability)" instruction; the
# management one is distinguished by "This is part of finishing a turn".
MGMT_OBSERVABILITY_ANCHOR = (
    "    - 'Recording what happened (observability): at the end of each turn, use the record_event\n"
    "      tool to record the meaningful things you did — one event per distinct action\n"
    "      or decision. This is part of finishing a turn, not optional. Record an event\n"
)

# Management chat is the client authority channel (teren, 2026-07-20: "any
# instruction on management chat, christopher should accept"). Every case-shaped
# operation in the registry is permitted here — the prior read-plus-attention
# scoping is REVERSED for case operations. What stays out — christopher's own
# config, chat routing/allowlists, data purges — has no operation in the
# registry at all, so refusal-by-absence still holds via the same mechanism
# (agent/pa_constitution.py business_operations ->
# tools/pa_business_tools.py _scope_operations_to_job_brief). The explicit
# `allowed` list is kept as a pin: a future registry addition does not reach
# the management chat without a deliberate widening here.

# Exact runtime registry for the PA business bridge. Constitution prose may
# mention only these names after the word "operation"; aliases otherwise fail
# at runtime even when the model recovers later in the same turn.
CANONICAL_OPERATIONS = {
    "agent_action_record",
    "agent_config_read",
    "ilinked_lookup",
    "ilinked_status",
    "ilinked_wc_lookup",
    "job_work_costings",
    "message_search",
    "tgg_attention_annotate",
    "tgg_attention_list",
    "tgg_attention_raise",
    "tgg_attention_read",
    "tgg_case_create",
    "tgg_case_list",
    "tgg_case_lookup",
    "tgg_case_media",
    "tgg_case_observation",
    "tgg_case_query",
    "tgg_case_search",
    "tgg_case_update",
    "tgg_case_wc_attach",
    "tgg_clarification_raise",
    "work_costing_ingest_ilinked",
    "work_costing_lookup",
    "work_costing_upsert",
}
CONFIGURED_OPERATIONS = CANONICAL_OPERATIONS | {"tgg_media_retention"}
OPERATION_REFERENCE_RE = re.compile(
    r"\boperation\s+([a-z][a-z0-9]*_[a-z0-9_]+)\b"
)

MOFEX_OPERATION_OLD = (
    "    - If a TGG chat asks for a Mofex fact, call pa_business_read with operation mofex_lookup\n"
    "      and report inability if the tool blocks it.\n"
)
MOFEX_OPERATION_NEW = (
    "    - If a TGG chat asks for a Mofex fact, route it out of TGG scope and report that\n"
    "      Christopher's configured TGG operations cannot retrieve Mofex data. Do not invent\n"
    "      or call a cross-client operation.\n"
)

# Corpus finding (2026-07-20, teren-ratified): workers send photo albums FIRST
# and describe them 7s–2m14s LATER. The June 10s passive window splits every
# album from its caption, so caption-borne identifiers (unit numbers, job
# sheets) never reach the turn holding the media and low-confidence attaches
# follow. 5 minutes bundles album + caption into one turn. Addressed window
# stays 1500ms — patience applies only while passively observing.
DEBOUNCE_PASSIVE_OLD = "    debounce_passive_ms: 10000\n"
DEBOUNCE_PASSIVE_NEW = "    debounce_passive_ms: 300000\n"

# The single universal jobNo/create policy replaces the June create instruction
# 1-for-1 — no other instruction anywhere in the constitution may permit a
# broader create path (codex 2026-07-19 finding 1: two rules of differing
# breadth on jobNo = nondeterministic create behavior).
CREATE_POLICY_OLD = (
    "    - For clearly new case reports with zone, address, problem, and WhatsApp source,\n"
    "      call pa_business_write with operation tgg_case_create before any state update.\n"
    "      Include jobNo when present; otherwise let PS generate a WA job number. If address,\n"
    "      problem, source, or confidence is unclear, do not create; surface the missing\n"
    "      facts for clarification.\n"
)
CREATE_POLICY_NEW = (
    "    - 'tgg_case_create has exactly one trigger — a genuinely new case report (a new\n"
    "      job sheet, or a new problem report with zone, address, problem, and WhatsApp\n"
    "      source). The source document or job sheet in the current site-chat turn IS the\n"
    "      citation for that new event. No registry confirmation exists or is required.\n"
    "      Bind the create to the current turn''s exact message ids in sourceRefs and preserve\n"
    "      its media references as evidence. When a job number is present, first call\n"
    "      pa_business_read with operation tgg_case_lookup only to prevent a duplicate; if\n"
    "      a case already exists, record the material as a tgg_case_observation\n"
    "      on that case instead of creating. Only a genuinely new report without a job\n"
    "      number may let PS generate a WA job number. Never create a placeholder case\n"
    "      for photos, evidence, works orders, completion reports, or anything you have\n"
    "      raised a clarification or attention item about — those paths are observation,\n"
    "      scope addition (tgg_case_wc_attach), attention item, or clarification. The Master\n"
    "      List is backup/audit only and must never enter the decision path: never query it,\n"
    "      wait for it, or use its absence to block a create.\n"
    "\n"
    "      '\n"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one {label} marker, found {count}")
    return text.replace(old, new, 1)


def _safe_config(source: str, slot: dict) -> str:
    model = slot["model"]
    effort = slot["reasoning_effort"]
    rendered = _replace_once(
        source,
        "pa:\n  enabled: true\n",
        "pa:\n  enabled: false\n" + MEDIA_RETENTION_BLOCK,
        label="pa.enabled",
    )
    rendered = _replace_once(
        rendered,
        "platforms:\n  whatsapp:\n    enabled: true\n",
        "platforms:\n  whatsapp:\n    enabled: false\n",
        label="platforms.whatsapp.enabled",
    )
    operations_snippet = (
        PATCHES_ROOT / "ops-judgment-operations.snippet.yaml"
    ).read_text(encoding="utf-8")
    rendered = _replace_once(
        rendered,
        OPERATIONS_ANCHOR,
        operations_snippet + OPERATIONS_ANCHOR,
        label="business-bridge operations",
    )
    rendered = _replace_once(
        rendered,
        GROUP_POLICY_EXTRA_OLD,
        GROUP_POLICY_EXTRA_NEW,
        label="platforms.whatsapp.extra group_policy",
    )
    rendered = _replace_once(
        rendered,
        GROUP_POLICY_ROOT_OLD,
        GROUP_POLICY_ROOT_NEW,
        label="top-level whatsapp group_policy",
    )
    if not rendered.endswith("group_sessions_per_user: false\n"):
        raise RuntimeError("config baseline no longer ends at group_sessions_per_user")
    rendered += MEMORY_OFF_BLOCK + PYTHON_SANDBOX_BLOCK
    if effort is not None:
        rendered = _replace_once(
            rendered,
            AGENT_SECTION,
            AGENT_SECTION + f"  reasoning_effort: {effort}\n",
            label="agent section",
        )
    if model != BASELINE_MODEL:
        rendered = rendered.replace(BASELINE_MODEL, model)
    return rendered


def _constitution(source: str, slot: dict) -> str:
    model = slot["model"]
    judgment_snippet = (PATCHES_ROOT / "ops-ingest-judgment.snippet.yaml").read_text(
        encoding="utf-8"
    )
    rendered = _replace_once(
        source,
        DEBOUNCE_PASSIVE_OLD,
        DEBOUNCE_PASSIVE_NEW,
        label="ops-ingest passive debounce",
    )
    rendered = _replace_once(
        rendered,
        OPS_TEST_SELECTOR_ANCHOR,
        OPS_TEST_SELECTOR_ANCHOR + OPS_TEST_SELECTOR_SNIPPET,
        label="ops-test ingest selector",
    )
    rendered = _replace_once(
        rendered,
        CREATE_POLICY_OLD,
        CREATE_POLICY_NEW,
        label="ops-ingest create policy",
    )
    rendered = _replace_once(
        rendered,
        OPS_INGEST_OBSERVABILITY_ANCHOR,
        judgment_snippet + OPS_INGEST_OBSERVABILITY_ANCHOR,
        label="ops-ingest observability instruction",
    )
    rendered = _replace_once(
        rendered,
        EVENT_LABELS_OLD,
        EVENT_LABELS_NEW,
        label="ops-ingest event label list",
    )
    rendered = _replace_once(
        rendered,
        INGEST_TOOLSETS_ANCHOR,
        INGEST_TOOLSETS_ANCHOR + INGEST_DENIED_OPERATIONS_SNIPPET,
        label="ops-ingest toolsets block",
    )
    mgmt_behavior_snippet = (PATCHES_ROOT / "mgmt-chat-behavior.snippet.yaml").read_text(
        encoding="utf-8"
    )
    python_sandbox_snippet = (
        PATCHES_ROOT / "python-sandbox-management.snippet.yaml"
    ).read_text(encoding="utf-8")
    rendered = _replace_once(
        rendered,
        MGMT_OBSERVABILITY_ANCHOR,
        mgmt_behavior_snippet + python_sandbox_snippet + MGMT_OBSERVABILITY_ANCHOR,
        label="management observability instruction",
    )
    mgmt_operations_snippet = (
        PATCHES_ROOT / "mgmt-business-operations.snippet.yaml"
    ).read_text(encoding="utf-8")
    rendered = _replace_once(
        rendered,
        MGMT_TOOLSETS_ANCHOR,
        MGMT_TOOLSETS_ANCHOR.replace(
            "    disabled_toolsets:\n",
            "    - python-sandbox\n    disabled_toolsets:\n",
        )
        + mgmt_operations_snippet,
        label="management toolsets block",
    )
    rendered = _replace_once(
        rendered,
        "      - /always\n      compression:\n",
        "      - /always\n      max_output_tokens: 8192\n      compression:\n",
        label="management output token cap",
    )
    # The June baseline names a cross-client operation that is not present in
    # Christopher's runtime registry (once in each job brief). Keep the existing
    # out-of-scope behavior, but remove the impossible tool call.
    if rendered.count(MOFEX_OPERATION_OLD) != 2:
        raise RuntimeError(
            "expected two legacy Mofex operation instructions, found "
            f"{rendered.count(MOFEX_OPERATION_OLD)}"
        )
    rendered = rendered.replace(MOFEX_OPERATION_OLD, MOFEX_OPERATION_NEW)
    if model != BASELINE_MODEL:
        replaced = rendered.replace(BASELINE_MODEL, model)
        if replaced == rendered:
            raise RuntimeError(f"constitution has no {BASELINE_MODEL} selectors")
        rendered = replaced
    return rendered


def _validate(
    config_path: Path,
    constitution_path: Path,
    slot: dict,
    *,
    baseline_instruction_count: int,
    baseline_mgmt_instruction_count: int,
) -> None:
    model = slot["model"]
    effort = slot["reasoning_effort"]
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8"))

    assert config["pa"]["enabled"] is False
    assert config["pa"]["media_retention"] == {
        "enabled": True,
        "media_root": "/home/pclaw/.systems-pcl/data/media/tgg/hermes",
        "media_ref_prefix": "/media/tgg/hermes",
        "source_roots": ["/var/lib/tgg-capture/whatsapp/media"],
        "operation": "tgg_media_retention",
        "min_free_percent": 20,
    }
    assert config["group_sessions_per_user"] is False
    assert config["platforms"]["whatsapp"]["enabled"] is False
    assert config["model"]["provider"] == "openai-direct-primary"
    assert config["model"]["default"] == model
    assert config["providers"]["openai-direct-primary"]["default_model"] == model
    for task in ("compression", "session_search", "title_generation"):
        assert config["auxiliary"][task]["model"] == model
    assert config["memory"] == {
        "memory_enabled": False,
        "user_profile_enabled": False,
    }
    assert config["python_sandbox"] == {
        "enabled": True,
        "datasets": {
            "cases": {
                "type": "sqlite",
                "path": "/home/pclaw/.systems-pcl/data/tenants/tgg.db",
                "description": "Operational case/work-costing database (read-only snapshot; sqlite)",
            },
            "media": {
                "type": "path",
                "path": "/home/pclaw/.systems-pcl/data/media/tgg/hermes",
                "description": "Retained WhatsApp media/attachments (read-only; spreadsheets land here)",
            },
        },
        "limits": {
            "wall_seconds": 120,
            "max_wall_seconds": 300,
            "cpu_seconds": 60,
            "memory_mb": 1024,
            "file_size_mb": 64,
            "scratch_mb": 64,
            "max_processes": 64,
            "max_open_files": 256,
            "max_snapshot_mb": 512,
        },
        "artifact_ttl_days": 7,
        "max_runs_kept": 40,
    }

    # Chat-scoped inbound allowlist, staged in BOTH blocks (the top-level block
    # bridges into extra and wins; extra has no env fallback for the allowlist).
    expected_jids = list(GROUP_ALLOWLIST_JIDS)
    for block in (config["whatsapp"], config["platforms"]["whatsapp"]["extra"]):
        assert block["group_policy"] == "allowlist", block["group_policy"]
        assert block["group_allow_from"] == expected_jids, block["group_allow_from"]
        # Activation (2026-07-21) flips the staging-era subset invariant:
        # inbound scope now includes the reply-suppressed maintenance site
        # groups, which must NEVER be outbound-authorized. Outbound stays
        # governed solely by outbound_allowed_chats + the bridge trust stack;
        # inbound membership must not imply outbound. Assert the boundary in
        # both directions instead:
        outbound = set(block["outbound_allowed_chats"])
        assert MGMT_INBOUND_JIDS <= outbound, sorted(MGMT_INBOUND_JIDS - outbound)
        assert not (SITE_GROUP_JIDS & outbound), sorted(SITE_GROUP_JIDS & outbound)
        # DM policy is explicitly out of scope for this change.
        assert block["dm_policy"] == "allowlist", block["dm_policy"]

    # Selector <-> allowlist alignment: every allowlisted WHATSAPP group must
    # bind to a selector of the class its partition claims — site groups to
    # the reply-suppressed ingest brief, mgmt entries to the management brief.
    selector_class = {
        str(sel["match"]["source.chat_id"]): sel["job_type"]
        for sel in constitution.get("selectors", [])
        if sel.get("match", {}).get("source.platform") == "whatsapp"
    }
    for jid in SITE_GROUP_JIDS:
        assert selector_class.get(jid) == "tgg_ops_ingest", (jid, selector_class.get(jid))
    for jid in MGMT_INBOUND_JIDS:
        assert selector_class.get(jid) == "tgg_management", (jid, selector_class.get(jid))
    if effort is None:
        assert "reasoning_effort" not in config["agent"]
    else:
        assert config["agent"]["reasoning_effort"] == effort

    operations = config["pa"]["overlay"]["client"]["business_bridge"]["operations"]
    assert set(operations) == CONFIGURED_OPERATIONS, (
        sorted(set(operations) - CONFIGURED_OPERATIONS),
        sorted(CONFIGURED_OPERATIONS - set(operations)),
    )
    for name in NEW_OPERATIONS:
        assert name in operations, name
        assert operations[name]["type"] == "http"
        assert operations[name]["method"] == "POST"
    assert operations["tgg_case_wc_attach"]["path_params"] == ["jobNo"]
    assert operations["tgg_case_media"]["method"] == "GET"
    assert operations["tgg_case_media"]["path_params"] == ["jobNo"]
    assert operations["tgg_media_retention"]["method"] == "POST"

    assert constitution["runtime"] == {
        "provider": "openai-direct-primary",
        "model": model,
    }
    for job in ("tgg_ops_ingest", "tgg_management"):
        assert constitution["job_briefs"][job]["runtime"] == {
            "model": model,
            "provider": "openai-direct-primary",
        }
    ingest_brief = constitution["job_briefs"]["tgg_ops_ingest"]
    assert ingest_brief["debounce_passive_ms"] == 300000
    assert ingest_brief["debounce_addressed_ms"] == 1500

    instructions = constitution["job_briefs"]["tgg_ops_ingest"]["instructions"]
    assert (
        len(instructions) == baseline_instruction_count + NEW_INSTRUCTION_COUNT
    ), len(instructions)
    joined = "\n".join(instructions)
    assert "work_items" in joined
    assert "tgg_attention_raise" in joined
    assert "tgg_clarification_raise" in joined
    assert "tgg_case_wc_attach" in joined
    assert "attention_raised" in joined
    # Teren 2026-07-22 21:22/22:03: the source job sheet is the citation;
    # Master is an audit surface and clarification is exception-only.
    assert "source document or job sheet in the current site-chat turn IS" in joined
    assert "No registry confirmation exists or is required" in joined
    assert "Master List is backup/audit only" in joined
    assert "must never enter the decision path" in joined
    assert "Clarification boundary and routing" in joined
    assert "Route every clarification to the manager chat for now" in joined
    assert "do not ask in the source site thread" in joined
    all_instructions = "\n".join(
        instruction
        for job in constitution["job_briefs"].values()
        for instruction in job["instructions"]
    )
    referenced_operations = set(OPERATION_REFERENCE_RE.findall(all_instructions))
    assert referenced_operations <= CANONICAL_OPERATIONS, sorted(
        referenced_operations - CANONICAL_OPERATIONS
    )
    assert "`tgg_clarification_request`" in joined
    assert "`tgg_message_history_search`" in joined
    assert "`tgg_case_update_state`" in joined
    assert "mofex_lookup" not in all_instructions
    assert "thread it arrived in is" in joined
    assert "the strongest signal" in joined
    assert "timing against active work" in joined
    assert "Doubt with no named rival never triggers a clarification" in joined
    assert "resolves ONLY to dormant or completed cases" in joined
    assert "never an attach target" in joined
    assert "message_search for the same chat_jid with limit 10" in joined
    assert "LOW confidence" in joined
    assert "runtime supplies" in joined
    assert "current-turn refs when they are omitted" in joined
    assert "exact date-only source string in receiptDate" in joined
    assert "Do not calculate midnight or dueAt" in joined
    assert "runtime mechanically normalizes receiptDate" in joined
    assert "actual timestamp, pass it as receivedAt" in joined
    # Stage-1 roll-forward (2026-07-20): placeholder sourceRefs ban + the
    # evidence-attach justification contract with refs-preserving retry.
    assert "Never write a placeholder token" in joined
    assert "current_turn" in joined
    assert "Evidence-attach justification contract" in joined
    assert "ATTACH_UNJUSTIFIED" in joined
    assert "identifier_match" in joined
    assert "thread_continuation" in joined
    assert "operator_directive" in joined
    assert "block_unit" in joined
    assert "retry with the SAME sourceRefs" in joined
    assert "never remove photo or media" in joined
    assert "pure social acknowledgement" in joined
    # Scope rule (2026-07-20, teren-ruled): the task set closes at case creation;
    # completion reports update tasks under their ESTABLISHED label instead of
    # minting new ones. Label reuse is the load-bearing half — downstream work
    # state is keyed by normalized label, so re-wording a task forks one portal
    # row into two, which is the defect this rule exists to close.
    assert "task set closes when the case opens" in joined
    assert "does not grow from worker chatter" in joined
    assert "UPDATES the existing task set and never extends it" in joined
    assert "ESTABLISHED label, copied verbatim" in joined
    assert "re-labelling forks one task into two" in joined
    assert "ONE task closed, not one per step" in joined
    assert "Never fragment a task" in joined
    assert "replacement closes the defect it replaces" in joined
    assert "do NOT raise a clarification for it" in joined
    assert "Scope grows only on an explicit statement" in joined
    assert "When in doubt, GROUP" in joined
    assert "never add a work_item for it silently" in joined
    assert "tgg_clarification_raise naming the case and the unmatched item" in joined
    # The completion path must reuse the established label, not re-word it —
    # the original "uses the trade's own words" phrasing is now conditional.
    assert "reuse that task's established label verbatim" in joined
    # Composes with, and does not contradict, the dormant-identifier hold: that
    # rule still owns evidence resolving only to dormant/completed cases and
    # must survive this change untouched.
    assert "A dormant or completed case is never a rival and never an attach target." in joined
    # Exactly one create policy: the consolidated rule is present, the broader
    # June create instruction is gone, and no other instruction names the
    # create operation.
    assert "tgg_case_create has exactly one trigger" in joined
    assert "before any state update" not in joined
    create_mentions = [item for item in instructions if "tgg_case_create" in item]
    assert len(create_mentions) == 2
    assert sum("has exactly one trigger" in item for item in create_mentions) == 1
    assert sum("exact closed vocabulary" in item for item in create_mentions) == 1

    # ── Management chat: behavior section + business-operation scope ──────────
    mgmt_brief = constitution["job_briefs"]["tgg_management"]
    assert mgmt_brief["response_policy"]["max_output_tokens"] == 8192
    mgmt_instructions = mgmt_brief["instructions"]
    assert (
        len(mgmt_instructions)
        == baseline_mgmt_instruction_count + MGMT_NEW_INSTRUCTION_COUNT
    ), len(mgmt_instructions)
    mgmt_joined = "\n".join(mgmt_instructions)
    assert "python_sandbox is your batch-computation tool" in mgmt_joined
    assert "more than ~50 items" in mgmt_joined
    assert "never hand-simulate a batch computation" in mgmt_joined
    assert "python-sandbox" in mgmt_brief["enabled_toolsets"]
    # Register + grounded answers.
    assert "you are TGG's operations coordinator" in mgmt_joined
    assert "carries that case's job number" in mgmt_joined
    assert "Ground every answer in this turn's tool results" in mgmt_joined
    assert "Re-query rather than recall" in mgmt_joined
    assert "no case found for" in mgmt_joined
    # Aggregate questions route to the SQL-backed aggregate surface and never
    # demand a single-case identifier. Sender metadata never enters identifier
    # extraction; the negative case-specific refusal remains explicit.
    assert "Aggregate management questions use the aggregate register" in mgmt_joined
    assert "how many completed" in mgmt_joined
    assert "which are not completed for HG" in mgmt_joined
    assert "oldest open case" in mgmt_joined
    assert "operation tgg_case_query" in mgmt_joined
    assert "SELECT-only SQL query" in mgmt_joined
    assert "Never refuse an aggregate ask" in mgmt_joined
    assert "Sender identity is context, never case identity" in mgmt_joined
    assert "sender's display name" in mgmt_joined
    assert "NEVER a job number, case id, block, unit, or search term" in mgmt_joined
    assert "Do not pass any sender identity value" in mgmt_joined
    assert "message body or quoted case content" in mgmt_joined
    assert "asks about one particular case" in mgmt_joined
    assert "ask exactly ONE clarifying question for the missing identifier" in mgmt_joined
    assert "Aggregate questions follow the aggregate rule" in mgmt_joined
    # Proactive push, batched one message per wave.
    assert "Push attention items unprompted" in mgmt_joined
    assert "one wave into ONE message" in mgmt_joined
    assert "only thing you raise unprompted" in mgmt_joined
    # Wave-complete trigger [WB:ee014d48]: a synthetic "[system] processing
    # wave complete" inbound message is the mechanical instruction to push the
    # batched digest NOW, scoped to exactly the ids the trigger names. The
    # dedupe (nothing re-announced) lives in the emitting watcher's marker
    # file, not in model memory — the trigger names only never-announced ids.
    assert "processing wave complete" in mgmt_joined
    assert "machinery, not a person" in mgmt_joined
    assert "exactly the item ids the trigger names" in mgmt_joined
    assert "never re-appear in a digest" in mgmt_joined
    assert "the operator sees only the digest" in mgmt_joined
    assert "send nothing this turn" in mgmt_joined
    # Open-ended attention-queue question: live read this turn, not recall.
    assert "request to read the OPEN attention queue" in mgmt_joined
    assert "tgg_attention_list" in mgmt_joined
    assert "nothing outstanding right now" in mgmt_joined
    assert "do not fill the gap with" in mgmt_joined
    # Write authority + three-flow (teren 2026-07-20: management chat is the
    # client authority channel; explicit-instruction-only).
    assert "Management instructions are authoritative" in mgmt_joined
    assert "attach evidence or photos" in mgmt_joined
    assert "closing" in mgmt_joined and "marking it submitted" in mgmt_joined
    assert "confirm what changed" in mgmt_joined
    assert "exactly ONE clarifying question" in mgmt_joined
    assert "write nothing until the operator says yes" in mgmt_joined
    assert "Never infer an instruction" in mgmt_joined
    assert "explicit say-so" in mgmt_joined
    # Provenance: operator-directed writes are audit-distinguishable.
    assert "Operator-directed writes carry provenance" in mgmt_joined
    assert "instructing message's id in sourceRefs" in mgmt_joined
    assert "operator-directed via management chat" in mgmt_joined
    assert "operator_directive" in mgmt_joined
    assert "the observation is the audit trail" in mgmt_joined
    # Hold-framing (teren rule: the operator must learn they are the blocker).
    assert "Lead with the hold" in mgmt_joined
    assert "the FIRST thing you say is the pending decision" in mgmt_joined
    assert "I need your call on" in mgmt_joined
    assert "the operator must learn the decision is waiting on them" in mgmt_joined
    # Reply-action substance retained.
    assert "promise-then-track a chase" in mgmt_joined
    # Annotate targets an existing item by id, never a new tgg_attention_raise.
    assert "THAT existing item by its id" in mgmt_joined
    assert "never raise a new attention item to record a reply to an old one" in mgmt_joined
    # Refusals: config/routing/allowlist/purge stay out, by absence.
    assert "your own configuration" in mgmt_joined
    assert "group allowlists" in mgmt_joined
    assert "deleting or purging data" in mgmt_joined
    assert "Nothing you send goes to anyone outside this chat" in mgmt_joined
    assert "call tgg_case_photos with that job number" in mgmt_joined
    assert "destination is always this fresh management chat" in mgmt_joined
    assert "Never invent a numeric case id" in mgmt_joined
    assert "Never relay site-group content wholesale" in mgmt_joined
    assert "not a courtesy" in mgmt_joined

    # The scope block is the mechanism the prose above describes. The management
    # chat now carries the FULL case-shaped registry — permitted must equal the
    # canonical operation set exactly, so nothing case-shaped is missing and
    # nothing outside the registry is named. Refusal-by-absence for config/
    # routing/purge asks holds because no such operation exists in the registry.
    scope = mgmt_brief["business_operations"]
    assert set(scope) == {"allowed"}, sorted(scope)
    permitted = set(scope["allowed"])
    assert permitted == CANONICAL_OPERATIONS, (
        sorted(permitted - CANONICAL_OPERATIONS),
        sorted(CANONICAL_OPERATIONS - permitted),
    )
    assert permitted == set(operations) - {"tgg_media_retention"}, sorted(
        permitted ^ (set(operations) - {"tgg_media_retention"})
    )
    # The writes the rulings name, spelled out for the reader.
    for required in (
        "tgg_case_observation",  # attach evidence / notes
        "tgg_case_update",  # state changes incl. close / submitted
        "tgg_case_create",
        "tgg_case_wc_attach",  # work items
        "tgg_clarification_raise",
        "tgg_attention_annotate",
        "tgg_attention_raise",
        "agent_action_record",
    ):
        assert required in permitted, required
    # Read-only SQL aggregate query [WB:1e543b12]: management's aggregate
    # surface. Read-only is enforced server-side (SELECT-only vet + read-only
    # sqlite connection); it is a read, never a case mutation. Ingest inherits
    # it through the unscoped registry.
    assert "tgg_case_query" in permitted
    assert operations["tgg_case_query"]["method"] == "POST"
    assert operations["tgg_case_query"]["url"].endswith("/api/operator/query")
    # The ingest brief stays unscoped except for a single denied-only
    # subtraction: it keeps every operation it had (including the new reads)
    # and loses only the management-only annotate write. `allowed` absent
    # means the allow-filter is skipped entirely — this narrows, never widens.
    ingest_scope = ingest_brief["business_operations"]
    assert set(ingest_scope) == {"denied"}, sorted(ingest_scope)
    assert ingest_scope["denied"] == [
        "tgg_attention_annotate",
        "tgg_case_media",
        "tgg_media_retention",
    ], ingest_scope["denied"]

    # Prose and mechanism must agree: no management instruction may tell
    # Christopher to call an operation the scope denies him, or he is being
    # instructed to do something the runtime will refuse.
    mgmt_referenced = set(OPERATION_REFERENCE_RE.findall(mgmt_joined))
    assert mgmt_referenced <= permitted, sorted(mgmt_referenced - permitted)


def main() -> int:
    baseline_config = (BASELINE_ROOT / "config.live-2026-06-19.yaml").read_text(
        encoding="utf-8"
    )
    baseline_constitution = (
        BASELINE_ROOT / "christopher_tgg_constitution.live-2026-06-19.yaml"
    ).read_text(encoding="utf-8")
    baseline_briefs = yaml.safe_load(baseline_constitution)["job_briefs"]
    baseline_instruction_count = len(baseline_briefs["tgg_ops_ingest"]["instructions"])
    baseline_mgmt_instruction_count = len(
        baseline_briefs["tgg_management"]["instructions"]
    )

    slot_files: list[Path] = []
    for slot_id, slot in SLOTS.items():
        slot_root = SLOTS_ROOT / slot_id
        slot_root.mkdir(parents=True, exist_ok=True)
        config_path = slot_root / "config.yaml"
        constitution_path = slot_root / "christopher_tgg_constitution.yaml"
        config_path.write_text(_safe_config(baseline_config, slot), encoding="utf-8")
        constitution_path.write_text(
            _constitution(baseline_constitution, slot), encoding="utf-8"
        )
        _validate(
            config_path,
            constitution_path,
            slot,
            baseline_instruction_count=baseline_instruction_count,
            baseline_mgmt_instruction_count=baseline_mgmt_instruction_count,
        )
        slot_files.extend((config_path, constitution_path))

    # The historical root paths remain the default authored deployment view.
    # They are generated from, and must remain byte-identical to, the default slot.
    default_slot = SLOTS_ROOT / DEFAULT_SLOT
    root_config = DEPLOY_ROOT / "config.yaml"
    root_constitution = DEPLOY_ROOT / "christopher_tgg_constitution.yaml"
    root_config.write_bytes((default_slot / "config.yaml").read_bytes())
    root_constitution.write_bytes(
        (default_slot / "christopher_tgg_constitution.yaml").read_bytes()
    )
    checksum_lines = []
    for path in slot_files:
        checksum_lines.append(f"{_sha256(path)}  {path.relative_to(SLOTS_ROOT)}")
    (SLOTS_ROOT / "SHA256SUMS").write_text(
        "\n".join(checksum_lines) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
