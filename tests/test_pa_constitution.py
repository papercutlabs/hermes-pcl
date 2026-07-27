from __future__ import annotations

from pathlib import Path

import pytest

from agent.pa_constitution import (
    PAConstitution,
    behavior_hash,
    load_constitution,
    load_constitution_data,
    render_identity_prompt,
    render_job_prompt,
    resolve_context,
)


FIXTURE = Path(__file__).parent / "fixtures" / "pa" / "bobby_tgg_constitution.yaml"
TGG_PRODUCTION_CONSTITUTION = (
    Path(__file__).parents[1]
    / "deploy"
    / "tgg"
    / "christopher"
    / "christopher_tgg_constitution.yaml"
)


def test_fixture_loads() -> None:
    constitution = load_constitution(FIXTURE)

    assert isinstance(constitution, PAConstitution)
    assert constitution.id == "bobby_tgg"
    assert constitution.agent_name == "Bobby"
    assert set(constitution.job_briefs) == {"tgg_ops_ingest", "tgg_management"}
    assert len(constitution.selectors) == 4
    assert len(constitution.hash) == 64
    assert "Personal assistant for TGG" in render_identity_prompt(constitution)
    assert constitution.job_briefs["tgg_ops_ingest"].response_policy["slash_commands"] == []
    assert constitution.job_briefs["tgg_ops_ingest"].response_policy["max_output_tokens"] == 2048
    assert constitution.job_briefs["tgg_management"].response_policy["slash_commands"] == [
        "/pause",
        "/summary",
    ]
    assert constitution.job_briefs["tgg_management"].response_policy["max_output_tokens"] == 8192


def test_invalid_fixture_fails_loudly() -> None:
    with pytest.raises(ValueError, match="agent_name"):
        load_constitution_data(
            {
                "id": "broken",
                "identity": {},
                "client": {},
                "job_briefs": {},
            },
            source="broken.yaml",
        )

    with pytest.raises(ValueError, match="unknown job_type"):
        load_constitution_data(
            {
                "id": "broken",
                "agent_name": "Broken",
                "identity": {},
                "client": {},
                "job_briefs": {
                    "known": {
                        "title": "Known",
                        "purpose": "Known purpose",
                        "instructions": ["Do the known thing."],
                    }
                },
                "selectors": [{"job_type": "missing", "match": {"source.chat_id": "x"}}],
            },
            source="broken.yaml",
        )


def test_same_bobby_identity_hash_across_ops_and_management() -> None:
    constitution = load_constitution(FIXTURE)
    ops = resolve_context({"constitution": constitution}, {"source": {"platform": "whatsapp", "chat_id": "tgg-ops"}})
    management = resolve_context(
        {"constitution": constitution},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-management"}},
    )

    assert ops is not None
    assert management is not None
    assert ops.identity_hash == management.identity_hash == constitution.hash
    assert ops.job_hash != management.job_hash


def test_ops_and_management_render_different_job_prompts() -> None:
    constitution = load_constitution(FIXTURE)
    ops = resolve_context({"constitution": constitution}, {"source": {"platform": "whatsapp", "chat_id": "tgg-ops"}})
    management = resolve_context(
        {"constitution": constitution},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-management"}},
    )

    assert ops is not None
    assert management is not None
    ops_prompt = render_job_prompt(ops)
    management_prompt = render_job_prompt(management)

    assert ops_prompt != management_prompt
    assert "TGG Operations Ingest" in ops_prompt
    assert "create_fact" in ops_prompt
    assert "TGG Management Brief" in management_prompt
    assert "management_brief" in management_prompt


def test_tgg_management_defaults_to_operator_db_before_ilinked() -> None:
    constitution = load_constitution(TGG_PRODUCTION_CONSTITUTION)
    brief = constitution.job_briefs["tgg_management"]
    prompt = "\n".join(brief.instructions)

    assert "operation tgg_case_search" in prompt
    assert "receivedAgeLabel" in prompt
    assert "General lists should stay case-shaped" in prompt
    assert "job_work_costings" in prompt
    assert "work_costing_lookup" in prompt
    assert "work_costing_ingest_ilinked" in prompt
    assert "operator DB only" in prompt
    assert "iLinked is opt-in only" in prompt
    assert "Do not include Recommendations or Open Questions sections by default" in prompt
    assert "Do not use imperative advice" in prompt
    assert "attribute it as recorded data" in prompt
    assert "Do not expose tenant/contact phone numbers" in prompt
    assert "Do not tell the operator that a case can be closed" in prompt
    assert "use \"Chase target\", \"Evidence\", and \"Status\"" in prompt
    assert "Do not write \"you should\" or \"you need to\"" in prompt
    assert "suppress that identifier" in prompt
    assert "Do not infer an iLinked request from vague operator wording" in prompt
    assert "Use neutral labels such as \"system status\"" in prompt
    assert "source_system_requested=true" in prompt
    assert "state the assumption before answering" in prompt
    assert "223A got what outstanding?" in prompt
    assert "serviceLine/serviceLineLabel" in prompt
    assert "Maintenance cases" in prompt
    assert "Sprucing/EASE cases" in prompt
    assert "Do not interleave them" in prompt
    assert "serviceLine=maintenance or serviceLine=sprucing" in prompt
    assert "resultServiceLineSplit" in prompt
    assert "Do NOT infer totals by counting returned rows" in prompt
    assert "Run separate tgg_case_search calls" in prompt
    assert "Sort each section by receivedAgeDays descending" in prompt
    assert "syntheticJobNo=true" in prompt
    assert "name-only lookups" in prompt
    assert "quoted-message context" in prompt
    assert "no nested bullets" in prompt
    assert "Honor shortness signals" in prompt
    assert "Response length should track the question's information need" in prompt
    assert "custom" in brief.enabled_toolsets
    assert "terminal" in brief.disabled_toolsets
    assert "shell" not in brief.disabled_toolsets
    assert brief.response_policy["include_recommendation"] is False
    assert {"/new", "/reset", "/approve", "/always"}.issubset(
        set(brief.response_policy["slash_commands"])
    )
    assert brief.response_policy["max_output_tokens"] == 8192


def test_tgg_production_management_selectors_include_live_wa_groups() -> None:
    constitution = load_constitution(TGG_PRODUCTION_CONSTITUTION)

    legacy_management = resolve_context(
        {"constitution": constitution},
        {"source": {"platform": "whatsapp", "chat_id": "120363409954029949@g.us"}},
    )
    live_test_management = resolve_context(
        {"constitution": constitution},
        {"source": {"platform": "whatsapp", "chat_id": "120363426509183563@g.us"}},
    )

    assert legacy_management is not None
    assert legacy_management.job_type == "tgg_management"
    assert live_test_management is not None
    assert live_test_management.job_type == "tgg_management"


def test_selector_resolves_tgg_ops_ingest_vs_tgg_management_from_metadata() -> None:
    constitution = load_constitution(FIXTURE)

    ops = resolve_context({"constitution": constitution}, {"source": {"platform": "whatsapp", "chat_id": "tgg-ops"}})
    management = resolve_context(
        {"constitution": constitution},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-management"}},
    )
    telegram_ops = resolve_context(
        {"constitution": constitution},
        {"source": {"platform": "telegram", "chat_id": "-5192935862"}},
    )
    telegram_management = resolve_context(
        {"constitution": constitution},
        {"source": {"platform": "telegram", "chat_id": "-5295904349"}},
    )
    unresolved = resolve_context(
        {"constitution": constitution},
        {"source": {"platform": "whatsapp", "chat_id": "other"}},
    )

    assert ops is not None
    assert ops.job_type == "tgg_ops_ingest"
    assert management is not None
    assert management.job_type == "tgg_management"
    assert telegram_ops is not None
    assert telegram_ops.job_type == "tgg_ops_ingest"
    assert telegram_management is not None
    assert telegram_management.job_type == "tgg_management"
    assert unresolved is None


def test_job_brief_runtime_is_loaded_and_hashes() -> None:
    constitution = load_constitution_data(
        {
            "id": "runtime-test",
            "agent_name": "Runtime Test",
            "identity": {"role": "PA"},
            "client": {"name": "Client"},
            "job_briefs": {
                "ops": {
                    "title": "Ops",
                    "purpose": "Ingest ops.",
                    "instructions": ["Extract facts."],
                    "runtime": {"model": "gemini-3.1-flash-lite"},
                },
                "management": {
                    "title": "Management",
                    "purpose": "Answer management.",
                    "instructions": ["Answer questions."],
                    "runtime": {"model": "gemini-3-flash-preview"},
                },
            },
            "selectors": [],
        },
        source="runtime.yaml",
    )

    ops = constitution.job_briefs["ops"]
    management = constitution.job_briefs["management"]

    assert ops.runtime["model"] == "gemini-3.1-flash-lite"
    assert management.runtime["model"] == "gemini-3-flash-preview"
    assert ops.hash != management.hash


def test_stable_behavior_hashes() -> None:
    constitution_a = load_constitution(FIXTURE)
    constitution_b = load_constitution(FIXTURE)
    ops_a = resolve_context(
        {"constitution": constitution_a},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-ops"}, "message_id": "first"},
    )
    ops_b = resolve_context(
        {"constitution": constitution_b},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-ops"}, "message_id": "second"},
    )

    assert ops_a is not None
    assert ops_b is not None
    assert constitution_a.hash == constitution_b.hash
    assert ops_a.job_hash == ops_b.job_hash
    assert ops_a.behavior_hash == ops_b.behavior_hash
    assert behavior_hash({"b": 2, "a": [1, {"c": True}]}) == behavior_hash({"a": [1, {"c": True}], "b": 2})
