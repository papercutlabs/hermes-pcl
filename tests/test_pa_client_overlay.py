from pathlib import Path

from agent.pa_constitution import load_constitution, render_identity_prompt, resolve_context


PA_FIXTURE = Path(__file__).parent / "fixtures" / "pa" / "bobby_tgg_constitution.yaml"
TGG_OVERLAY = Path(__file__).parent / "fixtures" / "clients" / "tgg" / "overlay.yaml"
EXAMPLECO_OVERLAY = Path(__file__).parent / "fixtures" / "clients" / "exampleco" / "overlay.yaml"


def test_client_overlay_changes_display_identity_without_changing_job_briefs():
    tgg = resolve_context(
        {"constitution_path": str(PA_FIXTURE), "client_overlay_path": str(TGG_OVERLAY)},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-ops"}},
    )
    exampleco = resolve_context(
        {"constitution_path": str(PA_FIXTURE), "client_overlay_path": str(EXAMPLECO_OVERLAY)},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-ops"}},
    )

    assert tgg is not None
    assert exampleco is not None
    assert tgg.constitution.identity["display_name"] == "TGG Assistant"
    assert exampleco.constitution.identity["display_name"] == "ExampleCo Assistant"
    assert tgg.identity_hash != exampleco.identity_hash
    assert tgg.job_hash == exampleco.job_hash
    assert tgg.behavior_hash != exampleco.behavior_hash


def test_same_overlay_keeps_identity_hash_across_jobs():
    ops = resolve_context(
        {"constitution_path": str(PA_FIXTURE), "client_overlay_path": str(TGG_OVERLAY)},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-ops"}},
    )
    management = resolve_context(
        {"constitution_path": str(PA_FIXTURE), "client_overlay_path": str(TGG_OVERLAY)},
        {"source": {"platform": "whatsapp", "chat_id": "tgg-management"}},
    )

    assert ops is not None
    assert management is not None
    assert ops.identity_hash == management.identity_hash
    assert ops.job_hash != management.job_hash
    assert ops.behavior_hash != management.behavior_hash


def test_overlay_identity_renders_client_surface():
    base = load_constitution(PA_FIXTURE)
    overlaid = resolve_context(
        {"constitution": base, "client_overlay_path": str(TGG_OVERLAY)},
        {"job_type": "tgg_management"},
    )

    assert overlaid is not None
    prompt = render_identity_prompt(overlaid.constitution)
    assert "display_name: TGG Assistant" in prompt
    assert "refer_to_self: your TGG assistant" in prompt
