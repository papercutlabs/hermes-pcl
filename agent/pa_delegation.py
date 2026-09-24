"""PA scope for native delegation children on the TGG Management route.

Native delegation builds every child, synchronous, batch or background, in
``tools/delegate_tool.py::_build_child_agent`` from the parent agent's
``enabled_toolsets`` attribute.  A child whose parent omits ``toolsets``
inherits that list, and an explicit request is intersected with it.  The
parent's own tool schemas are fixed when the parent is constructed, and no
other runtime reader uses the attribute after construction.

On the Management route (interactive Management turns and scheduled
``tgg_management`` jobs) the PA layer therefore sets that attribute to exactly
two capability toolsets: the read-only investigation toolset, and the explicit
case-edit toolset (the same reads plus ``tgg_whatsapp_case_record_result``,
which only records a result for a run the parent prepared).  A child receives
those two when the parent names none, and only the part of an explicit request
that falls inside them otherwise.  No other write tool (business writes, case
create/update, per-case prepare/submit, file return, sandbox, file writes,
scheduling) can reach a child on this route.  Background mode is not gated
here; it builds its child through the same function and gets the same scope.

Any other route is left exactly as native delegation has it.
"""

from __future__ import annotations

from typing import Any, Iterable

MANAGEMENT_JOB_TYPE = "tgg_management"
MANAGEMENT_READONLY_CHILD_TOOLSET = "tgg-management-readonly-investigation"
MANAGEMENT_EXPLICIT_EDIT_CHILD_TOOLSET = "tgg-management-explicit-case-edit"
# The only toolsets a Management delegation child may ever receive.
MANAGEMENT_CHILD_TOOLSETS = (
    MANAGEMENT_READONLY_CHILD_TOOLSET,
    MANAGEMENT_EXPLICIT_EDIT_CHILD_TOOLSET,
)

# Marks an agent whose ``enabled_toolsets`` the PA layer replaced, so a cached
# gateway agent that later serves another route gets its own list back.
_SCOPED_MARKER = "_pa_delegation_child_scope"


def delegation_child_toolsets(
    pa_context: Any,
    parent_enabled_toolsets: Iterable[str] | None,
) -> list[str] | None:
    """Return the only toolsets a delegated child may receive on this route.

    ``None`` means the PA layer sets no child scope (not the Management
    route).  On the Management route the answer is those of the two allowed
    child toolsets that the parent itself has; when it has neither, an empty
    list, which gives a child no tools rather than the parent's.
    """
    if pa_context is None:
        return None
    if str(getattr(pa_context, "job_type", "") or "").strip() != MANAGEMENT_JOB_TYPE:
        return None
    if parent_enabled_toolsets is None:
        # The parent loaded every toolset, so both allowed ones are included.
        return list(MANAGEMENT_CHILD_TOOLSETS)
    parent = {str(name).strip() for name in parent_enabled_toolsets}
    return [name for name in MANAGEMENT_CHILD_TOOLSETS if name in parent]


def scope_delegation_children(
    agent: Any,
    parent_enabled_toolsets: list[str] | None,
    pa_context: Any,
) -> None:
    """Bound what native delegation may hand a child of ``agent``.

    ``parent_enabled_toolsets`` is the list the agent was constructed with.
    Call this after construction, and on every turn for a cached agent.
    """
    scope = delegation_child_toolsets(pa_context, parent_enabled_toolsets)
    state = getattr(agent, "__dict__", {})
    if scope is None:
        if state.get(_SCOPED_MARKER):
            agent.enabled_toolsets = parent_enabled_toolsets
            setattr(agent, _SCOPED_MARKER, False)
        return
    agent.enabled_toolsets = scope
    setattr(agent, _SCOPED_MARKER, True)
