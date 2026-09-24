"""PA scope for native delegation children on the TGG Management route.

Native delegation builds every child, synchronous, batch or background, in
``tools/delegate_tool.py::_build_child_agent`` from the parent agent's
``enabled_toolsets`` attribute.  A child whose parent omits ``toolsets``
inherits that list, and an explicit request is intersected with it.  The
parent's own tool schemas are fixed when the parent is constructed, and no
other runtime reader uses the attribute after construction.

On the Management route (interactive Management turns and scheduled
``tgg_management`` jobs) the PA layer therefore sets that attribute to the
capability's read-only investigation toolset.  A child then receives that
toolset when the parent names none, and only the part of an explicit request
that falls inside it otherwise.  A child on this route can never receive a
business-write tool.  Background mode is not gated here; it builds its child
through the same function and gets the same scope.

Any other route is left exactly as native delegation has it.
"""

from __future__ import annotations

from typing import Any, Iterable

MANAGEMENT_JOB_TYPE = "tgg_management"
MANAGEMENT_READONLY_CHILD_TOOLSET = "tgg-management-readonly-investigation"

# Marks an agent whose ``enabled_toolsets`` the PA layer replaced, so a cached
# gateway agent that later serves another route gets its own list back.
_SCOPED_MARKER = "_pa_delegation_child_scope"


def delegation_child_toolsets(
    pa_context: Any,
    parent_enabled_toolsets: Iterable[str] | None,
) -> list[str] | None:
    """Return the only toolsets a delegated child may receive on this route.

    ``None`` means the PA layer sets no child scope (not the Management
    route).  On the Management route the answer is the read-only
    investigation toolset when the parent itself has it, and otherwise an
    empty list, which gives a child no tools rather than the parent's.
    """
    if pa_context is None:
        return None
    if str(getattr(pa_context, "job_type", "") or "").strip() != MANAGEMENT_JOB_TYPE:
        return None
    if parent_enabled_toolsets is None:
        # The parent loaded every toolset, so the read-only one is included.
        return [MANAGEMENT_READONLY_CHILD_TOOLSET]
    parent = {str(name).strip() for name in parent_enabled_toolsets}
    if MANAGEMENT_READONLY_CHILD_TOOLSET in parent:
        return [MANAGEMENT_READONLY_CHILD_TOOLSET]
    return []


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
