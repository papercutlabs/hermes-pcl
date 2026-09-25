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

PA-203 round 2 (Teren, 25 Sep): on this route the *kind* of helper fixes its
tools; the model does not choose them.  There are two kinds, read-only
investigation (the default) and explicit single-case edit, selected by the
delegation ``profile``.  Any toolset names the model passes are ignored.  A
helper whose kind's toolset the parent lacks, or whose tools would be empty or
wider than the parent's, is refused before it starts: no helper starts with
no tools, and none gets more than its parent.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

MANAGEMENT_JOB_TYPE = "tgg_management"
MANAGEMENT_READONLY_CHILD_TOOLSET = "tgg-management-readonly-investigation"
MANAGEMENT_EXPLICIT_EDIT_CHILD_TOOLSET = "tgg-management-explicit-case-edit"
# The only toolsets a Management delegation child may ever receive.
MANAGEMENT_CHILD_TOOLSETS = (
    MANAGEMENT_READONLY_CHILD_TOOLSET,
    MANAGEMENT_EXPLICIT_EDIT_CHILD_TOOLSET,
)

# The two helper kinds and the one toolset each receives.  The model selects a
# kind through the delegation ``profile``; the aliases are the names earlier
# guidance and the per-case plugin already use.
HELPER_KIND_READONLY = "readonly-investigation"
HELPER_KIND_EXPLICIT_EDIT = "explicit-case-edit"
HELPER_KIND_TOOLSETS = {
    HELPER_KIND_READONLY: MANAGEMENT_READONLY_CHILD_TOOLSET,
    HELPER_KIND_EXPLICIT_EDIT: MANAGEMENT_EXPLICIT_EDIT_CHILD_TOOLSET,
}
_HELPER_KIND_ALIASES = {
    HELPER_KIND_READONLY: HELPER_KIND_READONLY,
    MANAGEMENT_READONLY_CHILD_TOOLSET: HELPER_KIND_READONLY,
    HELPER_KIND_EXPLICIT_EDIT: HELPER_KIND_EXPLICIT_EDIT,
    MANAGEMENT_EXPLICIT_EDIT_CHILD_TOOLSET: HELPER_KIND_EXPLICIT_EDIT,
    # tgg-per-case-whatsapp's investigator profile (the prepare -> delegate spec).
    "whatsapp_case_investigator": HELPER_KIND_EXPLICIT_EDIT,
}


class HelperScopeError(ValueError):
    """A helper cannot be started with a fixed, non-empty, parent-bounded scope."""


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


def is_scoped(agent: Any) -> bool:
    """True when the PA layer fixed this agent's delegation children."""
    return bool(getattr(agent, "__dict__", {}).get(_SCOPED_MARKER))


def helper_kind(profile: Any) -> str:
    """Resolve a delegation profile to one of the two helper kinds.

    No profile means read-only investigation.  Any other unknown name is
    refused, so a typo cannot silently widen or empty a helper.
    """
    name = str(profile or "").strip()
    if not name:
        return HELPER_KIND_READONLY
    kind = _HELPER_KIND_ALIASES.get(name)
    if kind is None:
        raise HelperScopeError(
            f"Unknown helper kind {name!r}. On this route a helper is either "
            f"profile={HELPER_KIND_READONLY!r} (the default) or "
            f"profile={HELPER_KIND_EXPLICIT_EDIT!r} for one explicitly requested case edit; "
            "toolsets are fixed by the kind."
        )
    return kind


def fixed_helper_toolsets(parent_agent: Any, profile: Any) -> list[str] | None:
    """The only toolsets a helper of this parent may receive, or ``None``.

    ``None`` means the parent is not on the scoped Management route and native
    delegation applies unchanged.  Otherwise the answer is exactly one toolset,
    chosen by the helper kind; it must be one the parent itself has.
    """
    if not is_scoped(parent_agent):
        return None
    kind = helper_kind(profile)
    toolset = HELPER_KIND_TOOLSETS[kind]
    available = list(getattr(parent_agent, "enabled_toolsets", None) or [])
    if toolset not in available:
        raise HelperScopeError(
            f"A {kind} helper needs the {toolset} toolset, which this conversation "
            "does not have, so no helper was started."
        )
    return [toolset]


def check_helper_tools(
    parent_agent: Any,
    helper_tools: Iterable[str],
    parent_tool_names: Iterable[str] | None = None,
    *,
    check_width: bool = True,
) -> set[str]:
    """Refuse a scoped helper whose tools are empty or wider than its parent's.

    ``helper_tools`` are the helper's tool names.  The parent's are read from
    its ``valid_tool_names`` unless given.  Returns the helper's tool names.
    """
    tools = {str(name) for name in helper_tools}
    if not tools:
        raise HelperScopeError(
            "The helper would start with no tools, so it was not started."
        )
    if not check_width:
        return tools
    parent_tools = parent_tool_names
    if parent_tools is None:
        candidate = getattr(parent_agent, "valid_tool_names", None)
        parent_tools = candidate if isinstance(candidate, (set, frozenset, list, tuple)) else None
    if parent_tools is not None:
        extra = tools - {str(name) for name in parent_tools}
        if extra:
            raise HelperScopeError(
                "The helper would receive tools its parent does not have "
                f"({', '.join(sorted(extra))}), so it was not started."
            )
    return tools


# Teren (25 Sep): helpers check, Christopher builds.  A helper has no sandbox
# or file tools, so a helper asked to build or edit a file can only fail
# (r2e replay s3).  The refusal happens before it starts, with the next step.
FILE_BUILDING_REFUSAL = (
    "Helpers only gather evidence; build the file yourself with the python "
    "sandbox, then send it. No helper was started."
)
_FILE_NOUN = (
    r"(?:work\s*books?|spread\s*sheets?|excel(?:\s+files?)?|xlsx|xls|csv|docx|files?|sheets?|"
    r"attachments?|[\w\-.()]+\.(?:xlsx|xls|csv|docx))\b"
)
_WORD = r"\s+[\w/\-.()']+"
_FILE_BUILDING = re.compile(
    # Build verbs, with up to five words before the file noun.
    rf"\b(?:build|create|produce|generate|make|prepare|edit|modify|write|save|export|"
    rf"populate|fill\s+in|fill)\b(?:{_WORD}){{0,5}}?\s+{_FILE_NOUN}"
    # Verbs that are often nouns, only right before the file noun.
    rf"|\b(?:update|return|send|attach)\b(?:{_WORD}){{0,2}}?\s+{_FILE_NOUN}",
    re.IGNORECASE,
)


def check_not_file_building(goal: Any) -> None:
    """Refuse a helper whose own task asks it to build, edit or send a file."""
    if _FILE_BUILDING.search(str(goal or "")):
        raise HelperScopeError(FILE_BUILDING_REFUSAL)
