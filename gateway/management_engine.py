"""Engine for the Management chats, when it differs from the gateway engine.

The gateway runs on one engine (``model.provider`` / ``model.default`` and
``agent.reasoning_effort``).  A deployment may give the chats bound to the
``tgg_management`` selector their own model and reasoning effort with one
block in ``config.yaml``::

    pa:
      management_engine:
        model: gpt-6.2-sol
        reasoning_effort: medium

The provider is always the gateway's own.  With no block, Management runs on
the gateway engine and nothing here changes behaviour.  The gateway (which
runs the turn) and the durable consumer (which checks the turn) both read the
block through this module, so they cannot disagree about what Management runs.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from hermes_constants import VALID_REASONING_EFFORTS

MANAGEMENT_JOB_TYPE = "tgg_management"
_KEYS = frozenset({"model", "reasoning_effort"})


class ManagementEngineConfigError(ValueError):
    """``pa.management_engine`` is present but not usable."""


@dataclass(frozen=True)
class ManagementEngine:
    model: str
    reasoning_effort: str


def parse_management_engine(config: Mapping[str, Any] | None) -> ManagementEngine | None:
    """Return the Management engine declared in a loaded config, or None."""
    pa = config.get("pa") if isinstance(config, Mapping) else None
    block = pa.get("management_engine") if isinstance(pa, Mapping) else None
    if block is None:
        return None
    if not isinstance(block, Mapping):
        raise ManagementEngineConfigError("pa.management_engine must be a mapping")
    unknown = sorted(str(key) for key in set(block) - _KEYS)
    if unknown:
        raise ManagementEngineConfigError(
            f"pa.management_engine has unknown keys: {', '.join(unknown)}"
        )
    model = block.get("model")
    effort = block.get("reasoning_effort")
    if not isinstance(model, str) or not model.strip():
        raise ManagementEngineConfigError("pa.management_engine.model is required")
    if effort not in VALID_REASONING_EFFORTS:
        raise ManagementEngineConfigError(
            "pa.management_engine.reasoning_effort must be one of "
            + ", ".join(VALID_REASONING_EFFORTS)
        )
    return ManagementEngine(model=model.strip(), reasoning_effort=effort)


def is_management_job(pa_context: Any) -> bool:
    return getattr(pa_context, "job_type", None) == MANAGEMENT_JOB_TYPE
