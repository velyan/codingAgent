from __future__ import annotations

from dataclasses import dataclass

from agentbus.models import CONTROL_SEVERITY, ControlRequestView


@dataclass
class ArbitrationResult:
    chosen: ControlRequestView | None


def choose_control(requests: list[ControlRequestView]) -> ArbitrationResult:
    if not requests:
        return ArbitrationResult(chosen=None)

    sorted_requests = sorted(
        requests,
        key=lambda req: (req.severity, req.requested_at, req.event_id),
        reverse=True,
    )
    return ArbitrationResult(chosen=sorted_requests[0])


def control_severity(action: str) -> int:
    return CONTROL_SEVERITY.get(action, -1)
