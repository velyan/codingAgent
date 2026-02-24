from __future__ import annotations

from typing import Any
from uuid import uuid4

from agentbus.models import Actor, format_ts, utc_now


def make_event(
    *,
    kind: str,
    actor: Actor,
    task_id: str | None = None,
    chain_id: str | None = None,
    run_id: str | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "v": 1,
        "event_id": str(uuid4()),
        "ts": format_ts(utc_now()),
        "kind": kind,
        "actor": actor.to_dict(),
        "task_id": task_id,
        "chain_id": chain_id,
        "run_id": run_id,
        "data": data or {},
    }
