from datetime import timedelta

from agentbus.control import choose_control
from agentbus.models import ControlRequestView, utc_now


def test_choose_control_by_severity_then_latest() -> None:
    now = utc_now()
    requests = [
        ControlRequestView(
            event_id="a",
            run_id="run1",
            action="nudge",
            message="nudge",
            severity=1,
            source="agent",
            actor_id="r1",
            requested_at=now,
        ),
        ControlRequestView(
            event_id="b",
            run_id="run1",
            action="pause",
            message="pause",
            severity=3,
            source="agent",
            actor_id="r1",
            requested_at=now - timedelta(seconds=2),
        ),
        ControlRequestView(
            event_id="c",
            run_id="run1",
            action="pause",
            message="pause2",
            severity=3,
            source="agent",
            actor_id="r1",
            requested_at=now,
        ),
    ]

    chosen = choose_control(requests).chosen
    assert chosen is not None
    assert chosen.event_id == "c"
