from datetime import timedelta

import agentbus.runner as runner
from agentbus.control import choose_control
from agentbus.models import ControlRequestView, RunView, utc_now


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


def test_authorize_control_requires_claimed_reviewer_for_agent_controls() -> None:
    now = utc_now()
    run_view = RunView(
        run_id="run-1",
        task_id="task-1",
        chain_id="chain-1",
        executor_agent_id="exec-1",
        backend="codex",
        status="running",
        started_at=now,
        updated_at=now,
    )

    agent_control = ControlRequestView(
        event_id="c1",
        run_id="run-1",
        action="pause",
        message="hold",
        severity=3,
        source="agent",
        actor_id="rev-1",
        requested_at=now,
    )
    human_control = ControlRequestView(
        event_id="c2",
        run_id="run-1",
        action="pause",
        message="hold",
        severity=3,
        source="human",
        actor_id="human",
        requested_at=now,
    )

    authorized, reason = runner._authorize_control(run_view=run_view, control=agent_control)
    assert not authorized
    assert "active reviewer supervision" in reason

    authorized_human, _ = runner._authorize_control(run_view=run_view, control=human_control)
    assert authorized_human

    run_view.reviewer_agent_id = "rev-owner"
    authorized_other, reason_other = runner._authorize_control(run_view=run_view, control=agent_control)
    assert not authorized_other
    assert "non-owner reviewer" in reason_other

    agent_control.actor_id = "rev-owner"
    authorized_owner, _ = runner._authorize_control(run_view=run_view, control=agent_control)
    assert authorized_owner
