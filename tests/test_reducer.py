from datetime import timedelta

from agentbus.events import make_event
from agentbus.models import (
    REVIEWER_CONTROL_APPLIED,
    REVIEWER_CONTROL_REQUESTED,
    REVIEWER_SUPERVISION_CLAIMED,
    RUN_PAUSED,
    TASK_CLAIMED,
    TASK_CREATED,
    TASK_STARTED,
    Actor,
    format_ts,
    utc_now,
)
from agentbus.reducer import get_top_control, list_supervisable_runs, reduce_events


def test_supervision_exclusivity_and_control_state() -> None:
    now = utc_now()
    actor_user = Actor(type="user", id="u1")
    actor_exec = Actor(type="agent", id="exec1", backend="codex")

    events = [
        make_event(
            kind=TASK_CREATED,
            actor=actor_user,
            task_id="t1",
            chain_id="c1",
            data={
                "prompt": "do thing",
                "role_target": "executor",
                "stage": "execution",
                "priority": 100,
                "quality_gate": {"acceptance_criteria": [], "required_checks": [], "review_mode": "hard"},
                "budgets": {"max_handoffs": 8, "max_reworks": 2, "max_failures": 3},
                "targets": {"backends": ["codex"], "agent_ids": []},
                "attempt": 1,
            },
        ),
        make_event(
            kind=TASK_CLAIMED,
            actor=actor_exec,
            task_id="t1",
            chain_id="c1",
            run_id="run1",
            data={"agent_id": "exec1", "backend": "codex", "lease_expires_at": format_ts(now + timedelta(seconds=30))},
        ),
        make_event(
            kind=TASK_STARTED,
            actor=actor_exec,
            task_id="t1",
            chain_id="c1",
            run_id="run1",
            data={"executor_agent_id": "exec1", "backend": "codex"},
        ),
        make_event(
            kind=REVIEWER_SUPERVISION_CLAIMED,
            actor=Actor(type="agent", id="rev1", backend="claude"),
            task_id="t1",
            chain_id="c1",
            run_id="run1",
            data={"reviewer_agent_id": "rev1", "lease_expires_at": format_ts(now + timedelta(seconds=60))},
        ),
        make_event(
            kind=REVIEWER_CONTROL_REQUESTED,
            actor=Actor(type="agent", id="rev1", backend="claude"),
            task_id="t1",
            chain_id="c1",
            run_id="run1",
            data={"action": "pause", "message": "hold", "severity": 3, "source": "agent", "ts_request": format_ts(now)},
        ),
        make_event(
            kind=REVIEWER_CONTROL_REQUESTED,
            actor=Actor(type="agent", id="rev1", backend="claude"),
            task_id="t1",
            chain_id="c1",
            run_id="run1",
            data={
                "action": "nudge",
                "message": "adjust",
                "severity": 1,
                "source": "agent",
                "ts_request": format_ts(now + timedelta(seconds=1)),
            },
        ),
        make_event(
            kind=REVIEWER_CONTROL_APPLIED,
            actor=actor_exec,
            task_id="t1",
            chain_id="c1",
            run_id="run1",
            data={"chosen_event_id": "will-not-match"},
        ),
        make_event(
            kind=RUN_PAUSED,
            actor=actor_exec,
            task_id="t1",
            chain_id="c1",
            run_id="run1",
            data={"reason": "manual"},
        ),
    ]

    state = reduce_events(events)
    run = state.runs["run1"]
    assert run.reviewer_agent_id == "rev1"
    assert run.status == "paused"

    supervisable_for_other = list_supervisable_runs(state, now=now, reviewer_agent_id="rev2")
    assert not supervisable_for_other

    top = get_top_control(state, "run1")
    assert top is not None
    assert top.action == "pause"
