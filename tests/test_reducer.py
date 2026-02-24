from datetime import timedelta

from agentbus.events import make_event
from agentbus.models import (
    REVIEW_PASSED,
    REVIEWER_CONTROL_APPLIED,
    REVIEWER_CONTROL_REJECTED,
    REVIEWER_CONTROL_REQUESTED,
    REVIEWER_SUPERVISION_CLAIMED,
    REVIEWER_SUPERVISION_HEARTBEAT,
    RUN_PAUSED,
    TASK_CLAIMED,
    TASK_COMPLETED,
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


def test_rejected_control_is_consumed() -> None:
    now = utc_now()
    actor = Actor(type="agent", id="rev1", backend="claude")
    events = [
        {
            "v": 1,
            "event_id": "bad-control",
            "ts": format_ts(now),
            "kind": REVIEWER_CONTROL_REQUESTED,
            "actor": actor.to_dict(),
            "task_id": "t1",
            "chain_id": "c1",
            "run_id": "run1",
            "data": {
                "action": "stop",
                "message": "bad reviewer",
                "severity": 4,
                "source": "agent",
                "ts_request": format_ts(now),
            },
        },
        {
            "v": 1,
            "event_id": "reject-bad",
            "ts": format_ts(now + timedelta(seconds=1)),
            "kind": REVIEWER_CONTROL_REJECTED,
            "actor": {"type": "agent", "id": "exec1", "backend": "codex"},
            "task_id": "t1",
            "chain_id": "c1",
            "run_id": "run1",
            "data": {"reason": "non-owner", "rejected_event_id": "bad-control"},
        },
        {
            "v": 1,
            "event_id": "good-control",
            "ts": format_ts(now + timedelta(seconds=2)),
            "kind": REVIEWER_CONTROL_REQUESTED,
            "actor": actor.to_dict(),
            "task_id": "t1",
            "chain_id": "c1",
            "run_id": "run1",
            "data": {
                "action": "pause",
                "message": "valid",
                "severity": 3,
                "source": "agent",
                "ts_request": format_ts(now + timedelta(seconds=2)),
            },
        },
    ]
    state = reduce_events(events)
    top = get_top_control(state, "run1")
    assert top is not None
    assert top.event_id == "good-control"


def test_non_owner_supervision_heartbeat_is_ignored() -> None:
    now = utc_now()
    initial_lease = now + timedelta(seconds=60)
    forged_lease = now + timedelta(seconds=300)
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
            data={"reviewer_agent_id": "rev1", "lease_expires_at": format_ts(initial_lease)},
        ),
        make_event(
            kind=REVIEWER_SUPERVISION_HEARTBEAT,
            actor=Actor(type="agent", id="rev2", backend="claude"),
            task_id="t1",
            chain_id="c1",
            run_id="run1",
            data={"reviewer_agent_id": "rev2", "lease_expires_at": format_ts(forged_lease)},
        ),
    ]

    state = reduce_events(events)
    run = state.runs["run1"]
    assert run.reviewer_agent_id == "rev1"
    assert format_ts(run.reviewer_lease_expires_at) == format_ts(initial_lease)


def test_terminal_unreviewed_run_is_supervisable_after_lease_expiry() -> None:
    now = utc_now()
    actor_user = Actor(type="user", id="u1")
    actor_exec = Actor(type="agent", id="exec1", backend="codex")
    stale_lease = now - timedelta(seconds=10)

    events = [
        make_event(
            kind=TASK_CREATED,
            actor=actor_user,
            task_id="t2",
            chain_id="c2",
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
            task_id="t2",
            chain_id="c2",
            run_id="run2",
            data={"agent_id": "exec1", "backend": "codex", "lease_expires_at": format_ts(now + timedelta(seconds=30))},
        ),
        make_event(
            kind=TASK_STARTED,
            actor=actor_exec,
            task_id="t2",
            chain_id="c2",
            run_id="run2",
            data={"executor_agent_id": "exec1", "backend": "codex"},
        ),
        make_event(
            kind=REVIEWER_SUPERVISION_CLAIMED,
            actor=Actor(type="agent", id="rev-stale", backend="claude"),
            task_id="t2",
            chain_id="c2",
            run_id="run2",
            data={"reviewer_agent_id": "rev-stale", "lease_expires_at": format_ts(stale_lease)},
        ),
        make_event(
            kind=TASK_COMPLETED,
            actor=actor_exec,
            task_id="t2",
            chain_id="c2",
            run_id="run2",
            data={"exit_code": 0, "duration_ms": 100, "stdout": "", "stderr": "", "final_output": "", "backend": "codex"},
        ),
    ]

    state = reduce_events(events)
    candidates = list_supervisable_runs(state, now=now, reviewer_agent_id="rev-new")
    assert any(run.run_id == "run2" for run in candidates)


def test_terminal_run_is_not_supervisable_after_review_outcome() -> None:
    now = utc_now()
    actor_user = Actor(type="user", id="u1")
    actor_exec = Actor(type="agent", id="exec1", backend="codex")

    events = [
        make_event(
            kind=TASK_CREATED,
            actor=actor_user,
            task_id="t3",
            chain_id="c3",
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
            task_id="t3",
            chain_id="c3",
            run_id="run3",
            data={"agent_id": "exec1", "backend": "codex", "lease_expires_at": format_ts(now + timedelta(seconds=30))},
        ),
        make_event(
            kind=TASK_STARTED,
            actor=actor_exec,
            task_id="t3",
            chain_id="c3",
            run_id="run3",
            data={"executor_agent_id": "exec1", "backend": "codex"},
        ),
        make_event(
            kind=TASK_COMPLETED,
            actor=actor_exec,
            task_id="t3",
            chain_id="c3",
            run_id="run3",
            data={"exit_code": 0, "duration_ms": 100, "stdout": "", "stderr": "", "final_output": "", "backend": "codex"},
        ),
        make_event(
            kind=REVIEW_PASSED,
            actor=Actor(type="agent", id="rev1", backend="claude"),
            task_id="t3",
            chain_id="c3",
            run_id="run3",
            data={"reason": "gate passed"},
        ),
    ]

    state = reduce_events(events)
    run = state.runs["run3"]
    assert run.review_outcome == "passed"

    candidates = list_supervisable_runs(state, now=now, reviewer_agent_id="rev2")
    assert all(candidate.run_id != "run3" for candidate in candidates)
