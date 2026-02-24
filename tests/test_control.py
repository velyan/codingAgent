from datetime import timedelta
from pathlib import Path

import agentbus.runner as runner
from agentbus.events import make_event
from agentbus.control import choose_control
from agentbus.models import TASK_CLAIMED, TASK_COMPLETED, TASK_CREATED, TASK_STARTED, Actor, ControlRequestView, RunConfig, RunView, format_ts, utc_now
from agentbus.reducer import reduce_events
from agentbus.store import JsonlEventStore


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


def test_reviewer_owns_run_requires_exact_owner_match() -> None:
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
        reviewer_agent_id="rev-owner",
    )

    assert runner._reviewer_owns_run(run=run_view, reviewer_agent_id="rev-owner")
    assert not runner._reviewer_owns_run(run=run_view, reviewer_agent_id="rev-other")

    run_view.reviewer_agent_id = None
    assert not runner._reviewer_owns_run(run=run_view, reviewer_agent_id="rev-owner")
    assert not runner._reviewer_owns_run(run=None, reviewer_agent_id="rev-owner")


def test_claim_supervision_allows_terminal_unreviewed_run(tmp_path: Path) -> None:
    now = utc_now()
    log_file = tmp_path / "bus.jsonl"
    store = JsonlEventStore(str(log_file))
    actor_user = Actor(type="user", id="u1")
    actor_exec = Actor(type="agent", id="exec1", backend="codex")
    task_id = "task-1"
    chain_id = "chain-1"
    run_id = "run-1"

    store.append_many(
        [
            make_event(
                kind=TASK_CREATED,
                actor=actor_user,
                task_id=task_id,
                chain_id=chain_id,
                data={
                    "prompt": "execute",
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
                task_id=task_id,
                chain_id=chain_id,
                run_id=run_id,
                data={
                    "agent_id": "exec1",
                    "backend": "codex",
                    "lease_expires_at": format_ts(now + timedelta(seconds=30)),
                },
            ),
            make_event(
                kind=TASK_STARTED,
                actor=actor_exec,
                task_id=task_id,
                chain_id=chain_id,
                run_id=run_id,
                data={"executor_agent_id": "exec1", "backend": "codex"},
            ),
            make_event(
                kind=TASK_COMPLETED,
                actor=actor_exec,
                task_id=task_id,
                chain_id=chain_id,
                run_id=run_id,
                data={
                    "exit_code": 0,
                    "duration_ms": 120,
                    "stdout": "",
                    "stderr": "",
                    "final_output": "",
                    "backend": "codex",
                },
            ),
        ]
    )

    reviewer_config = RunConfig(
        log_file=str(log_file),
        agent_id="rev2",
        backend="claude",
        role="reviewer",
        cwd=str(tmp_path),
    )
    claimed = runner._claim_supervision(
        store=store,
        config=reviewer_config,
        run_id=run_id,
        task_id=task_id,
        chain_id=chain_id,
    )
    assert claimed

    state = reduce_events(store.read_all())
    run = state.runs[run_id]
    assert run.reviewer_agent_id == "rev2"
