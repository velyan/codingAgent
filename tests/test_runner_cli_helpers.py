from pathlib import Path

from agentbus.models import Budgets
from agentbus.reducer import reduce_events
from agentbus.runner import post_objective, post_task, post_control
from agentbus.store import JsonlEventStore


def test_post_objective_creates_chain_and_planner_task(tmp_path: Path) -> None:
    log_file = tmp_path / "bus.jsonl"
    chain_id = post_objective(
        log_file=str(log_file),
        objective="Ship feature",
        done_when="All checks pass",
        priority=100,
        preferred_backends=["codex"],
        budgets=Budgets(max_handoffs=8, max_reworks=2, max_failures=3),
    )

    state = reduce_events(JsonlEventStore(str(log_file)).read_all())
    assert chain_id in state.chains
    tasks = [task for task in state.tasks.values() if task.chain_id == chain_id]
    assert tasks
    assert tasks[0].role_target == "planner"


def test_post_task_and_control_roundtrip(tmp_path: Path) -> None:
    log_file = tmp_path / "bus.jsonl"
    chain_id, _task_id = post_task(
        log_file=str(log_file),
        prompt="do work",
        target_role="executor",
        target_backends=["codex"],
        chain_id=None,
        priority=100,
        budgets=Budgets(),
    )

    store = JsonlEventStore(str(log_file))
    state = reduce_events(store.read_all())
    task = next(iter(state.tasks.values()))

    claim = {
        "v": 1,
        "event_id": "e-claim",
        "ts": "2026-01-01T00:00:00Z",
        "kind": "task.claimed",
        "actor": {"type": "agent", "id": "exec1", "backend": "codex"},
        "task_id": task.task_id,
        "chain_id": chain_id,
        "run_id": "run1",
        "data": {"agent_id": "exec1", "backend": "codex", "lease_expires_at": "2026-01-01T00:02:00Z"},
    }
    started = {
        "v": 1,
        "event_id": "e-start",
        "ts": "2026-01-01T00:00:01Z",
        "kind": "task.started",
        "actor": {"type": "agent", "id": "exec1", "backend": "codex"},
        "task_id": task.task_id,
        "chain_id": chain_id,
        "run_id": "run1",
        "data": {"executor_agent_id": "exec1", "backend": "codex"},
    }
    store.append_many([claim, started])

    post_control(
        log_file=str(log_file),
        run_id="run1",
        action="pause",
        message="hold",
    )

    state2 = reduce_events(store.read_all())
    assert "run1" in state2.pending_controls
