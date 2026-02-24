from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import agentbus.runner as runner
from agentbus.events import make_event
from agentbus.models import (
    CHAIN_COMPLETED,
    CONTROL_SEVERITY,
    ESCALATION_RAISED,
    GUARDRAIL_BREACHED,
    OBJECTIVE_CREATED,
    REVIEWER_CONTROL_REQUESTED,
    REVIEWER_SUPERVISION_CLAIMED,
    RUN_PAUSED,
    RUN_RESTARTED,
    TASK_CREATED,
    TASK_FAILED,
    TASK_STARTED,
    Actor,
    AgentState,
    Budgets,
    QualityGate,
    RunConfig,
    TaskTargets,
    TaskView,
    format_ts,
    utc_now,
)
from agentbus.reducer import list_claimable_tasks, reduce_events, summarize_state
from agentbus.store import JsonlEventStore
from agentbus.streaming import StreamRunResult


class _FakeAdapter:
    def build_command(self, *, prompt, config, agent_state, resume):
        del prompt, config, agent_state, resume

        class _Cmd:
            argv = ["fake-backend", "run"]
            env = None

        return _Cmd()

    def after_run(self, *, stdout, stderr, agent_state):
        del stdout, stderr, agent_state

    def extract_final_output(self, stdout, stderr):
        return stdout or stderr


class _FakeProc:
    def __init__(self) -> None:
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode

    def send_signal(self, _sig) -> None:
        self.returncode = 130

    def terminate(self) -> None:
        self.returncode = 143


class _TickClock:
    def __init__(self, *, start: float = 0.0, step: float = 1.0) -> None:
        self._value = start
        self._step = step

    def __call__(self) -> float:
        self._value += self._step
        return self._value


def _sample_task(*, task_id: str = "task-1", chain_id: str = "chain-1", budgets: Budgets | None = None) -> TaskView:
    return TaskView(
        task_id=task_id,
        chain_id=chain_id,
        created_at=utc_now(),
        prompt="execute task",
        role_target="executor",
        stage="execution",
        priority=100,
        quality_gate=QualityGate(),
        budgets=budgets or Budgets(),
        targets=TaskTargets(backends=["codex"], agent_ids=[]),
    )


def _default_run_config(tmp_path: Path, *, escalation_file: str | None = None) -> RunConfig:
    return RunConfig(
        log_file=str(tmp_path / "bus.jsonl"),
        agent_id="exec-1",
        backend="codex",
        role="executor",
        cwd=str(tmp_path),
        run_timeout_seconds=30,
        pause_timeout_seconds=30,
        max_nudges_per_run=3,
        max_restarts_per_run=6,
        max_identical_failures=3,
        escalation_file=escalation_file,
    )


def _seed_task_created(store: JsonlEventStore, task: TaskView) -> None:
    store.append(
        make_event(
            kind=TASK_CREATED,
            actor=Actor(type="user", id="user"),
            task_id=task.task_id,
            chain_id=task.chain_id,
            data={
                "prompt": task.prompt,
                "role_target": task.role_target,
                "stage": task.stage,
                "priority": task.priority,
                "quality_gate": task.quality_gate.to_dict(),
                "budgets": task.budgets.to_dict(),
                "targets": task.targets.to_dict(),
                "attempt": task.attempt,
            },
        )
    )


def test_execute_worker_task_escalates_on_run_timeout(monkeypatch, tmp_path: Path) -> None:
    cfg = _default_run_config(tmp_path)
    cfg.run_timeout_seconds = 1
    store = JsonlEventStore(cfg.log_file)
    task = _sample_task()
    _seed_task_created(store, task)

    monkeypatch.setattr(runner, "get_adapter", lambda _backend: _FakeAdapter())
    monkeypatch.setattr(runner.time, "monotonic", _TickClock(step=1.0))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    def _fake_stream(**kwargs):
        on_tick = kwargs["on_tick"]
        proc = _FakeProc()
        on_tick(proc)
        return StreamRunResult(stdout="timeout", stderr="", exit_code=130, duration_ms=50)

    monkeypatch.setattr(runner, "run_streaming_subprocess", _fake_stream)

    runner._execute_worker_task(
        store=store,
        config=cfg,
        agent_state=AgentState(),
        task=task,
        run_id="run-timeout",
    )

    events = store.read_all()
    assert any(event["kind"] == GUARDRAIL_BREACHED and event["data"].get("rule") == "run_timeout" for event in events)
    assert any(event["kind"] == TASK_FAILED for event in events)
    assert any(event["kind"] == ESCALATION_RAISED for event in events)


def test_execute_worker_task_escalates_on_pause_timeout(monkeypatch, tmp_path: Path) -> None:
    cfg = _default_run_config(tmp_path)
    cfg.pause_timeout_seconds = 1
    cfg.run_timeout_seconds = 300
    store = JsonlEventStore(cfg.log_file)
    task = _sample_task()
    _seed_task_created(store, task)
    run_id = "run-pause-timeout"

    monkeypatch.setattr(runner, "get_adapter", lambda _backend: _FakeAdapter())
    monkeypatch.setattr(runner.time, "monotonic", _TickClock(step=1.0))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    def _fake_stream(**kwargs):
        on_tick = kwargs["on_tick"]
        proc = _FakeProc()
        store.append(
            make_event(
                kind=REVIEWER_SUPERVISION_CLAIMED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "reviewer_agent_id": "rev-1",
                    "lease_expires_at": format_ts(utc_now() + timedelta(seconds=60)),
                },
            )
        )
        store.append(
            make_event(
                kind=REVIEWER_CONTROL_REQUESTED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "action": "pause",
                    "message": "hold",
                    "severity": CONTROL_SEVERITY["pause"],
                    "source": "agent",
                    "ts_request": format_ts(utc_now()),
                },
            )
        )
        on_tick(proc)
        return StreamRunResult(stdout="", stderr="", exit_code=130, duration_ms=50)

    monkeypatch.setattr(runner, "run_streaming_subprocess", _fake_stream)

    runner._execute_worker_task(
        store=store,
        config=cfg,
        agent_state=AgentState(),
        task=task,
        run_id=run_id,
    )

    events = store.read_all()
    assert any(event["kind"] == GUARDRAIL_BREACHED and event["data"].get("rule") == "pause_timeout" for event in events)
    assert any(event["kind"] == ESCALATION_RAISED for event in events)


def test_execute_worker_task_escalates_on_max_nudges(monkeypatch, tmp_path: Path) -> None:
    cfg = _default_run_config(tmp_path)
    cfg.max_nudges_per_run = 0
    cfg.max_restarts_per_run = 10
    cfg.run_timeout_seconds = 300
    store = JsonlEventStore(cfg.log_file)
    task = _sample_task()
    _seed_task_created(store, task)
    run_id = "run-max-nudges"

    monkeypatch.setattr(runner, "get_adapter", lambda _backend: _FakeAdapter())
    monkeypatch.setattr(runner.time, "monotonic", _TickClock(step=1.0))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    def _fake_stream(**kwargs):
        on_tick = kwargs["on_tick"]
        proc = _FakeProc()
        store.append(
            make_event(
                kind=REVIEWER_SUPERVISION_CLAIMED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "reviewer_agent_id": "rev-1",
                    "lease_expires_at": format_ts(utc_now() + timedelta(seconds=60)),
                },
            )
        )
        store.append(
            make_event(
                kind=REVIEWER_CONTROL_REQUESTED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "action": "nudge",
                    "message": "retry",
                    "severity": CONTROL_SEVERITY["nudge"],
                    "source": "agent",
                    "ts_request": format_ts(utc_now()),
                },
            )
        )
        on_tick(proc)
        return StreamRunResult(stdout="", stderr="", exit_code=130, duration_ms=50)

    monkeypatch.setattr(runner, "run_streaming_subprocess", _fake_stream)

    runner._execute_worker_task(
        store=store,
        config=cfg,
        agent_state=AgentState(),
        task=task,
        run_id=run_id,
    )

    events = store.read_all()
    assert any(event["kind"] == GUARDRAIL_BREACHED and event["data"].get("rule") == "max_nudges" for event in events)
    assert any(event["kind"] == ESCALATION_RAISED for event in events)


def test_execute_worker_task_escalates_on_paused_nudge_limit(monkeypatch, tmp_path: Path) -> None:
    cfg = _default_run_config(tmp_path)
    cfg.max_nudges_per_run = 0
    cfg.pause_timeout_seconds = 60
    cfg.run_timeout_seconds = 300
    store = JsonlEventStore(cfg.log_file)
    task = _sample_task()
    _seed_task_created(store, task)
    run_id = "run-paused-nudge-limit"

    monkeypatch.setattr(runner, "get_adapter", lambda _backend: _FakeAdapter())
    monkeypatch.setattr(runner.time, "monotonic", _TickClock(step=1.0))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    def _fake_stream(**kwargs):
        on_tick = kwargs["on_tick"]
        proc = _FakeProc()
        store.append(
            make_event(
                kind=REVIEWER_SUPERVISION_CLAIMED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "reviewer_agent_id": "rev-1",
                    "lease_expires_at": format_ts(utc_now() + timedelta(seconds=60)),
                },
            )
        )
        store.append(
            make_event(
                kind=REVIEWER_CONTROL_REQUESTED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "action": "pause",
                    "message": "hold",
                    "severity": CONTROL_SEVERITY["pause"],
                    "source": "agent",
                    "ts_request": format_ts(utc_now()),
                },
            )
        )
        store.append(
            make_event(
                kind=REVIEWER_CONTROL_REQUESTED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "action": "nudge",
                    "message": "adjust while paused",
                    "severity": CONTROL_SEVERITY["nudge"],
                    "source": "agent",
                    "ts_request": format_ts(utc_now()),
                },
            )
        )
        on_tick(proc)
        return StreamRunResult(stdout="", stderr="", exit_code=130, duration_ms=50)

    monkeypatch.setattr(runner, "run_streaming_subprocess", _fake_stream)

    runner._execute_worker_task(
        store=store,
        config=cfg,
        agent_state=AgentState(),
        task=task,
        run_id=run_id,
    )

    events = store.read_all()
    assert any(event["kind"] == GUARDRAIL_BREACHED and event["data"].get("rule") == "max_nudges" for event in events)
    assert any(event["kind"] == ESCALATION_RAISED for event in events)


def test_execute_worker_task_escalates_on_max_restarts(monkeypatch, tmp_path: Path) -> None:
    cfg = _default_run_config(tmp_path)
    cfg.max_nudges_per_run = 10
    cfg.max_restarts_per_run = 0
    cfg.run_timeout_seconds = 300
    store = JsonlEventStore(cfg.log_file)
    task = _sample_task()
    _seed_task_created(store, task)
    run_id = "run-max-restarts"

    monkeypatch.setattr(runner, "get_adapter", lambda _backend: _FakeAdapter())
    monkeypatch.setattr(runner.time, "monotonic", _TickClock(step=1.0))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    def _fake_stream(**kwargs):
        on_tick = kwargs["on_tick"]
        proc = _FakeProc()
        store.append(
            make_event(
                kind=REVIEWER_SUPERVISION_CLAIMED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "reviewer_agent_id": "rev-1",
                    "lease_expires_at": format_ts(utc_now() + timedelta(seconds=60)),
                },
            )
        )
        store.append(
            make_event(
                kind=REVIEWER_CONTROL_REQUESTED,
                actor=Actor(type="agent", id="rev-1", backend="claude"),
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "action": "nudge",
                    "message": "retry",
                    "severity": CONTROL_SEVERITY["nudge"],
                    "source": "agent",
                    "ts_request": format_ts(utc_now()),
                },
            )
        )
        on_tick(proc)
        return StreamRunResult(stdout="", stderr="", exit_code=130, duration_ms=50)

    monkeypatch.setattr(runner, "run_streaming_subprocess", _fake_stream)

    runner._execute_worker_task(
        store=store,
        config=cfg,
        agent_state=AgentState(),
        task=task,
        run_id=run_id,
    )

    events = store.read_all()
    assert any(event["kind"] == GUARDRAIL_BREACHED and event["data"].get("rule") == "max_restarts" for event in events)
    assert any(event["kind"] == ESCALATION_RAISED for event in events)


def test_apply_actions_rejects_create_task_when_max_handoffs_exceeded(tmp_path: Path) -> None:
    log_file = tmp_path / "bus.jsonl"
    store = JsonlEventStore(str(log_file))
    chain_id = "chain-1"
    root_task_id = "root-task"

    actor_user = Actor(type="user", id="user")
    actor_agent = Actor(type="agent", id="planner-1", backend="codex")
    budgets = Budgets(max_handoffs=1, max_reworks=2, max_failures=3)
    store.append_many(
        [
            make_event(
                kind=OBJECTIVE_CREATED,
                actor=actor_user,
                chain_id=chain_id,
                data={"objective": "obj", "done_when": "done", "preferred_backends": ["codex"]},
            ),
            make_event(
                kind="task.created",
                actor=actor_user,
                task_id=root_task_id,
                chain_id=chain_id,
                data={
                    "prompt": "planner work",
                    "role_target": "planner",
                    "stage": "planning",
                    "priority": 100,
                    "quality_gate": {"acceptance_criteria": [], "required_checks": [], "review_mode": "hard"},
                    "budgets": budgets.to_dict(),
                    "targets": {"backends": ["codex"], "agent_ids": []},
                    "attempt": 1,
                },
            ),
            make_event(
                kind="task.created",
                actor=actor_agent,
                task_id="handoff-1",
                chain_id=chain_id,
                data={
                    "prompt": "already handed off",
                    "role_target": "executor",
                    "stage": "execution",
                    "priority": 100,
                    "quality_gate": {"acceptance_criteria": [], "required_checks": [], "review_mode": "hard"},
                    "budgets": budgets.to_dict(),
                    "targets": {"backends": ["codex"], "agent_ids": []},
                    "attempt": 1,
                },
            ),
        ]
    )

    state = reduce_events(store.read_all())
    task = state.tasks[root_task_id]
    cfg = _default_run_config(tmp_path)
    cfg.role = "planner"

    runner._apply_actions(
        store=store,
        config=cfg,
        task=task,
        run_id="run-handoff",
        final_output=(
            '{"agentbus_actions":['
            '{"type":"create_task","target_role":"executor","prompt":"new task"},'
            '{"type":"mark_objective_complete","chain_id":"chain-1"}'
            "]}"
        ),
    )

    events = store.read_all()
    assert any(
        event["kind"] == "action.rejected" and event.get("data", {}).get("reason") == "max_handoffs exceeded"
        for event in events
    )
    assert any(event["kind"] == GUARDRAIL_BREACHED and event["data"].get("rule") == "max_handoffs" for event in events)
    assert any(event["kind"] == ESCALATION_RAISED for event in events)
    assert not any(event["kind"] == CHAIN_COMPLETED for event in events)

    chain = reduce_events(events).chains[chain_id]
    assert chain.paused is True
    assert chain.completed is False


def test_claim_next_task_escalates_on_max_failures_and_pauses_chain(tmp_path: Path) -> None:
    log_file = tmp_path / "bus.jsonl"
    store = JsonlEventStore(str(log_file))
    chain_id = "chain-failures"
    budgets = Budgets(max_handoffs=8, max_reworks=2, max_failures=2)
    actor_user = Actor(type="user", id="user")
    actor_exec = Actor(type="agent", id="exec-old", backend="codex")

    store.append_many(
        [
            make_event(
                kind=OBJECTIVE_CREATED,
                actor=actor_user,
                chain_id=chain_id,
                data={"objective": "obj", "done_when": "done", "preferred_backends": ["codex"]},
            ),
            make_event(
                kind="task.created",
                actor=actor_user,
                task_id="pending-task",
                chain_id=chain_id,
                data={
                    "prompt": "do work",
                    "role_target": "executor",
                    "stage": "execution",
                    "priority": 100,
                    "quality_gate": {"acceptance_criteria": [], "required_checks": [], "review_mode": "hard"},
                    "budgets": budgets.to_dict(),
                    "targets": {"backends": ["codex"], "agent_ids": []},
                    "attempt": 1,
                },
            ),
            make_event(
                kind="task.created",
                actor=actor_user,
                task_id="failed-1",
                chain_id=chain_id,
                data={
                    "prompt": "failed 1",
                    "role_target": "executor",
                    "stage": "execution",
                    "priority": 100,
                    "quality_gate": {"acceptance_criteria": [], "required_checks": [], "review_mode": "hard"},
                    "budgets": budgets.to_dict(),
                    "targets": {"backends": ["codex"], "agent_ids": []},
                    "attempt": 1,
                },
            ),
            make_event(
                kind=TASK_FAILED,
                actor=actor_exec,
                task_id="failed-1",
                chain_id=chain_id,
                data={"error_signature": "same-signature"},
            ),
            make_event(
                kind="task.created",
                actor=actor_user,
                task_id="failed-2",
                chain_id=chain_id,
                data={
                    "prompt": "failed 2",
                    "role_target": "executor",
                    "stage": "execution",
                    "priority": 100,
                    "quality_gate": {"acceptance_criteria": [], "required_checks": [], "review_mode": "hard"},
                    "budgets": budgets.to_dict(),
                    "targets": {"backends": ["codex"], "agent_ids": []},
                    "attempt": 1,
                },
            ),
            make_event(
                kind=TASK_FAILED,
                actor=actor_exec,
                task_id="failed-2",
                chain_id=chain_id,
                data={"error_signature": "same-signature"},
            ),
        ]
    )

    cfg = _default_run_config(tmp_path, escalation_file=str(tmp_path / "escalations.log"))
    claimed = runner._claim_next_task(store, config=cfg)
    assert claimed is None

    events = store.read_all()
    assert any(event["kind"] == GUARDRAIL_BREACHED and event["data"].get("rule") == "max_failures" for event in events)
    assert any(event["kind"] == ESCALATION_RAISED for event in events)

    state = reduce_events(events)
    task = state.tasks["pending-task"]
    claimable = list_claimable_tasks(state, now=utc_now(), role="executor", backend="codex", agent_id="exec-new")
    assert task.chain_id in {chain.chain_id for chain in state.chains.values() if chain.paused}
    assert all(item.task_id != "pending-task" for item in claimable)

    escalation_file = Path(cfg.escalation_file or "")
    assert escalation_file.exists()
    assert "max_failures exceeded" in escalation_file.read_text(encoding="utf-8")


def test_claim_next_task_escalates_on_no_progress_signature(tmp_path: Path) -> None:
    log_file = tmp_path / "bus.jsonl"
    store = JsonlEventStore(str(log_file))
    chain_id = "chain-no-progress"
    budgets = Budgets(max_handoffs=8, max_reworks=2, max_failures=10)
    actor_user = Actor(type="user", id="user")
    actor_exec = Actor(type="agent", id="exec-old", backend="codex")

    seed_events: list[dict[str, object]] = [
        make_event(
            kind=OBJECTIVE_CREATED,
            actor=actor_user,
            chain_id=chain_id,
            data={"objective": "obj", "done_when": "done", "preferred_backends": ["codex"]},
        ),
        make_event(
            kind="task.created",
            actor=actor_user,
            task_id="pending-task",
            chain_id=chain_id,
            data={
                "prompt": "do work",
                "role_target": "executor",
                "stage": "execution",
                "priority": 100,
                "quality_gate": {"acceptance_criteria": [], "required_checks": [], "review_mode": "hard"},
                "budgets": budgets.to_dict(),
                "targets": {"backends": ["codex"], "agent_ids": []},
                "attempt": 1,
            },
        ),
    ]
    for idx in range(1, 4):
        seed_events.extend(
            [
                make_event(
                    kind="task.created",
                    actor=actor_user,
                    task_id=f"failed-{idx}",
                    chain_id=chain_id,
                    data={
                        "prompt": f"failed {idx}",
                        "role_target": "executor",
                        "stage": "execution",
                        "priority": 100,
                        "quality_gate": {"acceptance_criteria": [], "required_checks": [], "review_mode": "hard"},
                        "budgets": budgets.to_dict(),
                        "targets": {"backends": ["codex"], "agent_ids": []},
                        "attempt": 1,
                    },
                ),
                make_event(
                    kind=TASK_FAILED,
                    actor=actor_exec,
                    task_id=f"failed-{idx}",
                    chain_id=chain_id,
                    data={"error_signature": "same-signature"},
                ),
            ]
        )
    store.append_many(seed_events)

    cfg = _default_run_config(tmp_path)
    cfg.max_identical_failures = 3
    claimed = runner._claim_next_task(store, config=cfg)
    assert claimed is None

    events = store.read_all()
    assert any(
        event["kind"] == GUARDRAIL_BREACHED and event["data"].get("rule") == "no_progress_signature"
        for event in events
    )
    assert any(event["kind"] == ESCALATION_RAISED for event in events)


def test_summarize_state_contains_guardrail_fields() -> None:
    now = utc_now()
    chain_id = "chain-status"
    run_id = "run-status"
    task_id = "task-status"
    actor_user = Actor(type="user", id="user")
    actor_exec = Actor(type="agent", id="exec-1", backend="codex")

    events = [
        make_event(
            kind=OBJECTIVE_CREATED,
            actor=actor_user,
            chain_id=chain_id,
            data={"objective": "obj", "done_when": "done", "preferred_backends": ["codex"]},
        ),
        make_event(
            kind="task.created",
            actor=actor_user,
            task_id=task_id,
            chain_id=chain_id,
            data={
                "prompt": "run",
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
            kind=TASK_STARTED,
            actor=actor_exec,
            task_id=task_id,
            chain_id=chain_id,
            run_id=run_id,
            data={"executor_agent_id": "exec-1", "backend": "codex"},
        ),
        make_event(
            kind=REVIEWER_CONTROL_REQUESTED,
            actor=Actor(type="agent", id="rev-1", backend="claude"),
            task_id=task_id,
            chain_id=chain_id,
            run_id=run_id,
            data={
                "action": "nudge",
                "message": "adjust",
                "severity": CONTROL_SEVERITY["nudge"],
                "source": "agent",
                "ts_request": format_ts(now),
            },
        ),
        make_event(
            kind="reviewer.control.applied",
            actor=actor_exec,
            task_id=task_id,
            chain_id=chain_id,
            run_id=run_id,
            data={"chosen_event_id": "ignored", "action": "nudge", "executor_agent_id": "exec-1", "outcome": "accepted"},
        ),
        make_event(
            kind=RUN_PAUSED,
            actor=actor_exec,
            task_id=task_id,
            chain_id=chain_id,
            run_id=run_id,
            data={"reason": "manual"},
        ),
        make_event(
            kind=RUN_RESTARTED,
            actor=actor_exec,
            task_id=task_id,
            chain_id=chain_id,
            run_id=run_id,
            data={"reason": "resume"},
        ),
        make_event(
            kind=GUARDRAIL_BREACHED,
            actor=actor_exec,
            task_id=task_id,
            chain_id=chain_id,
            run_id=run_id,
            data={
                "scope": "run",
                "rule": "max_restarts",
                "observed": 7,
                "threshold": 6,
                "action": "escalate_pause_chain",
                "detail": "restart limit exceeded",
            },
        ),
    ]

    summary = summarize_state(reduce_events(events))
    assert "guardrail_config" in summary
    assert summary["chains"][0]["chain_guardrail_state"]["last_guardrail"] == "max_restarts"
    assert summary["active_runs"][0]["nudge_count"] >= 1
    assert summary["active_runs"][0]["restart_count"] >= 1
    assert summary["active_runs"][0]["last_guardrail"] == "max_restarts"
