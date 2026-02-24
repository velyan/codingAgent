from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from agentbus.models import (
    ACTION_REJECTED,
    CHAIN_COMPLETED,
    CONTROL_SEVERITY,
    ESCALATION_RAISED,
    OBJECTIVE_CREATED,
    REVIEWER_CONTROL_APPLIED,
    REVIEWER_CONTROL_REQUESTED,
    REVIEWER_SUPERVISION_CLAIMED,
    REVIEWER_SUPERVISION_HEARTBEAT,
    REVIEW_PASSED,
    REVIEW_REWORK_REQUESTED,
    RUN_INTERRUPTED,
    RUN_PAUSED,
    RUN_RESTARTED,
    RUN_RESUMED,
    STREAM_CHUNK,
    TASK_CLAIMED,
    TASK_COMPLETED,
    TASK_CREATED,
    TASK_FAILED,
    TASK_HEARTBEAT,
    TASK_REQUEUED,
    TASK_STARTED,
    Budgets,
    ChainView,
    ControlRequestView,
    ReducedState,
    Role,
    RunView,
    TaskTargets,
    TaskView,
    QualityGate,
    parse_ts,
)


def _get_or_create_chain(state: ReducedState, chain_id: str) -> ChainView:
    chain = state.chains.get(chain_id)
    if chain is None:
        chain = ChainView(chain_id=chain_id)
        state.chains[chain_id] = chain
    return chain


def _parse_event_ts(event: dict[str, Any]) -> datetime:
    return parse_ts(str(event.get("ts", "")))


def _ensure_run(
    state: ReducedState,
    *,
    run_id: str,
    task_id: str,
    chain_id: str,
    executor_agent_id: str,
    backend: str,
    ts: datetime,
) -> RunView:
    run = state.runs.get(run_id)
    if run is None:
        run = RunView(
            run_id=run_id,
            task_id=task_id,
            chain_id=chain_id,
            executor_agent_id=executor_agent_id,
            backend=backend,
            status="running",
            started_at=ts,
            updated_at=ts,
        )
        state.runs[run_id] = run
    return run


def reduce_events(events: list[dict[str, Any]]) -> ReducedState:
    state = ReducedState()

    for event in events:
        kind = str(event.get("kind", ""))
        task_id = str(event.get("task_id") or "")
        chain_id = str(event.get("chain_id") or "")
        run_id = str(event.get("run_id") or "")
        data = event.get("data")
        if not isinstance(data, dict):
            data = {}
        event_ts = _parse_event_ts(event)

        chain = _get_or_create_chain(state, chain_id) if chain_id else None

        if kind == OBJECTIVE_CREATED and chain is not None:
            chain.objective = str(data.get("objective", ""))
            chain.done_when = str(data.get("done_when", ""))
            chain.preferred_backends = [str(x) for x in data.get("preferred_backends", [])]
            chain.paused = False

        elif kind == TASK_CREATED and task_id and chain is not None:
            task = TaskView(
                task_id=task_id,
                chain_id=chain_id,
                created_at=event_ts,
                prompt=str(data.get("prompt", "")),
                role_target=str(data.get("role_target", "executor")),
                stage=str(data.get("stage", "execution")),
                priority=int(data.get("priority", 100)),
                quality_gate=QualityGate.from_dict(data.get("quality_gate")),
                budgets=Budgets.from_dict(data.get("budgets")),
                targets=TaskTargets.from_dict(data.get("targets")),
                status="pending",
                parent_task_id=str(data.get("parent_task_id") or "") or None,
                attempt=int(data.get("attempt", 1)),
            )
            state.tasks[task.task_id] = task
            actor = event.get("actor")
            if isinstance(actor, dict) and actor.get("type") == "agent":
                chain.handoff_count += 1

        elif kind == TASK_CLAIMED and task_id:
            task = state.tasks.get(task_id)
            if task is not None:
                task.status = "claimed"
                task.run_id = run_id or task.run_id
                task.claimed_by = str(data.get("agent_id", "")) or None
                task.claimed_backend = str(data.get("backend", "")) or None
                lease_raw = data.get("lease_expires_at")
                if isinstance(lease_raw, str) and lease_raw:
                    task.lease_expires_at = parse_ts(lease_raw)

        elif kind == TASK_STARTED and task_id:
            task = state.tasks.get(task_id)
            if task is not None:
                task.status = "running"
                task.started_at = event_ts
                if run_id:
                    task.run_id = run_id
                    run = _ensure_run(
                        state,
                        run_id=run_id,
                        task_id=task.task_id,
                        chain_id=task.chain_id,
                        executor_agent_id=str(data.get("executor_agent_id", task.claimed_by or "")),
                        backend=str(data.get("backend", task.claimed_backend or "")),
                        ts=event_ts,
                    )
                    run.status = "running"
                    run.updated_at = event_ts

        elif kind == TASK_HEARTBEAT and task_id:
            task = state.tasks.get(task_id)
            if task is not None:
                lease_raw = data.get("lease_expires_at")
                if isinstance(lease_raw, str) and lease_raw:
                    task.lease_expires_at = parse_ts(lease_raw)

        elif kind == TASK_COMPLETED and task_id:
            task = state.tasks.get(task_id)
            if task is not None:
                task.status = "completed"
                task.completed_at = event_ts
                if task.run_id and task.run_id in state.runs:
                    run = state.runs[task.run_id]
                    run.status = "completed"
                    run.completed_at = event_ts
                    run.updated_at = event_ts

        elif kind == TASK_FAILED and task_id:
            task = state.tasks.get(task_id)
            if task is not None:
                task.status = "failed"
                task.completed_at = event_ts
                signature = str(data.get("error_signature") or "")
                task.last_error_signature = signature or None
                if chain is not None:
                    chain.failure_count += 1
                    if signature:
                        chain.recent_failure_signatures.append(signature)
                        chain.recent_failure_signatures = chain.recent_failure_signatures[-3:]
                if task.run_id and task.run_id in state.runs:
                    run = state.runs[task.run_id]
                    run.status = "failed"
                    run.completed_at = event_ts
                    run.updated_at = event_ts

        elif kind == TASK_REQUEUED and task_id:
            task = state.tasks.get(task_id)
            if task is not None:
                task.status = "pending"
                task.run_id = None
                task.claimed_by = None
                task.claimed_backend = None
                task.lease_expires_at = None
                task.started_at = None
                task.completed_at = None
                task.attempt = int(data.get("attempt", task.attempt + 1))

        elif kind == RUN_INTERRUPTED and run_id and run_id in state.runs:
            run = state.runs[run_id]
            run.status = "interrupted"
            run.updated_at = event_ts

        elif kind == RUN_PAUSED and run_id and run_id in state.runs:
            run = state.runs[run_id]
            run.status = "paused"
            run.updated_at = event_ts

        elif kind in {RUN_RESUMED, RUN_RESTARTED} and run_id and run_id in state.runs:
            run = state.runs[run_id]
            run.status = "running"
            run.updated_at = event_ts

        elif kind == REVIEWER_SUPERVISION_CLAIMED and run_id and run_id in state.runs:
            run = state.runs[run_id]
            run.reviewer_agent_id = str(data.get("reviewer_agent_id") or "") or None
            lease_raw = data.get("lease_expires_at")
            if isinstance(lease_raw, str) and lease_raw:
                run.reviewer_lease_expires_at = parse_ts(lease_raw)
            run.updated_at = event_ts

        elif kind == REVIEWER_SUPERVISION_HEARTBEAT and run_id and run_id in state.runs:
            run = state.runs[run_id]
            lease_raw = data.get("lease_expires_at")
            if isinstance(lease_raw, str) and lease_raw:
                run.reviewer_lease_expires_at = parse_ts(lease_raw)
            run.updated_at = event_ts

        elif kind == REVIEWER_CONTROL_REQUESTED and run_id:
            action = str(data.get("action") or "")
            if action not in CONTROL_SEVERITY:
                continue
            requested_at = parse_ts(str(data.get("ts_request") or event.get("ts") or ""))
            request = ControlRequestView(
                event_id=str(event.get("event_id") or ""),
                run_id=run_id,
                action=action,
                message=str(data.get("message") or ""),
                severity=int(data.get("severity", CONTROL_SEVERITY[action])),
                source=str(data.get("source") or "agent"),
                actor_id=str((event.get("actor") or {}).get("id") if isinstance(event.get("actor"), dict) else ""),
                requested_at=requested_at,
            )
            state.pending_controls.setdefault(run_id, []).append(request)

        elif kind == REVIEWER_CONTROL_APPLIED:
            chosen_id = str(data.get("chosen_event_id") or "")
            if chosen_id:
                state.applied_control_event_ids.add(chosen_id)
            if run_id in state.pending_controls and chosen_id:
                state.pending_controls[run_id] = [
                    request for request in state.pending_controls[run_id] if request.event_id != chosen_id
                ]

        elif kind == REVIEW_REWORK_REQUESTED and chain is not None:
            chain.rework_count += 1

        elif kind == REVIEW_PASSED:
            pass

        elif kind == ESCALATION_RAISED and chain is not None:
            chain.escalations += 1
            chain.paused = True

        elif kind == CHAIN_COMPLETED and chain is not None:
            chain.completed = True
            chain.paused = False

        elif kind in {ACTION_REJECTED, REVIEWER_CONTROL_REJECTED, STREAM_CHUNK}:
            pass

    return state


def task_fingerprint(task: TaskView) -> str:
    backend_bits = ",".join(sorted(task.targets.backends))
    return f"{task.role_target}|{task.prompt}|{backend_bits}|{task.stage}"


def pending_task_fingerprints(state: ReducedState, chain_id: str) -> set[str]:
    fingerprints: set[str] = set()
    for task in state.tasks.values():
        if task.chain_id != chain_id:
            continue
        if task.status in {"pending", "claimed", "running"}:
            fingerprints.add(task_fingerprint(task))
    return fingerprints


def list_claimable_tasks(
    state: ReducedState,
    *,
    now: datetime,
    role: Role,
    backend: str,
    agent_id: str,
) -> list[TaskView]:
    items: list[TaskView] = []
    for task in state.tasks.values():
        if task.role_target != role:
            continue
        chain = state.chains.get(task.chain_id)
        if chain is not None and (chain.completed or chain.paused):
            continue
        if task.is_terminal():
            continue
        if not task.claimable(now):
            continue
        if task.targets.backends and backend not in task.targets.backends:
            continue
        if task.targets.agent_ids and agent_id not in task.targets.agent_ids:
            continue
        items.append(task)

    items.sort(key=lambda t: (-t.priority, t.created_at, t.task_id))
    return items


def list_supervisable_runs(
    state: ReducedState,
    *,
    now: datetime,
    reviewer_agent_id: str,
) -> list[RunView]:
    runs: list[RunView] = []
    for run in state.runs.values():
        if not run.is_active(now):
            continue
        if run.reviewer_agent_id == reviewer_agent_id:
            runs.append(run)
            continue
        if run.reviewer_agent_id is None:
            runs.append(run)
            continue
        if run.reviewer_lease_expires_at is not None and now >= run.reviewer_lease_expires_at:
            runs.append(run)
    runs.sort(key=lambda r: (r.started_at, r.run_id))
    return runs


def get_top_control(state: ReducedState, run_id: str) -> ControlRequestView | None:
    requests = state.pending_controls.get(run_id, [])
    if not requests:
        return None

    filtered = [request for request in requests if request.event_id not in state.applied_control_event_ids]
    if not filtered:
        return None

    filtered.sort(key=lambda request: (request.severity, request.requested_at, request.event_id), reverse=True)
    return filtered[0]


def summarize_state(state: ReducedState, *, chain_id: str | None = None) -> dict[str, Any]:
    chain_filter = chain_id or None
    task_counts: dict[str, int] = defaultdict(int)
    chain_summaries: list[dict[str, Any]] = []
    active_runs: list[dict[str, Any]] = []

    for task in state.tasks.values():
        if chain_filter and task.chain_id != chain_filter:
            continue
        task_counts[task.status] += 1

    for chain in state.chains.values():
        if chain_filter and chain.chain_id != chain_filter:
            continue
        chain_summaries.append(
            {
                "chain_id": chain.chain_id,
                "completed": chain.completed,
                "paused": chain.paused,
                "escalations": chain.escalations,
                "handoffs": chain.handoff_count,
                "reworks": chain.rework_count,
                "failures": chain.failure_count,
                "objective": chain.objective,
            }
        )

    for run in state.runs.values():
        if chain_filter and run.chain_id != chain_filter:
            continue
        if run.status in {"running", "paused", "interrupted"}:
            active_runs.append(
                {
                    "run_id": run.run_id,
                    "task_id": run.task_id,
                    "status": run.status,
                    "executor": run.executor_agent_id,
                    "reviewer": run.reviewer_agent_id,
                    "backend": run.backend,
                }
            )

    chain_summaries.sort(key=lambda x: x["chain_id"])
    active_runs.sort(key=lambda x: x["run_id"])

    return {
        "task_counts": dict(task_counts),
        "chains": chain_summaries,
        "active_runs": active_runs,
        "total_tasks": sum(task_counts.values()),
    }
