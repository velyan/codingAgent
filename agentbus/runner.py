from __future__ import annotations

import signal
import subprocess
import time
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from agentbus.actions import parse_agentbus_actions
from agentbus.adapters import get_adapter
from agentbus.control import choose_control
from agentbus.events import make_event
from agentbus.models import (
    ACTION_REJECTED,
    CHAIN_COMPLETED,
    ESCALATION_RAISED,
    OBJECTIVE_CREATED,
    REVIEWER_CONTROL_APPLIED,
    REVIEWER_CONTROL_REJECTED,
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
    TASK_STARTED,
    Actor,
    AgentState,
    Budgets,
    ExecutionResult,
    QualityGate,
    RunConfig,
    TaskTargets,
    TaskView,
    format_ts,
    utc_now,
)
from agentbus.reducer import (
    get_top_control,
    list_claimable_tasks,
    list_supervisable_runs,
    pending_task_fingerprints,
    reduce_events,
)
from agentbus.store import EventCursor, JsonlEventStore
from agentbus.streaming import run_streaming_subprocess


RISK_PATTERNS: list[tuple[str, str, str]] = [
    ("rm -rf /", "stop", "dangerous destructive command detected"),
    ("git reset --hard", "stop", "destructive git reset detected"),
    ("traceback", "pause", "runtime traceback detected"),
    ("exception", "nudge", "exception text detected"),
    ("segmentation fault", "stop", "segmentation fault detected"),
]


def _truncate_text(text: str, max_bytes: int) -> tuple[str, bool]:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text, False
    clipped = encoded[: max(0, max_bytes - 32)].decode("utf-8", errors="ignore")
    return f"{clipped}\n...[TRUNCATED]...", True


def _error_signature(stdout: str, stderr: str) -> str:
    source = stderr.strip() or stdout.strip()
    if not source:
        return "unknown-error"
    first_line = source.splitlines()[0].strip().lower()
    return first_line[:240] if first_line else "unknown-error"


def _append_events(store: JsonlEventStore, events: list[dict[str, Any]]) -> None:
    if events:
        store.append_many(events)


def _task_created_event(
    *,
    actor: Actor,
    task_id: str,
    chain_id: str,
    prompt: str,
    role_target: str,
    stage: str,
    priority: int,
    targets: TaskTargets,
    quality_gate: QualityGate,
    budgets: Budgets,
    parent_task_id: str | None,
    attempt: int,
) -> dict[str, Any]:
    return make_event(
        kind=TASK_CREATED,
        actor=actor,
        task_id=task_id,
        chain_id=chain_id,
        data={
            "prompt": prompt,
            "role_target": role_target,
            "stage": stage,
            "priority": priority,
            "targets": targets.to_dict(),
            "quality_gate": quality_gate.to_dict(),
            "budgets": budgets.to_dict(),
            "parent_task_id": parent_task_id,
            "attempt": attempt,
        },
    )


def _claim_next_task(store: JsonlEventStore, *, config: RunConfig) -> tuple[TaskView, str] | None:
    now = utc_now()
    with store.locked() as locked:
        events = locked.read_events()
        state = reduce_events(events)
        candidates = list_claimable_tasks(
            state,
            now=now,
            role=config.role,
            backend=config.backend,
            agent_id=config.agent_id,
        )
        if not candidates:
            return None

        task = candidates[0]
        run_id = str(uuid4())
        claim_event = make_event(
            kind=TASK_CLAIMED,
            actor=Actor(type="agent", id=config.agent_id, backend=config.backend),
            task_id=task.task_id,
            chain_id=task.chain_id,
            run_id=run_id,
            data={
                "agent_id": config.agent_id,
                "backend": config.backend,
                "lease_expires_at": format_ts(now + timedelta(seconds=config.lease_seconds)),
                "attempt": task.attempt,
            },
        )
        locked.append_events([claim_event])
        return task, run_id


def _emit_action_rejected_events(
    *,
    actor: Actor,
    chain_id: str,
    task_id: str,
    run_id: str,
    reasons: list[str],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for reason in reasons:
        events.append(
            make_event(
                kind=ACTION_REJECTED,
                actor=actor,
                chain_id=chain_id,
                task_id=task_id,
                run_id=run_id,
                data={"reason": reason},
            )
        )
    return events


def _emit_control_rejected(
    *,
    store: JsonlEventStore,
    config: RunConfig,
    chain_id: str,
    task_id: str,
    run_id: str,
    reason: str,
) -> None:
    event = make_event(
        kind=REVIEWER_CONTROL_REJECTED,
        actor=Actor(type="agent", id=config.agent_id, backend=config.backend),
        chain_id=chain_id,
        task_id=task_id,
        run_id=run_id,
        data={"reason": reason},
    )
    store.append(event)


def _apply_actions(
    *,
    store: JsonlEventStore,
    config: RunConfig,
    task: TaskView,
    run_id: str,
    final_output: str,
) -> None:
    actor = Actor(type="agent", id=config.agent_id, backend=config.backend)
    result = parse_agentbus_actions(final_output)
    events: list[dict[str, Any]] = []

    if result.rejected_reasons:
        events.extend(
            _emit_action_rejected_events(
                actor=actor,
                chain_id=task.chain_id,
                task_id=task.task_id,
                run_id=run_id,
                reasons=result.rejected_reasons,
            )
        )

    if not result.actions:
        _append_events(store, events)
        return

    with store.locked() as locked:
        existing_state = reduce_events(locked.read_events())
        fingerprints = pending_task_fingerprints(existing_state, task.chain_id)
        for action in result.actions:
            if action.type == "create_task":
                payload = action.payload
                new_targets = TaskTargets(backends=payload.get("target_backend", []), agent_ids=[])
                new_quality = QualityGate(
                    acceptance_criteria=payload.get("acceptance_criteria", []),
                    required_checks=payload.get("required_checks", []),
                    review_mode=str(payload.get("review_mode", "hard")),
                )
                synthetic_task = TaskView(
                    task_id="synthetic",
                    chain_id=task.chain_id,
                    created_at=utc_now(),
                    prompt=str(payload["prompt"]),
                    role_target=str(payload["target_role"]),
                    stage=str(payload.get("stage", "execution")),
                    priority=int(payload.get("priority", 100)),
                    quality_gate=new_quality,
                    budgets=task.budgets,
                    targets=new_targets,
                )
                fingerprint = (
                    f"{synthetic_task.role_target}|{synthetic_task.prompt}|"
                    f"{','.join(sorted(synthetic_task.targets.backends))}|{synthetic_task.stage}"
                )
                if fingerprint in fingerprints:
                    events.append(
                        make_event(
                            kind=ACTION_REJECTED,
                            actor=actor,
                            chain_id=task.chain_id,
                            task_id=task.task_id,
                            run_id=run_id,
                            data={"reason": "duplicate task suppressed"},
                        )
                    )
                    continue
                new_task_id = str(uuid4())
                events.append(
                    _task_created_event(
                        actor=actor,
                        task_id=new_task_id,
                        chain_id=task.chain_id,
                        prompt=synthetic_task.prompt,
                        role_target=synthetic_task.role_target,
                        stage=synthetic_task.stage,
                        priority=synthetic_task.priority,
                        targets=synthetic_task.targets,
                        quality_gate=synthetic_task.quality_gate,
                        budgets=synthetic_task.budgets,
                        parent_task_id=task.task_id,
                        attempt=1,
                    )
                )
                fingerprints.add(fingerprint)

            elif action.type == "request_rework":
                events.append(
                    make_event(
                        kind=REVIEW_REWORK_REQUESTED,
                        actor=actor,
                        chain_id=task.chain_id,
                        task_id=task.task_id,
                        run_id=run_id,
                        data={"reason": action.payload.get("message", "")},
                    )
                )

            elif action.type == "mark_objective_complete":
                events.append(
                    make_event(
                        kind=CHAIN_COMPLETED,
                        actor=actor,
                        chain_id=task.chain_id,
                        task_id=task.task_id,
                        run_id=run_id,
                        data={"by_task": task.task_id},
                    )
                )

            elif action.type == "raise_escalation":
                events.append(
                    make_event(
                        kind=ESCALATION_RAISED,
                        actor=actor,
                        chain_id=task.chain_id,
                        task_id=task.task_id,
                        run_id=run_id,
                        data={"reason": action.payload.get("reason", "")},
                    )
                )

            elif action.type == "steer":
                payload = action.payload
                target_run_id = str(payload["run_id"])
                action_name = str(payload["action"])
                events.append(
                    make_event(
                        kind=REVIEWER_CONTROL_REQUESTED,
                        actor=actor,
                        chain_id=task.chain_id,
                        task_id=task.task_id,
                        run_id=target_run_id,
                        data={
                            "action": action_name,
                            "message": payload.get("message", ""),
                            "severity": {"resume": 0, "nudge": 1, "rework": 2, "pause": 3, "stop": 4}[action_name],
                            "source": "agent",
                            "ts_request": format_ts(utc_now()),
                        },
                    )
                )

        locked.append_events(events)


def _interrupt_process(proc: subprocess.Popen[bytes], *, hard_after_seconds: float = 5.0) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGINT)
    except ProcessLookupError:
        return

    deadline = time.monotonic() + hard_after_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.05)

    if proc.poll() is None:
        proc.terminate()


def _execute_worker_task(
    *,
    store: JsonlEventStore,
    config: RunConfig,
    agent_state: AgentState,
    task: TaskView,
    run_id: str,
) -> None:
    adapter = get_adapter(config.backend)
    actor = Actor(type="agent", id=config.agent_id, backend=config.backend)

    prompt = task.prompt
    resume_mode = bool(agent_state.backend_state)
    seq = 0
    pending_pause = False
    paused_note = ""

    while True:
        started_event = make_event(
            kind=TASK_STARTED,
            actor=actor,
            task_id=task.task_id,
            chain_id=task.chain_id,
            run_id=run_id,
            data={
                "executor_agent_id": config.agent_id,
                "backend": config.backend,
                "cwd": config.cwd,
            },
        )
        store.append(started_event)

        command = adapter.build_command(
            prompt=prompt,
            config=config,
            agent_state=agent_state,
            resume=resume_mode,
        )

        chosen_control_id: str | None = None
        chosen_control_action: str | None = None
        chosen_control_message: str = ""
        last_heartbeat = time.monotonic()
        last_control_check = 0.0

        def on_chunk(channel: str, text: str) -> None:
            nonlocal seq
            seq += 1
            clipped, truncated = _truncate_text(text, config.stream_chunk_bytes)
            event = make_event(
                kind=STREAM_CHUNK,
                actor=actor,
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "seq": seq,
                    "channel": channel,
                    "text": clipped,
                    "truncated": truncated,
                },
            )
            store.append(event)

        def on_tick(proc: subprocess.Popen[bytes]) -> None:
            nonlocal chosen_control_id, chosen_control_action, chosen_control_message
            nonlocal last_heartbeat, last_control_check

            now_monotonic = time.monotonic()
            if now_monotonic - last_heartbeat >= config.heartbeat_seconds:
                lease_event = make_event(
                    kind=TASK_HEARTBEAT,
                    actor=actor,
                    task_id=task.task_id,
                    chain_id=task.chain_id,
                    run_id=run_id,
                    data={
                        "lease_expires_at": format_ts(utc_now() + timedelta(seconds=config.lease_seconds)),
                    },
                )
                store.append(lease_event)
                last_heartbeat = now_monotonic

            if now_monotonic - last_control_check < (config.control_poll_ms / 1000.0):
                return
            last_control_check = now_monotonic

            state = reduce_events(store.read_all())
            control = get_top_control(state, run_id)
            if control is None:
                return
            if chosen_control_id == control.event_id:
                return

            run_view = state.runs.get(run_id)
            if run_view is not None and run_view.reviewer_agent_id:
                if control.actor_id != run_view.reviewer_agent_id and control.source != "human":
                    _emit_control_rejected(
                        store=store,
                        config=config,
                        chain_id=task.chain_id,
                        task_id=task.task_id,
                        run_id=run_id,
                        reason="control from non-owner reviewer",
                    )
                    return

            chosen_control_id = control.event_id
            chosen_control_action = control.action
            chosen_control_message = control.message
            applied = make_event(
                kind=REVIEWER_CONTROL_APPLIED,
                actor=actor,
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "chosen_event_id": control.event_id,
                    "action": control.action,
                    "executor_agent_id": config.agent_id,
                    "outcome": "accepted",
                },
            )
            store.append(applied)

            if control.action in {"nudge", "pause", "stop", "rework"}:
                _interrupt_process(proc)

        run_result = run_streaming_subprocess(
            command=command.argv,
            cwd=config.cwd,
            env=command.env,
            chunk_bytes=config.stream_chunk_bytes,
            flush_ms=config.stream_flush_ms,
            on_chunk=on_chunk,
            on_tick=on_tick,
        )

        stdout_clipped, _ = _truncate_text(run_result.stdout, config.max_output_bytes)
        stderr_clipped, _ = _truncate_text(run_result.stderr, config.max_output_bytes)
        final_output = adapter.extract_final_output(run_result.stdout, run_result.stderr)
        final_clipped, _ = _truncate_text(final_output, config.max_output_bytes)
        adapter.after_run(stdout=run_result.stdout, stderr=run_result.stderr, agent_state=agent_state)
        resume_mode = True

        # Handle control actions after process exits.
        if chosen_control_action == "nudge":
            store.append(
                make_event(
                    kind=RUN_INTERRUPTED,
                    actor=actor,
                    task_id=task.task_id,
                    chain_id=task.chain_id,
                    run_id=run_id,
                    data={"reason": chosen_control_message or "nudged by reviewer"},
                )
            )
            prompt = (
                f"{task.prompt}\n\n"
                f"[Reviewer steer]\n{chosen_control_message or 'Adjust approach and continue.'}"
            )
            store.append(
                make_event(
                    kind=RUN_RESTARTED,
                    actor=actor,
                    task_id=task.task_id,
                    chain_id=task.chain_id,
                    run_id=run_id,
                    data={"reason": "nudge"},
                )
            )
            continue

        if chosen_control_action == "pause":
            store.append(
                make_event(
                    kind=RUN_PAUSED,
                    actor=actor,
                    task_id=task.task_id,
                    chain_id=task.chain_id,
                    run_id=run_id,
                    data={"reason": chosen_control_message or "paused by reviewer"},
                )
            )
            pending_pause = True
            paused_note = chosen_control_message

        if chosen_control_action == "stop":
            fail_event = make_event(
                kind=TASK_FAILED,
                actor=actor,
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "exit_code": run_result.exit_code,
                    "duration_ms": run_result.duration_ms,
                    "stdout": stdout_clipped,
                    "stderr": stderr_clipped,
                    "final_output": final_clipped,
                    "error": "stopped by reviewer",
                    "error_signature": _error_signature(run_result.stdout, run_result.stderr),
                    "backend": config.backend,
                },
            )
            escalation = make_event(
                kind=ESCALATION_RAISED,
                actor=actor,
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={"reason": chosen_control_message or "run stopped"},
            )
            _append_events(store, [fail_event, escalation])
            store.save_agent_state(config.agent_id, agent_state)
            return

        if chosen_control_action == "rework":
            requested = make_event(
                kind=REVIEW_REWORK_REQUESTED,
                actor=actor,
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={"reason": chosen_control_message or "rework requested during execution"},
            )

            if task.attempt >= task.budgets.max_reworks + 1:
                escalation = make_event(
                    kind=ESCALATION_RAISED,
                    actor=actor,
                    task_id=task.task_id,
                    chain_id=task.chain_id,
                    run_id=run_id,
                    data={"reason": "rework budget exceeded"},
                )
                fail_event = make_event(
                    kind=TASK_FAILED,
                    actor=actor,
                    task_id=task.task_id,
                    chain_id=task.chain_id,
                    run_id=run_id,
                    data={
                        "exit_code": run_result.exit_code,
                        "duration_ms": run_result.duration_ms,
                        "stdout": stdout_clipped,
                        "stderr": stderr_clipped,
                        "final_output": final_clipped,
                        "error": "rework budget exceeded",
                        "error_signature": _error_signature(run_result.stdout, run_result.stderr),
                        "backend": config.backend,
                    },
                )
                _append_events(store, [requested, fail_event, escalation])
                store.save_agent_state(config.agent_id, agent_state)
                return

            new_task_id = str(uuid4())
            rework_task = _task_created_event(
                actor=actor,
                task_id=new_task_id,
                chain_id=task.chain_id,
                prompt=f"{task.prompt}\n\n[Rework]\n{chosen_control_message}",
                role_target="executor",
                stage="rework",
                priority=task.priority,
                targets=task.targets,
                quality_gate=task.quality_gate,
                budgets=task.budgets,
                parent_task_id=task.task_id,
                attempt=task.attempt + 1,
            )
            fail_event = make_event(
                kind=TASK_FAILED,
                actor=actor,
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "exit_code": run_result.exit_code,
                    "duration_ms": run_result.duration_ms,
                    "stdout": stdout_clipped,
                    "stderr": stderr_clipped,
                    "final_output": final_clipped,
                    "error": "rework requested",
                    "error_signature": _error_signature(run_result.stdout, run_result.stderr),
                    "backend": config.backend,
                },
            )
            _append_events(store, [requested, fail_event, rework_task])
            store.save_agent_state(config.agent_id, agent_state)
            return

        if pending_pause:
            while True:
                time.sleep(max(0.2, config.poll_seconds))
                state = reduce_events(store.read_all())
                control = get_top_control(state, run_id)
                if control is None:
                    continue
                run_view = state.runs.get(run_id)
                if run_view is not None and run_view.reviewer_agent_id:
                    if control.actor_id != run_view.reviewer_agent_id and control.source != "human":
                        _emit_control_rejected(
                            store=store,
                            config=config,
                            chain_id=task.chain_id,
                            task_id=task.task_id,
                            run_id=run_id,
                            reason="control from non-owner reviewer",
                        )
                        continue
                applied = make_event(
                    kind=REVIEWER_CONTROL_APPLIED,
                    actor=actor,
                    task_id=task.task_id,
                    chain_id=task.chain_id,
                    run_id=run_id,
                    data={
                        "chosen_event_id": control.event_id,
                        "action": control.action,
                        "executor_agent_id": config.agent_id,
                        "outcome": "accepted",
                    },
                )
                store.append(applied)

                if control.action == "resume":
                    store.append(
                        make_event(
                            kind=RUN_RESUMED,
                            actor=actor,
                            task_id=task.task_id,
                            chain_id=task.chain_id,
                            run_id=run_id,
                            data={"reason": control.message or paused_note or "resumed"},
                        )
                    )
                    prompt = f"{task.prompt}\n\n[Resume Guidance]\n{control.message or paused_note}"
                    pending_pause = False
                    break
                if control.action == "stop":
                    fail_event = make_event(
                        kind=TASK_FAILED,
                        actor=actor,
                        task_id=task.task_id,
                        chain_id=task.chain_id,
                        run_id=run_id,
                        data={
                            "exit_code": run_result.exit_code,
                            "duration_ms": run_result.duration_ms,
                            "stdout": stdout_clipped,
                            "stderr": stderr_clipped,
                            "final_output": final_clipped,
                            "error": "stopped while paused",
                            "error_signature": _error_signature(run_result.stdout, run_result.stderr),
                            "backend": config.backend,
                        },
                    )
                    escalation = make_event(
                        kind=ESCALATION_RAISED,
                        actor=actor,
                        task_id=task.task_id,
                        chain_id=task.chain_id,
                        run_id=run_id,
                        data={"reason": control.message or "stopped while paused"},
                    )
                    _append_events(store, [fail_event, escalation])
                    store.save_agent_state(config.agent_id, agent_state)
                    return
            if pending_pause:
                continue
            store.append(
                make_event(
                    kind=RUN_RESTARTED,
                    actor=actor,
                    task_id=task.task_id,
                    chain_id=task.chain_id,
                    run_id=run_id,
                    data={"reason": "resume"},
                )
            )
            continue

        if run_result.exit_code == 0:
            complete_event = make_event(
                kind=TASK_COMPLETED,
                actor=actor,
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "exit_code": run_result.exit_code,
                    "duration_ms": run_result.duration_ms,
                    "stdout": stdout_clipped,
                    "stderr": stderr_clipped,
                    "final_output": final_clipped,
                    "backend": config.backend,
                },
            )
            store.append(complete_event)
            _apply_actions(
                store=store,
                config=config,
                task=task,
                run_id=run_id,
                final_output=run_result.final_output if hasattr(run_result, "final_output") else final_output,
            )
        else:
            fail_event = make_event(
                kind=TASK_FAILED,
                actor=actor,
                task_id=task.task_id,
                chain_id=task.chain_id,
                run_id=run_id,
                data={
                    "exit_code": run_result.exit_code,
                    "duration_ms": run_result.duration_ms,
                    "stdout": stdout_clipped,
                    "stderr": stderr_clipped,
                    "final_output": final_clipped,
                    "error": "executor command failed",
                    "error_signature": _error_signature(run_result.stdout, run_result.stderr),
                    "backend": config.backend,
                },
            )
            store.append(fail_event)

        store.save_agent_state(config.agent_id, agent_state)
        return


def _maybe_emit_escalation_file(config: RunConfig, message: str) -> None:
    if not config.escalation_file:
        return
    path = Path(config.escalation_file).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{format_ts(utc_now())} {message}\n")


def _reviewer_heuristic_actions(window_text: str) -> list[tuple[str, str]]:
    text_lower = window_text.lower()
    actions: list[tuple[str, str]] = []
    for pattern, action, reason in RISK_PATTERNS:
        if pattern in text_lower:
            actions.append((action, reason))
            break
    return actions


def _claim_supervision(
    *,
    store: JsonlEventStore,
    config: RunConfig,
    run_id: str,
    task_id: str,
    chain_id: str,
) -> bool:
    now = utc_now()
    with store.locked() as locked:
        state = reduce_events(locked.read_events())
        run = state.runs.get(run_id)
        if run is None:
            return False
        if run.status not in {"running", "paused", "interrupted"}:
            return False
        if run.reviewer_agent_id and run.reviewer_agent_id != config.agent_id:
            if run.reviewer_lease_expires_at and now < run.reviewer_lease_expires_at:
                return False
        claim = make_event(
            kind=REVIEWER_SUPERVISION_CLAIMED,
            actor=Actor(type="agent", id=config.agent_id, backend=config.backend),
            task_id=task_id,
            chain_id=chain_id,
            run_id=run_id,
            data={
                "reviewer_agent_id": config.agent_id,
                "lease_expires_at": format_ts(now + timedelta(seconds=config.reviewer_lease_seconds)),
            },
        )
        locked.append_events([claim])
    return True


def _reviewer_loop(*, store: JsonlEventStore, config: RunConfig, agent_state: AgentState) -> None:
    actor = Actor(type="agent", id=config.agent_id, backend=config.backend)
    cursor = EventCursor(line_no=0)
    supervised_run_id: str | None = None
    stream_buffer: list[str] = []
    last_review = time.monotonic()
    last_supervision_heartbeat = 0.0

    while True:
        now = utc_now()
        state = reduce_events(store.read_all())

        if supervised_run_id is None:
            candidates = list_supervisable_runs(state, now=now, reviewer_agent_id=config.agent_id)
            if candidates:
                run = candidates[0]
                if _claim_supervision(
                    store=store,
                    config=config,
                    run_id=run.run_id,
                    task_id=run.task_id,
                    chain_id=run.chain_id,
                ):
                    supervised_run_id = run.run_id
                    stream_buffer = []
                    last_review = time.monotonic()

        new_events, cursor = store.read_from(cursor)
        for event in new_events:
            if supervised_run_id is None:
                continue
            if str(event.get("run_id") or "") != supervised_run_id:
                continue
            if str(event.get("kind") or "") == STREAM_CHUNK:
                data = event.get("data") or {}
                if isinstance(data, dict):
                    stream_buffer.append(str(data.get("text") or ""))

        if supervised_run_id is not None:
            run = state.runs.get(supervised_run_id)
            if run is None or run.status in {"completed", "failed", "stopped"}:
                if run is not None:
                    kind = REVIEW_PASSED if run.status == "completed" else REVIEW_REWORK_REQUESTED
                    store.append(
                        make_event(
                            kind=kind,
                            actor=actor,
                            task_id=run.task_id,
                            chain_id=run.chain_id,
                            run_id=run.run_id,
                            data={
                                "reason": "live supervision final gate",
                            },
                        )
                    )
                supervised_run_id = None
                stream_buffer = []
                time.sleep(max(0.2, config.poll_seconds))
                continue

            now_monotonic = time.monotonic()
            if now_monotonic - last_supervision_heartbeat >= config.reviewer_heartbeat_seconds:
                heartbeat = make_event(
                    kind=REVIEWER_SUPERVISION_HEARTBEAT,
                    actor=actor,
                    task_id=run.task_id,
                    chain_id=run.chain_id,
                    run_id=run.run_id,
                    data={
                        "reviewer_agent_id": config.agent_id,
                        "lease_expires_at": format_ts(utc_now() + timedelta(seconds=config.reviewer_lease_seconds)),
                    },
                )
                store.append(heartbeat)
                last_supervision_heartbeat = now_monotonic

            if now_monotonic - last_review >= config.review_cadence_seconds and stream_buffer:
                window = "\n".join(stream_buffer[-200:])
                actions = _reviewer_heuristic_actions(window)
                events: list[dict[str, Any]] = []
                for action_name, reason in actions:
                    events.append(
                        make_event(
                            kind=REVIEWER_CONTROL_REQUESTED,
                            actor=actor,
                            task_id=run.task_id,
                            chain_id=run.chain_id,
                            run_id=run.run_id,
                            data={
                                "action": action_name,
                                "message": reason,
                                "severity": {"resume": 0, "nudge": 1, "rework": 2, "pause": 3, "stop": 4}[action_name],
                                "source": "agent",
                                "ts_request": format_ts(utc_now()),
                            },
                        )
                    )
                _append_events(store, events)
                last_review = now_monotonic

        time.sleep(max(0.2, config.poll_seconds))


def _planner_loop(*, store: JsonlEventStore, config: RunConfig, agent_state: AgentState) -> None:
    # Planner execution currently uses task-claim and backend execution semantics.
    while True:
        claimed = _claim_next_task(store, config=config)
        if claimed is None:
            time.sleep(max(0.2, config.poll_seconds))
            continue
        task, run_id = claimed
        _execute_worker_task(store=store, config=config, agent_state=agent_state, task=task, run_id=run_id)


def _executor_loop(*, store: JsonlEventStore, config: RunConfig, agent_state: AgentState) -> None:
    while True:
        claimed = _claim_next_task(store, config=config)
        if claimed is None:
            time.sleep(max(0.2, config.poll_seconds))
            continue
        task, run_id = claimed
        _execute_worker_task(store=store, config=config, agent_state=agent_state, task=task, run_id=run_id)


def run_agent(config: RunConfig) -> None:
    store = JsonlEventStore(config.log_file)
    agent_state = store.load_agent_state(config.agent_id)

    if config.role == "reviewer":
        _reviewer_loop(store=store, config=config, agent_state=agent_state)
        return

    if config.role == "planner":
        _planner_loop(store=store, config=config, agent_state=agent_state)
        return

    _executor_loop(store=store, config=config, agent_state=agent_state)


def post_objective(
    *,
    log_file: str,
    objective: str,
    done_when: str,
    priority: int,
    preferred_backends: list[str],
    budgets: Budgets,
) -> str:
    store = JsonlEventStore(log_file)
    chain_id = str(uuid4())
    actor = Actor(type="user", id="cli", backend="system")
    events = [
        make_event(
            kind=OBJECTIVE_CREATED,
            actor=actor,
            chain_id=chain_id,
            data={
                "objective": objective,
                "done_when": done_when,
                "preferred_backends": preferred_backends,
            },
        ),
        _task_created_event(
            actor=actor,
            task_id=str(uuid4()),
            chain_id=chain_id,
            prompt=(
                "You are the planner. Break this objective into executor tasks and reviewer gates.\n"
                f"Objective: {objective}\n"
                f"Done when: {done_when}\n"
                "Return JSON action blocks only."
            ),
            role_target="planner",
            stage="planning",
            priority=priority,
            targets=TaskTargets(backends=preferred_backends, agent_ids=[]),
            quality_gate=QualityGate(
                acceptance_criteria=[done_when],
                required_checks=["objective-complete"],
                review_mode="hard",
            ),
            budgets=budgets,
            parent_task_id=None,
            attempt=1,
        ),
    ]
    _append_events(store, events)
    return chain_id


def post_task(
    *,
    log_file: str,
    prompt: str,
    target_role: str,
    target_backends: list[str],
    chain_id: str | None,
    priority: int,
    budgets: Budgets,
) -> tuple[str, str]:
    store = JsonlEventStore(log_file)
    actor = Actor(type="user", id="cli", backend="system")
    chain = chain_id or str(uuid4())
    task_id = str(uuid4())
    event = _task_created_event(
        actor=actor,
        task_id=task_id,
        chain_id=chain,
        prompt=prompt,
        role_target=target_role,
        stage="execution",
        priority=priority,
        targets=TaskTargets(backends=target_backends, agent_ids=[]),
        quality_gate=QualityGate(review_mode="hard"),
        budgets=budgets,
        parent_task_id=None,
        attempt=1,
    )
    store.append(event)
    return chain, task_id


def post_control(
    *,
    log_file: str,
    run_id: str,
    action: str,
    message: str,
    actor_id: str = "cli",
) -> None:
    store = JsonlEventStore(log_file)
    state = reduce_events(store.read_all())
    run = state.runs.get(run_id)
    if run is None:
        raise ValueError(f"run not found: {run_id}")

    event = make_event(
        kind=REVIEWER_CONTROL_REQUESTED,
        actor=Actor(type="user", id=actor_id, backend="system"),
        task_id=run.task_id,
        chain_id=run.chain_id,
        run_id=run_id,
        data={
            "action": action,
            "message": message,
            "severity": {"resume": 0, "nudge": 1, "rework": 2, "pause": 3, "stop": 4}[action],
            "source": "human",
            "ts_request": format_ts(utc_now()),
        },
    )
    store.append(event)


def requeue_task(*, log_file: str, task_id: str, reason: str) -> None:
    store = JsonlEventStore(log_file)
    state = reduce_events(store.read_all())
    task = state.tasks.get(task_id)
    if task is None:
        raise ValueError(f"task not found: {task_id}")

    event = make_event(
        kind="task.requeued",
        actor=Actor(type="user", id="cli", backend="system"),
        task_id=task.task_id,
        chain_id=task.chain_id,
        data={"reason": reason, "attempt": task.attempt + 1},
    )
    store.append(event)


def compact_log(*, log_file: str, before_ts: str) -> dict[str, int]:
    store = JsonlEventStore(log_file)
    cutoff = before_ts
    with store.locked() as locked:
        events = locked.read_events()
        keep: list[dict[str, Any]] = []
        dropped = 0
        for event in events:
            ts = str(event.get("ts") or "")
            if ts and ts < cutoff:
                dropped += 1
                continue
            keep.append({k: v for k, v in event.items() if k != "_line_no"})

        store.log_path.write_text("", encoding="utf-8")
        locked.append_events(keep)

    return {"kept": len(keep), "dropped": dropped}
