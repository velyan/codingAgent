# Protocol

AgentBus protocol is event-sourced. Each JSON line is one event object.

## Base Event Shape

```json
{
  "v": 1,
  "event_id": "uuid",
  "ts": "2026-02-24T19:00:00Z",
  "kind": "task.created",
  "actor": {"type": "agent", "id": "exec-1", "backend": "codex"},
  "task_id": "uuid-or-null",
  "chain_id": "uuid-or-null",
  "run_id": "uuid-or-null",
  "data": {}
}
```

## Core Task Events

- `task.created`
- `task.claimed`
- `task.started`
- `task.heartbeat`
- `task.completed`
- `task.failed`
- `task.requeued`

`task.created.data` fields:
- `prompt`
- `role_target`
- `stage`
- `priority`
- `targets.backends[]`
- `targets.agent_ids[]`
- `quality_gate.acceptance_criteria[]`
- `quality_gate.required_checks[]`
- `quality_gate.review_mode`
- `budgets.max_handoffs`
- `budgets.max_reworks`
- `budgets.max_failures`

## Chain Events

- `objective.created`
- `review.passed`
- `review.rework_requested`
- `escalation.raised`
- `chain.completed`

## Live Supervision Events

- `stream.chunk`
- `reviewer.supervision.claimed`
- `reviewer.supervision.heartbeat`
- `reviewer.control.requested`
- `reviewer.control.applied`
- `reviewer.control.rejected`
- `run.interrupted`
- `run.paused`
- `run.resumed`
- `run.restarted`

`stream.chunk.data`:
- `seq`
- `channel` (`stdout` or `stderr`)
- `text`
- `truncated`

`reviewer.control.requested.data`:
- `action` (`nudge`, `pause`, `stop`, `rework`, `resume`)
- `message`
- `severity`
- `source` (`agent` or `human`)
- `ts_request`

## Structured Action Envelope

```json
{
  "agentbus_actions": [
    {"type": "create_task", "target_role": "executor", "prompt": "..."},
    {"type": "steer", "run_id": "uuid", "action": "pause", "message": "..."}
  ]
}
```

Unsupported or malformed actions are ignored and logged via rejection events.
