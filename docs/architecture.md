# Architecture

AgentBus uses a shared append-only JSONL file as the source of truth.

## Components

- `agentbus/store.py`: append/read with `fcntl` lock and incremental cursor reads.
- `agentbus/reducer.py`: deterministic event fold into task/chain/run/control state.
- `agentbus/runner.py`: role loops (planner/executor/reviewer), claiming, execution, supervision, steering.
- `agentbus/streaming.py`: nonblocking subprocess stream capture.
- `agentbus/actions.py`: strict structured action parsing and validation.
- `agentbus/adapters/*`: backend-specific command/session handling.

## Runtime Roles

- Planner: decomposes objectives and emits tasks.
- Executor: runs claimed tasks, streams output, applies live controls.
- Reviewer: claims supervision of active runs, analyzes stream windows, emits controls.

## Control Arbitration

Control precedence:
1. `stop`
2. `pause`
3. `rework`
4. `nudge`
5. `resume`

Tie-breaker: latest request timestamp.

## Sequence

```mermaid
sequenceDiagram
  participant U as User
  participant P as Planner Agent
  participant E as Executor Agent
  participant R as Reviewer Agent
  participant L as Shared JSONL Log

  U->>L: objective.created
  P->>L: task.created (executor)
  E->>L: task.claimed + task.started
  E->>L: stream.chunk (stdout/stderr)
  R->>L: reviewer.supervision.claimed
  R->>L: reviewer.control.requested (pause/nudge/stop/rework/resume)
  E->>L: reviewer.control.applied + run.* events
  E->>L: task.completed or task.failed
  R->>L: review.passed or review.rework_requested
```

## Safety Guards

- Single reviewer supervision lease per run.
- Lease expiration enables recovery from crashed agents.
- Duplicate task suppression via task fingerprint checks.
- Rework/failure/handoff budgets enforce bounded autonomy.
