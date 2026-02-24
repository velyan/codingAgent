# AgentBus

AgentBus is a POSIX Python CLI for collaborative coding agents that coordinate through a single append-only JSONL log.

It supports:
- Multi-agent execution in separate terminal sessions.
- Multi-backend workers (`codex`, `claude`, `cursor-agent`).
- Live reviewer supervision over executor stream output.
- Mid-run steering controls (`nudge`, `pause`, `stop`, `rework`, `resume`).
- Deterministic control arbitration (`stop > pause > rework > nudge > resume`, then latest).
- Bounded autonomy guardrails (run timeout, pause timeout, restart/nudge caps, failure/handoff budgets).

## Why AgentBus

AgentBus is designed for:
- Better quality through continuous reviewer oversight.
- Faster delivery through parallel agents.
- Less human steering through objective-driven automation and bounded rework loops.

## Install

```bash
python3 -m pip install -e .
```

## Quickstart

### 1) Post an objective

```bash
agentbus post-objective \
  --log-file /tmp/agentbus.jsonl \
  --objective "Implement feature X" \
  --done-when "Tests pass and reviewer approves" \
  --preferred-backends codex,claude
```

### 2) Start executor (tab 1)

```bash
agentbus run \
  --log-file /tmp/agentbus.jsonl \
  --agent-id exec-codex \
  --backend codex \
  --role executor \
  --cwd /path/to/repo \
  --run-timeout-seconds 1800 \
  --pause-timeout-seconds 900 \
  --max-nudges-per-run 3 \
  --max-restarts-per-run 6 \
  --max-identical-failures 3 \
  --autonomous
```

### 3) Start reviewer (tab 2)

```bash
agentbus run \
  --log-file /tmp/agentbus.jsonl \
  --agent-id rev-claude \
  --backend claude \
  --role reviewer \
  --cwd /path/to/repo \
  --autonomous
```

### 4) Live manual steering (optional)

```bash
agentbus steer \
  --log-file /tmp/agentbus.jsonl \
  --run-id <run-uuid> \
  --action pause \
  --message "Pause and rethink approach"
```

### 5) Inspect system state

```bash
agentbus status --log-file /tmp/agentbus.jsonl
agentbus tail --log-file /tmp/agentbus.jsonl --follow
```

## CLI Commands

- `agentbus run`
- `agentbus post-objective`
- `agentbus post-task`
- `agentbus steer`
- `agentbus status`
- `agentbus requeue`
- `agentbus tail`
- `agentbus compact`

Run `agentbus <command> --help` for full options.

## Structured Action Contract

Agent output can include explicit JSON action blocks:

```json
{
  "agentbus_actions": [
    {
      "type": "steer",
      "run_id": "uuid",
      "action": "pause",
      "message": "hold"
    }
  ]
}
```

Supported actions:
- `create_task`
- `request_rework`
- `mark_objective_complete`
- `raise_escalation`
- `steer`

Invalid actions are rejected and logged as `action.rejected` or `reviewer.control.rejected`.

## Guardrail Defaults

- `run_timeout_seconds=1800`
- `pause_timeout_seconds=900`
- `max_nudges_per_run=3`
- `max_restarts_per_run=6`
- `max_identical_failures=3`

Guardrail breaches emit `guardrail.breached` and then `escalation.raised`, pausing the chain until explicit recovery actions.

## Architecture and Protocol

- [Architecture](docs/architecture.md)
- [Protocol](docs/protocol.md)
- [Operations](docs/operations.md)
- [Contributing](CONTRIBUTING.md)

## Runtime Requirements

- Python 3.9+
- macOS or Linux (POSIX file locking)

## License

MIT (see [LICENSE](LICENSE)).
