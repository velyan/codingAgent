# Operations

## Start Agents

- One process per role/backend per terminal.
- All processes point to the same `--log-file`.

Example:

```bash
agentbus run --log-file /tmp/agentbus.jsonl --agent-id exec1 --backend codex --role executor --cwd /repo --autonomous
agentbus run --log-file /tmp/agentbus.jsonl --agent-id rev1 --backend claude --role reviewer --cwd /repo --autonomous
```

## Monitor

```bash
agentbus status --log-file /tmp/agentbus.jsonl
agentbus tail --log-file /tmp/agentbus.jsonl --follow
```

## Manual Control

```bash
agentbus steer --log-file /tmp/agentbus.jsonl --run-id <id> --action pause --message "Hold"
agentbus steer --log-file /tmp/agentbus.jsonl --run-id <id> --action resume --message "Continue"
```

## Recovery

- If an executor dies, stale task lease eventually expires and another executor can claim the task.
- If reviewer dies, supervision lease eventually expires and another reviewer can claim.
- Use `requeue` for manual retry after terminal failure.

## Incident Playbook

1. `escalation.raised` appears in status/tail.
2. Inspect associated `run_id` and `task_id` stream history.
3. Post targeted `steer` or `post-task` for remediation.
4. Resume with explicit reviewer/executor actions.

## Compaction

For long-lived logs:

```bash
agentbus compact --log-file /tmp/agentbus.jsonl --before 2026-02-24T00:00:00Z
```

Compaction rewrites the file, dropping events older than `--before`.
