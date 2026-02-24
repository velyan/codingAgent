from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from agentbus.models import (
    DEFAULT_CONTROL_POLL_MS,
    DEFAULT_HEARTBEAT_SECONDS,
    DEFAULT_LEASE_SECONDS,
    DEFAULT_MAX_FAILURES,
    DEFAULT_MAX_HANDOFFS,
    DEFAULT_MAX_REWORKS,
    DEFAULT_POLL_SECONDS,
    DEFAULT_PRIORITY,
    DEFAULT_REVIEW_CADENCE_SECONDS,
    DEFAULT_REVIEWER_LEASE_SECONDS,
    DEFAULT_STREAM_CHUNK_BYTES,
    Budgets,
    RunConfig,
)
from agentbus.reducer import summarize_state, reduce_events
from agentbus.runner import compact_log, post_control, post_objective, post_task, requeue_task, run_agent
from agentbus.store import EventCursor, JsonlEventStore


def _parse_backends(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _add_common_budget_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--max-handoffs", type=int, default=DEFAULT_MAX_HANDOFFS)
    parser.add_argument("--max-reworks", type=int, default=DEFAULT_MAX_REWORKS)
    parser.add_argument("--max-failures", type=int, default=DEFAULT_MAX_FAILURES)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agentbus", description="Shared-log collaborative execution agents")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run an agent loop")
    run.add_argument("--log-file", required=True)
    run.add_argument("--agent-id", required=True)
    run.add_argument("--backend", required=True, choices=["codex", "claude", "cursor"])
    run.add_argument("--role", required=True, choices=["planner", "executor", "reviewer"])
    run.add_argument("--cwd", required=True)
    run.add_argument("--backend-cmd")
    run.add_argument("--model")
    run.add_argument("--autonomous", action="store_true")
    run.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    run.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    run.add_argument("--heartbeat-seconds", type=int, default=DEFAULT_HEARTBEAT_SECONDS)
    _add_common_budget_flags(run)
    run.add_argument("--escalation-file")
    run.add_argument("--stream-chunk-bytes", type=int, default=DEFAULT_STREAM_CHUNK_BYTES)
    run.add_argument("--control-poll-ms", type=int, default=DEFAULT_CONTROL_POLL_MS)
    run.add_argument("--review-cadence-seconds", type=float, default=DEFAULT_REVIEW_CADENCE_SECONDS)
    run.add_argument("--reviewer-lease-seconds", type=int, default=DEFAULT_REVIEWER_LEASE_SECONDS)

    post_obj = sub.add_parser("post-objective", help="Post a new objective chain")
    post_obj.add_argument("--log-file", required=True)
    post_obj.add_argument("--objective", required=True)
    post_obj.add_argument("--done-when", required=True)
    post_obj.add_argument("--priority", type=int, default=DEFAULT_PRIORITY)
    post_obj.add_argument("--preferred-backends", default="")
    _add_common_budget_flags(post_obj)

    post = sub.add_parser("post-task", help="Post a task")
    post.add_argument("--log-file", required=True)
    post.add_argument("--prompt", required=True)
    post.add_argument("--target-role", required=True, choices=["planner", "executor", "reviewer"])
    post.add_argument("--target-backend", action="append", default=[])
    post.add_argument("--chain-id")
    post.add_argument("--priority", type=int, default=DEFAULT_PRIORITY)
    _add_common_budget_flags(post)

    steer = sub.add_parser("steer", help="Request control action on a run")
    steer.add_argument("--log-file", required=True)
    steer.add_argument("--run-id", required=True)
    steer.add_argument("--action", required=True, choices=["nudge", "pause", "stop", "rework", "resume"])
    steer.add_argument("--message", default="")

    status = sub.add_parser("status", help="Summarize current state")
    status.add_argument("--log-file", required=True)
    status.add_argument("--chain-id")

    requeue = sub.add_parser("requeue", help="Requeue a failed task")
    requeue.add_argument("--log-file", required=True)
    requeue.add_argument("--task-id", required=True)
    requeue.add_argument("--reason", required=True)

    tail = sub.add_parser("tail", help="Tail event log")
    tail.add_argument("--log-file", required=True)
    tail.add_argument("--follow", action="store_true")
    tail.add_argument("--interval", type=float, default=0.5)

    compact = sub.add_parser("compact", help="Compact log by dropping events older than timestamp")
    compact.add_argument("--log-file", required=True)
    compact.add_argument("--before", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        cfg = RunConfig(
            log_file=args.log_file,
            agent_id=args.agent_id,
            backend=args.backend,
            role=args.role,
            cwd=str(Path(args.cwd).expanduser().resolve()),
            backend_cmd=args.backend_cmd,
            model=args.model,
            autonomous=bool(args.autonomous),
            poll_seconds=float(args.poll_seconds),
            lease_seconds=int(args.lease_seconds),
            heartbeat_seconds=int(args.heartbeat_seconds),
            max_handoffs=int(args.max_handoffs),
            max_reworks=int(args.max_reworks),
            max_failures=int(args.max_failures),
            escalation_file=args.escalation_file,
            stream_chunk_bytes=int(args.stream_chunk_bytes),
            control_poll_ms=int(args.control_poll_ms),
            review_cadence_seconds=float(args.review_cadence_seconds),
            reviewer_lease_seconds=int(args.reviewer_lease_seconds),
        )
        run_agent(cfg)
        return 0

    if args.command == "post-objective":
        chain_id = post_objective(
            log_file=args.log_file,
            objective=args.objective,
            done_when=args.done_when,
            priority=int(args.priority),
            preferred_backends=_parse_backends(args.preferred_backends),
            budgets=Budgets(
                max_handoffs=int(args.max_handoffs),
                max_reworks=int(args.max_reworks),
                max_failures=int(args.max_failures),
            ),
        )
        print(chain_id)
        return 0

    if args.command == "post-task":
        chain_id, task_id = post_task(
            log_file=args.log_file,
            prompt=args.prompt,
            target_role=args.target_role,
            target_backends=[str(item) for item in args.target_backend],
            chain_id=args.chain_id,
            priority=int(args.priority),
            budgets=Budgets(
                max_handoffs=int(args.max_handoffs),
                max_reworks=int(args.max_reworks),
                max_failures=int(args.max_failures),
            ),
        )
        print(json.dumps({"chain_id": chain_id, "task_id": task_id}))
        return 0

    if args.command == "steer":
        post_control(
            log_file=args.log_file,
            run_id=args.run_id,
            action=args.action,
            message=args.message,
        )
        return 0

    if args.command == "status":
        store = JsonlEventStore(args.log_file)
        state = reduce_events(store.read_all())
        summary = summarize_state(state, chain_id=args.chain_id)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    if args.command == "requeue":
        requeue_task(log_file=args.log_file, task_id=args.task_id, reason=args.reason)
        return 0

    if args.command == "tail":
        store = JsonlEventStore(args.log_file)
        cursor = EventCursor(line_no=0)
        while True:
            events, cursor = store.read_from(cursor)
            for event in events:
                print(json.dumps(event, ensure_ascii=True))
            if not args.follow:
                break
            time.sleep(max(0.1, float(args.interval)))
        return 0

    if args.command == "compact":
        result = compact_log(log_file=args.log_file, before_ts=args.before)
        print(json.dumps(result, sort_keys=True))
        return 0

    parser.error("unsupported command")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
