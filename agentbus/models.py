from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

Backend = Literal["codex", "claude", "cursor"]
Role = Literal["planner", "executor", "reviewer"]
TaskStatus = Literal["pending", "claimed", "running", "completed", "failed"]
ReviewMode = Literal["hard", "soft"]
RunStatus = Literal["running", "paused", "completed", "failed", "interrupted", "stopped"]
ControlAction = Literal["nudge", "pause", "stop", "rework", "resume"]

SUPPORTED_BACKENDS: tuple[Backend, ...] = ("codex", "claude", "cursor")
SUPPORTED_ROLES: tuple[Role, ...] = ("planner", "executor", "reviewer")
SUPPORTED_CONTROL_ACTIONS: tuple[ControlAction, ...] = (
    "nudge",
    "pause",
    "stop",
    "rework",
    "resume",
)

DEFAULT_POLL_SECONDS = 2.0
DEFAULT_LEASE_SECONDS = 120
DEFAULT_HEARTBEAT_SECONDS = 15
DEFAULT_PRIORITY = 100
DEFAULT_MAX_HANDOFFS = 8
DEFAULT_MAX_REWORKS = 2
DEFAULT_MAX_FAILURES = 3
DEFAULT_REVIEW_MODE: ReviewMode = "hard"
DEFAULT_MAX_OUTPUT_BYTES = 1024 * 1024
DEFAULT_STREAM_CHUNK_BYTES = 4096
DEFAULT_STREAM_FLUSH_MS = 250
DEFAULT_CONTROL_POLL_MS = 500
DEFAULT_REVIEW_CADENCE_SECONDS = 7.0
DEFAULT_REVIEWER_LEASE_SECONDS = 60
DEFAULT_REVIEWER_HEARTBEAT_SECONDS = 10

TASK_CREATED = "task.created"
TASK_CLAIMED = "task.claimed"
TASK_STARTED = "task.started"
TASK_HEARTBEAT = "task.heartbeat"
TASK_COMPLETED = "task.completed"
TASK_FAILED = "task.failed"
TASK_REQUEUED = "task.requeued"

OBJECTIVE_CREATED = "objective.created"
REVIEW_PASSED = "review.passed"
REVIEW_REWORK_REQUESTED = "review.rework_requested"
ESCALATION_RAISED = "escalation.raised"
CHAIN_COMPLETED = "chain.completed"
ACTION_REJECTED = "action.rejected"

STREAM_CHUNK = "stream.chunk"
REVIEWER_SUPERVISION_CLAIMED = "reviewer.supervision.claimed"
REVIEWER_SUPERVISION_HEARTBEAT = "reviewer.supervision.heartbeat"
REVIEWER_CONTROL_REQUESTED = "reviewer.control.requested"
REVIEWER_CONTROL_APPLIED = "reviewer.control.applied"
REVIEWER_CONTROL_REJECTED = "reviewer.control.rejected"
RUN_INTERRUPTED = "run.interrupted"
RUN_PAUSED = "run.paused"
RUN_RESUMED = "run.resumed"
RUN_RESTARTED = "run.restarted"

TASK_LIFECYCLE_KINDS: set[str] = {
    TASK_CREATED,
    TASK_CLAIMED,
    TASK_STARTED,
    TASK_HEARTBEAT,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_REQUEUED,
}

ACTIVE_RUN_STATUSES: set[str] = {"running", "paused", "interrupted"}

CONTROL_SEVERITY: dict[ControlAction, int] = {
    "resume": 0,
    "nudge": 1,
    "rework": 2,
    "pause": 3,
    "stop": 4,
}


@dataclass
class Actor:
    type: Literal["user", "agent", "system"]
    id: str
    backend: str = "system"

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "id": self.id, "backend": self.backend}


@dataclass
class QualityGate:
    acceptance_criteria: list[str] = field(default_factory=list)
    required_checks: list[str] = field(default_factory=list)
    review_mode: ReviewMode = DEFAULT_REVIEW_MODE

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "QualityGate":
        if payload is None:
            return cls()
        return cls(
            acceptance_criteria=[str(x) for x in payload.get("acceptance_criteria", [])],
            required_checks=[str(x) for x in payload.get("required_checks", [])],
            review_mode=str(payload.get("review_mode", DEFAULT_REVIEW_MODE)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "acceptance_criteria": list(self.acceptance_criteria),
            "required_checks": list(self.required_checks),
            "review_mode": self.review_mode,
        }


@dataclass
class Budgets:
    max_handoffs: int = DEFAULT_MAX_HANDOFFS
    max_reworks: int = DEFAULT_MAX_REWORKS
    max_failures: int = DEFAULT_MAX_FAILURES

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "Budgets":
        if payload is None:
            return cls()
        return cls(
            max_handoffs=int(payload.get("max_handoffs", DEFAULT_MAX_HANDOFFS)),
            max_reworks=int(payload.get("max_reworks", DEFAULT_MAX_REWORKS)),
            max_failures=int(payload.get("max_failures", DEFAULT_MAX_FAILURES)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_handoffs": self.max_handoffs,
            "max_reworks": self.max_reworks,
            "max_failures": self.max_failures,
        }


@dataclass
class TaskTargets:
    backends: list[str] = field(default_factory=list)
    agent_ids: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "TaskTargets":
        if payload is None:
            return cls()
        return cls(
            backends=[str(x) for x in payload.get("backends", [])],
            agent_ids=[str(x) for x in payload.get("agent_ids", [])],
        )

    def to_dict(self) -> dict[str, Any]:
        return {"backends": list(self.backends), "agent_ids": list(self.agent_ids)}


@dataclass
class TaskView:
    task_id: str
    chain_id: str
    created_at: datetime
    prompt: str
    role_target: Role
    stage: str
    priority: int
    quality_gate: QualityGate
    budgets: Budgets
    targets: TaskTargets
    status: TaskStatus = "pending"
    run_id: str | None = None
    parent_task_id: str | None = None
    attempt: int = 1
    claimed_by: str | None = None
    claimed_backend: str | None = None
    lease_expires_at: datetime | None = None
    last_error_signature: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def is_terminal(self) -> bool:
        return self.status in {"completed", "failed"}

    def claimable(self, now: datetime) -> bool:
        if self.status == "pending":
            return True
        if self.status in {"claimed", "running"} and self.lease_expires_at is not None:
            return now >= self.lease_expires_at
        return False


@dataclass
class ChainView:
    chain_id: str
    objective: str = ""
    done_when: str = ""
    preferred_backends: list[str] = field(default_factory=list)
    completed: bool = False
    paused: bool = False
    escalations: int = 0
    handoff_count: int = 0
    rework_count: int = 0
    failure_count: int = 0
    recent_failure_signatures: list[str] = field(default_factory=list)


@dataclass
class ControlRequestView:
    event_id: str
    run_id: str
    action: ControlAction
    message: str
    severity: int
    source: Literal["agent", "human"]
    actor_id: str
    requested_at: datetime


@dataclass
class RunView:
    run_id: str
    task_id: str
    chain_id: str
    executor_agent_id: str
    backend: str
    status: RunStatus
    started_at: datetime
    updated_at: datetime
    reviewer_agent_id: str | None = None
    reviewer_lease_expires_at: datetime | None = None
    completed_at: datetime | None = None

    def is_active(self, now: datetime) -> bool:
        if self.status not in ACTIVE_RUN_STATUSES:
            return False
        return self.completed_at is None


@dataclass
class ReducedState:
    tasks: dict[str, TaskView] = field(default_factory=dict)
    chains: dict[str, ChainView] = field(default_factory=dict)
    runs: dict[str, RunView] = field(default_factory=dict)
    pending_controls: dict[str, list[ControlRequestView]] = field(default_factory=dict)
    applied_control_event_ids: set[str] = field(default_factory=set)


@dataclass
class ExecutionResult:
    command: list[str]
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: int
    final_output: str
    session_ref: str | None = None


@dataclass
class StreamChunk:
    channel: Literal["stdout", "stderr"]
    text: str
    seq: int
    truncated: bool = False


@dataclass
class RunConfig:
    log_file: str
    agent_id: str
    backend: Backend
    role: Role
    cwd: str
    backend_cmd: str | None = None
    model: str | None = None
    autonomous: bool = False
    poll_seconds: float = DEFAULT_POLL_SECONDS
    lease_seconds: int = DEFAULT_LEASE_SECONDS
    heartbeat_seconds: int = DEFAULT_HEARTBEAT_SECONDS
    max_handoffs: int = DEFAULT_MAX_HANDOFFS
    max_reworks: int = DEFAULT_MAX_REWORKS
    max_failures: int = DEFAULT_MAX_FAILURES
    max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES
    escalation_file: str | None = None
    stream_chunk_bytes: int = DEFAULT_STREAM_CHUNK_BYTES
    stream_flush_ms: int = DEFAULT_STREAM_FLUSH_MS
    control_poll_ms: int = DEFAULT_CONTROL_POLL_MS
    review_cadence_seconds: float = DEFAULT_REVIEW_CADENCE_SECONDS
    reviewer_lease_seconds: int = DEFAULT_REVIEWER_LEASE_SECONDS
    reviewer_heartbeat_seconds: int = DEFAULT_REVIEWER_HEARTBEAT_SECONDS


@dataclass
class PendingTask:
    task: TaskView
    run_id: str


@dataclass
class AgentState:
    backend_state: dict[str, Any] = field(default_factory=dict)
    supervisor_state: dict[str, Any] = field(default_factory=dict)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    normalized = value.strip()
    if not normalized:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized)


def format_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
