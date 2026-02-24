from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

Backend = Literal["codex", "claude", "cursor"]
Role = Literal["planner", "executor", "reviewer"]
TaskStatus = Literal["pending", "claimed", "running", "completed", "failed"]
ReviewMode = Literal["hard", "soft"]

SUPPORTED_BACKENDS: tuple[Backend, ...] = ("codex", "claude", "cursor")
SUPPORTED_ROLES: tuple[Role, ...] = ("planner", "executor", "reviewer")

DEFAULT_POLL_SECONDS = 2.0
DEFAULT_LEASE_SECONDS = 120
DEFAULT_HEARTBEAT_SECONDS = 15
DEFAULT_PRIORITY = 100
DEFAULT_MAX_HANDOFFS = 8
DEFAULT_MAX_REWORKS = 2
DEFAULT_MAX_FAILURES = 3
DEFAULT_REVIEW_MODE: ReviewMode = "hard"
DEFAULT_MAX_OUTPUT_BYTES = 1024 * 1024

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

TASK_LIFECYCLE_KINDS: set[str] = {
    TASK_CREATED,
    TASK_CLAIMED,
    TASK_STARTED,
    TASK_HEARTBEAT,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_REQUEUED,
}


@dataclass(slots=True)
class Actor:
    type: Literal["user", "agent", "system"]
    id: str
    backend: str = "system"

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "id": self.id, "backend": self.backend}


@dataclass(slots=True)
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


@dataclass(slots=True)
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


@dataclass(slots=True)
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


@dataclass(slots=True)
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


@dataclass(slots=True)
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


@dataclass(slots=True)
class ReducedState:
    tasks: dict[str, TaskView] = field(default_factory=dict)
    chains: dict[str, ChainView] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionResult:
    command: list[str]
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: int
    final_output: str
    session_ref: str | None = None


@dataclass(slots=True)
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


@dataclass(slots=True)
class PendingTask:
    task: TaskView
    run_id: str


@dataclass(slots=True)
class AgentState:
    backend_state: dict[str, Any] = field(default_factory=dict)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def format_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
