from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import fcntl

from agentbus.models import AgentState


@dataclass
class EventCursor:
    line_no: int = 0


class LockedStore:
    def __init__(self, parent: "JsonlEventStore") -> None:
        self._parent = parent

    def read_events(self) -> list[dict[str, Any]]:
        return self._parent._read_events_unlocked()

    def read_events_from(self, cursor: EventCursor) -> tuple[list[dict[str, Any]], EventCursor]:
        return self._parent._read_events_from_unlocked(cursor)

    def append_events(self, events: list[dict[str, Any]]) -> None:
        self._parent._append_events_unlocked(events)


class JsonlEventStore:
    def __init__(self, log_file: str) -> None:
        self.log_path = Path(log_file).expanduser().resolve()
        self.lock_path = Path(f"{self.log_path}.lock")
        self.state_root = Path(f"{self.log_path}.state")
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_root.mkdir(parents=True, exist_ok=True)
        if not self.log_path.exists():
            self.log_path.touch()

    @contextmanager
    def locked(self) -> Iterator[LockedStore]:
        with self.lock_path.open("a+") as lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
            try:
                yield LockedStore(self)
            finally:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)

    def read_all(self) -> list[dict[str, Any]]:
        with self.locked() as locked:
            return locked.read_events()

    def read_from(self, cursor: EventCursor) -> tuple[list[dict[str, Any]], EventCursor]:
        with self.locked() as locked:
            return locked.read_events_from(cursor)

    def append(self, event: dict[str, Any]) -> None:
        self.append_many([event])

    def append_many(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        with self.locked() as locked:
            locked.append_events(events)

    def _read_events_unlocked(self) -> list[dict[str, Any]]:
        events, _ = self._read_events_from_unlocked(EventCursor(line_no=0))
        return events

    def _read_events_from_unlocked(self, cursor: EventCursor) -> tuple[list[dict[str, Any]], EventCursor]:
        events: list[dict[str, Any]] = []
        line_no = 0
        if not self.log_path.exists():
            return events, cursor

        with self.log_path.open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                if line_no <= cursor.line_no:
                    continue
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    event["_line_no"] = line_no
                    events.append(event)

        next_cursor = EventCursor(line_no=max(cursor.line_no, line_no))
        return events, next_cursor

    def _append_events_unlocked(self, events: list[dict[str, Any]]) -> None:
        with self.log_path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, separators=(",", ":"), ensure_ascii=True))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

    def agent_state_path(self, agent_id: str) -> Path:
        safe_agent_id = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in agent_id)
        return self.state_root / f"{safe_agent_id}.json"

    def load_agent_state(self, agent_id: str) -> AgentState:
        path = self.agent_state_path(agent_id)
        if not path.exists():
            return AgentState()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return AgentState()

        backend_state = payload.get("backend_state")
        if not isinstance(backend_state, dict):
            backend_state = {}
        supervisor_state = payload.get("supervisor_state")
        if not isinstance(supervisor_state, dict):
            supervisor_state = {}
        return AgentState(backend_state=backend_state, supervisor_state=supervisor_state)

    def save_agent_state(self, agent_id: str, state: AgentState) -> None:
        path = self.agent_state_path(agent_id)
        payload = {
            "backend_state": state.backend_state,
            "supervisor_state": state.supervisor_state,
        }
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
