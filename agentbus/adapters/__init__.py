from __future__ import annotations

from agentbus.adapters.base import BackendAdapter
from agentbus.adapters.claude import ClaudeAdapter
from agentbus.adapters.codex import CodexAdapter
from agentbus.adapters.cursor import CursorAdapter


def get_adapter(backend: str) -> BackendAdapter:
    if backend == "codex":
        return CodexAdapter()
    if backend == "claude":
        return ClaudeAdapter()
    if backend == "cursor":
        return CursorAdapter()
    raise ValueError(f"unsupported backend: {backend}")
