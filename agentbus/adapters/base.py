from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from agentbus.models import AgentState, RunConfig


@dataclass
class AdapterCommand:
    argv: list[str]
    env: dict[str, str] | None = None


class BackendAdapter:
    name: str

    def build_command(
        self,
        *,
        prompt: str,
        config: RunConfig,
        agent_state: AgentState,
        resume: bool,
    ) -> AdapterCommand:
        raise NotImplementedError

    def after_run(
        self,
        *,
        stdout: str,
        stderr: str,
        agent_state: AgentState,
    ) -> None:
        del stdout, stderr, agent_state

    def extract_final_output(self, stdout: str, stderr: str) -> str:
        text = stdout.strip() or stderr.strip()
        if not text:
            return ""

        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            return text

        # Try extracting text from a trailing JSON object line.
        last = lines[-1]
        try:
            payload = json.loads(last)
        except json.JSONDecodeError:
            return last

        if isinstance(payload, dict):
            for key in ("final", "output", "text", "message", "content"):
                value = payload.get(key)
                if isinstance(value, str):
                    return value
        return last
