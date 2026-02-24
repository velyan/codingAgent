from __future__ import annotations

import os
from uuid import uuid4

from agentbus.adapters.base import AdapterCommand, BackendAdapter
from agentbus.models import AgentState, RunConfig


class ClaudeAdapter(BackendAdapter):
    name = "claude"

    def build_command(
        self,
        *,
        prompt: str,
        config: RunConfig,
        agent_state: AgentState,
        resume: bool,
    ) -> AdapterCommand:
        del resume
        cmd = config.backend_cmd or "claude"
        session_id = str(agent_state.backend_state.get("claude_session_id") or "")
        if not session_id:
            session_id = str(uuid4())
            agent_state.backend_state["claude_session_id"] = session_id

        argv = [
            cmd,
            "-p",
            "--output-format",
            "stream-json",
            "--session-id",
            session_id,
            prompt,
        ]

        if config.model:
            argv.extend(["--model", config.model])

        return AdapterCommand(argv=argv, env=dict(os.environ))
