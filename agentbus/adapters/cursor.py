from __future__ import annotations

import os
import subprocess

from agentbus.adapters.base import AdapterCommand, BackendAdapter
from agentbus.models import AgentState, RunConfig


class CursorAdapter(BackendAdapter):
    name = "cursor"

    def _ensure_chat_id(self, *, cmd: str, agent_state: AgentState) -> str:
        chat_id = str(agent_state.backend_state.get("cursor_chat_id") or "")
        if chat_id:
            return chat_id

        try:
            output = subprocess.check_output([cmd, "create-chat"], text=True).strip()
        except subprocess.CalledProcessError:
            output = ""

        chat_id = output.splitlines()[-1].strip() if output else ""
        if not chat_id:
            # Fallback to "--continue" semantics if chat creation fails.
            chat_id = "latest"
        agent_state.backend_state["cursor_chat_id"] = chat_id
        return chat_id

    def build_command(
        self,
        *,
        prompt: str,
        config: RunConfig,
        agent_state: AgentState,
        resume: bool,
    ) -> AdapterCommand:
        del resume
        cmd = config.backend_cmd or "cursor-agent"
        chat_id = self._ensure_chat_id(cmd=cmd, agent_state=agent_state)

        argv = [
            cmd,
            "-p",
            "--output-format",
            "stream-json",
            "--stream-partial-output",
            "--workspace",
            config.cwd,
        ]
        if chat_id != "latest":
            argv.extend(["--resume", chat_id])
        else:
            argv.append("--continue")

        if config.model:
            argv.extend(["--model", config.model])

        argv.append(prompt)
        return AdapterCommand(argv=argv, env=dict(os.environ))
