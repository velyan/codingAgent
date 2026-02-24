from __future__ import annotations

import os
from pathlib import Path

from agentbus.adapters.base import AdapterCommand, BackendAdapter
from agentbus.models import AgentState, RunConfig


class CodexAdapter(BackendAdapter):
    name = "codex"

    def build_command(
        self,
        *,
        prompt: str,
        config: RunConfig,
        agent_state: AgentState,
        resume: bool,
    ) -> AdapterCommand:
        cmd = config.backend_cmd or "codex"
        has_session = bool(agent_state.backend_state.get("codex_has_session"))

        if resume and has_session:
            argv = [cmd, "exec", "resume", "--last", "--json", prompt]
        else:
            argv = [cmd, "exec", "--json", "--cd", config.cwd, prompt]

        # Isolate codex history/session artifacts per agent process.
        state_dir = Path(config.log_file).expanduser().resolve().parent / ".agentbus" / config.agent_id / "codex_home"
        state_dir.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ)
        env["CODEX_HOME"] = str(state_dir)

        if config.model:
            argv.extend(["--model", config.model])

        return AdapterCommand(argv=argv, env=env)

    def after_run(self, *, stdout: str, stderr: str, agent_state: AgentState) -> None:
        del stdout, stderr
        agent_state.backend_state["codex_has_session"] = True
