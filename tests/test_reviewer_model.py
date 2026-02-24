import subprocess
from pathlib import Path

import agentbus.runner as runner
from agentbus.models import AgentState, RunConfig


class _FakeAdapter:
    def build_command(self, *, prompt, config, agent_state, resume):
        del prompt, config, agent_state, resume

        class _Cmd:
            argv = ["fake-cmd", "arg"]
            env = None

        return _Cmd()

    def after_run(self, *, stdout, stderr, agent_state):
        del stdout, stderr
        agent_state.backend_state["reviewed"] = True


class _Completed:
    def __init__(self, stdout: str, stderr: str = "") -> None:
        self.stdout = stdout
        self.stderr = stderr


def test_reviewer_model_actions_parses_steer(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runner, "get_adapter", lambda _backend: _FakeAdapter())
    called_argv = []

    def _fake_run(argv, cwd, env, capture_output, text, timeout):
        called_argv.extend(argv)
        del cwd, env, capture_output, text, timeout
        return _Completed(
            '{"agentbus_actions":[{"type":"steer","run_id":"run-1","action":"pause","message":"hold"}]}'
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    cfg = RunConfig(
        log_file=str(tmp_path / "bus.jsonl"),
        agent_id="rev1",
        backend="claude",
        role="reviewer",
        cwd=str(tmp_path),
    )
    state = AgentState()

    actions, rejected = runner._reviewer_model_actions(
        config=cfg,
        agent_state=state,
        run_id="run-1",
        chain_objective="obj",
        done_when="done",
        task_prompt="task",
        window_text="some output",
    )

    assert actions == [("pause", "hold")]
    assert not rejected
    assert state.backend_state.get("reviewed") is True
    assert "--permission-mode" in called_argv
    assert "plan" in called_argv


def test_reviewer_model_actions_rejects_wrong_run(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runner, "get_adapter", lambda _backend: _FakeAdapter())

    def _fake_run(argv, cwd, env, capture_output, text, timeout):
        del argv, cwd, env, capture_output, text, timeout
        return _Completed(
            '{"agentbus_actions":[{"type":"steer","run_id":"other","action":"stop","message":"x"}]}'
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    cfg = RunConfig(
        log_file=str(tmp_path / "bus.jsonl"),
        agent_id="rev1",
        backend="claude",
        role="reviewer",
        cwd=str(tmp_path),
    )
    actions, rejected = runner._reviewer_model_actions(
        config=cfg,
        agent_state=AgentState(),
        run_id="run-1",
        chain_objective="obj",
        done_when="done",
        task_prompt="task",
        window_text="some output",
    )

    assert not actions
    assert any("run_id mismatch" in reason for reason in rejected)


def test_enforce_reviewer_readonly_command_flags() -> None:
    codex = runner._enforce_reviewer_readonly_command("codex", ["codex", "exec", "--json", "prompt"])
    assert "--sandbox" in codex
    assert "read-only" in codex

    claude = runner._enforce_reviewer_readonly_command("claude", ["claude", "-p", "prompt"])
    assert "--permission-mode" in claude
    assert "plan" in claude

    cursor = runner._enforce_reviewer_readonly_command("cursor", ["cursor-agent", "-p", "prompt"])
    assert "--mode" in cursor
    assert "plan" in cursor
