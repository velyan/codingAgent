from pathlib import Path

from agentbus import cli


def test_default_planner_command_arguments(tmp_path: Path) -> None:
    parser = cli._build_parser()
    args = parser.parse_args(
        [
            "planner",
            "--log-file",
            str(tmp_path / "agentbus.jsonl"),
            "--cwd",
            str(tmp_path),
        ]
    )

    assert args.command == "planner"
    assert args.role == "planner"
    assert args.agent_id == "planner-agent"
    assert args.backend == "codex"
    assert args.autonomous is True


def test_default_executor_command_arguments(tmp_path: Path) -> None:
    parser = cli._build_parser()
    args = parser.parse_args(
        [
            "executor",
            "--log-file",
            str(tmp_path / "agentbus.jsonl"),
            "--cwd",
            str(tmp_path),
        ]
    )

    assert args.command == "executor"
    assert args.role == "executor"
    assert args.agent_id == "executor-agent"
    assert args.backend == "codex"
    assert args.autonomous is True


def test_default_reviewer_command_arguments(tmp_path: Path) -> None:
    parser = cli._build_parser()
    args = parser.parse_args(
        [
            "reviewer",
            "--log-file",
            str(tmp_path / "agentbus.jsonl"),
            "--cwd",
            str(tmp_path),
        ]
    )

    assert args.command == "reviewer"
    assert args.role == "reviewer"
    assert args.agent_id == "reviewer-agent"
    assert args.backend == "claude"
    assert args.autonomous is True


def test_run_keeps_explicit_requirements() -> None:
    parser = cli._build_parser()
    args = parser.parse_args(
        [
            "run",
            "--log-file",
            "/tmp/x.jsonl",
            "--agent-id",
            "r1",
            "--backend",
            "codex",
            "--role",
            "planner",
            "--cwd",
            "/tmp",
        ]
    )

    assert args.command == "run"
    assert args.role == "planner"
    assert args.backend == "codex"
    assert args.agent_id == "r1"
