import sys

from agentbus.streaming import run_streaming_subprocess


def test_streaming_subprocess_emits_chunks(tmp_path) -> None:
    captured: list[tuple[str, str]] = []
    command = [
        sys.executable,
        "-c",
        "import sys,time;"
        "sys.stdout.write('hello\\n');sys.stdout.flush();"
        "time.sleep(0.05);"
        "sys.stderr.write('warn\\n');sys.stderr.flush();"
        "time.sleep(0.05)",
    ]

    result = run_streaming_subprocess(
        command=command,
        cwd=str(tmp_path),
        env=None,
        chunk_bytes=8,
        flush_ms=10,
        on_chunk=lambda ch, txt: captured.append((ch, txt)),
        on_tick=None,
    )

    assert result.exit_code == 0
    assert "hello" in result.stdout
    assert "warn" in result.stderr
    assert any(channel == "stdout" for channel, _ in captured)
    assert any(channel == "stderr" for channel, _ in captured)
