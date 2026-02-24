from __future__ import annotations

import queue
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Callable


@dataclass
class StreamRunResult:
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: int


def _reader_thread(
    *,
    stream,
    channel: str,
    chunk_bytes: int,
    out_queue: "queue.Queue[tuple[str, bytes | None]]",
) -> None:
    try:
        while True:
            chunk = stream.read(chunk_bytes)
            if not chunk:
                break
            out_queue.put((channel, chunk))
    finally:
        out_queue.put((channel, None))


def run_streaming_subprocess(
    *,
    command: list[str],
    cwd: str,
    env: dict[str, str] | None,
    chunk_bytes: int,
    flush_ms: int,
    on_chunk: Callable[[str, str], None],
    on_tick: Callable[[subprocess.Popen[bytes]], None] | None = None,
) -> StreamRunResult:
    started = time.monotonic()
    proc = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        env=env,
    )

    if proc.stdout is None or proc.stderr is None:
        raise RuntimeError("failed to capture process output")

    out_q: "queue.Queue[tuple[str, bytes | None]]" = queue.Queue()
    threads = [
        threading.Thread(
            target=_reader_thread,
            kwargs={
                "stream": proc.stdout,
                "channel": "stdout",
                "chunk_bytes": chunk_bytes,
                "out_queue": out_q,
            },
            daemon=True,
        ),
        threading.Thread(
            target=_reader_thread,
            kwargs={
                "stream": proc.stderr,
                "channel": "stderr",
                "chunk_bytes": chunk_bytes,
                "out_queue": out_q,
            },
            daemon=True,
        ),
    ]

    for thread in threads:
        thread.start()

    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    eof_channels: set[str] = set()

    flush_sleep = max(0.001, flush_ms / 1000.0)

    while True:
        drained = False
        while True:
            try:
                channel, chunk = out_q.get_nowait()
            except queue.Empty:
                break
            drained = True
            if chunk is None:
                eof_channels.add(channel)
                continue
            text = chunk.decode("utf-8", errors="replace")
            if channel == "stdout":
                stdout_parts.append(text)
            else:
                stderr_parts.append(text)
            on_chunk(channel, text)

        if on_tick is not None:
            on_tick(proc)

        if proc.poll() is not None and len(eof_channels) == 2 and not drained:
            break

        time.sleep(flush_sleep)

    for thread in threads:
        thread.join(timeout=1.0)

    ended = time.monotonic()
    return StreamRunResult(
        stdout="".join(stdout_parts),
        stderr="".join(stderr_parts),
        exit_code=proc.returncode if proc.returncode is not None else 1,
        duration_ms=int((ended - started) * 1000),
    )
