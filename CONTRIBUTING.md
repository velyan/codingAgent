# Contributing

Thanks for contributing to AgentBus.

## Development Setup

```bash
python3 -m pip install -e .
python3 -m pip install pytest
```

## Run Tests

```bash
python3 -m pytest
```

## Style

- Keep dependencies minimal (stdlib-first).
- Preserve append-only protocol compatibility for event schema changes.
- Add or update tests with each behavior change.
- Keep docs in `docs/` updated when CLI/protocol changes.

## Pull Requests

- Explain behavior changes and migration implications.
- Include test coverage for new protocol/runtime behavior.
- Avoid breaking existing event keys without compatibility path.
