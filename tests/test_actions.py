from agentbus.actions import parse_agentbus_actions


def test_parse_valid_actions() -> None:
    payload = """
```json
{
  "agentbus_actions": [
    {"type": "steer", "run_id": "r1", "action": "pause", "message": "hold"},
    {"type": "create_task", "target_role": "executor", "prompt": "do x", "target_backend": ["codex"]}
  ]
}
```
"""
    result = parse_agentbus_actions(payload)
    assert len(result.actions) == 2
    assert not result.rejected_reasons
    assert result.actions[0].type == "steer"
    assert result.actions[0].payload["action"] == "pause"


def test_parse_rejects_invalid_action() -> None:
    payload = """
```json
{"agentbus_actions": [{"type": "steer", "run_id": "r1", "action": "bad"}]}
```
"""
    result = parse_agentbus_actions(payload)
    assert len(result.actions) == 0
    assert any("unsupported steer action" in reason for reason in result.rejected_reasons)


def test_parse_stream_json_envelope_extracts_actions() -> None:
    payload = "\n".join(
        [
            '{"type":"meta","run":"abc"}',
            '{"type":"delta","text":"still thinking"}',
            '{"type":"message","payload":{"text":"{\\"agentbus_actions\\":[{\\"type\\":\\"steer\\",\\"run_id\\":\\"run-1\\",\\"action\\":\\"pause\\",\\"message\\":\\"hold\\"}]}"}}',
        ]
    )
    result = parse_agentbus_actions(payload)
    assert len(result.actions) == 1
    assert result.actions[0].type == "steer"
    assert result.actions[0].payload["run_id"] == "run-1"
    assert result.actions[0].payload["action"] == "pause"


def test_parse_stream_json_fragmented_action_payload() -> None:
    payload = "\n".join(
        [
            '{"type":"delta","text":"{\\"agentbus_actions\\":["}',
            '{"type":"delta","text":"{\\"type\\":\\"steer\\",\\"run_id\\":\\"run-2\\",\\"action\\":\\"stop\\",\\"message\\":\\"danger\\"}"}',
            '{"type":"delta","text":"]}"}',
        ]
    )
    result = parse_agentbus_actions(payload)
    assert len(result.actions) == 1
    assert result.actions[0].type == "steer"
    assert result.actions[0].payload["run_id"] == "run-2"
    assert result.actions[0].payload["action"] == "stop"
