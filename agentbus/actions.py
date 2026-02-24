from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from agentbus.models import SUPPORTED_CONTROL_ACTIONS

_JSON_FENCE_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)
_MISSING = object()


@dataclass
class ParsedAction:
    type: str
    payload: dict[str, Any]


@dataclass
class ParseResult:
    actions: list[ParsedAction]
    rejected_reasons: list[str]


class ActionValidationError(ValueError):
    pass


def _coerce_int(value: Any, *, field: str, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise ActionValidationError(f"{field} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ActionValidationError(f"{field} must be an integer") from exc


def _coerce_string_list(
    value: Any,
    *,
    field: str,
    default: list[str] | None = None,
    allow_single_string: bool = False,
) -> list[str]:
    if value is _MISSING:
        return list(default or [])
    if value is None:
        raise ActionValidationError(f"{field} must be a list")
    if allow_single_string and isinstance(value, str):
        return [value]
    if not isinstance(value, list):
        raise ActionValidationError(f"{field} must be a list")
    return [str(item) for item in value]


def _extract_json_payloads(text: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    seen: set[str] = set()
    stream_fragments: list[str] = []

    def _push(obj: dict[str, Any]) -> None:
        try:
            signature = json.dumps(obj, sort_keys=True, ensure_ascii=True)
        except (TypeError, ValueError):
            signature = repr(obj)
        if signature in seen:
            return
        seen.add(signature)
        payloads.append(obj)

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            _push(value)
            # Keep text-bearing envelope fields to reconstruct stream-json partial output.
            is_stream_envelope = "agentbus_actions" not in value and any(
                key in value for key in ("text", "delta", "content")
            )
            if is_stream_envelope:
                for key in ("text", "delta", "content"):
                    field = value.get(key)
                    if isinstance(field, str):
                        stream_fragments.append(field)
            for nested in value.values():
                _walk(nested)
            return
        if isinstance(value, list):
            for nested in value:
                _walk(nested)
            return
        if isinstance(value, str):
            for match in _JSON_FENCE_RE.finditer(value):
                raw = match.group(1)
                try:
                    obj = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    _walk(obj)
            stripped = value.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    obj = json.loads(stripped)
                except json.JSONDecodeError:
                    obj = None
                if isinstance(obj, dict):
                    _walk(obj)
            return

    def _extract_balanced_json_objects(raw: str) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        start_idx: int | None = None
        depth = 0
        in_string = False
        escaped = False
        for idx, ch in enumerate(raw):
            if in_string:
                if escaped:
                    escaped = False
                    continue
                if ch == "\\":
                    escaped = True
                    continue
                if ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
                continue
            if ch == "{":
                if depth == 0:
                    start_idx = idx
                depth += 1
                continue
            if ch == "}":
                if depth == 0:
                    continue
                depth -= 1
                if depth == 0 and start_idx is not None:
                    candidate = raw[start_idx : idx + 1]
                    try:
                        obj = json.loads(candidate)
                    except json.JSONDecodeError:
                        obj = None
                    if isinstance(obj, dict):
                        found.append(obj)
                    start_idx = None
        return found

    # Parse JSON code fences from the full text.
    for match in _JSON_FENCE_RE.finditer(text):
        raw = match.group(1)
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            _walk(obj)

    # Parse full output as a single JSON object when applicable.
    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            obj = None
        if isinstance(obj, dict):
            _walk(obj)

    # Parse JSONL / stream-json style line envelopes.
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            _walk(obj)

    if stream_fragments:
        reconstructed = "".join(stream_fragments)
        for obj in _extract_balanced_json_objects(reconstructed):
            _walk(obj)

    return payloads


def _validate_action(raw: dict[str, Any]) -> ParsedAction:
    action_type = str(raw.get("type") or "").strip()
    if not action_type:
        raise ActionValidationError("missing action type")

    if action_type == "steer":
        run_id = str(raw.get("run_id") or "").strip()
        action = str(raw.get("action") or "").strip()
        if not run_id:
            raise ActionValidationError("steer requires run_id")
        if action not in SUPPORTED_CONTROL_ACTIONS:
            raise ActionValidationError(f"unsupported steer action: {action}")
        return ParsedAction(
            type="steer",
            payload={
                "run_id": run_id,
                "action": action,
                "message": str(raw.get("message") or "").strip(),
            },
        )

    if action_type == "create_task":
        target_role = str(raw.get("target_role") or "").strip()
        prompt = str(raw.get("prompt") or "").strip()
        if target_role not in {"planner", "executor", "reviewer"}:
            raise ActionValidationError("create_task requires valid target_role")
        if not prompt:
            raise ActionValidationError("create_task requires prompt")
        return ParsedAction(
            type="create_task",
            payload={
                "target_role": target_role,
                "target_backend": _coerce_string_list(
                    raw.get("target_backend", _MISSING),
                    field="target_backend",
                    default=[],
                    allow_single_string=True,
                ),
                "prompt": prompt,
                "priority": _coerce_int(raw.get("priority", 100), field="priority", default=100),
                "acceptance_criteria": _coerce_string_list(
                    raw.get("acceptance_criteria", _MISSING),
                    field="acceptance_criteria",
                    default=[],
                ),
                "required_checks": _coerce_string_list(
                    raw.get("required_checks", _MISSING),
                    field="required_checks",
                    default=[],
                ),
                "review_mode": str(raw.get("review_mode", "hard")),
                "stage": str(raw.get("stage", "execution")),
            },
        )

    if action_type == "request_rework":
        task_id = str(raw.get("task_id") or "").strip()
        message = str(raw.get("message") or "").strip()
        if not task_id:
            raise ActionValidationError("request_rework requires task_id")
        return ParsedAction(type="request_rework", payload={"task_id": task_id, "message": message})

    if action_type == "mark_objective_complete":
        chain_id = str(raw.get("chain_id") or "").strip()
        if not chain_id:
            raise ActionValidationError("mark_objective_complete requires chain_id")
        return ParsedAction(type="mark_objective_complete", payload={"chain_id": chain_id})

    if action_type == "raise_escalation":
        chain_id = str(raw.get("chain_id") or "").strip()
        reason = str(raw.get("reason") or "").strip()
        if not chain_id:
            raise ActionValidationError("raise_escalation requires chain_id")
        if not reason:
            raise ActionValidationError("raise_escalation requires reason")
        return ParsedAction(type="raise_escalation", payload={"chain_id": chain_id, "reason": reason})

    raise ActionValidationError(f"unsupported action type: {action_type}")


def parse_agentbus_actions(text: str) -> ParseResult:
    payloads = _extract_json_payloads(text)
    actions: list[ParsedAction] = []
    rejected: list[str] = []

    for payload in payloads:
        raw_actions = payload.get("agentbus_actions")
        if not isinstance(raw_actions, list):
            continue
        for item in raw_actions:
            if not isinstance(item, dict):
                rejected.append("action item is not an object")
                continue
            try:
                actions.append(_validate_action(item))
            except (ActionValidationError, TypeError, ValueError) as exc:
                rejected.append(str(exc))
            except Exception as exc:  # pragma: no cover - defensive safety net
                rejected.append(f"unexpected action validation error: {exc}")

    return ParseResult(actions=actions, rejected_reasons=rejected)
