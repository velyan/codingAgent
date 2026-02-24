from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from agentbus.models import SUPPORTED_CONTROL_ACTIONS

_JSON_FENCE_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)


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


def _extract_json_payloads(text: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []

    for match in _JSON_FENCE_RE.finditer(text):
        raw = match.group(1)
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            payloads.append(obj)

    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            obj = None
        if isinstance(obj, dict):
            payloads.append(obj)

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
        backends = raw.get("target_backend") or []
        if isinstance(backends, str):
            backends = [backends]
        return ParsedAction(
            type="create_task",
            payload={
                "target_role": target_role,
                "target_backend": [str(x) for x in backends],
                "prompt": prompt,
                "priority": int(raw.get("priority", 100)),
                "acceptance_criteria": [str(x) for x in raw.get("acceptance_criteria", [])],
                "required_checks": [str(x) for x in raw.get("required_checks", [])],
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
            except ActionValidationError as exc:
                rejected.append(str(exc))

    return ParseResult(actions=actions, rejected_reasons=rejected)
