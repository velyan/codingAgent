#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: agentbus-collab \
  --log-file <path> \
  --cwd <path> \
  [--planner-agent-id planner-1] \
  [--executor-agent-id executor-1] \
  [--reviewer-agent-id reviewer-1] \
  [--planner-backend codex|claude|cursor] \
  [--executor-backend codex|claude|cursor] \
  [--reviewer-backend codex|claude|cursor] \
  [--planner-model <model>] \
  [--executor-model <model>] \
  [--reviewer-model <model>] \
  [--no-watch]

This script starts three local agents and prints per-agent process ids.
By default, it also prints a live, colorized event stream from the shared log
and prefixes events by actor role.
USAGE
}

log_file=""
cwd=""
planner_agent="planner-1"
executor_agent="executor-1"
reviewer_agent="reviewer-1"
planner_backend="codex"
executor_backend="codex"
reviewer_backend="claude"
planner_model=""
executor_model=""
reviewer_model=""
watch=1

while (( $# > 0 )); do
  case "$1" in
    --log-file)
      log_file="$2"
      shift 2
      ;;
    --cwd)
      cwd="$2"
      shift 2
      ;;
    --planner-agent-id)
      planner_agent="$2"
      shift 2
      ;;
    --executor-agent-id)
      executor_agent="$2"
      shift 2
      ;;
    --reviewer-agent-id)
      reviewer_agent="$2"
      shift 2
      ;;
    --planner-backend)
      planner_backend="$2"
      shift 2
      ;;
    --executor-backend)
      executor_backend="$2"
      shift 2
      ;;
    --reviewer-backend)
      reviewer_backend="$2"
      shift 2
      ;;
    --planner-model)
      planner_model="$2"
      shift 2
      ;;
    --executor-model)
      executor_model="$2"
      shift 2
      ;;
    --reviewer-model)
      reviewer_model="$2"
      shift 2
      ;;
    --no-watch)
      watch=0
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$log_file" || -z "$cwd" ]]; then
  echo "--log-file and --cwd are required"
  usage
  exit 1
fi

if ! command -v agentbus >/dev/null 2>&1; then
  echo "agentbus executable not found in PATH"
  exit 1
fi

mkdir -p "$(dirname "$log_file")"
touch "$log_file"

log_dir="$(dirname "$log_file")/.agentbus"
mkdir -p "$log_dir"

declare -a pids
monitor_pid=""

cleanup() {
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  if [[ -n "$monitor_pid" ]]; then
    if kill -0 "$monitor_pid" >/dev/null 2>&1; then
      kill "$monitor_pid" >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT INT TERM

start_agent() {
  local role=$1
  local agent_id=$2
  local backend=$3
  local model=$4
  local out_file=$5

  local cmd=(agentbus "$role" --log-file "$log_file" --agent-id "$agent_id" --backend "$backend" --cwd "$cwd" --autonomous)
  if [[ -n "$model" ]]; then
    cmd+=(--model "$model")
  fi

  "${cmd[@]}" >>"$out_file" 2>&1 &
  pids+=("$!")
  echo "$role agent started (id=$agent_id, pid=${pids[-1]}) -> $out_file"
}

start_agent "planner" "$planner_agent" "$planner_backend" "$planner_model" "$log_dir/planner.out"
start_agent "executor" "$executor_agent" "$executor_backend" "$executor_model" "$log_dir/executor.out"
start_agent "reviewer" "$reviewer_agent" "$reviewer_backend" "$reviewer_model" "$log_dir/reviewer.out"

echo "Shared log: $log_file"
echo "Per-agent outputs:"
echo "  planner: $log_dir/planner.out"
echo "  executor: $log_dir/executor.out"
echo "  reviewer: $log_dir/reviewer.out"

echo "All agent pids: ${pids[*]}"

if (( watch )); then
  python3 - "$log_file" "$planner_agent" "$executor_agent" "$reviewer_agent" <<'PY' &
import json
import sys
import time

log_path = sys.argv[1]
planner = sys.argv[2]
executor = sys.argv[3]
reviewer = sys.argv[4]

ansi = {
    "planner": "\033[36m",
    "executor": "\033[32m",
    "reviewer": "\033[35m",
    "system": "\033[90m",
}
reset = "\033[0m"


def role_for_event(event: dict) -> str:
    actor = event.get("actor") or {}
    actor_id = actor.get("id")
    if actor_id == planner:
        return "planner"
    if actor_id == executor:
        return "executor"
    if actor_id == reviewer:
        return "reviewer"
    return "system"


def emit(line: str) -> None:
    try:
        event = json.loads(line)
    except Exception:
        print(line.rstrip())
        return

    role = role_for_event(event)
    kind = event.get("kind", "(missing kind)")
    event_id = event.get("event_id", "(missing event_id)")
    data = event.get("data")
    data_preview = ""
    if isinstance(data, dict) and data:
        if "task_id" in data:
            data_preview = f" task_id={data.get('task_id')}"
        elif "run_id" in data:
            data_preview = f" run_id={data.get('run_id')}"
        elif "chain_id" in data:
            data_preview = f" chain_id={data.get('chain_id')}"

    print(f"{ansi[role]}[{role}] {event_id} {kind}{data_preview}{reset}")


with open(log_path, "r") as f:
    for line in f:
        emit(line)

    while True:
        line = f.readline()
        if not line:
            time.sleep(0.2)
            continue
        emit(line)
PY
  monitor_pid=$!
  echo "monitor pid=$monitor_pid"
fi

echo "Press Ctrl-C to stop all agents."
wait
