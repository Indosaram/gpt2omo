"""Python eval helper for mass-ulw -> delegate_to_chatgpt_web workflows."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import subprocess
from typing import Any, Mapping, Sequence

MAX_WEB_WORKERS = 2
SPAWN_STAGGER_SECONDS = 10


@dataclass(frozen=True)
class Invocation:
    program: str
    args: tuple[str, ...]
    stdin: str | None = None


def _text(value: str, name: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{name} must be non-empty")
    return value


def _common_args(bridge_url: str | None) -> list[str]:
    return ["--bridge-url", bridge_url] if bridge_url else []


def single_invocation(
    *,
    task: str,
    label: str = "web",
    workspace: str | None = None,
    bridge_url: str | None = None,
    binary: str = "delegate_to_chatgpt_web",
) -> Invocation:
    _text(label, "label")
    args = _common_args(bridge_url)
    if workspace:
        args.extend(["--workspace", workspace])
    args.extend(["--stdin", "--json"])
    return Invocation(binary, tuple(args), _text(task, "task"))


def parallel_pair_invocation(
    *,
    tasks: Sequence[Mapping[str, str]],
    bridge_url: str | None = None,
    binary: str = "delegate_to_chatgpt_web",
) -> Invocation:
    if len(tasks) != MAX_WEB_WORKERS:
        raise ValueError(f"parallel_pair_invocation requires exactly {MAX_WEB_WORKERS} tasks")
    normalized: list[dict[str, str]] = []
    for item in tasks:
        entry = {
            "label": _text(item["label"], "label"),
            "task": _text(item["task"], "task"),
        }
        if item.get("workspace"):
            entry["workspace"] = item["workspace"]
        normalized.append(entry)
    args = [*_common_args(bridge_url), "--batch-stdin", "--json"]
    return Invocation(binary, tuple(args), json.dumps({"tasks": normalized}))


def resume_invocation(
    *,
    scope_id: str,
    task: str,
    bridge_url: str | None = None,
    binary: str = "delegate_to_chatgpt_web",
) -> Invocation:
    args = [
        *_common_args(bridge_url),
        "--resume-scope",
        _text(scope_id, "scope_id"),
        "--stdin",
        "--json",
    ]
    return Invocation(binary, tuple(args), _text(task, "task"))


def close_invocation(
    *,
    scope_id: str,
    bridge_url: str | None = None,
    binary: str = "delegate_to_chatgpt_web",
) -> Invocation:
    args = [
        *_common_args(bridge_url),
        "--close-scope",
        _text(scope_id, "scope_id"),
        "--json",
    ]
    return Invocation(binary, tuple(args))


def run_invocation(
    invocation: Invocation,
    *,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    completed = subprocess.run(
        [invocation.program, *invocation.args],
        input=invocation.stdin,
        text=True,
        capture_output=True,
        cwd=cwd,
        env=dict(env) if env is not None else os.environ.copy(),
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"delegate_to_chatgpt_web exited {completed.returncode}: {completed.stderr.strip()}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"invalid delegate JSON: {error}; stdout={completed.stdout}"
        ) from error


def retained_scope(result: Mapping[str, Any], label: str) -> str:
    if not result.get("terminal"):
        raise ValueError("delegate result is not terminal")
    for item in result.get("delegations", []):
        if item.get("label") == label:
            if item.get("session_retained") and item.get("resumable") and item.get("scope_id"):
                return str(item["scope_id"])
            raise ValueError(f"delegation {label} is not retained/resumable")
    raise ValueError(f"missing delegation label: {label}")


def fan_in_prompt(
    result: Mapping[str, Any],
    integration_goal: str,
    local_verification_feedback: str = "",
) -> str:
    if not result.get("terminal"):
        raise ValueError("delegate result is not terminal")
    evidence = "\n".join(
        f"- {item.get('label', 'unlabeled')}: terminal={item.get('terminal_state', 'UNKNOWN')}; "
        f"detail={item.get('terminal_detail', 'none')}"
        for item in result.get("delegations", [])
    )
    feedback = (
        local_verification_feedback.strip()
        or "No additional local verification feedback was supplied."
    )
    return (
        "Fan-in integration pass. Both parallel Web workers are terminal. Continue in this same "
        "retained session; do not create another Web worker.\n\n"
        f"Integration goal:\n{integration_goal.strip()}\n\n"
        f"Parallel terminal evidence:\n{evidence}\n\n"
        f"Local verification feedback:\n{feedback}\n\n"
        "Inspect the current workspace state independently, reconcile both domains, fix integration "
        "defects, run the required local verification, and finish only after authoritative "
        "completion_check is ready=true."
    )
