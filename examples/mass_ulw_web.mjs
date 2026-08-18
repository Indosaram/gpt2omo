import { spawn } from "node:child_process";

export const MAX_WEB_WORKERS = 2;
export const SPAWN_STAGGER_SECONDS = 10;

function commonArgs(bridgeUrl) {
  return bridgeUrl ? ["--bridge-url", bridgeUrl] : [];
}

function requireText(value, name) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${name} must be non-empty`);
  }
  return value.trim();
}

export function singleInvocation({
  task,
  label = "web",
  workspace,
  bridgeUrl,
  binary = "delegate_to_chatgpt_web",
}) {
  requireText(label, "label");
  const args = commonArgs(bridgeUrl);
  if (workspace) args.push("--workspace", workspace);
  args.push("--stdin", "--json");
  return { program: binary, args, stdin: requireText(task, "task") };
}

export function parallelPairInvocation({
  tasks,
  bridgeUrl,
  binary = "delegate_to_chatgpt_web",
}) {
  if (!Array.isArray(tasks) || tasks.length !== MAX_WEB_WORKERS) {
    throw new Error(`parallelPairInvocation requires exactly ${MAX_WEB_WORKERS} tasks`);
  }
  const normalized = tasks.map((item) => ({
    label: requireText(item.label, "label"),
    task: requireText(item.task, "task"),
    ...(item.workspace ? { workspace: item.workspace } : {}),
  }));
  return {
    program: binary,
    args: [...commonArgs(bridgeUrl), "--batch-stdin", "--json"],
    stdin: JSON.stringify({ tasks: normalized }),
  };
}

export function resumeInvocation({
  scopeId,
  task,
  bridgeUrl,
  binary = "delegate_to_chatgpt_web",
}) {
  return {
    program: binary,
    args: [
      ...commonArgs(bridgeUrl),
      "--resume-scope",
      requireText(scopeId, "scopeId"),
      "--stdin",
      "--json",
    ],
    stdin: requireText(task, "task"),
  };
}

export function closeInvocation({
  scopeId,
  bridgeUrl,
  binary = "delegate_to_chatgpt_web",
}) {
  return {
    program: binary,
    args: [
      ...commonArgs(bridgeUrl),
      "--close-scope",
      requireText(scopeId, "scopeId"),
      "--json",
    ],
  };
}

export async function runInvocation(invocation, { cwd, env = process.env } = {}) {
  return await new Promise((resolve, reject) => {
    const child = spawn(invocation.program, invocation.args, {
      cwd,
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`delegate_to_chatgpt_web exited ${code}: ${stderr.trim()}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch (error) {
        reject(new Error(`invalid delegate JSON: ${error.message}; stdout=${stdout}`));
      }
    });
    if (invocation.stdin !== undefined) child.stdin.write(invocation.stdin);
    child.stdin.end();
  });
}

export function retainedScope(result, label) {
  if (!result?.terminal) throw new Error("delegate result is not terminal");
  const item = result.delegations?.find((entry) => entry.label === label);
  if (!item) throw new Error(`missing delegation label: ${label}`);
  if (!item.session_retained || !item.resumable || !item.scope_id) {
    throw new Error(`delegation ${label} is not retained/resumable`);
  }
  return item.scope_id;
}

export function fanInPrompt(result, integrationGoal, localVerificationFeedback = "") {
  if (!result?.terminal) throw new Error("delegate result is not terminal");
  const evidence = (result.delegations ?? [])
    .map(
      (item) =>
        `- ${item.label ?? "unlabeled"}: terminal=${item.terminal_state ?? "UNKNOWN"}; detail=${item.terminal_detail ?? "none"}`,
    )
    .join("\n");
  const feedback = localVerificationFeedback.trim() || "No additional local verification feedback was supplied.";
  return `Fan-in integration pass. Both parallel Web workers are terminal. Continue in this same retained session; do not create another Web worker.\n\nIntegration goal:\n${integrationGoal.trim()}\n\nParallel terminal evidence:\n${evidence}\n\nLocal verification feedback:\n${feedback}\n\nInspect the current workspace state independently, reconcile both domains, fix integration defects, run the required local verification, and finish only after authoritative completion_check is ready=true.`;
}
