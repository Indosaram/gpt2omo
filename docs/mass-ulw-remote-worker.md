# Registering a Remote-Chrome Attach-Only Account as a mass-ulw Worker

This guide walks operators through registering an externally managed Chromium or Google Chrome instance as an attach-only worker, then executing mass-ulw Directed Acyclic Graph (DAG) transport nodes against that pinned account.

## 1. accounts.json Setup

Configure the bridge with an `accounts.json` file inside the bridge directory (default: `~/.omo/bridge/accounts.json`). The schema uses `version: 1` and matches `src/accounts.rs`.

### Standalone attach-only account snippet

For a dedicated remote Chrome instance running at `http://127.0.0.1:9333`, configure the `browser` block with `launch_mode: "attach_only"`, specify `cdp_endpoint`, and omit `user_data_dir`:

```json
{
  "version": 1,
  "routing": {
    "strategy": "least_loaded",
    "reservation_ttl_seconds": 120,
    "selection_failure_backoff_seconds": 30
  },
  "defaults": {
    "limits": {
      "window_seconds": 3600,
      "max_dispatches": 12,
      "max_active_workers": 2
    },
    "cooldown": {
      "unknown_rate_limit_seconds": 900,
      "delivery_failure_seconds": 30
    }
  },
  "accounts": [
    {
      "id": "remote-chrome",
      "enabled": true,
      "draining": false,
      "browser": {
        "driver": "chrome",
        "instance": "remote-chrome",
        "launch_mode": "attach_only",
        "cdp_endpoint": "http://127.0.0.1:9333",
        "worktree": "active"
      }
    }
  ]
}
```

### Safe migration for existing legacy hosts

When `accounts.json` is missing from the bridge directory, `load_accounts_config` falls back to `AccountsConfig::legacy`. It synthesizes an in-memory configuration with `legacy_fallback: true` and a single account named `"default"` (`LEGACY_ACCOUNT_ID`), which points to the default local browser configuration.

When you create `accounts.json` on disk, `load_accounts_config` parses the file and sets `legacy_fallback: false`. Stored scopes created prior to this transition resolve their account ID to `"default"`. If `accounts.json` contains only `remote-chrome`, looking up account `"default"` fails, which breaks resume and close operations for any retained legacy scopes.

To transition safely without breaking in-flight or retained scopes, include a `"default"` entry alongside `remote-chrome`. Set `"draining": true` on the legacy account so new dispatches avoid it while existing retained scopes remain resumable:

```json
{
  "version": 1,
  "routing": {
    "strategy": "least_loaded",
    "reservation_ttl_seconds": 120,
    "selection_failure_backoff_seconds": 30
  },
  "defaults": {
    "limits": {
      "window_seconds": 3600,
      "max_dispatches": 12,
      "max_active_workers": 2
    },
    "cooldown": {
      "unknown_rate_limit_seconds": 900,
      "delivery_failure_seconds": 30
    }
  },
  "accounts": [
    {
      "id": "default",
      "enabled": true,
      "draining": true,
      "browser": {
        "driver": "chrome",
        "instance": "legacy",
        "launch_mode": "managed_local",
        "cdp_endpoint": "http://127.0.0.1:9222",
        "worktree": "active"
      }
    },
    {
      "id": "remote-chrome",
      "enabled": true,
      "draining": false,
      "browser": {
        "driver": "chrome",
        "instance": "remote-chrome",
        "launch_mode": "attach_only",
        "cdp_endpoint": "http://127.0.0.1:9333",
        "worktree": "active"
      }
    }
  ]
}
```

## 2. Literal mass-ulw DAG Web-Transport Node Recipe

mass-ulw coordinates workflow stages using DAG task nodes. The bridge helper module `src/mass_ulw_web.rs` (`DelegateWebConfig`, `WebTask`, `DelegateWebResult`) creates shell-free command invocations (`program`, `args`, `stdin`) for process runners.

Key rules for DAG construction:
- `dependsOn` controls scheduling order only. It does not pipe standard output or forward execution artifacts between nodes.
- Downstream nodes read the retained `scope_id` directly from the transport node's parsed JSON output (`delegations[0].scope_id` or `retained_scope_for_label()`).
- Fresh batch tasks are capped at `DELEGATE_WEB_MAX_WORKERS = 2` with an internal 10 second stagger between worker spawns.
- Target the remote Chrome account with `--account remote-chrome` (or via `OMO_DELEGATE_ACCOUNT=remote-chrome`).

### Copy-pasteable DAG node workflow (JSON)

```json
{
  "nodes": [
    {
      "id": "web_transport_fanout",
      "type": "process",
      "description": "Spawn two isolated domain workers on the remote Chrome account",
      "command": {
        "program": "delegate_to_chatgpt_web",
        "args": [
          "--account",
          "remote-chrome",
          "--batch-stdin",
          "--json"
        ],
        "stdin": "{\"tasks\":[{\"label\":\"core\",\"task\":\"Refactor core transport logic and run unit tests.\",\"workspace\":\"/workspaces/project\"},{\"label\":\"ui\",\"task\":\"Update worker status dashboard and verify UI components.\",\"workspace\":\"/workspaces/project\"}]}"
      }
    },
    {
      "id": "local_verify",
      "type": "shell",
      "dependsOn": ["web_transport_fanout"],
      "description": "Run local verification tests across modified workspaces",
      "command": "cargo test --workspace"
    },
    {
      "id": "web_transport_fanin",
      "type": "process",
      "dependsOn": ["local_verify"],
      "description": "Resume the core worker retained scope with local test results",
      "command": {
        "program": "delegate_to_chatgpt_web",
        "args": [
          "--resume-scope",
          "{{nodes.web_transport_fanout.output.delegations[0].scope_id}}",
          "--stdin",
          "--json"
        ],
        "stdin": "Local verification passed with 0 errors. Reconcile both domains and run the final check."
      }
    },
    {
      "id": "web_transport_cleanup",
      "type": "process",
      "dependsOn": ["web_transport_fanin"],
      "description": "Close both retained browser scopes after coordinator approval",
      "command": {
        "program": "delegate_to_chatgpt_web",
        "args": [
          "--close-scope",
          "{{nodes.web_transport_fanout.output.delegations[0].scope_id}}",
          "--json"
        ]
      }
    }
  ]
}
```

### Single worker variation

For single-task invocations, pass the prompt string on stdin with `--stdin --json`:

```bash
printf '%s' 'Implement the feature and complete via completion_check.' | \
  delegate_to_chatgpt_web --account remote-chrome --stdin --json
```

## 3. Operator Preflight

Verify browser availability and bridge health before scheduling production DAG runs.

### Step 1: Direct CDP version probe

Check that the remote Chrome process or SSH port forward responds on loopback:

```bash
curl -s http://127.0.0.1:9333/json/version
```

Expected output includes the Chrome version, user agent, and `webSocketDebuggerUrl`:

```json
{
  "Browser": "Chrome/133.0.6943.127",
  "Protocol-Version": "1.3",
  "User-Agent": "Mozilla/5.0 ...",
  "V8-Version": "13.3.178.20",
  "WebKit-Version": "537.36 ...",
  "webSocketDebuggerUrl": "ws://127.0.0.1:9333/devtools/browser/..."
}
```

### Step 2: Chrome window visibility

CDP-created targets stay dormant when the Chrome window is minimized, hidden, or absent: the tab exists in `/json/list` with an empty URL and never loads, so prompt readiness fails (`ChatGPT prompt did not become ready`). This is the same desktop-window contract the orca/cmux drivers enforce (`orca open --json`). Before dispatching, bring the remote Chrome window to the foreground (`open -a "Google Chrome"`) and confirm a fresh tab actually navigates; a background-only Chrome host needs a real window (e.g. run Chrome inside a visible VNC/desktop session), not just the process.

### Step 3: Diagnostic account status

Run `gpt2omo-account-status` to inspect account reachability, capacity limits, and current active scopes:

```bash
gpt2omo-account-status --port 18800 --compact
```

Verify that `remote-chrome` reports reachability as `reachable` and login state as `ready`.

### Step 4: Remote Chrome probe example

Run the built-in probe example. It attaches to the browser at `http://127.0.0.1:9333`, validates reachability, opens a ChatGPT login page, checks the exact one-page target delta in `/json/list`, and cleans up:

```bash
cargo run --example remote_chrome_probe
```

Successful execution ends with:

```text
=== Remote Chrome Probe ===
Connecting to CDP endpoint: http://127.0.0.1:9333
[CDP /json/list before] Total targets: 1
[BrowserPool Health]
  Account ID: remote-chrome
  Instance: remote-chrome
  Reachability: Reachable
  Login state: Unknown
[Opening ChatGPT Login Page via BrowserPool]
  Successfully created page id: ...
[CDP /json/list after] Total targets: 2
[Delta / New Target(s)] Count: 1
Remote Chrome probe succeeded with verified 1-page delta.
```

## 4. Isolated-Bridge QA Pattern

Before updating production configuration, validate the account pin and attach-only lifecycle inside an isolated temporary directory. This confirms the bridge fails closed without starting a local browser when the remote endpoint is unavailable.

### Step-by-step QA test

```bash
# 1. Create isolated control and scope directories
QA_DIR=$(mktemp -d /tmp/omo-bridge-qa-XXXXXX)
QA_BRIDGE="$QA_DIR/bridge"
QA_SCOPES="$QA_DIR/scopes"
QA_MOUNT="$QA_DIR/mount"
mkdir -p "$QA_BRIDGE" "$QA_SCOPES" "$QA_MOUNT"

# 2. Write test accounts configuration
cat <<'EOF' > "$QA_BRIDGE/accounts.json"
{
  "version": 1,
  "accounts": [
    {
      "id": "remote-chrome",
      "enabled": true,
      "draining": false,
      "browser": {
        "driver": "chrome",
        "instance": "remote-chrome",
        "launch_mode": "attach_only",
        "cdp_endpoint": "http://127.0.0.1:9333"
      }
    }
  ]
}
EOF

# 3. Verify dry-run scope creation with account pinning
OMO_BRIDGE_HOME="$QA_BRIDGE" \
OMO_SCOPE_DIR="$QA_SCOPES" \
OMO_DELEGATE_ACCOUNT="remote-chrome" \
cargo run --bin delegate_to_chatgpt_web -- \
  --mount-root "$QA_MOUNT" \
  --workspace "$QA_MOUNT" \
  --dry-run \
  --json \
  "QA validation task"

# 4. Verify fail-closed behavior
# If the remote CDP port (9333) is stopped, dispatch must report an error
# and must never spawn a local Chrome process.
```

When finished with verification, remove the temporary QA directory:

```bash
rm -rf "$QA_DIR"
```
