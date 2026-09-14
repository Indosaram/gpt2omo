# Local bridge and relay supervision on macOS

`gpt2omo` and `gpt2omo-relay` must be owned by a process supervisor when they are
used as shared local infrastructure. Starting either binary from an interactive
terminal, a tool PTY, or a one-off agent command makes its lifetime depend on that
session. A turn ending must not make the bridge disappear.

This guide covers these per-user `launchd` agents:

- `com.omo.gpt2omo.bridge`: owns the loopback MCP/SSE bridge.
- `com.omo.gpt2omo.relay`: owns continuation-event delivery and retained-session
  reaping.
- `com.omo.gpt2omo.tunnel`: owns the outbound Secure MCP Tunnel client; depends on
  the bridge being healthy on `127.0.0.1:18800`.
- `com.omo.gpt2omo.chrome.remote-chrome`: owns the dedicated `remote-chrome` Chrome
  instance (CDP port 9353).
- `com.omo.gpt2omo.chrome.account2`: owns the dedicated second-account Chrome
  instance (CDP port 9354).

These agents do not delete scopes. The bridge and relay share the existing scope
directory, so a restart preserves generation state and retained-session leases.

Every `delegate_to_chatgpt_web` invocation must use the same broad mount root as the
bridge. The helper validates a retained scope's stored workspace before it contacts
the bridge, so its default of the current directory can reject a valid scope from
another repository:

```bash
./target/debug/delegate_to_chatgpt_web \
  --bridge-url http://127.0.0.1:18800 \
  --mount-root / \
  --resume-scope '<exact-retained-scope-id>' --stdin --json
```

## Prerequisites

Build both binaries before installation:

```bash
cargo build --bin gpt2omo --bin gpt2omo-relay
```

The examples use these local paths:

```text
repository: /Users/YOU/code/project/omo-bridge
scope directory: /Users/YOU/.omo/bridge/scopes-18800
Chrome binary: /Applications/Google Chrome.app/Contents/MacOS/Google Chrome
```

Replace `YOU` with your macOS account name. `launchd` does not expand `~` inside a
plist, so every path in the service definition must be absolute.

## Install the bridge agent

Create `~/Library/LaunchAgents/com.omo.gpt2omo.bridge.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.omo.gpt2omo.bridge</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/YOU/code/project/omo-bridge/target/debug/gpt2omo</string>
    <string>--mount-root</string>
    <string>/</string>
    <string>--scope-dir</string>
    <string>/Users/YOU/.omo/bridge/scopes-18800</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/Users/YOU/code/project/omo-bridge</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/Users/YOU/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>HOME</key>
    <string>/Users/YOU</string>
    <key>CARGO_HOME</key>
    <string>/Users/YOU/.cargo</string>
    <key>RUSTUP_HOME</key>
    <string>/Users/YOU/.rustup</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>StandardOutPath</key>
  <string>/Users/YOU/.omo/bridge/gpt2omo.launchd.out.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/YOU/.omo/bridge/gpt2omo.launchd.err.log</string>
</dict>
</plist>
```

By default, the bridge resolves its transport Bearer token from `~/.omo/bridge/token`.
The `--insecure-no-auth` flag exists solely as an explicit bypass for isolated local
debugging; it removes transport Bearer authentication entirely. Never expose an
unauthenticated bridge publicly, including through an inbound tunnel pointed at
loopback. Mutating tools still require both `scope_id` and `capability_secret`, but
that tool authorization cannot replace transport security. Secure MCP Tunnel removes
public ingress; it doesn't replace the bridge's local authentication and scope
isolation.

`run_command` intentionally clears each child environment and rebuilds it from the
bridge's own PATH. Adding the Rust toolchain directory here is therefore required for
allowlisted `cargo` and `rustc` commands. It also makes `rust-analyzer` available to
the MCP language-server tool. The same rule covers the JavaScript toolchain
directories (`~/.bun/bin` for `bun`/`bunx`/`tsc`, `~/.local/bin` and
`/opt/homebrew/bin` for `node`/`npm`); without them, delegated UI verification such
as `bun --cwd ui run test` or `bunx tsc --noEmit` fails before the command runs. Do
not widen command policy or add an arbitrary-command exception just to compensate
for a missing LaunchAgent PATH.

## Install the relay agent

Create `~/Library/LaunchAgents/com.omo.gpt2omo.relay.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.omo.gpt2omo.relay</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/YOU/code/project/omo-bridge/target/debug/gpt2omo-relay</string>
    <string>--mount-root</string>
    <string>/</string>
    <string>--scope-dir</string>
    <string>/Users/YOU/.omo/bridge/scopes-18800</string>
    <string>--events-url</string>
    <string>http://127.0.0.1:18800/events</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/Users/YOU/code/project/omo-bridge</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/Users/YOU/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>StandardOutPath</key>
  <string>/Users/YOU/.omo/bridge/gpt2omo-relay.launchd.out.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/YOU/.omo/bridge/gpt2omo-relay.launchd.err.log</string>
</dict>
</plist>
```

The relay intentionally leaves `--browser-driver` unset so it uses Chrome/CDP by
default. Do not pin cmux or Orca here: retained browser scopes are affine to their
stored driver, and the direct-CDP path resolves Chrome bindings from `accounts.json`.
If a legacy CLI browser driver is deliberately used, pin it in that account's
`browser.driver` configuration rather than in the shared relay service.

## Install the Secure MCP Tunnel agent

Follow the [Secure MCP Tunnel operator runbook](secure-mcp-tunnel.md) to register
the tunnel manually, issue a restricted **Tunnels Read + Use** runtime key (never
Manage), pin the latest official client release, initialize the HTTP MCP profile,
and run `tunnel-client doctor --profile <PROFILE_NAME> --explain`.

Render [the agent template](../examples/com.omo.gpt2omo.tunnel.plist) to
`~/Library/LaunchAgents/com.omo.gpt2omo.tunnel.plist` using the runbook. It runs the
pinned client directly with `run --profile <PROFILE_NAME>` and uses `RunAtLoad`,
`KeepAlive`, and `ThrottleInterval` 15s, like the dedicated Chrome agents below.
The HOME and base PATH follow the bridge/relay conventions. Both stdout and stderr
go to `~/Library/Logs/gpt2omo-tunnel.log`.

The client depends on a healthy bridge at `127.0.0.1:18800` and forwards only to
`http://127.0.0.1:18800/mcp`. Its startup wait tolerates initial bridge startup, but
launchd does not guarantee dependency ordering. Keep its `/ui`, `/healthz`, and
`/readyz` listener on a separate loopback port. Supply only a native `file:` key
reference in `EnvironmentVariables`; the secret itself must remain in a 0600 file
outside the repository, never in a committed plist or profile.

The runbook contains installation, health checks, association of all workspaces
used by `remote-chrome` and `remote-chrome-2`, parallel-verification gates, and an
authenticated cloudflared rollback. Preserve the old cloudflared configuration;
do not retire its forwarding until the secure route passes those gates. Never keep
an unauthenticated public route online for comparison. Service installation,
cutover, and reboot/login verification are manual operator actions, not actions for
an active coding worker. Per-user LaunchAgents start after GUI login following a
reboot; they do not provide pre-login service availability.

## Load and verify

Validate and load the bridge and relay agents (install the tunnel separately after
the bridge is healthy, as described above):

```bash
plutil -lint ~/Library/LaunchAgents/com.omo.gpt2omo.bridge.plist
plutil -lint ~/Library/LaunchAgents/com.omo.gpt2omo.relay.plist
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.omo.gpt2omo.bridge.plist
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.omo.gpt2omo.relay.plist
curl -fsS http://127.0.0.1:18800/healthz
launchctl print "gui/$(id -u)/com.omo.gpt2omo.bridge"
launchctl print "gui/$(id -u)/com.omo.gpt2omo.relay"
```

`KeepAlive` restarts a bridge process that exits or receives a signal. Validate that
behavior only with zero active and reserved workers:

```bash
launchctl kill SIGTERM "gui/$(id -u)/com.omo.gpt2omo.bridge"
curl -fsS http://127.0.0.1:18800/healthz
```

Inspect the persistent logs when a service does not start:

```bash
tail -n 100 ~/.omo/bridge/gpt2omo.launchd.err.log
tail -n 100 ~/.omo/bridge/gpt2omo-relay.launchd.err.log
```

## Upgrade and recovery

Rebuild first, then restart an agent only after confirming that no Web workers are
active or reserved:

```bash
cargo build --bin gpt2omo --bin gpt2omo-relay
launchctl kickstart -k "gui/$(id -u)/com.omo.gpt2omo.bridge"
launchctl kickstart -k "gui/$(id -u)/com.omo.gpt2omo.relay"
```

Do not remove `~/.omo/bridge/scopes-18800`, delete a retained scope, or close a
browser tab as part of service recovery. The relay's normal expiry checks already
preserve a scope when its browser binding cannot be safely closed.

## Dedicated Chrome supervision agents (2026-09-11)

The two `attach_only` ChatGPT accounts bind to dedicated Chrome instances that
were previously launched manually. After a bulk quit left them down, dispatches
failed at the transport layer, so they are now supervised the same way as the
bridge and relay:

| Label | CDP port | Profile |
|---|---|---|
| `com.omo.gpt2omo.chrome.remote-chrome` | 9353 | `~/.omo/bridge/browser-profiles/remote-chrome-cdp` |
| `com.omo.gpt2omo.chrome.account2` | 9354 | `~/.omo/bridge/browser-profiles/account2-aside-cdp` |

- `RunAtLoad` + `KeepAlive` (restart on any exit, `ThrottleInterval` 15s); logs
  `~/Library/Logs/gpt2omo-chrome-*.log`.
- Ports moved 9333→9353 and 9334→9354: the old ports are contested. A
  Discord-automation Chrome from the hermes side binds 9333 on its own schedule,
  and an `attach_only` account trusts the CDP port as account identity, so a
  foreign browser on that port passes the reachability check and only fails at
  the auth check (`unauth` bootstrap failure, 2026-09-11). Keep these ports
  exclusive to gpt2omo.
- The Chrome window must stay visible (not minimized) for CDP-created tabs to
  load — same desktop-window contract as the orca/cmux drivers.
- Inspect live browser state, message turns, alerts, and rate limits non-destructively:
  `gpt2omo-account-status --inspect` or `delegate_to_chatgpt_web --inspect-account <ACCOUNT_ID>`.
