# Secure MCP Tunnel operator runbook

This is the documentation/template portion of [Phase 1](secure-mcp-tunnel-and-hardening-plan.md#2-phase-1--secure-mcp-tunnel-경로-구현-p0).
It replaces the legacy inbound `code.checka.cc` route with OpenAI's outbound
Secure MCP Tunnel. The bridge stays at `127.0.0.1:18800`; the MCP target stays
`http://127.0.0.1:18800/mcp`. No Rust transport change is required by this procedure.

**Operator actions only:** publishing these instructions does not register a tunnel,
install a service, or complete the migration. The operator performs Platform/ChatGPT
registration and service operations manually. Do not restart the bridge, relay, or
Chrome, or interrupt active/reserved Web workers to try this runbook. Schedule
cutover, rollback, upgrade, and reboot tests for a maintenance window.

**Never expose the bridge publicly without authentication. Never place an API key,
bridge token, or tunnel credentials inside any committed file.** Keep credentials,
generated profiles, rendered plists, and operational evidence outside the repository.
The legacy path must be authenticated before parallel verification or rollback;
protecting only `/events` does not protect `/mcp`.

## 1. Security model and prerequisites

```text
ChatGPT workspace -> OpenAI tunnel endpoint (tunnel_id + workspace association)
                              ^
                              | outbound HTTPS long-poll / response posting
                       tunnel-client
                              |
                              v
                   http://127.0.0.1:18800/mcp

Local continuation relay -> http://127.0.0.1:18800/events (unchanged)
```

The client fetches queued JSON-RPC work and returns results over outbound HTTPS.
There are **no inbound internet ports and no public URL for the bridge** in the
target topology. The OpenAI endpoint is not a new public origin URL to paste into
the bridge helper. Keep both the bridge and the tunnel admin listener on loopback.
Scope authorization with `scope_id` and `capability_secret` provides a **second
defense-in-depth layer**, behind restricted tunnel access, rather than serving as the
only protection on an exposed endpoint. Tool isolation remains sensitive: preserve
scope boundaries, local bridge authentication, command policy, and the independent
hardening work in the plan. Installing a tunnel doesn't implement those other phases
or secure a legacy route left online.

Before starting, confirm:

- Platform organization access: a tunnel manager can create/edit the tunnel with
  **Tunnels Read + Manage**; runtime users and app creators have **Read + Use**.
  Ask the organization owner/RBAC administrator to grant the appropriate role.
- ChatGPT developer mode is allowed and enabled in **each target workspace**.
  This is separate from Platform permissions; a workspace administrator may need
  to enable access. This runbook uses private developer-mode apps, not public
  plugin submission.
- The supervised bridge is already healthy on `127.0.0.1:18800`, with its current
  local authentication policy intact. The client host can make outbound HTTPS
  connections to `api.openai.com:443` (or `mtls.api.openai.com:443` when configured
  for control-plane mTLS) and can reach the loopback MCP endpoint.
- The operator has macOS `launchd`, Bash, Python 3, `curl`, `unzip`, and `plutil`,
  an unused loopback admin port, and access to both `remote-chrome` and
  `remote-chrome-2`. Preserve the old cloudflared configuration, credentials,
  service definition, and authenticated connector details in private storage.

See the [official Secure MCP Tunnel guide][openai-guide] and
[permission reference][permissions] for the access model. Do not add inbound
firewall rules, bind to `0.0.0.0`, or expose the admin UI for this migration.

### Operator shell and placeholders

Use a dedicated **Bash** terminal on the bridge host. Run the numbered sections in
order in that same shell. Replace every `<...>` placeholder with its real local
value before executing a block; placeholders are not shell redirections. Values
assigned below are identifiers or paths, never secret contents. Do not use shell
tracing, paste secrets into command arguments, or run these blocks as MCP tools.

```bash
set -euo pipefail
umask 077
export REPO='<ABSOLUTE_PATH_TO_OMO_BRIDGE>'
export TUNNEL_PROFILE='gpt2omo-http'
export TUNNEL_PROFILE_DIR="$HOME/.config/gpt2omo-tunnel/profiles"
export TUNNEL_API_KEY_FILE="$HOME/.config/gpt2omo-tunnel/secrets/runtime-api-key"
export TUNNEL_ADMIN_PORT='<UNUSED_LOOPBACK_PORT>'
export BRIDGE_CURL_CONFIG='<ABSOLUTE_PATH_TO_0600_LOCAL_BRIDGE_CURL_CONFIG>'
mkdir -p "$TUNNEL_PROFILE_DIR" "$(dirname "$TUNNEL_API_KEY_FILE")"
chmod 700 "$TUNNEL_PROFILE_DIR" "$(dirname "$TUNNEL_API_KEY_FILE")"
curl --config "$BRIDGE_CURL_CONFIG" --noproxy '*' \
  --connect-timeout 5 --max-time 10 --fail --silent --show-error \
  http://127.0.0.1:18800/healthz
```

`BRIDGE_CURL_CONFIG` is an operator-owned 0600 curl config outside the repository.
For a token-protected health endpoint it contains the appropriate Authorization
header; use an empty 0600 file only when the deployed local health endpoint does
not require authentication. The tunnel runtime key is **not** a bridge token.
A failed health check is a stop condition, not a reason to disable authentication.

## 2. Create the tunnel and restricted runtime key

In [Platform tunnel settings][platform-tunnels], select the intended organization,
create a tunnel with a recognizable name, and capture the returned `tunnel_id`.
Record the owning organization and tunnel name in the private deployment record.
Creation/association changes belong to the human operator, not the runtime client.

In [Platform runtime API keys][runtime-keys], create a separate **Restricted** key.
Allow **only Tunnels Read + Use** and leave unrelated permissions disabled. Ensure
the key's principal has the corresponding organization/per-tunnel access.
**Never grant Manage to the runtime key; never use an admin key or an unrestricted
personal API key in the agent.** If the UI cannot express the required restriction,
ask the organization administrator to provision a suitable runtime principal/key
instead of broadening access. The management credential stays off the client host.

Set the identifier and save the newly issued key with a hidden prompt. This writes
only the key value to a new 0600 file outside the repository, not to shell history
or a plist. It intentionally refuses to overwrite an existing credential.

```bash
export CONTROL_PLANE_TUNNEL_ID='<TUNNEL_ID_FROM_PLATFORM>'
python3 - <<'PY'
import getpass
import os
from pathlib import Path

path = Path(os.environ['TUNNEL_API_KEY_FILE']).expanduser().resolve()
repo = Path(os.environ['REPO']).expanduser().resolve()
if path == repo or repo in path.parents:
    raise SystemExit('The runtime key must be outside the repository')
key = getpass.getpass('Restricted runtime API key (Tunnels Read + Use only): ').strip()
if not key or '\n' in key or '\r' in key:
    raise SystemExit('Expected one non-empty key value')
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as out:
    out.write(key + '\n')
PY
export CONTROL_PLANE_API_KEY="file:$TUNNEL_API_KEY_FILE"
unset OPENAI_API_KEY OPENAI_ADMIN_KEY
```

`file:` is the client's native secret reference, not a shell substitution and not
an invented `CONTROL_PLANE_API_KEY_FILE` setting. The template supplies this
reference through `EnvironmentVariables.CONTROL_PLANE_API_KEY`. See the
[configuration reference][configuration]. Do not copy a key into this repository,
chat, screenshots, logs, or a profile. Treat saved diagnostic archives as sensitive
even when the client says they are redacted.

**Gate:** the tunnel exists in Platform and the restricted runtime key has been
issued and stored privately. This does not yet prove runtime connectivity.

## 3. Download the latest release, then pin the installed artifact

Start at [openai/tunnel-client/releases/latest][latest-release] every time a release
is selected. Follow its redirect, review the release notes, and copy the resolved
tag and the **exact macOS ZIP asset name** matching `uname -m` (Apple silicon or
Intel). Do not use a version copied from an old runbook, a source archive, or an
unversioned executable path. The release-discovery URL stays current; the installed
binary, checksum, and launchd path are pinned until an explicit operator upgrade.

```bash
open 'https://github.com/openai/tunnel-client/releases/latest'
uname -m
export TUNNEL_RELEASE_TAG='<TAG_RESOLVED_FROM_LATEST_RELEASE>'
export TUNNEL_RELEASE_ASSET='<EXACT_MACOS_ZIP_ASSET_NAME_FROM_THAT_RELEASE>'
export TUNNEL_RELEASE_BASE="https://github.com/openai/tunnel-client/releases/download/$TUNNEL_RELEASE_TAG"
export TUNNEL_DOWNLOAD_DIR="$HOME/Downloads/gpt2omo-tunnel/$TUNNEL_RELEASE_TAG"
export TUNNEL_INSTALL_DIR="$HOME/.local/share/gpt2omo/tunnel-client/$TUNNEL_RELEASE_TAG"
mkdir -p "$TUNNEL_DOWNLOAD_DIR"
test ! -e "$TUNNEL_INSTALL_DIR"
curl --fail --location --proto '=https' --tlsv1.2 \
  "$TUNNEL_RELEASE_BASE/$TUNNEL_RELEASE_ASSET" \
  --output "$TUNNEL_DOWNLOAD_DIR/$TUNNEL_RELEASE_ASSET"
curl --fail --location --proto '=https' --tlsv1.2 \
  "$TUNNEL_RELEASE_BASE/SHA256SUMS.txt" \
  --output "$TUNNEL_DOWNLOAD_DIR/SHA256SUMS.txt"
python3 - <<'PY'
import hashlib
import os
from pathlib import Path

directory = Path(os.environ['TUNNEL_DOWNLOAD_DIR'])
asset = os.environ['TUNNEL_RELEASE_ASSET']
if Path(asset).name != asset or '<' in asset:
    raise SystemExit('Use the exact release asset basename')
matches = []
for line in (directory / 'SHA256SUMS.txt').read_text().splitlines():
    fields = line.split(maxsplit=1)
    if len(fields) == 2 and fields[1].lstrip('*') in (asset, './' + asset):
        matches.append(fields[0].lower())
if len(matches) != 1:
    raise SystemExit('Expected exactly one checksum entry for the selected asset')
digest = hashlib.sha256()
with (directory / asset).open('rb') as archive:
    for block in iter(lambda: archive.read(1024 * 1024), b''):
        digest.update(block)
if digest.hexdigest() != matches[0]:
    raise SystemExit('Release checksum mismatch; do not install')
print('Verified release archive SHA-256:', digest.hexdigest())
PY
mkdir -p "$TUNNEL_INSTALL_DIR"
unzip -q "$TUNNEL_DOWNLOAD_DIR/$TUNNEL_RELEASE_ASSET" -d "$TUNNEL_INSTALL_DIR"
export TUNNEL_CLIENT_BIN='<ABSOLUTE_PATH_TO_TUNNEL_CLIENT_IN_THAT_VERSIONED_DIRECTORY>'
"$TUNNEL_CLIENT_BIN" --version
"$TUNNEL_CLIENT_BIN" help quickstart
```

Keep the whole extracted distribution together, including any companion files.
Record the tag, exact asset URL, archive SHA-256, executable SHA-256, architecture,
and `--version` output in the private deployment record. The upstream release
validation instructions also cover available provenance and SBOM checks; an archive
checksum alone is not an independent publisher-signature check.

**macOS Gatekeeper:** the [upstream installation guidance][client-readme] currently
recommends the official Homebrew tap because directly downloaded ZIPs may not be
notarized. Do not bypass Gatekeeper with `xattr`, `spctl`, or Open Anyway. If macOS
blocks the downloaded binary, use the supported installation path instead:

```bash
brew install openai/tools/tunnel-client
brew pin tunnel-client
export TUNNEL_CLIENT_BIN="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' \
  "$(brew --prefix tunnel-client)/bin/tunnel-client")"
"$TUNNEL_CLIENT_BIN" --version
```

Use the resolved versioned Cellar path, not a moving `bin`/`opt` symlink, in the
agent. Confirm it matches the release selected above; stop to reconcile a mismatch.
Do not delete that version during cleanup. Never download or upgrade from a
`latest` URL as part of launchd startup.

## 4. Initialize the HTTP MCP profile and run doctor

Use the existing HTTP server, not a stdio child and not an embedded demo server.
The `sample_mcp_remote_no_auth` sample is for HTTP servers without OAuth discovery
metadata; its name does **not** remove the required tunnel runtime authentication
or authorize public exposure of the bridge. Review the sample from the pinned
binary before using it. If the deployed bridge now advertises OAuth, select the
corresponding supported sample and verify its auth flow instead.

```bash
export TUNNEL_CLIENT_PROFILE_DIR="$TUNNEL_PROFILE_DIR"
export HEALTH_LISTEN_ADDR="127.0.0.1:$TUNNEL_ADMIN_PORT"
export MCP_STARTUP_WAIT_TIMEOUT='60s'
export ALLOW_REMOTE_UI='false'
export OPEN_WEB_UI='false'
"$TUNNEL_CLIENT_BIN" profiles samples show sample_mcp_remote_no_auth
"$TUNNEL_CLIENT_BIN" init \
  --sample sample_mcp_remote_no_auth \
  --profile "$TUNNEL_PROFILE" \
  --tunnel-id "$CONTROL_PLANE_TUNNEL_ID" \
  --mcp-server-url http://127.0.0.1:18800/mcp
chmod 600 "$TUNNEL_PROFILE_DIR/$TUNNEL_PROFILE.yaml"
"$TUNNEL_CLIENT_BIN" doctor --profile "$TUNNEL_PROFILE" --explain
```

Review the generated profile locally: it must select the recorded `tunnel_id`, the
`main` HTTP binding at `http://127.0.0.1:18800/mcp`, and only `env:`/`file:` references
for credentials. No literal keys. Leave optional Cloudflare companion/public-origin
configuration disabled for this outbound-only procedure. The old standalone
cloudflared route and its credentials remain separate rollback infrastructure.

If the deployed `/mcp` endpoint requires a local bearer token, configure both
runtime and discovery/probe headers in the private profile using the supported
secret-reference fields. For example, the following is a **fragment to merge into
the existing `mcp` mapping**, not a second mapping or a replacement for its URL:

```yaml
mcp:
  extra_headers:
    Authorization: "file:<ABSOLUTE_PATH_TO_0600_MCP_AUTH_HEADER_FILE>"
  discovery_extra_headers:
    Authorization: "file:<ABSOLUTE_PATH_TO_0600_MCP_AUTH_HEADER_FILE>"
```

That separate file contains the complete header value (`Bearer <LOCAL_MCP_TOKEN>`),
not the OpenAI runtime API key. Use the deployed bridge's actual auth contract;
do not assume that an `/events` control token also protects `/mcp`, or disable
bridge authentication to make discovery pass. Edit with the client's profile editor
and repeat the preflight before service installation:

```bash
"$TUNNEL_CLIENT_BIN" profiles edit "$TUNNEL_PROFILE"
"$TUNNEL_CLIENT_BIN" doctor --profile "$TUNNEL_PROFILE" --explain
```

**Gate:** doctor passes all applicable checks. Doctor is a local preflight, not proof
that the runtime key is authorized to poll OpenAI. Run it before the agent owns the
admin port; a later port-in-use diagnostic may simply mean the agent is running.
After startup, use the health endpoints, polling logs, and actual ChatGPT calls.
See [upstream troubleshooting][troubleshooting].

## 5. Install the launchd agent

Use [examples/com.omo.gpt2omo.tunnel.plist](../examples/com.omo.gpt2omo.tunnel.plist)
and the [local supervision conventions](local-bridge-supervision.md). It invokes
the pinned binary directly as `run --profile <PROFILE_NAME>`; no shell wrapper,
`nohup`, separate runtime manager, or secret-bearing command argument is needed.

The template sets `RunAtLoad=true`, `KeepAlive=true`, and `ThrottleInterval=15`.
It uses the same base PATH as the bridge/relay agents, an explicit HOME/profile
directory, and a loopback-only admin port. `MCP_STARTUP_WAIT_TIMEOUT=60s` tolerates
initial bridge startup, but is not a launchd ordering dependency or a substitute for
health checks. The bridge must be healthy on `127.0.0.1:18800` before acceptance.

The renderer below replaces XML placeholders with absolute paths and the selected
profile/port, without reading the key contents. It refuses to overwrite an existing
agent. For an upgrade, preserve the old plist and use a deliberate maintenance
procedure rather than running the first-install block over a live agent.

```bash
mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs"
touch "$HOME/Library/Logs/gpt2omo-tunnel.log"
chmod 600 "$HOME/Library/Logs/gpt2omo-tunnel.log"
python3 - <<'PY'
import os
from pathlib import Path
import plistlib
import re
import stat

repo = Path(os.environ['REPO']).expanduser().resolve()
home = Path.home()
port = int(os.environ['TUNNEL_ADMIN_PORT'])
if not 1024 <= port <= 65535 or port in (18800, 9353, 9354):
    raise SystemExit('Choose an unused non-privileged port, not bridge/Chrome ports')
profile = os.environ['TUNNEL_PROFILE']
if not re.fullmatch(r'[A-Za-z0-9_-]+', profile):
    raise SystemExit('Use a simple profile name without a path or extension')
paths = {key: Path(os.environ[key]).expanduser().resolve() for key in
         ('TUNNEL_CLIENT_BIN', 'TUNNEL_PROFILE_DIR', 'TUNNEL_API_KEY_FILE')}
for key, path in paths.items():
    if path == repo or repo in path.parents or not path.exists():
        raise SystemExit(key + ' must exist outside the repository')
if not os.access(paths['TUNNEL_CLIENT_BIN'], os.X_OK):
    raise SystemExit('Pinned tunnel-client must be executable')
if stat.S_IMODE(paths['TUNNEL_API_KEY_FILE'].stat().st_mode) != 0o600:
    raise SystemExit('Runtime key file must have mode 0600')
if not (paths['TUNNEL_PROFILE_DIR'] / (profile + '.yaml')).is_file():
    raise SystemExit('Initialize the named profile first')
values = {
    '<HOME>': str(home),
    '<PINNED_TUNNEL_CLIENT_BIN>': str(paths['TUNNEL_CLIENT_BIN']),
    '<PROFILE_NAME>': profile,
    '<TUNNEL_PROFILE_DIR>': str(paths['TUNNEL_PROFILE_DIR']),
    '<TUNNEL_API_KEY_FILE>': str(paths['TUNNEL_API_KEY_FILE']),
    '<TUNNEL_ADMIN_PORT>': str(port),
}
def render(value):
    if isinstance(value, dict):
        return {key: render(item) for key, item in value.items()}
    if isinstance(value, list):
        return [render(item) for item in value]
    if isinstance(value, str):
        for placeholder, replacement in values.items():
            value = value.replace(placeholder, replacement)
        if '<' in value or '>' in value:
            raise SystemExit('Unresolved template placeholder')
    return value
with (repo / 'examples/com.omo.gpt2omo.tunnel.plist').open('rb') as source:
    agent = render(plistlib.load(source))
destination = home / 'Library/LaunchAgents/com.omo.gpt2omo.tunnel.plist'
fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'wb') as out:
    plistlib.dump(agent, out, sort_keys=False)
print('Rendered:', destination)
PY
plutil -lint "$HOME/Library/LaunchAgents/com.omo.gpt2omo.tunnel.plist"
launchctl bootstrap "gui/$(id -u)" \
  "$HOME/Library/LaunchAgents/com.omo.gpt2omo.tunnel.plist"
launchctl print "gui/$(id -u)/com.omo.gpt2omo.tunnel"
```

`launchd` does not expand `~`, `$HOME`, or command substitutions inside a plist.
Do not load the unrendered template. Both stdout and stderr use
`~/Library/Logs/gpt2omo-tunnel.log` (a consolidated log for this template, rather
than the split out/err names sketched in the plan). Rotate it under the operator's
normal log policy. Never enable raw HTTP logging for routine verification.

This is a per-user LaunchAgent: after reboot it starts when that user logs into the
GUI session, not before login like a system LaunchDaemon. Validate persistence at
the next approved reboot/login with no active/reserved workers; then repeat the
following checks and the account smoke tests. Do not restart any service just to
exercise KeepAlive during an active delegation.

## 6. Validate the running client locally

```bash
export TUNNEL_ADMIN_URL="http://127.0.0.1:$TUNNEL_ADMIN_PORT"
curl --noproxy '*' --connect-timeout 5 --max-time 10 --fail --silent --show-error \
  "$TUNNEL_ADMIN_URL/healthz"
curl --noproxy '*' --connect-timeout 5 --max-time 10 --fail --silent --show-error \
  "$TUNNEL_ADMIN_URL/readyz"
open "$TUNNEL_ADMIN_URL/ui"
tail -n 100 "$HOME/Library/Logs/gpt2omo-tunnel.log"
launchctl print "gui/$(id -u)/com.omo.gpt2omo.tunnel"
```

Require HTTP 200 from both health endpoints, the expected `tunnel_id` and `main`
HTTP target in `/ui`, and successful polling without authorization/connectivity
errors in the logs. `/healthz` is process liveness; `/readyz` adds startup checks.
Neither alone proves a successful authenticated ChatGPT tool round-trip. Review
readiness details: a reachable endpoint requiring auth or a tolerated probe timeout
can still be reported as ready. These are the **client's** endpoints on the admin
port, not the bridge health endpoint on 18800. Never route this admin port through
the legacy public tunnel or enable remote UI access.

## 7. Associate ALL target ChatGPT workspaces and create Tunnel apps

In [Platform tunnel settings][platform-tunnels], edit the tunnel associations to
include its owning Platform organization and **every target ChatGPT workspace**.
The same `tunnel_id` can serve multiple associated workspaces. Account labels in
this repository are not workspace IDs. Resolve the actual workspace selected in
each account's ChatGPT UI; an organization association alone is not enough.

| Browser account | Operator must record | Acceptance |
|---|---|---|
| `remote-chrome` | Actual ChatGPT workspace name/ID and tunnel association | Tunnel app visible; tools load and a scoped call succeeds |
| `remote-chrome-2` | Actual ChatGPT workspace name/ID and tunnel association | Tunnel app visible; tools load and a scoped call succeeds |

Repeat for additional workspaces used by either account. For personal accounts,
use the corresponding personal organization/workspace mapping; do not assume an
association with one account covers the other. If the Platform/workspace mapping
cannot be verified by the UI, ask the workspace/organization administrator or the
OpenAI account team to resolve it rather than substituting a public URL.

While the client remains healthy, sign into **each** account, select its target
workspace, enable developer mode, and open [chatgpt.com/plugins][chatgpt-plugins].
Use the plus/create developer-mode app action, select **Connection = Tunnel**, and
select the associated tunnel (or supply the recorded `tunnel_id` when offered).
Use a distinct migration app name so the old authenticated connector remains
identifiable. Do not paste the runtime API key, local URL, or `code.checka.cc` into
the Tunnel selection. Complete any app authorization required by the deployed
server and verify that the app appears and its tools load in that workspace.

Expect the repository's **18 standard tools** with the normal production feature
flags; record any intentionally enabled optional `query_subagent` separately.
Do not attribute a changed catalog, connector safety scan, or discovery failure to
the transport without comparing the same bridge configuration. Missing app/tunnel:
check the active workspace, association, app creator's Read + Use, developer-mode
permission, and running client before changing network settings.

## 8. Parallel verification and cutover gates

Keep the legacy configuration intact. Parallel verification means comparing the
new route with an **already authenticated** legacy route, not leaving an
unauthenticated public listener running for convenience. If the legacy route cannot
reject anonymous access to all exposed bridge surfaces, disable its public
forwarding and record that live legacy comparison is blocked; never bypass this
security gate. Preserve its configuration for a later authenticated recovery.

Use operator/coordinator-created, read-only acceptance delegations in each account,
with fresh scope IDs. Select only the intended connector in each test conversation;
do not dispatch a second implementation worker or switch connectors under an
active worker. Use equivalent tasks/configurations for the old and new route.
Local `delegate_to_chatgpt_web --bridge-url` remains `http://127.0.0.1:18800`;
ChatGPT's selected app determines the remote MCP path. A local curl or helper call
alone does not test the outbound tunnel.

| Gate | Procedure | Required evidence |
|---|---|---|
| Tool round-trips | On both accounts, run `task_state`, `task_plan`, `read_file`, a harmless `run_command`, `git_status_diff`, `task_update`, and finally `completion_check` in an acceptance scope. | Correct content, command exit/status, normal scope enforcement, and final `ready=true`; unchanged standard tool catalog. |
| Latency | Time repeated identical small reads/commands over each connector, including a cold call and normal concurrent load. Exercise detached commands and `poll_command` too. | Per-request elapsed time, sample count, median/p95/max, failures, and new-minus-old latency delta. Every connector request must complete within the project's **60 s connector timeout**, with operating headroom. |
| Large output | Produce approximately **10 MiB** of synthetic output to exercise the bounded ring, then poll/drain available output until the command is terminal. | Success without transport timeout/deadlock; final marker and expected truncation/pagination behavior. Record limits and observed retained bytes. |
| No remote SSE dependency | Confirm tunnel tool calls and completion succeed without connecting ChatGPT to the bridge's `/events`. Keep `gpt2omo-relay` subscribed locally under its existing auth policy. | Local relay still delivers continuation/completion events; no public `/events` subscription or new SSE listener is required. |

For the harmless command, pass the following JSON arguments to the MCP
`run_command` tool, replacing the scope and secret placeholders with those from
that acceptance scope:

```json
{
  "scope_id": "<ACCEPTANCE_SCOPE_ID>",
  "capability_secret": "<ACCEPTANCE_CAPABILITY_SECRET>",
  "command": "python3 -c \"print('TUNNEL_SMOKE_OK')\"",
  "timeout_ms": 10000
}
```

For detached-command behavior, use `python3 -c "import time; time.sleep(20);
print('TUNNEL_DETACHED_OK')"` as a single-line command with a 30000 ms command
timeout. Recover the returned command ID with `poll_command`/`list_commands`; each
poll wait stays at or below 15000 ms. A command may run longer than 60 s only when
its individual MCP requests still return within the connector budget. Do not raise
connector timeouts or loosen command allowlists to pass the test.

For the large-output test, use:

```json
{
  "scope_id": "<ACCEPTANCE_SCOPE_ID>",
  "capability_secret": "<ACCEPTANCE_CAPABILITY_SECRET>",
  "command": "python3 -c \"import sys; chunk='x'*8191+'\\n'; [sys.stdout.write(chunk) for _ in range(1280)]; sys.stdout.write('TUNNEL_LARGE_OUTPUT_DONE\\n')\"",
  "timeout_ms": 30000
}
```

The bridge pages output (at most 32 KiB per stream / 64 KiB combined per response)
and retains only a bounded recent ring. Do **not** demand one lossless 10 MiB MCP
response or interpret documented ring truncation as tunnel data loss. Record the
actual configured retention and drain the retained pages to the final marker.
This is generated test data, never source dumps, tokens, or customer data.

The absence of a remote `/events` requirement is specific to this bridge's local
relay architecture. Secure MCP Tunnel can forward intermediate MCP SSE events;
this runbook does not claim that the tunnel lacks streaming support.

Store evidence outside the repository, including date, account/workspace, release
pin, connector route, tool/call identifier, elapsed time, output size/truncation,
result, and log references. Include a comparison table and an explicit pass/fail
for all four gates for both accounts. Don't record scope IDs or capability secrets
in shared reports. Publication of this runbook is not evidence that these live
gates passed.

## 9. Retire the public cloudflared route

Only the operator may cut over after all four parallel-verification gates and both
workspace/account checks pass. Drain active/reserved workers, designate the Tunnel
app for new dispatches, and retire public forwarding for `code.checka.cc` without
changing the bridge, relay, Chrome profiles, or scope directory.

For an existing **per-user launchd-managed** legacy tunnel, first verify its exact
service definition and then stop only that job. The following is a maintenance
command, not something the documentation worker executes:

```bash
export LEGACY_CLOUDFLARED_PLIST='<ABSOLUTE_PATH_TO_EXISTING_CLOUDFLARED_LAUNCHAGENT_PLIST>'
launchctl bootout "gui/$(id -u)" "$LEGACY_CLOUDFLARED_PLIST"
```

For a system service or another supervisor, use its preserved operator procedure
instead; do not guess a label or use broad process kills. Prevent automatic legacy
relaunch using that supervisor's existing policy, and record how to reverse it.
Keep the old configuration, tunnel/DNS mapping, credential files, agent definition,
and authenticated app settings; **do not delete them**. Confirm public forwarding
is no longer serving the bridge, then repeat the Tunnel smoke test in both accounts.

## 10. Rollback

Rollback restores only a previously verified **authenticated** legacy route. It
must never restore unauthenticated public `/mcp`, `/events`, or control endpoints,
change the bridge to a wildcard bind, or use a quick-tunnel command. A scope UUID
alone is not public endpoint authentication. If the retained route cannot enforce
that boundary, fail closed and repair the secure tunnel instead of exposing it.

At an approved maintenance window, drain workers and preserve failure evidence.
For the per-user launchd case, stop only the new tunnel client, then restore the
previously verified legacy agent from the preserved definition:

```bash
export LEGACY_CLOUDFLARED_PLIST='<ABSOLUTE_PATH_TO_EXISTING_AUTHENTICATED_CLOUDFLARED_LAUNCHAGENT_PLIST>'
launchctl bootout "gui/$(id -u)" \
  "$HOME/Library/LaunchAgents/com.omo.gpt2omo.tunnel.plist"
launchctl bootstrap "gui/$(id -u)" "$LEGACY_CLOUDFLARED_PLIST"
```

Use the original supervisor procedure for non-LaunchAgent deployments. Confirm
anonymous public access is denied and authorized connector requests succeed, select
the preserved authenticated legacy app in **both** accounts, and rerun the scoped
smoke checks. The local helper/relay URLs stay loopback. Do not delete or rotate
scope state, close retained tabs, or restart the bridge as part of transport rollback.
Keep the Secure MCP Tunnel profile, pinned binary, and Platform association for
repair and retesting; revoke/rotate credentials only when compromise or the normal
key policy requires it.

## 11. Troubleshooting and handoff

| Symptom | Check without weakening security |
|---|---|
| Platform denies access | Correct organization and manager/runtime role split; ChatGPT developer mode does not grant Platform permissions. |
| Doctor passes but polling gets 401/403 | Correct runtime key, Read + Use for its principal, tunnel ID, and associations. Do not substitute an admin key. |
| `/healthz` is 200 but tools fail | `/readyz` details, `/ui` polling status, loopback bridge health/auth, and the selected workspace/app. |
| App appears in only one account | Add every missing workspace association and check that account's developer-mode and Read + Use permissions. |
| Agent repeatedly exits | Absolute pinned executable/profile paths, 0600 key file readability, admin-port collision, and the consolidated log. |
| Long-poll failures or slow calls | Outbound HTTPS/proxy policy, polling logs, per-request timings, and bridge load; preserve TLS verification. |

For key rotation, issue a replacement with the same restricted permissions, store
it privately, and arrange an operator-controlled client restart/revalidation with
no active workers. Secret references resolve at startup; do not assume a running
client automatically reloads a changed file. Never print key contents while checking
permissions or include raw HTTP logs in a shared incident report.

Handoff is complete only after recording the release pin, private file locations,
workspace/account matrix, doctor result, health/UI/polling results, all four parallel
gates, authenticated rollback readiness, cutover result, and reboot/login persistence
check (or an explicit pending maintenance check). No registration, service lifecycle
operation, or live migration result is implied by a documentation-only change.

## Official references

Recheck these sources and the chosen binary's help before each operator upgrade;
this document intentionally contains no hard-coded release version.

- [OpenAI Secure MCP Tunnel guide][openai-guide]
- [Latest official tunnel-client release][latest-release] and [installation/release validation][client-readme]
- [Client configuration][configuration], [permissions][permissions], and [troubleshooting][troubleshooting]

[openai-guide]: https://developers.openai.com/api/docs/guides/secure-mcp-tunnels
[latest-release]: https://github.com/openai/tunnel-client/releases/latest
[client-readme]: https://github.com/openai/tunnel-client
[configuration]: https://github.com/openai/tunnel-client/blob/master/docs/configuration.md
[permissions]: https://github.com/openai/tunnel-client/blob/master/docs/permissions.md
[troubleshooting]: https://github.com/openai/tunnel-client/blob/master/docs/troubleshooting.md
[platform-tunnels]: https://platform.openai.com/settings/organization/tunnels
[runtime-keys]: https://platform.openai.com/settings/organization/api-keys
[chatgpt-plugins]: https://chatgpt.com/plugins
