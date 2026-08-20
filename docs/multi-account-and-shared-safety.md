# Multi-Account Routing, Browser Isolation, and Shared-Account Safety

## Status

Architecture and security design for `gpt2omo` Web delegation. This document is intentionally implementation-oriented: it records current constraints, target invariants, proposed schemas, routing rules, browser isolation options, threat boundaries, and an incremental roadmap.

## Executive recommendation

1. **Add a first-class account scheduler, not account awareness inside telemetry alone.** Each fresh Web delegation must be assigned to one stable `account_id` before a browser tab is created. Sliding-window quota, active-worker capacity, reservations, cooldown, and health are account-scoped. A retained scope is permanently affine to the account/browser instance that created it.
2. **Use a dedicated Chromium user-data directory per ChatGPT account.** The preferred deployment is one persistent browser process (or one Orca/browser-driver instance) per account, each with a distinct profile directory and loopback-only CDP/driver endpoint. Tabs in one shared profile are not account-isolated.
3. **Treat `scope_id` as an unguessable bearer capability, not an identity or conversation binding.** It prevents blind cross-workspace access because it is a UUIDv4 that must resolve to a registered scope, but any caller that learns a live scope ID can use that scope. There is no OpenAI-provided `context_id` or user principal in current MCP calls.
4. **Do not treat a shared ChatGPT account as a security boundary.** If another user of the same ChatGPT account can see the delegation conversation/sidebar, they can see the `scope_id`, workspace path, prompts, and tool results. If that shared account can invoke the same MCP connector, a leaked live scope is replayable. Shared-account use is acceptable only when every account user is mutually trusted, or the connector is reduced to a low-risk/read-only policy with additional host isolation.
5. **Close current host-security gaps before advertising shared-account safety.** Bearer authentication is conditional on `cli.token`; `main` currently calls `load_token_file()` rather than `ensure_auth()`. `run_command` is not an OS sandbox: it executes allowed programs as the daemon user with the workspace only as `cwd`. Control-plane secrets must also be kept outside any workspace-visible path.

## Current operational setup

Run `gpt2omo-account-onboard prepare --account <id> ...` to create the pending configuration, provision isolated profiles, and open the account login pages. After logging in, use `gpt2omo-account-onboard wait --timeout-seconds 600` (or `status` for one check), then `gpt2omo-account-onboard activate --confirm`. The command promotes `accounts.pending.json` to `accounts.json` only when every browser is ready and no active, retained, or unknown legacy scope remains. `docs/accounts.example.json` is a schema reference, not a file to copy over live routing.

Each enabled account must have a unique `browser.instance`, `browser.user_data_dir`, and loopback `browser.cdp_endpoint`. The runtime uses the CDP endpoint to address the exact profile that owns each page; a separate cmux workspace alone does not isolate ChatGPT cookies.

Before enabling the file:

1. Run `gpt2omo-account-onboard prepare --account primary --account secondary` and log into the intended ChatGPT account in each opened profile.
2. Run the bridge with a mount root narrower than `/` and containing every delegated repository, for example `/Users/example/code/project`. Profile paths must remain outside that mount root, so the current `--mount-root /` development launch intentionally cannot accept isolated profile configuration.
3. Run `gpt2omo-account-onboard status`; only after it reports every account ready may `gpt2omo-account-onboard activate --confirm` promote routing. The activation itself rejects active or retained legacy scopes. Restart the bridge and relay only after activation and after active delegations have completed, then run `gpt2omo-account-status --mount-root <mount-root>` to verify both accounts are reachable and have the expected login state.

Leave `browser.driver` unset when the intended policy is automatic **cmux first, then Orca fallback**. Set it only to deliberately pin a specific driver for an account.

Fresh work is scheduled account-by-account. Retained scopes stay bound to the account and browser instance that created them; disabling an account prevents new work but does not silently migrate retained scopes.

---

## 1. Current implementation: relevant facts

### 1.1 Dispatch and quota policy are global today

`src/bin/delegate_to_chatgpt_web.rs` currently creates one `OrcaConfig` from a single `worktree`, `terminal`, and binary. Fresh task staging is serialized by one `~/.omo/bridge/.dispatch.lock`.

Runtime policy is loaded from `~/.omo/bridge/config.json` and currently contains process-wide values:

- `max_new_dispatch_workers` (compiled default 2)
- `max_concurrent_in_flight_workers` (compiled default 3)
- `window_minutes` (default 60)
- `max_dispatches_per_window` (default 12)

`check_rate_limit_and_window_guards()` reads global telemetry. `active_rate_limit_lockout()` and `recent_dispatches_in_window()` do not distinguish ChatGPT accounts. `count_active_in_flight_workers()` counts every live scope in the `WorkspaceMux`.

This means a rate-limit banner seen in account A currently blocks new dispatches that could have used account B, and account A/B cannot have independent active-worker ceilings.

### 1.2 Browser identity is not persisted

`WorkspaceScope` currently stores:

- `scope_id`
- canonical workspace path
- optional terminal
- optional `browser_page_id`
- timestamps

It does **not** store an account ID, browser profile ID, driver instance ID, or CDP endpoint. Resume/cleanup code therefore assumes one global browser driver namespace and calls `verify_chatgpt_page` / `close_browser_page` through the same `OrcaConfig` used by the dispatcher.

A multi-account implementation must make browser identity part of scope persistence. A `browser_page_id` by itself is insufficient if page identifiers can collide across multiple browser-driver endpoints.

### 1.3 Current Orca/browser-driver abstraction has no profile selector

`BrowserDriverConfig` (`src/orca.rs`) currently has:

- optional driver kind
- optional binary
- `worktree`
- optional terminal
- cached driver detection

For Orca, a tab is created with `orca tab create --url https://chatgpt.com --worktree <worktree> --json`; subsequent evaluation and close calls address `--page <browserPageId>` only.

The current bridge therefore has no explicit way to request a distinct Chromium `user-data-dir`, named Orca profile, driver instance, or CDP endpoint per account. `worktree` must not be assumed to be a browser-security partition unless Orca explicitly guarantees that property.

### 1.4 Scope capability enforcement is real, but it is bearer-style

Every MCP tool schema is amended to require `scope_id`. The request handler:

1. extracts `scope_id`,
2. rejects an empty value,
3. calls `WorkspaceMux::resolve(scope_id)`, and only then
4. dispatches the tool against the resolved `Workspace`.

`WorkspaceMux` validates UUID syntax, loads an on-disk scope record whose `scope_id` must match, canonicalizes the recorded workspace, and verifies it remains beneath the configured mount root. A malformed scope is rejected before filesystem lookup; a well-formed random/expired UUID fails as unknown/expired.

Because `WorkspaceMux::register*()` generates UUIDv4 IDs and there is no MCP `list_scopes` tool, blind guessing is impractical. However, there is no caller identity, ChatGPT user ID, connector principal, or conversation ID bound to the scope. Possession of the live UUID is the authorization fact.

### 1.5 File-tool confinement is stronger than command confinement

`Workspace::resolve_relative()` rejects absolute paths, `..`, hidden/secret path components according to `PathPolicy`, and symlink escapes outside the canonical workspace.

`run_command`, however, is **not a chroot/container/capability-mode subprocess sandbox**. `CommandManager` starts the selected binary with `.current_dir(workspace_root)`, a sanitized environment, and process-group handling. The command allowlist includes general-purpose execution surfaces such as `python`, `python3`, `node`, `npm`, `make`, and other build tools. Argument validation catches obvious absolute paths and `..`, but it cannot prove that an interpreter or build script will not open files or sockets outside the workspace.

Therefore a stolen scope that is allowed to call `run_command` can have a substantially larger host impact than the file-tool path jail alone suggests. A true shared-user threat model requires OS-level subprocess isolation or removal/approval of the command capability.

### 1.6 Bearer authentication is transport authentication, not human identity

`verify_auth()` compares the HTTP `Authorization` header to `Bearer <configured token>` in constant-time style when `cli.token` is `Some`. If no token is configured, it accepts the request.

`Cli::ensure_auth()` exists and can create a random 32-byte token with mode `0600`, but the current daemon entry point calls `load_token_file()` rather than `ensure_auth()`. Thus the implementation does not currently enforce the stronger default described by `ensure_auth()` itself.

Also, a connector bearer token authenticates the connector/request to the bridge. If several humans use one shared ChatGPT account with one configured connector, the bearer token does not distinguish those humans.

---

## 2. Target security and scheduling invariants

A multi-account design should make these invariants explicit and testable.

### Routing invariants

1. Every fresh Web delegation is assigned exactly one `account_id` before browser creation.
2. Account assignment is made under an inter-process scheduler lock or equivalent transactional store.
3. Sliding-window dispatch usage is account-local.
4. Active-worker capacity is account-local.
5. Cooldown/health state is account-local except for explicitly classified platform-global failures.
6. A reservation consumes capacity while browser creation/bootstrap is in progress, preventing concurrent dispatch processes from overbooking an account.
7. A retained scope never silently migrates to another account. Resume has strict account affinity.
8. Removing/disabling an account prevents new assignments but does not invalidate already-retained scopes unless an administrator explicitly drains/closes them.

### Browser isolation invariants

1. Different ChatGPT accounts never share a Chromium cookie jar/localStorage profile.
2. Browser profile paths are never located inside a delegated source workspace.
3. Browser automation endpoints are loopback-only unless protected by a separate authenticated transport.
4. A persisted browser binding contains enough information to route `eval`, health-check, and close calls to the exact browser instance that owns the page.
5. Cookies/session tokens are not copied into `accounts.json`, telemetry, prompts, or scope files.

### Capability/security invariants

1. `scope_id` remains unguessable and non-enumerable through MCP.
2. A scope is bound to one canonical workspace and one browser/account binding.
3. Control-plane state (`token`, accounts config/state, browser profiles) is never readable as workspace content.
4. Remote/tunnel exposure fails closed without bearer authentication.
5. Mutating/command capabilities are not considered safe against mutually untrusted shared-account users without an additional identity/approval boundary.
6. Subprocesses are OS-sandboxed to an intentional filesystem/network policy, or command execution is disabled in the shared-user profile.

---

## 3. Proposed configuration model

Use a host/admin-owned file such as:

`~/.omo/bridge/accounts.json`

The directory should be mode `0700` and the file mode `0600` on Unix. Store profile *references*, never ChatGPT cookies or credentials.

Example:

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
      "max_active_workers": 3
    },
    "cooldown": {
      "unknown_rate_limit_seconds": 900,
      "delivery_failure_seconds": 30
    }
  },
  "accounts": [
    {
      "id": "web-a",
      "enabled": true,
      "limits": {
        "max_dispatches": 12,
        "max_active_workers": 3
      },
      "browser": {
        "driver": "orca",
        "instance": "chatgpt-web-a",
        "user_data_dir": "/Users/example/.omo/bridge/browser-profiles/web-a",
        "cdp_endpoint": "http://127.0.0.1:9223",
        "worktree": "active"
      }
    },
    {
      "id": "web-b",
      "enabled": true,
      "browser": {
        "driver": "orca",
        "instance": "chatgpt-web-b",
        "user_data_dir": "/Users/example/.omo/bridge/browser-profiles/web-b",
        "cdp_endpoint": "http://127.0.0.1:9224",
        "worktree": "active"
      }
    }
  ]
}
```

Notes:

- `id` should be an opaque stable identifier, not the email address. Account email is unnecessary PII for routing.
- Per-account `limits` inherit missing values from `defaults`.
- `user_data_dir` and `cdp_endpoint` are host-side control data and must not be sent to ChatGPT prompts.
- `instance` is a logical driver instance name. Its exact transport can be Orca-native, a wrapper, or a future browser broker.
- The existing global `config.json` can retain daemon-wide safety limits such as maximum batch size. Account-specific quotas should move to `accounts.json`.

Suggested Rust model:

```rust
struct AccountsConfig {
    version: u32,
    routing: RoutingConfig,
    defaults: AccountDefaults,
    accounts: Vec<AccountConfig>,
}

struct AccountConfig {
    id: AccountId,
    enabled: bool,
    limits: PartialAccountLimits,
    browser: BrowserInstanceConfig,
}

struct AccountLimits {
    window_ms: u64,
    max_dispatches: usize,
    max_active_workers: usize,
}

struct BrowserInstanceConfig {
    driver: BrowserDriverKind,
    instance: String,
    user_data_dir: Option<PathBuf>,
    cdp_endpoint: Option<Url>,
    worktree: String,
}
```

Validate at load time:

- unique account IDs,
- positive limits,
- loopback CDP endpoints by default,
- distinct `user_data_dir` values across enabled accounts,
- distinct driver instances/endpoints where the driver requires them,
- profile directories outside `mount_root` and outside every delegated workspace,
- no symlinked profile path escaping the bridge control directory unless explicitly allowed by the administrator.

---

## 4. Runtime state model

Do not make best-effort telemetry the authoritative quota state. Telemetry can be lost; a safety limiter should fail closed if its state is corrupt/unreadable.

Recommended layout:

```text
~/.omo/bridge/
  accounts.json
  router-state.json
  account-state/
    web-a.json
    web-b.json
  locks/
    router.lock
  browser-profiles/
    web-a/
    web-b/
```

`router-state.json`:

```json
{
  "version": 1,
  "round_robin_cursor": 17,
  "updated_ms": 1787187000000
}
```

Per-account runtime state:

```json
{
  "version": 1,
  "account_id": "web-a",
  "dispatches_ms": [1787184000000, 1787185200000],
  "reservations": [
    {
      "reservation_id": "uuid",
      "created_ms": 1787187000000,
      "expires_ms": 1787187120000
    }
  ],
  "cooldown_until_ms": null,
  "cooldown_reason": null,
  "health": "ready",
  "last_selected_ms": 1787187000000,
  "last_success_ms": 1787186800000,
  "consecutive_browser_failures": 0
}
```

Only persist what cannot be safely reconstructed.

- **Dispatch window:** persistent; prune entries older than `window_ms` on every routing transaction.
- **Reservations:** persistent with short TTL; reconcile stale reservations after crashes.
- **Active worker count:** preferably derive from live `WorkspaceScope` records + process locks, grouped by `account_id`, so state does not permanently drift after crashes.
- **Cooldown:** persistent because rate-limit evidence must survive dispatcher restart.
- **Health/auth-required:** persistent until a health probe or explicit admin action clears it.

Telemetry should add `account_id: Option<String>` for observability/backward compatibility, but rate enforcement should use the authoritative account-state transaction.

---

## 5. Scope/browser persistence model

Upgrade `WorkspaceScope` to a versioned browser binding rather than adding more parallel scalar fields.

```rust
struct WorkspaceScopeV2 {
    version: u32,
    scope_id: String,
    workspace: String,
    browser: Option<BrowserBinding>,
    created_ms: u64,
    updated_ms: u64,
}

struct BrowserBinding {
    account_id: AccountId,
    driver: BrowserDriverKind,
    instance: String,
    page_id: String,
}
```

The critical property is that `page_id` is interpreted only inside the recorded driver instance/account.

Migration:

- Read V1 scopes as `account_id = "default"` using the legacy global `OrcaConfig`.
- Write only V2 after multi-account is enabled.
- Do not infer account from a page title, ChatGPT UI text, or current browser login.
- When resuming, load the scope first, resolve its `BrowserBinding`, then route verification/send/close through that exact instance.

`StagedDelegation` should carry the same `account_id`/browser binding so telemetry and UI errors can update the correct account state.

---

## 6. Routing algorithm

### 6.1 Candidate filtering

For each enabled account, atomically compute:

- `window_used = committed dispatches within window`
- `reserved_window = live reservations`
- `active = live in-flight scopes for that account`
- `reserved_active = live reservations not yet represented by an active scope`

An account is eligible only if:

```text
enabled
AND health == ready
AND now >= cooldown_until_ms
AND window_used + reserved_window < max_dispatches
AND active + reserved_active < max_active_workers
```

If no account is eligible, return a structured exhaustion result containing the earliest relevant retry time per account. Do not silently ignore a rate limit and do not open a tab before capacity is reserved.

### 6.2 Atomic reservation

Under `router.lock`:

1. load/validate config,
2. load/reconcile router/account states,
3. prune expired dispatch history and reservations,
4. count live scopes by `account_id`,
5. select account,
6. create a reservation with TTL,
7. persist account/router state atomically,
8. release the lock.

Then create the browser tab.

On successful Web dispatch, reacquire the lock and convert the reservation into a committed `dispatches_ms` entry. If browser creation/bootstrap fails, release the reservation; optionally apply a short browser-health backoff. For conservative rate accounting, any attempt that actually caused a ChatGPT Web request may be committed even if readiness later fails.

This avoids holding one global file lock across slow browser navigation while still preventing two dispatcher processes from selecting the same last slot.

### 6.3 Round-robin

Persist a monotonically increasing cursor/sequence. Starting after the last selected account, choose the first eligible account in deterministic configuration order. Advance the cursor when a reservation is successfully created, not when the task finishes.

Properties:

- simple and predictable,
- naturally distributes bursty work,
- does not prefer an account with much more unused capacity.

Recommended when accounts have equal limits.

### 6.4 Least-loaded

Use a deterministic lexicographic score rather than a fragile magic-weight formula:

```text
(
  active_plus_reserved / max_active_workers,
  window_used_plus_reserved / max_dispatches,
  last_selected_ms,
  account_id
)
```

Choose the minimum tuple. Ratios should be compared as integer cross-products to avoid floating-point differences.

This first favors concurrency headroom, then rate-window headroom, then fairness. If desired, swap the first two dimensions when Web-rate conservation is more important than latency.

### 6.5 Batch dispatch

Allocate each task independently inside one routing transaction. Do not select one account once and place the whole batch there.

For a two-task batch and two idle accounts, round-robin/least-loaded should normally reserve one task on each account.

### 6.6 Resume semantics

Resume is not a routing decision.

1. Load the retained scope.
2. Read `browser.account_id` and driver instance.
3. Check that account configuration still exists (enabled status may be ignored for already-retained scopes, depending on drain policy).
4. Apply that account's window/cooldown/concurrency policy.
5. Resume on the exact recorded page/instance.

If that account is rate-limited or logged out, return `BLOCKED`/`AUTHENTICATION_REQUIRED`. **Never resume the same ChatGPT conversation in another account**, because the conversation and page storage belong to the original login.

---

## 7. Cooldown and failure classification

A single `cooldown_until_ms` is enough for an MVP, with a reason field for diagnostics.

Suggested reactions:

| Condition | Account action | Cross-account action |
|---|---|---|
| `too_many_requests` | cooldown until UI reset hint, else default 15m | continue other accounts |
| `usage_limit` | cooldown until reset hint, else conservative default | continue other accounts |
| `model_quota` | account/model cooldown if model identity is known; otherwise account cooldown | continue other accounts |
| `capacity` | short account backoff | optionally short global soft backoff; do not assume other accounts are exhausted |
| authentication required | mark `auth_required` until re-login/health check | continue other accounts |
| recoverable delivery error | no hard cooldown; retry policy | none |
| repeated browser-driver failure | mark account/instance degraded for short backoff | continue healthy instances |

When a DOM probe observes a rate limit, telemetry and account state must be updated with the `StagedDelegation.account_id`; a limit on account A must never activate a global one-hour lockout for account B.

---

## 8. Browser profile and account isolation

### 8.1 Option A — dedicated `--user-data-dir` per account (recommended)

**Architecture:** one persistent Chromium profile directory and browser process per ChatGPT account.

Example conceptual process layout:

```text
browser account web-a
  user-data-dir = ~/.omo/bridge/browser-profiles/web-a
  remote-debugging = 127.0.0.1:9223

browser account web-b
  user-data-dir = ~/.omo/bridge/browser-profiles/web-b
  remote-debugging = 127.0.0.1:9224
```

Each profile is logged into a different ChatGPT account once. Cookies, localStorage, IndexedDB, service-worker state, and anti-abuse/session state are then naturally separated by Chromium.

**Feasibility:** high at the Chromium/CDP level.

**Stability:** highest of the three approaches because it uses the browser's normal persistent-profile model instead of copying authentication state.

**Important constraint:** the same `user-data-dir` must not be opened concurrently by multiple independent Chrome processes. The bridge/browser broker should own a profile lock.

**Compatibility with current bridge:** requires an extension. The current `OrcaConfig` has no user-data-dir/endpoint selector, so gpt2omo cannot achieve this merely by changing `accounts.json`. Either Orca must expose account-specific browser instances, or gpt2omo needs a small browser-instance broker/adapter.

### 8.2 Option B — Orca native multi-profile / multi-instance / port multiplexing

This is attractive **if Orca provides a stable instance selector on all operations**.

Required semantics:

- create/open a browser instance with a specified profile/user-data directory,
- give it a stable `instance` or endpoint ID,
- create tabs against that instance,
- evaluate/close/verify against `(instance, page_id)` or a globally unique page handle,
- preserve the instance across dispatcher invocations.

The current bridge only demonstrates `--worktree` on Orca tab creation and uses `--page` thereafter. That is not sufficient evidence that `worktree` means isolated Chromium storage. Treat it as orchestration metadata, not a security partition, until the Orca API contract says otherwise.

If Orca supports multiple daemon/driver endpoints but not a profile flag, run one Orca endpoint per account and persist the endpoint/instance ID in `BrowserBinding`. This is a good practical compromise because the existing DOM evaluation logic can remain largely unchanged.

**Feasibility:** medium to high, conditional on Orca capabilities.

**Stability:** high if Orca owns process/profile lifecycle; medium if wrappers must manipulate environment variables or ports externally.

**Compatibility cost:** moderate. `BrowserDriverConfig` becomes per-account and `eval/close/create` must route through a selected instance rather than global detection.

### 8.3 Option C — cookie/storage partition isolation

Possible variants:

- CDP incognito `BrowserContext` per account,
- explicit cookie import/export,
- custom storage partition/container APIs.

**Advantages:** can share one Chromium process while isolating storage contexts.

**Problems:**

- ordinary tabs on `https://chatgpt.com` in one standard profile share cookies/localStorage;
- standard CDP incognito contexts are normally ephemeral, so re-login is needed after restart;
- exporting/importing auth cookies is sensitive and brittle, especially with Cloudflare, device/session binding, and rotating tokens;
- manually copying storage increases the chance of credential leakage into bridge state/logs;
- a single browser-process crash affects all accounts.

**Feasibility:** technically possible with a direct CDP/browser-context implementation.

**Stability:** lower for persistent ChatGPT sessions.

**Recommendation:** do not use raw cookie copying as the production design. Use dedicated persistent profiles; consider isolated BrowserContexts only for temporary/test accounts.

### 8.4 Recommended browser abstraction

Refactor from one `OrcaConfig` to an account-aware pool:

```rust
trait BrowserBackend {
    async fn create_chatgpt_page(&self, target: &BrowserTarget) -> Result<PageHandle>;
    async fn eval(&self, handle: &PageHandle, expression: &str) -> Result<Value>;
    async fn close(&self, handle: &PageHandle) -> Result<()>;
    async fn health(&self, target: &BrowserTarget) -> Result<BrowserHealth>;
}

struct BrowserTarget {
    account_id: AccountId,
    instance: String,
    driver: BrowserDriverKind,
}

struct PageHandle {
    target: BrowserTarget,
    page_id: String,
}
```

The scheduler chooses `BrowserTarget`; the browser backend never chooses the account.

---

## 9. Shared ChatGPT account threat analysis

### 9.1 What `scope_id` protects

A live `scope_id` is a high-entropy bearer capability that maps to exactly one persisted workspace scope. Its useful protections are:

- malformed IDs fail validation,
- random well-formed IDs normally fail lookup,
- callers cannot select an arbitrary workspace path directly,
- a scope cannot use relative file paths to walk into a sibling workspace,
- command records are checked for scope ownership before poll/cancel/list operations,
- lifecycle/task evidence is namespaced by the scope.

This is valuable isolation between concurrent workers **when the capability remains secret**.

### 9.2 What `scope_id` does not protect

It is not bound to:

- a ChatGPT account,
- a human user,
- a browser profile,
- a ChatGPT conversation ID,
- an OpenAI-signed request context,
- a client TLS identity.

Most tools also do not take `generation` as a caller-supplied authorization factor. Generation is used for lifecycle/verification correctness, not as a second credential for basic file access.

Therefore a caller that learns a live valid `scope_id` can invoke the tools for that workspace (subject to tool policy) until the scope is removed/expired.

### 9.3 Missing or spoofed scope behavior

**No `scope_id`:** request is rejected with `scope_id is required for every gpt2omo tool call`.

**Malformed `scope_id`:** rejected by UUID validation before normal workspace lookup.

**Random UUIDv4-shaped value:** lookup fails as unknown/expired unless it happens to name a real scope. Blind success probability is negligible.

**A valid scope copied from another conversation:** accepted today. There is no server-side conversation/user identity with which to detect that replay.

This last case is why `scope_id` alone does not make a mutually untrusted shared ChatGPT account safe.

### 9.4 Bearer token boundary

The bridge bearer token is an outer transport credential:

```text
ChatGPT connector/request --Bearer token--> gpt2omo --scope_id--> workspace capability
```

These are useful two layers against different threats:

- bearer token: keeps arbitrary network clients out,
- scope ID: limits an authenticated caller to a registered delegation workspace.

But on a shared ChatGPT account, all users of that configured connector effectively share the first layer. If one of them also learns a live scope from shared history, both layers are satisfied.

### 9.5 Localhost and tunnel boundary

Preferred daemon bind is loopback. A ChatGPT-hosted connector that cannot reach localhost requires a tunnel/reverse proxy; that endpoint becomes Internet-reachable and must be treated as hostile-network facing.

Required controls:

1. fail startup for non-loopback bind without a token,
2. require a token for any documented tunnel flow even if the daemon itself binds loopback,
3. rotate the token if connector configuration or tunnel credentials are exposed,
4. keep the tunnel origin private where possible and add tunnel-layer access controls/rate limiting,
5. wire Host/Origin validation as defense-in-depth; do not rely on CORS for server-to-server MCP authorization,
6. never put the bearer token into ChatGPT prompts/tool arguments.

**Current implementation gap:** `verify_auth()` only enforces when a token is configured, and daemon `main` does not currently call `ensure_auth()`. The target security model should make authentication fail-closed rather than documentation-only.

### 9.6 Mount root and workspace jail

For file tools, the chain is strong:

```text
scope_id -> persisted canonical workspace -> mount-root membership -> relative path policy -> symlink check
```

This prevents a caller from changing `path` to reach sibling projects or `/etc` through ordinary file tools.

Caveats:

1. A very broad workspace scope (for example the user's home directory) greatly increases the capability's blast radius.
2. `.omo` is an allowed dot component for project tooling. If a home-directory scope were permitted, bridge control-plane files under `~/.omo/bridge` could become reachable unless specifically denied. A file named `token` is not covered by the current component denylist. Control-plane paths need explicit exclusion independent of project dotfile policy.
3. `run_command` subprocesses are not OS-confined by `Workspace::resolve_relative`; cwd/path-argument validation is not equivalent to a filesystem sandbox.

Recommended policy: mount only a project parent; register only concrete repository roots; refuse `$HOME` as a workspace; explicitly deny the bridge control directory; OS-sandbox command children.

### 9.7 Shared-sidebar and prompt privacy

Even if tool authorization were perfect, a shared ChatGPT account has confidentiality risks outside MCP:

- delegation prompts include the `scope_id` and absolute workspace path,
- task prompts may disclose unreleased product details, source names, bugs, credentials accidentally pasted by the user, or customer data,
- tool results can contain source code and build output,
- retained conversations remain visible in the shared sidebar/history,
- account-level memory/customization can mix information across users,
- another user can delete/rename/interfere with conversations and retained workflow context.

The bridge cannot fix these UI/account-level privacy properties because it receives no trustworthy conversational `context_id`/human principal.

### 9.8 Is shared-account installation safe?

**For mutually untrusted humans with write/command tools: no.** The present scope model is capability isolation, not user isolation, and the capability is printed into the same shared conversation history.

It can be acceptable in these narrower modes:

- all users of the shared account are mutually trusted as equivalent bridge operators, or
- connector exposes only read-only low-risk tools, workspaces are narrow/non-sensitive, no `run_command` exists, scopes are short-lived, and the daemon runs inside a disposable OS sandbox/container with no host secrets.

A future OpenAI-provided, server-verifiable user/conversation identity could allow `(principal, context_id, scope_id)` binding. Until such metadata exists, the bridge should not claim to distinguish people sharing one ChatGPT account.

---

## 10. Recommended security hardening before multi-account rollout

### P0 — authentication and exposure

1. Call `ensure_auth()` during daemon startup (or otherwise fail closed) unless an explicit insecure local-development flag is set.
2. Reject non-loopback bind without bearer auth.
3. Require bearer auth in tunnel documentation/installer and add a startup warning/error if a known public bridge URL is configured without auth.
4. Wire Host validation as defense-in-depth and narrow CORS to expected origins where browser clients need it. CORS is not an auth replacement.

### P0 — control-plane secrecy

1. Keep `~/.omo/bridge/accounts.json`, account state, bearer token, and browser profiles outside delegated workspaces.
2. Explicitly deny the resolved bridge control directory in workspace file operations even if a scope is accidentally rooted at `$HOME`.
3. Refuse `$HOME` and other sensitive broad roots as delegation workspaces by default; allow only explicit administrative override.

### P0 — subprocess isolation

Choose one:

- run command workers in a per-scope container/VM/sandbox with only the scoped workspace mounted and controlled network access, or
- disable `run_command` for shared/untrusted connector profiles, or
- require an out-of-band local approval for command execution.

Do not describe `current_dir(workspace)` plus an executable allowlist as a complete filesystem jail.

### P0 — shared-account product policy

Add an explicit operational warning: a shared ChatGPT account must be considered one trust principal. If users are not mutually trusted, use separate ChatGPT accounts/connectors or a read-only/sandboxed bridge profile.

---

## 11. Implementation roadmap

### Phase 1 — model and scheduler

Add modules such as:

```text
src/accounts.rs        # config parsing/validation
src/account_state.rs   # durable quota/cooldown/reservations
src/router.rs          # RR / least-loaded selection
src/browser_pool.rs    # account -> browser driver target
```

Changes:

- add `account_id` to telemetry,
- create `WorkspaceScope` V2 browser binding,
- implement atomic account reservation,
- update fresh staging to ask router for an account,
- update UI rate-limit handling to cool down only that account,
- preserve legacy single-account behavior through an implicit `default` account.

### Phase 2 — persistent browser isolation

- provision one profile directory per account,
- start/attach one browser/Orca instance per account,
- make driver commands route by `BrowserTarget`,
- add account health probes (`logged_in`, `auth_required`, driver reachable),
- verify two profiles can simultaneously show different authenticated ChatGPT accounts without storage crossover.

### Phase 3 — resume, cleanup, drain

- route retained scope resume/close through persisted binding,
- make TTL janitor use the bound browser instance,
- support `draining` account state (no new work, retained work allowed),
- safely handle removed/missing account configs with a structured `BROWSER_ACCOUNT_UNAVAILABLE` terminal result.

### Phase 4 — security hardening

- enforce auth startup invariants,
- isolate control-plane paths,
- introduce OS sandbox or command-disabled/read-only connector policy,
- document shared-account trust semantics,
- optionally add per-scope expiry/rotation independent of retained-session TTL.

### Phase 5 — observability and operations

Expose local/admin diagnostics without giving ChatGPT a scope enumerator:

- account ID,
- enabled/draining/health state,
- active/reserved workers,
- window usage and next slot time,
- cooldown reason/until,
- browser instance reachability,
- login-required state.

Do **not** expose cookies, email addresses, tokens, profile file contents, or a list of live scope IDs through a general shared MCP tool.

---

## 12. Verification matrix

### Scheduler tests

- round-robin rotates across eligible accounts,
- disabled/cooldown/auth-required accounts are skipped,
- least-loaded chooses lower active/window utilization,
- window entries expire exactly at the configured boundary,
- reservations prevent concurrent overbooking,
- expired crash reservations are reclaimed,
- batch allocation spreads work,
- account-specific rate limit does not block another account,
- no-account-available result reports deterministic retry information.

### Scope/account binding tests

- fresh scope persists correct `account_id` and browser instance,
- resume always uses the original account,
- page IDs that are identical on two driver endpoints do not cross-route,
- disabling an account blocks new work but does not silently migrate retained sessions,
- V1 scope migration maps only to legacy `default` account.

### Browser isolation tests

- account A cookie/localStorage is invisible to account B,
- restarting the browser preserves each account's own login in its own profile,
- opening the same profile concurrently is rejected/serialized,
- CDP endpoints reject non-loopback exposure by default,
- authentication-required state marks only the affected account unhealthy.

### Security tests

- missing/malformed/random scopes fail,
- a valid scope cannot select another workspace path through file tools,
- bridge control-plane paths are denied even from a broad workspace,
- daemon cannot start remotely without auth,
- bearer mismatch is rejected,
- shared-account replay of a **known valid scope** is explicitly recognized as an unsupported trust scenario rather than claimed to be prevented,
- command policy test proves OS sandboxing when enabled, or proves `run_command` is absent in read-only/shared mode.

### Chaos/restart tests

- dispatcher crash after reservation but before tab creation,
- crash after tab creation but before scope persistence,
- daemon/browser restart with retained sessions,
- one account profile logout while others remain healthy,
- account config change while workers are active,
- corrupted account-state file fails closed without resetting quotas to zero.

---

## 13. Concrete recommendation

For production-quality multi-account `gpt2omo`, use this shape:

```text
                         +----------------------+
                         |  Account Scheduler   |
                         | RR / least-loaded    |
                         | quota + reservations |
                         +----------+-----------+
                                    |
                   +----------------+----------------+
                   |                                 |
          +--------v---------+              +--------v---------+
          | account web-a    |              | account web-b    |
          | profile dir A    |              | profile dir B    |
          | Orca/CDP inst A  |              | Orca/CDP inst B  |
          +--------+---------+              +--------+---------+
                   |                                 |
             ChatGPT A tabs                    ChatGPT B tabs
                   |                                 |
             scope binding                      scope binding
                   +---------------+-----------------+
                                   |
                            gpt2omo MCP tools
                                   |
                       scope -> canonical workspace
```

The scheduler owns **which account** receives new work. The browser layer owns **session isolation**. The scope layer owns **which workspace** the authenticated MCP caller may target. These are separate security/coordination dimensions and should stay separate in the implementation.

For a shared ChatGPT login, the correct operational statement is:

> `scope_id` prevents blind or accidental cross-scope access, but it is a bearer capability printed into the delegation conversation. Without a trustworthy upstream user/conversation identity, gpt2omo cannot distinguish mutually untrusted people sharing one ChatGPT account. Use separate ChatGPT accounts/connectors, or reduce the connector to a read-only + OS-sandboxed profile.
