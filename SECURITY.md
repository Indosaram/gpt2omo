# Security Policy

## Security Model & Threat Boundaries

`gpt2omo` is designed as a sandboxed local Model Context Protocol (MCP) server that provides filesystem, command execution, code intelligence, and verification capabilities to external LLM workers (such as ChatGPT Web) while bounding access by delegation scope and host policy.

### Core Security Guarantees

1. **Per-Delegation Scope Isolation (`scope_id`)**:
   - Every mutating or reading tool call requires a valid, registered `scope_id`.
   - Operations without a valid `scope_id` or with a malformed/expired scope are rejected immediately before any filesystem interaction.
   - File-tool operations in separate scopes are resolved independently against their registered workspace roots.

2. **Filesystem Capability Sandboxing**:
   - Built on `cap-std` capability-based security.
   - All file operations are resolved strictly relative to the verified workspace directory.
   - Symlink traversal, `..` path escapes, and absolute path injection outside the scoped workspace are structurally rejected.
   - Secret files, sensitive credentials (`.env`, `.git/config`, private keys), and system files outside the workspace are denied by the file-tool path policy.

3. **Command Execution Allowlist & Shell Rejection**:
   - `run_command` executes only strictly allowed build, test, and verification tools:
     - `cargo`, `rustc`, `npm`, `pnpm`, `yarn`, `bun`, `bunx`, `node`, `python`, `python3`, `pytest`, `uv`, `go`, `make`, `git`, `vitest`, `jest`, `tsc`, `biome`, `ruff`, `sg`, `ast-grep`.
   - Direct shell interpreters and command wrapper binaries (`sh`, `bash`, `zsh`, `fish`, `dash`, `env`, `xargs`, `eval`, `perl`, `ruby`, `awk`, `script`, `sudo`, `su`, `doas`, `cmd`, `powershell`, `pwsh`, `ksh`, `csh`, `tcsh`) are explicitly rejected.
   - Command injection and escape options in tools such as `git` (`-c`, `--exec-path`, `--upload-pack`, `--receive-pack`, `--config-env`) are rejected before execution.
   - Path arguments are strictly validated to prevent directory traversal (`..`) or explicit absolute path references outside the mounted workspace scope.
   - Child process environments are sanitized to scrub sensitive daemon secrets (`OMO_BRIDGE_TOKEN`, API keys, tokens).
   - **Override Flag**: The `--allow-arbitrary-commands` CLI flag (or `OMO_BRIDGE_ALLOW_ARBITRARY_COMMANDS=true` / `1` environment variable) can be enabled to bypass allowlist restrictions when arbitrary execution is explicitly permitted by the host.
   - Commands are executed with per-execution timeouts and bounded output buffers.

4. **Optimistic Concurrency & Atomic Writes**:
   - File edits require SHA-256 preconditions (`expected_sha256`) to prevent race conditions and blind overwrites.
   - Writes are performed atomically via temporary files and rename barriers.

5. **Authoritative Handshake & Lifecycle Verification**:
   - Worker readiness and task completion rely exclusively on authoritative server-side evidence (`task_state` calls, `completion_check.ready=true`).
   - Unverified textual claims from models are rejected.

### Important Security Boundaries

- Tool authorization requires `scope_id` plus the per-scope `capability_secret`. A `scope_id` alone isn't sufficient to execute mutating actions. The bridge marks `capability_secret` as schema-visible and mandatory on mutating tools (`patch_file`, `run_command`, `cancel_command`, `task_plan`, `task_update`, `completion_check`, `query_subagent`), while read-only tools tolerate it when passed. Callers cannot mutate files or run commands simply by discovering a live scope ID.
- A shared ChatGPT account must therefore be treated as one trust principal. If people sharing the account are not mutually trusted, do not expose write/command-capable `gpt2omo` tools through that shared account. Prefer separate ChatGPT accounts/connectors or a read-only, short-lived, host-sandboxed deployment.
- `run_command` is **not** an OS-level filesystem sandbox. The daemon starts allowed binaries as the daemon user with the scoped workspace as the current directory and a sanitized environment. General-purpose interpreters/build tools can have capabilities beyond the file-tool path resolver. Untrusted/shared-user deployments should place command workers in an OS sandbox/container, disable `run_command`, or require an out-of-band approval boundary.
- Keep bridge control-plane data (bearer token, account configuration/state, and browser profiles) outside delegated workspace roots. Avoid broad scopes such as the user's home directory.
- Transport Bearer token authentication is mandatory across all HTTP, SSE, and MCP endpoints. Resolution order checks `--token` on the CLI first, then an existing `~/.omo/bridge/token` file. When neither source exists, the bridge generates a 32-byte secure token, writes it to `~/.omo/bridge/token` with 0600 permissions, and prints it once to stdout. The explicit `--insecure-no-auth` flag is the only bypass. Bearer tokens protect network transport, but they don't distinguish multiple human users sharing a single ChatGPT connector.

See [`docs/multi-account-and-shared-safety.md`](docs/multi-account-and-shared-safety.md) for the multi-account browser-isolation design, shared-account threat analysis, and recommended hardening roadmap.

## Reporting a Vulnerability

If you discover a security vulnerability within `gpt2omo`, please report it responsibly:

- **Do not open a public GitHub issue.**
- Send a detailed advisory with reproduction steps, affected versions, and potential impact.
- All valid reports will be acknowledged promptly and addressed with priority.
