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
     - `cargo`, `rustc`, `npm`, `pnpm`, `yarn`, `bun`, `node`, `python`, `python3`, `pytest`, `uv`, `go`, `make`, `git`, `vitest`, `jest`, `tsc`, `biome`, `ruff`, `sg`, `ast-grep`.
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

- A `scope_id` is an unguessable **bearer capability**, not a human identity, ChatGPT account identity, or cryptographically bound conversation identity. MCP connector discovery and tool transport do not use a static Authorization header: a caller that learns a valid live scope ID and can reach the MCP connector can replay that capability for the corresponding scope.
- A shared ChatGPT account must therefore be treated as one trust principal. If people sharing the account are not mutually trusted, do not expose write/command-capable `gpt2omo` tools through that shared account. Prefer separate ChatGPT accounts/connectors or a read-only, short-lived, host-sandboxed deployment.
- `run_command` is **not** an OS-level filesystem sandbox. The daemon starts allowed binaries as the daemon user with the scoped workspace as the current directory and a sanitized environment. General-purpose interpreters/build tools can have capabilities beyond the file-tool path resolver. Untrusted/shared-user deployments should place command workers in an OS sandbox/container, disable `run_command`, or require an out-of-band approval boundary.
- Keep bridge control-plane data (bearer token, account configuration/state, and browser profiles) outside delegated workspace roots. Avoid broad scopes such as the user's home directory.
- If configured, the optional bridge bearer token protects bridge-control endpoints such as `/events` for the local relay. It is not an MCP connector credential and does not distinguish multiple humans using the same configured ChatGPT connector.

See [`docs/multi-account-and-shared-safety.md`](docs/multi-account-and-shared-safety.md) for the multi-account browser-isolation design, shared-account threat analysis, and recommended hardening roadmap.

## Reporting a Vulnerability

If you discover a security vulnerability within `gpt2omo`, please report it responsibly:

- **Do not open a public GitHub issue.**
- Send a detailed advisory with reproduction steps, affected versions, and potential impact.
- All valid reports will be acknowledged promptly and addressed with priority.
