# Security Policy

## Security Model & Threat Boundaries

`omo-bridge` is designed as a sandboxed local Model Context Protocol (MCP) server that provides filesystem, command execution, code intelligence, and verification capabilities to external LLM workers (such as ChatGPT Web) while strictly bounding access.

### Core Security Guarantees

1. **Per-Delegation Scope Isolation (`scope_id`)**:
   - Every mutating or reading tool call requires a valid, registered `scope_id`.
   - Operations without a valid `scope_id` or with a malformed/expired scope are rejected immediately before any filesystem interaction.
   - Separate scopes running concurrently are completely isolated in their respective workspace roots.

2. **Filesystem Capability Sandboxing**:
   - Built on `cap-std` capability-based security.
   - All file operations are resolved strictly relative to the verified workspace directory.
   - Symlink traversal, `..` path escapes, and absolute path injection outside the scoped workspace are structurally rejected.
   - Secret files, sensitive credentials (`.env`, `.git/config`, private keys), and system files outside the workspace are denied.

3. **Command Execution Whitelist**:
   - `run_command` executes only strictly whitelisted build, test, and verification tools (e.g., `cargo`, `npm`, `pnpm`, `pytest`, `vitest`, `go`, `git status`).
   - Direct shell invocation (`sh`, `bash`, `zsh`) is disabled.
   - Commands are executed with per-execution timeouts and bounded output buffers.

4. **Optimistic Concurrency & Atomic Writes**:
   - File edits require SHA-256 preconditions (`expected_sha256`) to prevent race conditions and blind overwrites.
   - Writes are performed atomically via temporary files and rename barriers.

5. **Authoritative Handshake & Lifecycle Verification**:
   - Worker readiness and task completion rely exclusively on authoritative server-side evidence (`task_state` calls, `completion_check.ready=true`).
   - Unverified textual claims from models are rejected.

## Reporting a Vulnerability

If you discover a security vulnerability within `omo-bridge`, please report it responsibly:

- **Do not open a public GitHub issue.**
- Send a detailed advisory with reproduction steps, affected versions, and potential impact.
- All valid reports will be acknowledged promptly and addressed with priority.
