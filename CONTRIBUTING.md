# Contributing to gpt2omo

Thank you for contributing to `gpt2omo`. This repository provides the sandboxed Model Context Protocol (MCP) server, Orca browser delegation harness, and authoritative lifecycle management for AI coding workflows.

## Prerequisites

- **Rust**: Version 1.80+ (stable toolchain)
- **Cargo**: Formatter (`rustfmt`) and linter (`clippy`)
- **Git**

## Development Setup

1. Clone the repository:
   ```bash
   git clone <repo-url> gpt2omo
   cd gpt2omo
   ```

2. Build debug and release targets:
   ```bash
   cargo build
   cargo build --release
   ```

3. Run the test suite:
   ```bash
   cargo test --all-targets
   cargo test --release
   ```

## Code Quality Standards

Every pull request and commit must satisfy all quality gates without warnings:

1. **Formatting**:
   ```bash
   cargo fmt -- --check
   ```

2. **Linter**:
   ```bash
   cargo clippy --all-targets -- -D warnings
   ```

3. **Tests**:
   ```bash
   cargo test
   cargo test --release
   ```

4. **Git Hygiene**:
   ```bash
   git diff --check
   ```

## Architecture & Design Rules

- **Schema Stability**: Maintain backwards compatibility for existing MCP tools. Do not add unexpected required parameters to the 15 core MCP tools without careful design.
- **Security Invariants**: Always preserve `cap-std` capability isolation, path canonicalization, symlink escape prevention, and command execution whitelists.
- **Authoritative Verification**: Never trust textual promises or informal agent output for lifecycle decisions. Enforce cryptographically verified or tool-backed evidence.
