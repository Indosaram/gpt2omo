use crate::security::Workspace;
use crate::tools::task_state::load_delegation_lifecycle;
use crate::tools::ToolCallResult;
use futures::StreamExt;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use url::{Host, Url};

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MIN_TIMEOUT_MS: u64 = 1_000;
const MAX_TIMEOUT_MS: u64 = 60_000;
const MAX_PROMPT_BYTES: usize = 32 * 1024;
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const MAX_CALLS_PER_GENERATION: usize = 4;
const GLOBAL_CONCURRENCY_LIMIT: usize = 2;
const QUOTA_STATE_VERSION: u32 = 1;

#[derive(Debug)]
struct SubagentRuntime {
    concurrency: Semaphore,
}

impl SubagentRuntime {
    fn new() -> Self {
        Self {
            concurrency: Semaphore::new(GLOBAL_CONCURRENCY_LIMIT),
        }
    }

    fn reserve_call(
        &self,
        ws: &Workspace,
        scope_id: &str,
        generation: u64,
    ) -> Result<usize, String> {
        reserve_persistent_call(ws, scope_id, generation)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct QuotaState {
    version: u32,
    generation: u64,
    calls: usize,
}

static SUBAGENT_RUNTIME: OnceLock<SubagentRuntime> = OnceLock::new();

fn runtime() -> &'static SubagentRuntime {
    SUBAGENT_RUNTIME.get_or_init(SubagentRuntime::new)
}

// The MCP boundary keeps request validation and independently configured upstream settings explicit.
#[allow(clippy::too_many_arguments)]
pub async fn handle_query_subagent(
    ws: &Workspace,
    scope_id: &str,
    prompt: Option<&str>,
    timeout_value: Option<&Value>,
    endpoint: Option<&str>,
    api_key: Option<&str>,
    model: &str,
    allow_remote: bool,
) -> ToolCallResult {
    if scope_id.trim().is_empty() {
        return ToolCallResult::err("scope_id is required for query_subagent");
    }

    let prompt = match validate_prompt(prompt) {
        Ok(prompt) => prompt,
        Err(error) => return ToolCallResult::err(error),
    };
    let timeout_ms = match parse_timeout_ms(timeout_value) {
        Ok(timeout_ms) => timeout_ms,
        Err(error) => return ToolCallResult::err(error),
    };
    let completion_url = match completion_url(endpoint, allow_remote) {
        Ok(url) => url,
        Err(error) => return ToolCallResult::err(error),
    };
    let model = model.trim();
    if model.is_empty() {
        return ToolCallResult::err("Subagent model must not be empty");
    }

    let lifecycle = match active_lifecycle(ws, scope_id) {
        Ok(lifecycle) => lifecycle,
        Err(error) => return ToolCallResult::err(error),
    };
    let expected_generation = lifecycle.generation;

    let _permit = match runtime().concurrency.acquire().await {
        Ok(permit) => permit,
        Err(_) => return ToolCallResult::err("Subagent concurrency gate is unavailable"),
    };

    // A request may have waited behind another advisory call. Re-read the authoritative lifecycle
    // after acquiring capacity so a terminal or superseded generation cannot reach the upstream.
    let lifecycle = match active_lifecycle(ws, scope_id) {
        Ok(lifecycle) if lifecycle.generation == expected_generation => lifecycle,
        Ok(_) => {
            return ToolCallResult::err(
                "query_subagent delegation generation changed while waiting for capacity",
            )
        }
        Err(error) => return ToolCallResult::err(error),
    };

    let call_number = match runtime().reserve_call(ws, scope_id, lifecycle.generation) {
        Ok(call_number) => call_number,
        Err(error) => return ToolCallResult::err(error),
    };

    let client = match reqwest::Client::builder().redirect(Policy::none()).build() {
        Ok(client) => client,
        Err(_) => return ToolCallResult::err("Subagent HTTP client initialization failed"),
    };
    let request_body = serde_json::json!({
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
    });
    let mut request = client
        .post(completion_url)
        .timeout(Duration::from_millis(timeout_ms))
        .json(&request_body);
    if let Some(api_key) = api_key.map(str::trim).filter(|value| !value.is_empty()) {
        request = request.bearer_auth(api_key);
    }

    let started = Instant::now();
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) if error.is_timeout() => {
            return ToolCallResult::err("Subagent request timed out")
        }
        Err(_) => return ToolCallResult::err("Subagent request failed"),
    };
    let status = response.status();
    if !status.is_success() {
        return ToolCallResult::err(format!(
            "Subagent upstream returned HTTP status {}",
            status.as_u16()
        ));
    }

    let mut raw = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(_) => return ToolCallResult::err("Subagent response stream failed"),
        };
        if raw.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return ToolCallResult::err(format!(
                "Subagent response exceeded the {MAX_RESPONSE_BYTES}-byte limit"
            ));
        }
        raw.extend_from_slice(&chunk);
    }
    let latency_ms = started.elapsed().as_millis() as u64;

    let parsed: Value = match serde_json::from_slice(&raw) {
        Ok(value) => value,
        Err(_) => return ToolCallResult::err("Subagent returned invalid JSON"),
    };
    let Some(advice) = extract_advice(&parsed) else {
        return ToolCallResult::err("Subagent response did not contain text advice");
    };
    if advice.is_empty() {
        return ToolCallResult::err("Subagent response contained empty text advice");
    }

    ToolCallResult::ok(serde_json::json!({
        "advice": advice,
        "usage": extract_usage(&parsed),
        "latency_ms": latency_ms,
        "generation": lifecycle.generation,
        "quota_call": call_number,
        "quota_limit": MAX_CALLS_PER_GENERATION,
        "trust": "untrusted_advisory",
    }))
}

fn active_lifecycle(
    ws: &Workspace,
    scope_id: &str,
) -> Result<crate::tools::task_state::DelegationLifecycle, String> {
    let lifecycle = match load_delegation_lifecycle(ws, scope_id) {
        Ok(Some(lifecycle)) => lifecycle,
        Ok(None) => {
            return Err(
                "query_subagent requires an active delegation lifecycle for this scope".to_string(),
            )
        }
        Err(_) => {
            return Err("query_subagent could not validate the delegation lifecycle".to_string())
        }
    };
    if lifecycle.terminal_state.is_some() {
        return Err(
            "query_subagent is unavailable for a terminal delegation generation".to_string(),
        );
    }
    Ok(lifecycle)
}

fn reserve_persistent_call(
    ws: &Workspace,
    scope_id: &str,
    generation: u64,
) -> Result<usize, String> {
    let path = quota_state_path(ws, scope_id);
    let parent = path
        .parent()
        .ok_or_else(|| "Subagent quota state path has no parent".to_string())?;
    fs::create_dir_all(parent).map_err(|_| "Subagent quota state is unavailable".to_string())?;

    let lock_path = path.with_extension("lock");
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|_| "Subagent quota state is unavailable".to_string())?;
    lock_file
        .lock()
        .map_err(|_| "Subagent quota state is unavailable".to_string())?;

    let result = (|| {
        let mut state = if path.exists() {
            let bytes =
                fs::read(&path).map_err(|_| "Subagent quota state is unavailable".to_string())?;
            let state: QuotaState = serde_json::from_slice(&bytes)
                .map_err(|_| "Subagent quota state is invalid".to_string())?;
            if state.version != QUOTA_STATE_VERSION {
                return Err("Subagent quota state is invalid".to_string());
            }
            state
        } else {
            QuotaState {
                version: QUOTA_STATE_VERSION,
                generation,
                calls: 0,
            }
        };

        if state.generation != generation {
            state.generation = generation;
            state.calls = 0;
        }
        if state.calls >= MAX_CALLS_PER_GENERATION {
            return Err(format!(
                "query_subagent quota exceeded for this delegation generation (maximum {MAX_CALLS_PER_GENERATION} calls)"
            ));
        }

        state.calls += 1;
        let bytes = serde_json::to_vec(&state)
            .map_err(|_| "Subagent quota state is unavailable".to_string())?;
        let temp = parent.join(format!(".subagent-quota-{}.tmp", uuid::Uuid::new_v4()));
        fs::write(&temp, bytes).map_err(|_| "Subagent quota state is unavailable".to_string())?;
        if fs::rename(&temp, &path).is_err() {
            let _ = fs::remove_file(&temp);
            return Err("Subagent quota state is unavailable".to_string());
        }
        Ok(state.calls)
    })();

    let _ = lock_file.unlock();
    result
}

fn quota_state_path(ws: &Workspace, scope_id: &str) -> PathBuf {
    let key = format!(
        "{:x}",
        Sha256::digest(format!("{}:{}", ws.root().to_string_lossy(), scope_id).as_bytes())
    );
    std::env::temp_dir()
        .join("omo-bridge")
        .join("subagent-quota")
        .join(format!("{key}.json"))
}

fn validate_prompt(prompt: Option<&str>) -> Result<&str, String> {
    let prompt = prompt.ok_or_else(|| "prompt is required for query_subagent".to_string())?;
    if prompt.trim().is_empty() {
        return Err("prompt must not be empty".to_string());
    }
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(format!("prompt exceeds the {MAX_PROMPT_BYTES}-byte limit"));
    }
    Ok(prompt)
}

fn parse_timeout_ms(value: Option<&Value>) -> Result<u64, String> {
    let Some(value) = value else {
        return Ok(DEFAULT_TIMEOUT_MS);
    };
    let timeout = if let Some(value) = value.as_i64() {
        if value <= 0 {
            0
        } else {
            value as u64
        }
    } else if let Some(value) = value.as_u64() {
        value
    } else {
        return Err("timeout_ms must be an integer".to_string());
    };
    Ok(timeout.clamp(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS))
}

fn completion_url(endpoint: Option<&str>, allow_remote: bool) -> Result<Url, String> {
    let endpoint = endpoint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            "query_subagent is disabled because no subagent endpoint is configured".to_string()
        })?;
    let mut base = Url::parse(endpoint).map_err(|_| "Invalid subagent endpoint URL".to_string())?;
    if !matches!(base.scheme(), "http" | "https") {
        return Err("Subagent endpoint must use http or https".to_string());
    }
    if !base.username().is_empty() || base.password().is_some() {
        return Err("Subagent endpoint must not contain embedded credentials".to_string());
    }
    if base.query().is_some() || base.fragment().is_some() {
        return Err("Subagent endpoint must not contain a query string or fragment".to_string());
    }
    if !allow_remote && !is_loopback_endpoint(&base) {
        return Err(
            "Remote subagent endpoints are disabled; use --subagent-allow-remote to opt in"
                .to_string(),
        );
    }
    base.set_path("/v1/chat/completions");
    Ok(base)
}

fn is_loopback_endpoint(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        Some(Host::Domain(domain)) => {
            domain.eq_ignore_ascii_case("localhost")
                || domain.to_ascii_lowercase().ends_with(".localhost")
        }
        None => false,
    }
}

fn extract_advice(value: &Value) -> Option<String> {
    let content = value
        .get("choices")?
        .as_array()?
        .first()?
        .get("message")?
        .get("content")?;
    if let Some(text) = content.as_str() {
        return Some(text.to_string());
    }
    let parts = content.as_array()?;
    let text = parts
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("");
    Some(text)
}

fn extract_usage(value: &Value) -> Value {
    let usage = value.get("usage");
    serde_json::json!({
        "prompt_tokens": usage.and_then(|usage| usage.get("prompt_tokens")).and_then(Value::as_u64),
        "completion_tokens": usage.and_then(|usage| usage.get("completion_tokens")).and_then(Value::as_u64),
        "total_tokens": usage.and_then(|usage| usage.get("total_tokens")).and_then(Value::as_u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn timeout_is_clamped_and_type_checked() {
        assert_eq!(parse_timeout_ms(None).unwrap(), DEFAULT_TIMEOUT_MS);
        assert_eq!(
            parse_timeout_ms(Some(&serde_json::json!(10))).unwrap(),
            1_000
        );
        assert_eq!(
            parse_timeout_ms(Some(&serde_json::json!(90_000))).unwrap(),
            60_000
        );
        assert!(parse_timeout_ms(Some(&serde_json::json!(1.5))).is_err());
        assert!(parse_timeout_ms(Some(&serde_json::json!("1000"))).is_err());
    }

    #[test]
    fn endpoint_is_local_by_default_and_normalized() {
        let url = completion_url(Some("http://127.0.0.1:1234/base"), false).unwrap();
        assert_eq!(url.as_str(), "http://127.0.0.1:1234/v1/chat/completions");
        assert!(completion_url(Some("https://example.com"), false).is_err());
        assert!(completion_url(Some("https://example.com"), true).is_ok());
    }

    #[test]
    fn prompt_limit_is_measured_in_bytes() {
        let valid = "a".repeat(MAX_PROMPT_BYTES);
        assert!(validate_prompt(Some(&valid)).is_ok());
        let oversized = format!("{}é", "a".repeat(MAX_PROMPT_BYTES - 1));
        assert!(validate_prompt(Some(&oversized)).is_err());
    }

    #[test]
    fn quota_is_persistent_across_runtime_instances_and_resets_by_generation() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let scope_id = uuid::Uuid::new_v4().to_string();
        let path = quota_state_path(&ws, &scope_id);

        let first_runtime = SubagentRuntime::new();
        for expected in 1..=MAX_CALLS_PER_GENERATION {
            assert_eq!(
                first_runtime.reserve_call(&ws, &scope_id, 7).unwrap(),
                expected
            );
        }

        let restarted_runtime = SubagentRuntime::new();
        assert!(restarted_runtime
            .reserve_call(&ws, &scope_id, 7)
            .unwrap_err()
            .contains("quota exceeded"));
        assert_eq!(
            restarted_runtime.reserve_call(&ws, &scope_id, 8).unwrap(),
            1
        );

        let _ = fs::remove_file(path.with_extension("lock"));
        let _ = fs::remove_file(path);
    }
}
