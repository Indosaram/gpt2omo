use crate::tools::task_state::DelegationTerminalState;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Hard safety contract shared with `delegate_to_chatgpt_web`.
///
/// mass-ulw wrappers must never schedule more fresh Web workers than this in one
/// bridge batch. The delegate CLI remains the authoritative runtime guard.
pub const DELEGATE_WEB_MAX_WORKERS: usize = 2;

/// The delegate CLI waits this long before creating the second browser worker.
///
/// Wrappers intentionally do not sleep themselves; the delay is centralized in
/// `delegate_to_chatgpt_web` so every caller gets identical anti-burst behavior.
pub const DELEGATE_WEB_SPAWN_STAGGER_SECS: u64 = 10;

pub const DEFAULT_DELEGATE_WEB_BINARY: &str = "delegate_to_chatgpt_web";

#[derive(Debug, Error)]
pub enum MassUlwWebError {
    #[error("Web delegation task must not be empty")]
    EmptyTask,
    #[error("Web delegation label must not be empty")]
    EmptyLabel,
    #[error("retained scope id must not be empty")]
    EmptyScope,
    #[error("Web batch requires 1..={max} tasks; received {count}")]
    InvalidBatchSize { count: usize, max: usize },
    #[error("delegate result is not terminal yet")]
    ResultNotTerminal,
    #[error("delegate result does not contain label `{0}`")]
    MissingLabel(String),
    #[error("delegation `{0}` is not retained/resumable")]
    ScopeNotResumable(String),
    #[error("delegate result must contain exactly one delegation; received {0}")]
    SingleDelegationExpected(usize),
    #[error("failed to encode/decode delegate JSON: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, MassUlwWebError>;

/// Language-neutral task shape accepted by the mass-ulw wrapper.
///
/// This serializes to the exact `--batch-stdin` task object understood by
/// `delegate_to_chatgpt_web`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebTask {
    pub label: String,
    pub task: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<PathBuf>,
}

impl WebTask {
    pub fn new(label: impl Into<String>, task: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            task: task.into(),
            workspace: None,
        }
    }

    pub fn with_workspace(mut self, workspace: impl Into<PathBuf>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }

    fn validate(&self) -> Result<()> {
        if self.label.trim().is_empty() {
            return Err(MassUlwWebError::EmptyLabel);
        }
        if self.task.trim().is_empty() {
            return Err(MassUlwWebError::EmptyTask);
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct BatchManifest<'a> {
    tasks: &'a [WebTask],
}

/// A shell-free command contract that can be consumed by OMO `tool.dag`, the
/// JavaScript SDK, Python `eval`, or any other process runner.
///
/// `stdin` is deliberately separated from argv so task text never needs shell
/// quoting. Callers should execute `program` directly and pipe `stdin` verbatim.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegateWebInvocation {
    pub program: PathBuf,
    pub args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
}

impl DelegateWebInvocation {
    pub fn argv(&self) -> Vec<String> {
        std::iter::once(self.program.to_string_lossy().into_owned())
            .chain(self.args.iter().cloned())
            .collect()
    }
}

/// Stable wrapper configuration for building delegate-web invocations.
///
/// The wrapper does not implement its own concurrency, staggering, retry, or
/// telemetry logic. Those controls remain centralized in
/// `delegate_to_chatgpt_web`, preventing mass-ulw from bypassing bridge policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelegateWebConfig {
    binary: PathBuf,
    bridge_url: Option<String>,
}

impl Default for DelegateWebConfig {
    fn default() -> Self {
        Self {
            binary: PathBuf::from(DEFAULT_DELEGATE_WEB_BINARY),
            bridge_url: None,
        }
    }
}

impl DelegateWebConfig {
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            bridge_url: None,
        }
    }

    pub fn with_bridge_url(mut self, bridge_url: impl Into<String>) -> Self {
        let bridge_url = bridge_url.into();
        self.bridge_url = (!bridge_url.trim().is_empty()).then_some(bridge_url);
        self
    }

    /// Build one fresh retained-session delegation.
    pub fn single(&self, item: &WebTask) -> Result<DelegateWebInvocation> {
        item.validate()?;
        let mut args = self.common_args();
        if let Some(workspace) = item.workspace.as_deref() {
            args.push("--workspace".into());
            args.push(workspace.to_string_lossy().into_owned());
        }
        args.extend(["--stdin".into(), "--json".into()]);
        Ok(DelegateWebInvocation {
            program: self.binary.clone(),
            args,
            stdin: Some(item.task.trim().to_string()),
        })
    }

    /// Build a fresh batch invocation with one or two tasks.
    ///
    /// For two-domain mass-ulw fan-out, use this as one process invocation.
    /// The delegate CLI then applies the authoritative max-worker guard, 10 s
    /// second-worker stagger, readiness barrier, and telemetry lockout checks.
    pub fn batch(&self, items: &[WebTask]) -> Result<DelegateWebInvocation> {
        if items.is_empty() || items.len() > DELEGATE_WEB_MAX_WORKERS {
            return Err(MassUlwWebError::InvalidBatchSize {
                count: items.len(),
                max: DELEGATE_WEB_MAX_WORKERS,
            });
        }
        for item in items {
            item.validate()?;
        }

        let mut args = self.common_args();
        args.extend(["--batch-stdin".into(), "--json".into()]);
        let stdin = serde_json::to_string(&BatchManifest { tasks: items })?;
        Ok(DelegateWebInvocation {
            program: self.binary.clone(),
            args,
            stdin: Some(stdin),
        })
    }

    /// Convenience builder for the canonical two-domain parallel fan-out.
    pub fn parallel_pair(
        &self,
        first: &WebTask,
        second: &WebTask,
    ) -> Result<DelegateWebInvocation> {
        let items = [first.clone(), second.clone()];
        self.batch(&items)
    }

    /// Resume the exact retained browser conversation for a serial correction,
    /// test-feedback, review, or fan-in generation.
    pub fn resume(
        &self,
        scope_id: &str,
        follow_up: impl AsRef<str>,
    ) -> Result<DelegateWebInvocation> {
        let scope_id = scope_id.trim();
        if scope_id.is_empty() {
            return Err(MassUlwWebError::EmptyScope);
        }
        let follow_up = follow_up.as_ref().trim();
        if follow_up.is_empty() {
            return Err(MassUlwWebError::EmptyTask);
        }

        let mut args = self.common_args();
        args.extend([
            "--resume-scope".into(),
            scope_id.into(),
            "--stdin".into(),
            "--json".into(),
        ]);
        Ok(DelegateWebInvocation {
            program: self.binary.clone(),
            args,
            stdin: Some(follow_up.to_string()),
        })
    }

    /// Close a retained session only after the orchestrator has accepted the
    /// final integration result.
    pub fn close_scope(&self, scope_id: &str) -> Result<DelegateWebInvocation> {
        let scope_id = scope_id.trim();
        if scope_id.is_empty() {
            return Err(MassUlwWebError::EmptyScope);
        }
        let mut args = self.common_args();
        args.extend(["--close-scope".into(), scope_id.into(), "--json".into()]);
        Ok(DelegateWebInvocation {
            program: self.binary.clone(),
            args,
            stdin: None,
        })
    }

    fn common_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        if let Some(bridge_url) = self.bridge_url.as_deref() {
            args.push("--bridge-url".into());
            args.push(bridge_url.into());
        }
        args
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DelegateWebResult {
    pub ok: bool,
    #[serde(default)]
    pub sent: bool,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub terminal: bool,
    #[serde(default)]
    pub delegations: Vec<DelegateWebDelegationResult>,
}

impl DelegateWebResult {
    pub fn parse(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }

    pub fn retained_scope_for_label(&self, label: &str) -> Result<&str> {
        let delegation = self
            .delegations
            .iter()
            .find(|delegation| delegation.label.as_deref() == Some(label))
            .ok_or_else(|| MassUlwWebError::MissingLabel(label.to_string()))?;
        retained_scope(delegation)
    }

    pub fn single_retained_scope(&self) -> Result<&str> {
        if self.delegations.len() != 1 {
            return Err(MassUlwWebError::SingleDelegationExpected(
                self.delegations.len(),
            ));
        }
        retained_scope(&self.delegations[0])
    }

    /// Build a serial fan-in handoff that resumes one already-retained worker
    /// after all parallel workers are terminal.
    pub fn fan_in_handoff(
        &self,
        primary_label: &str,
        integration_goal: &str,
        local_verification_feedback: &str,
    ) -> Result<FanInHandoff> {
        if !self.terminal {
            return Err(MassUlwWebError::ResultNotTerminal);
        }
        let primary_scope_id = self.retained_scope_for_label(primary_label)?.to_string();
        let evidence = self
            .delegations
            .iter()
            .map(|delegation| {
                let label = delegation.label.as_deref().unwrap_or("unlabeled");
                let state = delegation
                    .terminal_state
                    .map(|state| format!("{state:?}").to_ascii_uppercase())
                    .unwrap_or_else(|| "UNKNOWN".into());
                let detail = delegation.terminal_detail.as_deref().unwrap_or("none");
                format!("- {label}: terminal={state}; detail={detail}")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let feedback = if local_verification_feedback.trim().is_empty() {
            "No additional local verification feedback was supplied.".to_string()
        } else {
            local_verification_feedback.trim().to_string()
        };
        let prompt = format!(
            "Fan-in integration pass. Both parallel Web workers are terminal. Continue in this same retained session; do not create another Web worker.\n\nIntegration goal:\n{}\n\nParallel terminal evidence:\n{}\n\nLocal verification feedback:\n{}\n\nInspect the current workspace state independently, reconcile both domains, fix integration defects, run the required local verification, and finish only after authoritative completion_check is ready=true.",
            integration_goal.trim(), evidence, feedback
        );
        Ok(FanInHandoff {
            primary_scope_id,
            resume_prompt: prompt,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DelegateWebDelegationResult {
    #[serde(default)]
    pub label: Option<String>,
    pub scope_id: String,
    #[serde(default)]
    pub terminal_state: Option<DelegationTerminalState>,
    #[serde(default)]
    pub terminal_detail: Option<String>,
    #[serde(default)]
    pub session_retained: bool,
    #[serde(default)]
    pub resumable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FanInHandoff {
    pub primary_scope_id: String,
    pub resume_prompt: String,
}

impl FanInHandoff {
    pub fn resume_invocation(&self, config: &DelegateWebConfig) -> Result<DelegateWebInvocation> {
        config.resume(&self.primary_scope_id, &self.resume_prompt)
    }
}

fn retained_scope(delegation: &DelegateWebDelegationResult) -> Result<&str> {
    if delegation.session_retained && delegation.resumable && !delegation.scope_id.trim().is_empty()
    {
        return Ok(delegation.scope_id.as_str());
    }
    Err(MassUlwWebError::ScopeNotResumable(
        delegation
            .label
            .clone()
            .unwrap_or_else(|| delegation.scope_id.clone()),
    ))
}

/// A small helper for examples that need a normalized workspace string.
pub fn workspace_display(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
