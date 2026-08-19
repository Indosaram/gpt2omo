use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ToolCallResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ToolCallResult {
    pub fn ok(val: impl Serialize) -> Self {
        Self {
            success: true,
            data: Some(serde_json::to_value(val).unwrap_or(serde_json::Value::Null)),
            error: None,
        }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

/// Vendor/build trees that hold hundreds of thousands of generated files and
/// are never useful for source search or listing.
pub(crate) const SKIPPED_DIR_NAMES: &[&str] = &[
    "node_modules",
    "bower_components",
    "__pycache__",
    ".venv",
    "venv",
    "Pods",
    "target",
    "dist",
    "build",
    "out",
    "vendor",
    "coverage",
    "test-results",
    ".next",
    ".turbo",
    ".cache",
];

pub mod ast_grep;
pub mod command_manager;
pub mod completion;
pub mod git_status;
pub mod list_files;
pub mod lsp;
pub mod patch_file;
pub mod read_file;
pub mod run_command;
pub mod search_text;
pub mod subagent;
pub mod task_state;

pub use ast_grep::handle_ast_grep;
pub use command_manager::CommandManager;
pub use completion::{handle_completion_check, handle_completion_check_with_manager};
pub use git_status::handle_git_status;
pub use list_files::handle_list_files;
pub use lsp::{handle_lsp, LspOperation};
pub use patch_file::handle_patch_file;
pub use read_file::handle_read_file;
pub use run_command::handle_run_command;
pub use search_text::handle_search_text;
pub use subagent::handle_query_subagent;
pub use task_state::{
    handle_task_plan, handle_task_state, handle_task_update, record_mutation, record_verification,
};
