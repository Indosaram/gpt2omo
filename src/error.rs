use thiserror::Error;

#[derive(Error, Debug)]
pub enum BridgeError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Path error: {0}")]
    Path(String),

    #[error("Security violation: {0}")]
    Security(String),

    #[error("Precondition failed: {0}")]
    Precondition(String),

    #[error("Command execution error: {0}")]
    Command(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, BridgeError>;
