use crate::error::{BridgeError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

const FRESH_DISPATCH_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreshDispatchClaim {
    pub version: u32,
    pub dispatch_key: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub scope_ids: Vec<String>,
}

pub enum FreshDispatchDecision {
    Acquired(FreshDispatchClaimGuard),
    Duplicate(FreshDispatchClaim),
}

pub struct FreshDispatchClaimGuard {
    _lock: File,
    path: PathBuf,
    claim: FreshDispatchClaim,
}

impl FreshDispatchClaimGuard {
    pub fn register_scope(&mut self, scope_id: &str, now_ms: u64) -> Result<()> {
        if uuid::Uuid::parse_str(scope_id).is_err() {
            return Err(BridgeError::Security(
                "invalid scope id for fresh dispatch claim".into(),
            ));
        }
        if !self
            .claim
            .scope_ids
            .iter()
            .any(|existing| existing == scope_id)
        {
            self.claim.scope_ids.push(scope_id.to_string());
            self.claim.updated_ms = now_ms;
            write_json_atomic(&self.path, &self.claim)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FreshDispatchClaims {
    dir: PathBuf,
}

impl FreshDispatchClaims {
    pub fn new(bridge_dir: impl AsRef<Path>) -> Self {
        Self {
            dir: bridge_dir.as_ref().join("fresh-dispatch"),
        }
    }

    pub fn claim<F>(
        &self,
        dispatch_key: &str,
        now_ms: u64,
        mut is_active: F,
    ) -> Result<FreshDispatchDecision>
    where
        F: FnMut(&[String]) -> Result<bool>,
    {
        validate_dispatch_key(dispatch_key)?;
        prepare_private_dir(&self.dir)?;
        let lock_path = self.dir.join(format!("{dispatch_key}.lock"));
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path)?;
        lock.lock()?;

        let path = self.dir.join(format!("{dispatch_key}.json"));
        if let Some(existing) = load_claim(&path)? {
            if is_active(&existing.scope_ids)? {
                return Ok(FreshDispatchDecision::Duplicate(existing));
            }
        }

        let claim = FreshDispatchClaim {
            version: FRESH_DISPATCH_VERSION,
            dispatch_key: dispatch_key.to_string(),
            created_ms: now_ms,
            updated_ms: now_ms,
            scope_ids: Vec::new(),
        };
        write_json_atomic(&path, &claim)?;
        Ok(FreshDispatchDecision::Acquired(FreshDispatchClaimGuard {
            _lock: lock,
            path,
            claim,
        }))
    }
}

fn load_claim(path: &Path) -> Result<Option<FreshDispatchClaim>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(BridgeError::Io(error)),
    };
    let claim: FreshDispatchClaim = serde_json::from_slice(&bytes)?;
    if claim.version != FRESH_DISPATCH_VERSION {
        return Err(BridgeError::Precondition(format!(
            "unsupported fresh dispatch claim version {}",
            claim.version
        )));
    }
    validate_dispatch_key(&claim.dispatch_key)?;
    Ok(Some(claim))
}

fn validate_dispatch_key(key: &str) -> Result<()> {
    if key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(BridgeError::Security("invalid fresh dispatch key".into()));
    }
    Ok(())
}

fn prepare_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn write_json_atomic(path: &Path, value: &FreshDispatchClaim) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    #[cfg(unix)]
    fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc, Barrier};
    use tempfile::tempdir;

    #[test]
    fn concurrent_identical_claim_has_one_owner() {
        let root = tempdir().unwrap();
        let claims = FreshDispatchClaims::new(root.path());
        let second_claims = claims.clone();
        let key = "a".repeat(64);
        let start = Arc::new(Barrier::new(2));
        let second_start = start.clone();
        let (result_tx, result_rx) = mpsc::channel();
        let first_tx = result_tx.clone();
        let first = std::thread::spawn(move || {
            start.wait();
            let result = match claims
                .claim(&key, 1, |scope_ids| Ok(!scope_ids.is_empty()))
                .unwrap()
            {
                FreshDispatchDecision::Acquired(mut guard) => {
                    guard
                        .register_scope("11111111-1111-4111-8111-111111111111", 2)
                        .unwrap();
                    "acquired"
                }
                FreshDispatchDecision::Duplicate(_) => "duplicate",
            };
            first_tx.send(result).unwrap();
        });
        let second = std::thread::spawn(move || {
            second_start.wait();
            let result = match second_claims
                .claim(&"a".repeat(64), 3, |scope_ids| Ok(!scope_ids.is_empty()))
                .unwrap()
            {
                FreshDispatchDecision::Acquired(mut guard) => {
                    guard
                        .register_scope("22222222-2222-4222-8222-222222222222", 4)
                        .unwrap();
                    "acquired"
                }
                FreshDispatchDecision::Duplicate(claim) => {
                    assert_eq!(claim.scope_ids.len(), 1);
                    "duplicate"
                }
            };
            result_tx.send(result).unwrap();
        });

        let first_result = result_rx.recv().unwrap();
        let second_result = result_rx.recv().unwrap();
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(
            [first_result, second_result]
                .into_iter()
                .filter(|result| *result == "acquired")
                .count(),
            1
        );
    }

    #[test]
    fn inactive_claim_is_replaced_for_intentional_fresh_work() {
        let root = tempdir().unwrap();
        let claims = FreshDispatchClaims::new(root.path());
        let key = "b".repeat(64);
        let mut guard = match claims.claim(&key, 1, |_| Ok(false)).unwrap() {
            FreshDispatchDecision::Acquired(guard) => guard,
            FreshDispatchDecision::Duplicate(_) => panic!("first claim was duplicate"),
        };
        guard
            .register_scope("22222222-2222-4222-8222-222222222222", 2)
            .unwrap();
        drop(guard);

        match claims.claim(&key, 3, |_| Ok(false)).unwrap() {
            FreshDispatchDecision::Acquired(_) => {}
            FreshDispatchDecision::Duplicate(_) => panic!("inactive claim blocked fresh work"),
        }
    }

    #[test]
    fn active_claim_is_not_replaced_by_task_rewording_attempt() {
        let root = tempdir().unwrap();
        let claims = FreshDispatchClaims::new(root.path());
        let key = "c".repeat(64);
        let mut guard = match claims.claim(&key, 1, |_| Ok(false)).unwrap() {
            FreshDispatchDecision::Acquired(guard) => guard,
            FreshDispatchDecision::Duplicate(_) => panic!("first claim was duplicate"),
        };
        guard
            .register_scope("33333333-3333-4333-8333-333333333333", 2)
            .unwrap();
        drop(guard);

        let claim = match claims
            .claim(&key, 3, |scope_ids| Ok(!scope_ids.is_empty()))
            .unwrap()
        {
            FreshDispatchDecision::Duplicate(claim) => claim,
            FreshDispatchDecision::Acquired(_) => panic!("active claim was replaced"),
        };
        assert_eq!(
            claim.scope_ids,
            vec!["33333333-3333-4333-8333-333333333333"]
        );
    }
}
