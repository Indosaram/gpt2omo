use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

const EVENT_BUFFER: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HarnessEvent {
    pub seq: u64,
    pub kind: String,
    pub timestamp_ms: u64,
    pub workspace: String,
    pub data: Value,
}

#[derive(Clone, Debug)]
pub struct EventBus {
    sender: broadcast::Sender<HarnessEvent>,
    next_seq: Arc<AtomicU64>,
    workspace: String,
    pending_dir: Option<Arc<PathBuf>>,
}

impl EventBus {
    pub fn new(workspace: impl Into<String>) -> Self {
        Self::build(workspace.into(), None)
    }

    pub fn new_persistent(workspace: impl Into<String>, pending_dir: impl Into<PathBuf>) -> Self {
        Self::build(workspace.into(), Some(pending_dir.into()))
    }

    fn build(workspace: String, pending_dir: Option<PathBuf>) -> Self {
        let (sender, _) = broadcast::channel(EVENT_BUFFER);
        let pending_dir = pending_dir.map(Arc::new);
        let next_seq = pending_dir
            .as_deref()
            .map(|dir| max_pending_seq(dir, &workspace))
            .unwrap_or(0)
            .saturating_add(1)
            .max(1);
        Self {
            sender,
            next_seq: Arc::new(AtomicU64::new(next_seq)),
            workspace,
            pending_dir,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HarnessEvent> {
        self.sender.subscribe()
    }

    pub fn subscribe_with_replay(&self) -> (Vec<HarnessEvent>, broadcast::Receiver<HarnessEvent>) {
        let replayed = self.pending_events();
        (replayed, self.sender.subscribe())
    }

    fn pending_events(&self) -> Vec<HarnessEvent> {
        self.pending_dir
            .as_deref()
            .map(|dir| load_pending_events(dir, &self.workspace))
            .unwrap_or_default()
    }

    pub fn publish(&self, kind: impl Into<String>, data: Value) -> HarnessEvent {
        let kind = kind.into();
        let event = HarnessEvent {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            kind: kind.clone(),
            timestamp_ms: now_ms(),
            workspace: self.workspace.clone(),
            data,
        };

        if let Some(pending_dir) = self.pending_dir.as_deref() {
            if kind == "continuation_required" {
                if let Err(error) = persist_pending_continuation(pending_dir, &event) {
                    tracing::warn!(error = %error, "failed to persist continuation_required event");
                }
            } else if kind == "completion"
                && event.data.get("ready").and_then(Value::as_bool) == Some(true)
            {
                if let Some(scope_id) = event_scope_id(&event) {
                    if let Err(error) = clear_pending_continuation(pending_dir, scope_id) {
                        tracing::warn!(scope_id, error = %error, "failed to clear persisted continuation event");
                    }
                }
            }
        }

        let _ = self.sender.send(event.clone());
        event
    }

    pub fn connection_event(&self) -> HarnessEvent {
        HarnessEvent {
            seq: self.latest_seq(),
            kind: "connected".into(),
            timestamp_ms: now_ms(),
            workspace: self.workspace.clone(),
            data: serde_json::json!({
                "buffer_capacity": EVENT_BUFFER,
                "transport": "sse",
                "durable_continuation_replay": self.pending_dir.is_some(),
            }),
        }
    }

    pub fn latest_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed).saturating_sub(1)
    }
}

fn event_scope_id(event: &HarnessEvent) -> Option<&str> {
    event
        .data
        .get("scope_id")
        .and_then(Value::as_str)
        .filter(|scope_id| uuid::Uuid::parse_str(scope_id).is_ok())
}

fn pending_path(dir: &Path, scope_id: &str) -> Option<PathBuf> {
    uuid::Uuid::parse_str(scope_id).ok()?;
    Some(dir.join(format!("{scope_id}.json")))
}

fn persist_pending_continuation(dir: &Path, event: &HarnessEvent) -> std::io::Result<()> {
    let Some(scope_id) = event_scope_id(event) else {
        return Ok(());
    };
    fs::create_dir_all(dir)?;
    set_private_dir(dir)?;
    let Some(path) = pending_path(dir, scope_id) else {
        return Ok(());
    };
    let bytes = serde_json::to_vec_pretty(event).map_err(std::io::Error::other)?;
    let temp = dir.join(format!(".{scope_id}-{}.tmp", uuid::Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temp)?;
    if let Err(error) = file.write_all(&bytes).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    drop(file);
    if let Err(error) = fs::rename(&temp, &path) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    set_private_file(&path)?;
    if let Ok(directory) = File::open(dir) {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn clear_pending_continuation(dir: &Path, scope_id: &str) -> std::io::Result<()> {
    let Some(path) = pending_path(dir, scope_id) else {
        return Ok(());
    };
    match fs::remove_file(path) {
        Ok(()) => {
            if let Ok(directory) = File::open(dir) {
                let _ = directory.sync_all();
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn load_pending_events(dir: &Path, workspace: &str) -> Vec<HarnessEvent> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut events = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                return None;
            }
            let bytes = fs::read(&path).ok()?;
            let event: HarnessEvent = serde_json::from_slice(&bytes).ok()?;
            if event.workspace != workspace {
                return None;
            }
            if event.kind != "continuation_required" || event_scope_id(&event).is_none() {
                return None;
            }
            Some(event)
        })
        .collect::<Vec<_>>();
    events.sort_by_key(|event| event.seq);
    events
}

fn max_pending_seq(dir: &Path, workspace: &str) -> u64 {
    load_pending_events(dir, workspace)
        .into_iter()
        .map(|event| event.seq)
        .max()
        .unwrap_or(0)
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> std::io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> std::io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn publishes_monotonic_events_to_subscribers() {
        let bus = EventBus::new("/tmp/workspace");
        let mut rx = bus.subscribe();

        let first = bus.publish("tool_started", serde_json::json!({"tool": "read_file"}));
        let second = bus.publish("tool_finished", serde_json::json!({"tool": "read_file"}));

        assert_eq!(first.seq, 1);
        assert_eq!(second.seq, 2);
        assert_eq!(bus.latest_seq(), 2);
        assert_eq!(rx.recv().await.unwrap().kind, "tool_started");
        assert_eq!(rx.recv().await.unwrap().kind, "tool_finished");
    }

    #[test]
    fn connection_event_does_not_advance_sequence() {
        let bus = EventBus::new("workspace");
        let hello = bus.connection_event();
        assert_eq!(hello.seq, 0);
        assert_eq!(hello.kind, "connected");
        assert_eq!(bus.latest_seq(), 0);
    }

    #[tokio::test]
    async fn pending_continuation_is_replayed_to_reconnected_subscriber_and_cleared_on_ready() {
        let state = tempdir().unwrap();
        let scope_id = uuid::Uuid::new_v4().to_string();
        let bus = EventBus::new_persistent("workspace", state.path());
        let event = bus.publish(
            "continuation_required",
            serde_json::json!({
                "scope_id": scope_id,
                "prompt": "continue",
                "relay_to_same_chat": true
            }),
        );
        assert_eq!(load_pending_events(state.path(), "workspace").len(), 1);

        let (replayed, _reconnected) = bus.subscribe_with_replay();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].seq, event.seq);
        assert_eq!(replayed[0].kind, "continuation_required");

        bus.publish(
            "completion",
            serde_json::json!({"scope_id": scope_id, "ready": true}),
        );
        assert!(load_pending_events(state.path(), "workspace").is_empty());
    }

    #[tokio::test]
    async fn replayed_continuation_is_not_rebroadcast_to_existing_subscribers() {
        let state = tempdir().unwrap();
        let scope_id = uuid::Uuid::new_v4().to_string();
        let bus = EventBus::new_persistent("workspace", state.path());
        let mut existing = bus.subscribe();
        bus.publish(
            "continuation_required",
            serde_json::json!({"scope_id": scope_id, "prompt": "continue"}),
        );
        assert_eq!(existing.recv().await.unwrap().kind, "continuation_required");

        let (replayed, _new_subscriber) = bus.subscribe_with_replay();
        assert_eq!(replayed.len(), 1);
        assert!(existing.try_recv().is_err());
    }

    #[test]
    fn pending_events_from_other_workspaces_are_not_replayed() {
        let state = tempdir().unwrap();
        let scope_id = uuid::Uuid::new_v4().to_string();
        let other = EventBus::new_persistent("other-workspace", state.path());
        other.publish(
            "continuation_required",
            serde_json::json!({"scope_id": scope_id, "prompt": "continue"}),
        );
        drop(other);

        let bus = EventBus::new_persistent("workspace", state.path());
        assert_eq!(
            load_pending_events(state.path(), "other-workspace").len(),
            1
        );
        assert!(load_pending_events(state.path(), "workspace").is_empty());
        let (replayed, _receiver) = bus.subscribe_with_replay();
        assert!(replayed.is_empty());
    }

    #[test]
    fn persistent_bus_advances_sequence_past_replayed_events_after_restart() {
        let state = tempdir().unwrap();
        let scope_id = uuid::Uuid::new_v4().to_string();
        let first = EventBus::new_persistent("workspace", state.path());
        let pending = first.publish(
            "continuation_required",
            serde_json::json!({"scope_id": scope_id, "prompt": "continue"}),
        );
        drop(first);

        let restarted = EventBus::new_persistent("workspace", state.path());
        let next = restarted.publish("tool_started", serde_json::json!({}));
        assert!(next.seq > pending.seq);
    }

    #[cfg(unix)]
    #[test]
    fn pending_continuation_state_is_private() {
        let state = tempdir().unwrap();
        fs::set_permissions(state.path(), fs::Permissions::from_mode(0o777)).unwrap();
        let scope_id = uuid::Uuid::new_v4().to_string();
        let bus = EventBus::new_persistent("workspace", state.path());
        bus.publish(
            "continuation_required",
            serde_json::json!({"scope_id": scope_id, "prompt": "continue"}),
        );
        assert_eq!(
            fs::metadata(state.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let file = state.path().join(format!("{scope_id}.json"));
        assert_eq!(
            fs::metadata(file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
