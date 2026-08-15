use serde::{Deserialize, Serialize};
use serde_json::Value;
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
}

impl EventBus {
    pub fn new(workspace: impl Into<String>) -> Self {
        let (sender, _) = broadcast::channel(EVENT_BUFFER);
        Self {
            sender,
            next_seq: Arc::new(AtomicU64::new(1)),
            workspace: workspace.into(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HarnessEvent> {
        self.sender.subscribe()
    }

    pub fn publish(&self, kind: impl Into<String>, data: Value) -> HarnessEvent {
        let event = HarnessEvent {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            kind: kind.into(),
            timestamp_ms: now_ms(),
            workspace: self.workspace.clone(),
            data,
        };
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
            }),
        }
    }

    pub fn latest_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed).saturating_sub(1)
    }
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
}
