use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
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

pub struct EventReplay {
    pub events: Vec<HarnessEvent>,
    pub receiver: broadcast::Receiver<HarnessEvent>,
    pub replayed_through: u64,
    pub missed: Option<(u64, u64)>,
}

#[derive(Clone, Debug)]
pub struct EventBus {
    sender: broadcast::Sender<HarnessEvent>,
    next_seq: Arc<AtomicU64>,
    workspace: String,
    history: Arc<Mutex<VecDeque<HarnessEvent>>>,
}

impl EventBus {
    pub fn new(workspace: impl Into<String>) -> Self {
        let (sender, _) = broadcast::channel(EVENT_BUFFER);
        Self {
            sender,
            next_seq: Arc::new(AtomicU64::new(1)),
            workspace: workspace.into(),
            history: Arc::new(Mutex::new(VecDeque::with_capacity(EVENT_BUFFER))),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HarnessEvent> {
        self.sender.subscribe()
    }

    pub fn subscribe_from(&self, last_event_id: Option<u64>) -> EventReplay {
        let receiver = self.sender.subscribe();
        let requested = last_event_id.unwrap_or(0);
        let history = self.history.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let oldest = history.front().map(|event| event.seq);
        let events = last_event_id
            .map(|_| {
                history
                    .iter()
                    .filter(|event| event.seq > requested)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let missed = last_event_id
            .filter(|_| requested > 0)
            .and_then(|_| oldest)
            .filter(|oldest_seq| requested.saturating_add(1) < *oldest_seq)
            .map(|oldest_seq| (requested.saturating_add(1), oldest_seq.saturating_sub(1)));
        let replayed_through = events
            .last()
            .map(|event| event.seq)
            .unwrap_or(requested);
        EventReplay {
            events,
            receiver,
            replayed_through,
            missed,
        }
    }

    pub fn publish(&self, kind: impl Into<String>, data: Value) -> HarnessEvent {
        let event = HarnessEvent {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            kind: kind.into(),
            timestamp_ms: now_ms(),
            workspace: self.workspace.clone(),
            data,
        };
        {
            let mut history = self.history.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            history.push_back(event.clone());
            while history.len() > EVENT_BUFFER {
                history.pop_front();
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
                "history_capacity": EVENT_BUFFER,
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

    #[test]
    fn replays_events_after_last_event_id() {
        let bus = EventBus::new("workspace");
        bus.publish("one", Value::Null);
        bus.publish("two", Value::Null);
        bus.publish("three", Value::Null);

        let replay = bus.subscribe_from(Some(1));
        assert_eq!(replay.events.iter().map(|event| event.seq).collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(replay.replayed_through, 3);
        assert!(replay.missed.is_none());
    }

    #[test]
    fn reports_when_last_event_id_is_older_than_history() {
        let bus = EventBus::new("workspace");
        for index in 0..(EVENT_BUFFER + 2) {
            bus.publish("progress", serde_json::json!({"index": index}));
        }

        let replay = bus.subscribe_from(Some(1));
        assert_eq!(replay.missed, Some((2, 2)));
        assert_eq!(replay.events.first().map(|event| event.seq), Some(3));
        assert_eq!(replay.events.len(), EVENT_BUFFER);
    }
}
