use crate::orca::{validate_reset_after_seconds, BrowserDriverKind};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::thread;
#[cfg(test)]
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

const TELEMETRY_FILE_NAME: &str = "gpt2omo.jsonl";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryModelHint {
    Unknown,
    Auto,
    Gpt5Family,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryEventType {
    Dispatched,
    RateLimited,
    DeliveryError,
    AuthenticationRequired,
    ProbeUnsupported,
    ProbeUnknown,
    ReadinessBootstrapFailed,
    ReadinessHandshakeFailed,
    ReadinessInvalid,
    DispatchFailed,
    TerminalClaimFailed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryErrorCode {
    None,
    Dispatched,
    RateLimited,
    DeliveryError,
    AuthenticationRequired,
    ProbeUnsupported,
    ProbeUnknown,
    BootstrapFailed,
    ReadinessTimeout,
    ReadinessFailed,
    DispatchFailed,
    TerminalClaimFailed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryEvent {
    timestamp_ms: u64,
    scope_id: String,
    generation: u64,
    driver: BrowserDriverKind,
    model_hint: TelemetryModelHint,
    event_type: TelemetryEventType,
    reset_after_seconds: Option<u64>,
    error_code: TelemetryErrorCode,
}

impl TelemetryEvent {
    pub fn new(
        scope_id: &str,
        generation: u64,
        driver: BrowserDriverKind,
        model_hint: TelemetryModelHint,
        event_type: TelemetryEventType,
        reset_after_seconds: Option<u64>,
        error_code: TelemetryErrorCode,
    ) -> Option<Self> {
        if generation == 0 || uuid::Uuid::parse_str(scope_id).is_err() {
            return None;
        }
        Some(Self {
            timestamp_ms: now_ms(),
            scope_id: scope_id.to_string(),
            generation,
            driver,
            model_hint,
            event_type,
            reset_after_seconds: reset_after_seconds.and_then(validate_reset_after_seconds),
            error_code,
        })
    }
}

pub fn append_best_effort(event: &TelemetryEvent) -> io::Result<()> {
    let mut last_err = None;
    for path in telemetry_candidate_paths() {
        match try_append_to_path(&path, event) {
            Ok(()) => return Ok(()),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(ErrorKind::NotFound, "No candidate telemetry path available")
    }))
}

const TELEMETRY_READ_CHUNK_BYTES: usize = 64 * 1024;

pub fn read_recent_events(window_ms: u64, now_ms: u64) -> Vec<TelemetryEvent> {
    let cutoff = now_ms.saturating_sub(window_ms);
    let mut events = Vec::new();
    for path in telemetry_candidate_paths() {
        if let Ok(path_events) = read_recent_events_from_path(&path, cutoff, now_ms) {
            events.extend(path_events);
        }
    }
    events.sort_by(|a, b| {
        a.timestamp_ms
            .cmp(&b.timestamp_ms)
            .then_with(|| a.scope_id.cmp(&b.scope_id))
            .then_with(|| a.generation.cmp(&b.generation))
    });
    events.dedup();
    events
}

fn read_recent_events_from_path(
    path: &Path,
    cutoff: u64,
    now_ms: u64,
) -> io::Result<Vec<TelemetryEvent>> {
    let mut file = File::open(path)?;
    let mut position = file.seek(SeekFrom::End(0))?;
    let mut pending = Vec::new();
    let mut events = Vec::new();
    let mut reached_cutoff = false;

    while position > 0 && !reached_cutoff {
        let read_len = position.min(TELEMETRY_READ_CHUNK_BYTES as u64) as usize;
        position -= read_len as u64;
        file.seek(SeekFrom::Start(position))?;

        let mut chunk = vec![0; read_len];
        file.read_exact(&mut chunk)?;
        chunk.extend_from_slice(&pending);
        pending = chunk;

        while let Some(newline) = pending.iter().rposition(|byte| *byte == b'\n') {
            let line_start = newline + 1;
            reached_cutoff =
                parse_recent_telemetry_line(&pending[line_start..], cutoff, now_ms, &mut events);
            pending.truncate(newline);
            if reached_cutoff {
                break;
            }
        }
    }

    if !reached_cutoff && !pending.is_empty() {
        parse_recent_telemetry_line(&pending, cutoff, now_ms, &mut events);
    }

    Ok(events)
}

fn parse_recent_telemetry_line(
    line: &[u8],
    cutoff: u64,
    now_ms: u64,
    events: &mut Vec<TelemetryEvent>,
) -> bool {
    let Ok(event) = serde_json::from_slice::<TelemetryEvent>(line) else {
        return false;
    };
    if event.timestamp_ms < cutoff {
        return true;
    }
    if event.timestamp_ms <= now_ms {
        events.push(event);
    }
    false
}

pub fn active_rate_limit_lockout(now_ms: u64) -> Option<(u64, Option<u64>)> {
    let recent = read_recent_events(3600 * 1000, now_ms);
    for event in recent.iter().rev() {
        if event.event_type == TelemetryEventType::RateLimited {
            let reset_ms = event
                .reset_after_seconds
                .map(|sec| event.timestamp_ms + sec * 1000)
                .unwrap_or_else(|| event.timestamp_ms + 15 * 60 * 1000);
            if reset_ms > now_ms {
                let remaining_secs = (reset_ms - now_ms) / 1000;
                return Some((event.timestamp_ms, Some(remaining_secs)));
            }
        }
    }
    None
}

pub fn recent_dispatches_in_window(window_ms: u64, now_ms: u64) -> usize {
    let recent = read_recent_events(window_ms, now_ms);
    recent
        .iter()
        .filter(|event| event.event_type == TelemetryEventType::Dispatched)
        .count()
}

fn telemetry_candidate_paths() -> Vec<PathBuf> {
    let mut paths = Vec::with_capacity(2);
    if let Some(home) = std::env::var_os("HOME").filter(|value| !value.is_empty()) {
        paths.push(
            PathBuf::from(home)
                .join(".omo")
                .join("telemetry")
                .join(TELEMETRY_FILE_NAME),
        );
    }
    let fallback = std::env::temp_dir()
        .join("omo")
        .join("telemetry")
        .join(TELEMETRY_FILE_NAME);
    if !paths.contains(&fallback) {
        paths.push(fallback);
    }
    paths
}

pub fn append_to_path(path: &Path, event: &TelemetryEvent) -> io::Result<()> {
    append_to_path_with_lock(path, event, AppendLock::acquire)
}

pub fn try_append_to_path(path: &Path, event: &TelemetryEvent) -> io::Result<()> {
    append_to_path_with_lock(path, event, AppendLock::try_acquire)
}

fn append_to_path_with_lock<F>(
    path: &Path,
    event: &TelemetryEvent,
    acquire_lock: F,
) -> io::Result<()>
where
    F: FnOnce(&Path) -> io::Result<AppendLock>,
{
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "telemetry path has no parent"))?;
    prepare_directory(parent)?;
    let _lock = acquire_lock(path)?;

    let mut serialized =
        serde_json::to_vec(event).map_err(|error| io::Error::new(ErrorKind::InvalidData, error))?;
    serialized.push(b'\n');

    let mut options = OpenOptions::new();
    options.create(true).append(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(&serialized)?;
    file.flush()?;
    Ok(())
}

fn prepare_directory(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

pub struct AppendLock {
    file: File,
}

impl AppendLock {
    pub fn try_acquire(telemetry_path: &Path) -> io::Result<Self> {
        let lock_path = append_lock_path(telemetry_path)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options.open(&lock_path)?;
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { file }),
            Err(TryLockError::WouldBlock) => Err(io::Error::new(
                ErrorKind::WouldBlock,
                "telemetry append lock would block",
            )),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }

    pub fn acquire(telemetry_path: &Path) -> io::Result<Self> {
        let lock_path = append_lock_path(telemetry_path)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options.open(&lock_path)?;
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.lock()?;
        Ok(Self { file })
    }
}

impl Drop for AppendLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn append_lock_path(telemetry_path: &Path) -> io::Result<PathBuf> {
    let parent = telemetry_path
        .parent()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "telemetry path has no parent"))?;
    let file_name = telemetry_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidInput, "telemetry path has no file name")
        })?;
    Ok(parent.join(format!(".{file_name}.append.lock")))
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
    use std::collections::BTreeSet;
    use std::sync::{Arc, Barrier};
    use tempfile::tempdir;

    const SCOPE: &str = "33333333-3333-4333-8333-333333333333";

    fn event() -> TelemetryEvent {
        event_at(now_ms())
    }

    fn event_at(timestamp_ms: u64) -> TelemetryEvent {
        let mut event = TelemetryEvent::new(
            SCOPE,
            1,
            BrowserDriverKind::Orca,
            TelemetryModelHint::Unknown,
            TelemetryEventType::RateLimited,
            Some(90),
            TelemetryErrorCode::RateLimited,
        )
        .unwrap();
        event.timestamp_ms = timestamp_ms;
        event
    }

    #[test]
    fn schema_is_fixed_and_invalid_reset_is_discarded() {
        let event = TelemetryEvent::new(
            SCOPE,
            7,
            BrowserDriverKind::AgentBrowser,
            TelemetryModelHint::Auto,
            TelemetryEventType::ProbeUnknown,
            Some(crate::orca::MAX_RESET_AFTER_SECONDS + 1),
            TelemetryErrorCode::ProbeUnknown,
        )
        .unwrap();
        let value = serde_json::to_value(event).unwrap();
        let keys = value
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected = [
            "timestamp_ms",
            "scope_id",
            "generation",
            "driver",
            "model_hint",
            "event_type",
            "reset_after_seconds",
            "error_code",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
        assert_eq!(keys, expected);
        assert!(value["reset_after_seconds"].is_null());
        assert!(TelemetryEvent::new(
            "not-a-scope",
            1,
            BrowserDriverKind::Orca,
            TelemetryModelHint::Unknown,
            TelemetryEventType::ProbeUnknown,
            None,
            TelemetryErrorCode::ProbeUnknown,
        )
        .is_none());
    }

    #[test]
    fn reverse_read_returns_recent_events_without_deserializing_older_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("telemetry").join("events.jsonl");
        prepare_directory(path.parent().unwrap()).unwrap();
        let now = 10_000;
        let content = format!(
            "{}\nnot-json\n{}\n{}\n",
            serde_json::to_string(&event_at(1_000)).unwrap(),
            serde_json::to_string(&event_at(9_000)).unwrap(),
            serde_json::to_string(&event_at(11_000)).unwrap(),
        );
        fs::write(&path, content).unwrap();

        let events = read_recent_events_from_path(&path, 8_000, now).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].timestamp_ms, 9_000);
    }

    #[test]
    fn concurrent_jsonl_writes_are_complete_and_non_interleaved() {
        let dir = tempdir().unwrap();
        let path = Arc::new(dir.path().join("telemetry").join("events.jsonl"));
        let worker_count = 32;
        let barrier = Arc::new(Barrier::new(worker_count));
        let mut handles = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            let event = event();
            handles.push(thread::spawn(move || {
                barrier.wait();
                append_to_path(&path, &event).unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let content = fs::read_to_string(path.as_ref()).unwrap();
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), worker_count);
        for line in lines {
            let parsed: TelemetryEvent = serde_json::from_str(line).unwrap();
            assert_eq!(parsed.scope_id, SCOPE);
            assert_eq!(parsed.reset_after_seconds, Some(90));
        }
    }

    #[test]
    fn best_effort_path_does_not_wait_for_busy_append_lock() {
        use std::time::Instant;

        let dir = tempdir().unwrap();
        let path = dir.path().join("telemetry").join("events.jsonl");
        prepare_directory(path.parent().unwrap()).unwrap();
        let held = AppendLock::try_acquire(&path).unwrap();
        let started = Instant::now();
        let error = try_append_to_path(&path, &event()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::WouldBlock);
        assert!(started.elapsed() < Duration::from_millis(100));
        drop(held);
    }

    #[cfg(unix)]
    #[test]
    fn telemetry_permissions_are_private() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("telemetry").join("events.jsonl");
        append_to_path(&path, &event()).unwrap();
        let dir_mode = fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let file_mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }

    #[test]
    fn append_lock_releases_on_drop_allowing_subsequent_acquire() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("telemetry").join("events.jsonl");
        prepare_directory(path.parent().unwrap()).unwrap();
        let lock = AppendLock::try_acquire(&path).unwrap();
        // Trying to acquire while held should fail with WouldBlock
        assert!(AppendLock::try_acquire(&path).is_err());
        drop(lock);
        // After drop, lock is available again
        let lock2 = AppendLock::try_acquire(&path);
        assert!(lock2.is_ok());
    }
}
