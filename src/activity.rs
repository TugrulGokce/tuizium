//! Activity journal and post-action verification.
//!
//! Kafka Connect answers pause/resume/restart with a bare `202 Accepted` and does
//! the work asynchronously, and exposes no event log. So Tuizium keeps its own
//! journal (in memory, and appended to a file) and *watches* the next few
//! snapshots to confirm the requested state change actually happened.

use std::collections::VecDeque;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Local;

use crate::api::Connector;

const MAX_ENTRIES: usize = 500;
const MAX_FILE_BYTES: u64 = 1_000_000;
const MAX_LINE_CHARS: usize = 400;

/// How long a requested change is watched before giving up.
pub const WATCH_TIMEOUT: Duration = Duration::from_secs(15);
/// A restart that is already healthy this early may simply not have started yet.
const RESTART_GRACE: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Info,
    Ok,
    Warn,
    Error,
}

impl Severity {
    fn tag(self) -> &'static str {
        match self {
            Severity::Info => "INFO",
            Severity::Ok => "OK",
            Severity::Warn => "WARN",
            Severity::Error => "ERROR",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub time: String,
    pub severity: Severity,
    pub text: String,
}

/// Strips control characters (so nothing can forge log lines or terminal
/// escapes) and bounds the length.
pub fn sanitize(text: &str) -> String {
    let mut out: String =
        text.chars().map(|c| if c.is_control() { ' ' } else { c }).take(MAX_LINE_CHARS).collect();
    if text.chars().count() > MAX_LINE_CHARS {
        out.push('…');
    }
    out
}

#[derive(Default)]
pub struct ActivityLog {
    pub entries: VecDeque<Entry>,
    file: Option<PathBuf>,
    pub file_failed: bool,
}

impl ActivityLog {
    pub fn with_file(path: Option<PathBuf>) -> Self {
        Self { file: path, ..Default::default() }
    }

    pub fn path(&self) -> Option<&Path> {
        self.file.as_deref()
    }

    pub fn last(&self) -> Option<&Entry> {
        self.entries.back()
    }

    pub fn record(&mut self, cluster: &str, severity: Severity, text: &str) {
        let now = Local::now();
        let text = sanitize(text);
        if let Some(path) = &self.file {
            let line = format!("{} [{}] {}: {}\n", now.to_rfc3339(), severity.tag(), sanitize(cluster), text);
            self.file_failed = append_line(path, &line).is_err();
        }
        if self.entries.len() == MAX_ENTRIES {
            self.entries.pop_front();
        }
        self.entries.push_back(Entry { time: now.format("%H:%M:%S").to_string(), severity, text });
    }
}

/// Appends one line, rotating once the file passes `MAX_FILE_BYTES`. The file is
/// private to the user on Unix.
fn append_line(path: &Path, line: &str) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    if std::fs::metadata(path).is_ok_and(|m| m.len() > MAX_FILE_BYTES) {
        let rotated = path.with_extension("log.1");
        let _ = std::fs::remove_file(&rotated);
        let _ = std::fs::rename(path, rotated);
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)?.write_all(line.as_bytes())
}

pub fn default_path() -> Option<PathBuf> {
    Some(dirs::state_dir().or_else(dirs::data_local_dir)?.join("tuizium").join("actions.log"))
}

// ---- verification -----------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    Paused,
    Running,
    /// A just-created connector: it may legitimately be missing from the first snapshots.
    Created,
    /// Went through a non-running state and came back (or is healthy after the grace period).
    Restarted,
    Gone,
}

#[derive(Clone, Debug)]
pub struct Watch {
    pub name: String,
    pub expect: Expect,
    pub started: std::time::Instant,
    /// A transitional state (e.g. RESTARTING) seen while watching.
    seen: Option<String>,
}

impl Watch {
    pub fn new(name: String, expect: Expect) -> Self {
        Self { name, expect, started: std::time::Instant::now(), seen: None }
    }
}

fn settled(c: &Connector) -> bool {
    c.state == "RUNNING" && c.tasks.iter().all(|t| t.state == "RUNNING")
}

/// The first state that is neither healthy nor a (pre-existing) failure.
fn transitional(c: &Connector) -> Option<&str> {
    std::iter::once(c.state.as_str())
        .chain(c.tasks.iter().map(|t| t.state.as_str()))
        .find(|s| !matches!(*s, "RUNNING" | "FAILED"))
}

fn describe(c: &Connector) -> String {
    let failed = c.tasks.iter().filter(|t| t.state == "FAILED").count();
    if failed > 0 {
        format!("{} ({failed}/{} tasks FAILED)", c.state, c.tasks.len())
    } else {
        format!("{} ({}/{} tasks running)", c.state, c.running_tasks(), c.tasks.len())
    }
}

/// Judges one fresh snapshot. `None` means "keep watching".
pub fn judge(w: &mut Watch, c: Option<&Connector>, elapsed: Duration) -> Option<(Severity, String)> {
    let timed_out = elapsed >= WATCH_TIMEOUT;
    let n = &w.name;
    let Some(c) = c else {
        return Some(match w.expect {
            Expect::Gone => (Severity::Ok, format!("{n}: deleted — no longer listed")),
            Expect::Created => {
                return timed_out.then(|| {
                    (
                        Severity::Warn,
                        format!("{n}: created, but still not listed after {}s", elapsed.as_secs()),
                    )
                });
            }
            _ => (Severity::Warn, format!("{n}: not listed on the cluster any more")),
        });
    };
    match w.expect {
        Expect::Gone => timed_out
            .then(|| (Severity::Warn, format!("{n}: still listed {}s after delete", elapsed.as_secs()))),
        Expect::Paused => {
            if c.is_paused() {
                Some((Severity::Ok, format!("{n}: confirmed {}", c.state)))
            } else {
                timed_out.then(|| {
                    (
                        Severity::Warn,
                        format!("{n}: not paused after {}s — {}", elapsed.as_secs(), describe(c)),
                    )
                })
            }
        }
        Expect::Running => {
            if settled(c) {
                Some((Severity::Ok, format!("{n}: confirmed {}", describe(c))))
            } else {
                timed_out.then(|| {
                    (
                        Severity::Warn,
                        format!("{n}: not running after {}s — {}", elapsed.as_secs(), describe(c)),
                    )
                })
            }
        }
        Expect::Created => {
            if settled(c) {
                return Some((Severity::Ok, format!("{n}: created — confirmed {}", describe(c))));
            }
            let sev = if c.is_failed() { Severity::Error } else { Severity::Warn };
            timed_out.then(|| {
                (sev, format!("{n}: created, but not healthy after {}s — {}", elapsed.as_secs(), describe(c)))
            })
        }
        Expect::Restarted => {
            if let Some(s) = transitional(c) {
                w.seen = Some(s.to_string());
            }
            if settled(c) {
                if let Some(s) = &w.seen {
                    return Some((
                        Severity::Ok,
                        format!("{n}: restart confirmed ({s} → RUNNING, {})", describe(c)),
                    ));
                }
                if elapsed >= RESTART_GRACE {
                    return Some((
                        Severity::Info,
                        format!(
                            "{n}: restart accepted, healthy again ({}) — it finished between refreshes, so no RESTARTING state was seen",
                            describe(c)
                        ),
                    ));
                }
                return None;
            }
            let sev = if c.is_failed() { Severity::Error } else { Severity::Warn };
            timed_out.then(|| {
                (sev, format!("{n}: not healthy {}s after restart — {}", elapsed.as_secs(), describe(c)))
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{Kind, Task};
    use serde_json::Map;

    fn conn(state: &str, tasks: &[&str]) -> Connector {
        Connector {
            name: "c".into(),
            kind: Kind::Sink,
            state: state.into(),
            worker_id: String::new(),
            class: String::new(),
            config: Map::new(),
            tasks: tasks
                .iter()
                .enumerate()
                .map(|(i, s)| Task {
                    id: i as i64,
                    state: (*s).into(),
                    worker_id: String::new(),
                    trace: None,
                })
                .collect(),
            trace: None,
        }
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn sanitize_removes_control_chars_and_bounds_length() {
        assert_eq!(sanitize("a\nb\x1b[31mc\r"), "a b [31mc ");
        let long = sanitize(&"x".repeat(5000));
        assert_eq!(long.chars().count(), MAX_LINE_CHARS + 1);
        assert!(long.ends_with('…'));
    }

    #[test]
    fn restart_is_confirmed_only_after_a_transition() {
        let mut w = Watch::new("c".into(), Expect::Restarted);
        // Still healthy right after the request: Connect may not have started yet.
        assert!(judge(&mut w, Some(&conn("RUNNING", &["RUNNING"])), secs(1)).is_none());
        assert!(judge(&mut w, Some(&conn("RESTARTING", &["RUNNING"])), secs(2)).is_none());
        let (sev, msg) = judge(&mut w, Some(&conn("RUNNING", &["RUNNING"])), secs(3)).unwrap();
        assert_eq!(sev, Severity::Ok);
        assert!(msg.contains("RESTARTING → RUNNING"), "{msg}");
    }

    #[test]
    fn restart_that_was_too_fast_is_reported_honestly() {
        let mut w = Watch::new("c".into(), Expect::Restarted);
        let (sev, msg) = judge(&mut w, Some(&conn("RUNNING", &["RUNNING"])), secs(4)).unwrap();
        assert_eq!(sev, Severity::Info);
        assert!(msg.contains("no RESTARTING state was seen"), "{msg}");
    }

    #[test]
    fn restart_times_out_as_error_when_still_failed() {
        let mut w = Watch::new("c".into(), Expect::Restarted);
        assert!(judge(&mut w, Some(&conn("RUNNING", &["FAILED"])), secs(10)).is_none());
        let (sev, msg) = judge(&mut w, Some(&conn("RUNNING", &["FAILED"])), secs(15)).unwrap();
        assert_eq!(sev, Severity::Error);
        assert!(msg.contains("FAILED"), "{msg}");
    }

    #[test]
    fn pause_resume_and_delete_expectations() {
        let mut w = Watch::new("c".into(), Expect::Paused);
        assert!(judge(&mut w, Some(&conn("RUNNING", &["RUNNING"])), secs(1)).is_none());
        assert_eq!(judge(&mut w, Some(&conn("PAUSED", &["PAUSED"])), secs(2)).unwrap().0, Severity::Ok);
        assert_eq!(judge(&mut w, Some(&conn("RUNNING", &["RUNNING"])), secs(20)).unwrap().0, Severity::Warn);

        let mut w = Watch::new("c".into(), Expect::Running);
        assert!(judge(&mut w, Some(&conn("PAUSED", &["PAUSED"])), secs(1)).is_none());
        assert!(judge(&mut w, Some(&conn("RUNNING", &["UNASSIGNED"])), secs(2)).is_none());
        assert_eq!(judge(&mut w, Some(&conn("RUNNING", &["RUNNING"])), secs(3)).unwrap().0, Severity::Ok);

        let mut w = Watch::new("c".into(), Expect::Gone);
        assert!(judge(&mut w, Some(&conn("RUNNING", &[])), secs(1)).is_none());
        assert_eq!(judge(&mut w, None, secs(2)).unwrap().0, Severity::Ok);
        // A connector that vanished while we waited for something else is flagged.
        let mut w = Watch::new("c".into(), Expect::Paused);
        assert_eq!(judge(&mut w, None, secs(1)).unwrap().0, Severity::Warn);
    }

    #[test]
    fn created_connector_is_awaited_instead_of_flagged_as_missing() {
        let mut w = Watch::new("c".into(), Expect::Created);
        assert!(judge(&mut w, None, secs(1)).is_none(), "not listed yet is normal right after a create");
        assert!(judge(&mut w, Some(&conn("RUNNING", &["UNASSIGNED"])), secs(2)).is_none());
        let (sev, msg) = judge(&mut w, Some(&conn("RUNNING", &["RUNNING"])), secs(3)).unwrap();
        assert_eq!(sev, Severity::Ok);
        assert!(msg.contains("created"), "{msg}");

        let mut w = Watch::new("c".into(), Expect::Created);
        let (sev, msg) = judge(&mut w, None, WATCH_TIMEOUT).unwrap();
        assert_eq!(sev, Severity::Warn);
        assert!(msg.contains("not listed"), "{msg}");

        let mut w = Watch::new("c".into(), Expect::Created);
        let (sev, msg) = judge(&mut w, Some(&conn("RUNNING", &["FAILED"])), WATCH_TIMEOUT).unwrap();
        assert_eq!(sev, Severity::Error);
        assert!(msg.contains("FAILED"), "{msg}");
    }

    #[test]
    fn connector_without_tasks_counts_as_settled() {
        let mut w = Watch::new("c".into(), Expect::Running);
        assert_eq!(judge(&mut w, Some(&conn("RUNNING", &[])), secs(0)).unwrap().0, Severity::Ok);
    }

    #[test]
    fn journal_keeps_memory_bounded_and_writes_sanitized_lines() {
        let dir = std::env::temp_dir().join(format!("tuizium-act-{}", std::process::id()));
        let path = dir.join("actions.log");
        let mut log = ActivityLog::with_file(Some(path.clone()));
        log.record("local\nforged", Severity::Ok, "restart a\n2030-01-01T00:00:00Z [OK] forged");
        for i in 0..(MAX_ENTRIES + 20) {
            log.record("local", Severity::Info, &format!("e{i}"));
        }
        assert_eq!(log.entries.len(), MAX_ENTRIES);
        assert_eq!(log.last().unwrap().text, format!("e{}", MAX_ENTRIES + 19));
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), MAX_ENTRIES + 21, "one line per record, no injected lines");
        assert!(text.lines().next().unwrap().contains("local forged"));
        assert!(!log.file_failed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_rotates_when_large() {
        let dir = std::env::temp_dir().join(format!("tuizium-rot-{}", std::process::id()));
        let path = dir.join("actions.log");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, vec![b'x'; (MAX_FILE_BYTES + 10) as usize]).unwrap();
        let mut log = ActivityLog::with_file(Some(path.clone()));
        log.record("c", Severity::Info, "fresh");
        assert!(std::fs::metadata(&path).unwrap().len() < 1000);
        assert!(dir.join("actions.log.1").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unwritable_log_is_flagged_not_fatal() {
        // A path whose parent is a regular file can never be created.
        let file = std::env::temp_dir().join(format!("tuizium-notdir-{}", std::process::id()));
        std::fs::write(&file, "x").unwrap();
        let mut log = ActivityLog::with_file(Some(file.join("actions.log")));
        log.record("c", Severity::Info, "hello");
        assert!(log.file_failed);
        assert_eq!(log.entries.len(), 1);
        let _ = std::fs::remove_file(&file);
    }
}
