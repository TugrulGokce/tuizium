//! Application state and input handling. Rendering lives in `ui.rs`; all
//! network I/O is spawned onto the Tokio runtime and reported back as `Msg`s,
//! so the UI thread never blocks on the API.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::widgets::TableState;
use serde_json::{Map, Value};
use tokio::sync::mpsc::UnboundedSender;

use crate::activity::{self, ActivityLog, Expect, Severity, Watch};
use crate::api::{self, Client, Connector, Kind, Plugin, RootInfo, Snapshot};
use crate::config::{self, Cluster, Config};
use crate::setup::{self, SetupForm};
use crate::splash;
use crate::templates::{self, FormState, TEMPLATES};

/// Largest connector JSON file or edited config we are willing to read.
const MAX_JSON_BYTES: u64 = 5 * 1024 * 1024;

pub enum Msg {
    Key(KeyEvent),
    Tick,
    Redraw,
    /// Animation frame; only does work while the splash is showing.
    Frame,
    Quit,
    Loaded {
        generation: u64,
        result: Result<Snapshot, String>,
    },
    ActionDone {
        label: String,
        select: Option<String>,
        result: Result<(), ActionError>,
        /// What to verify on the next snapshots, per connector.
        watch: Vec<(String, Expect)>,
    },
    Validated {
        id: u64,
        result: Result<Vec<String>, String>,
    },
}

/// Failure of an action; for batches, `failed` names the connectors that did not
/// succeed (`None` = all of them) so the rest can still be verified.
#[derive(Debug)]
pub struct ActionError {
    pub msg: String,
    pub failed: Option<Vec<String>>,
}

impl From<anyhow::Error> for ActionError {
    fn from(e: anyhow::Error) -> Self {
        Self { msg: format!("{e:#}"), failed: None }
    }
}

#[derive(Clone, Debug)]
pub enum Conn {
    Connecting,
    Connected(Option<RootInfo>),
    Failed(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Focus {
    List,
    Details,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Panel {
    Overview,
    Tasks,
    Config,
    Trace,
}

impl Panel {
    pub const ALL: [Panel; 4] = [Panel::Overview, Panel::Tasks, Panel::Config, Panel::Trace];

    pub fn title(self) -> &'static str {
        match self {
            Panel::Overview => "Overview",
            Panel::Tasks => "Tasks",
            Panel::Config => "Config",
            Panel::Trace => "Trace",
        }
    }

    fn index(self) -> usize {
        Self::ALL.iter().position(|p| *p == self).unwrap_or(0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TypeFilter {
    All,
    Source,
    Sink,
}

impl TypeFilter {
    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Source => "source",
            Self::Sink => "sink",
        }
    }
    fn next(self) -> Self {
        match self {
            Self::All => Self::Source,
            Self::Source => Self::Sink,
            Self::Sink => Self::All,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StateFilter {
    All,
    Running,
    Paused,
    Failed,
    Unassigned,
}

impl StateFilter {
    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Failed => "failed",
            Self::Unassigned => "unassigned",
        }
    }
    fn next(self) -> Self {
        match self {
            Self::All => Self::Running,
            Self::Running => Self::Paused,
            Self::Paused => Self::Failed,
            Self::Failed => Self::Unassigned,
            Self::Unassigned => Self::All,
        }
    }
    fn matches(self, c: &Connector) -> bool {
        match self {
            Self::All => true,
            Self::Running => c.state == "RUNNING" && !c.is_failed(),
            Self::Paused => c.is_paused(),
            Self::Failed => c.is_failed(),
            Self::Unassigned => c.state == "UNASSIGNED",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    Name,
    Type,
    State,
    Tasks,
    Plugin,
}

impl SortKey {
    pub fn label(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Type => "type",
            Self::State => "state",
            Self::Tasks => "tasks",
            Self::Plugin => "plugin",
        }
    }
    fn next(self) -> Self {
        match self {
            Self::Name => Self::Type,
            Self::Type => Self::State,
            Self::State => Self::Tasks,
            Self::Tasks => Self::Plugin,
            Self::Plugin => Self::Name,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl From<Severity> for Level {
    fn from(s: Severity) -> Self {
        match s {
            Severity::Info | Severity::Ok => Level::Info,
            Severity::Warn => Level::Warn,
            Severity::Error => Level::Error,
        }
    }
}

pub struct Toast {
    pub text: String,
    pub level: Level,
    at: Instant,
}

pub struct Confirm {
    pub title: String,
    pub body: String,
    pub delete: Vec<String>,
}

/// A batch operation applied to one or more connectors.
#[derive(Clone, Copy)]
pub enum Op {
    Pause,
    Resume,
    Restart,
    RestartFailed,
    Delete,
}

impl Op {
    fn verb(self) -> &'static str {
        match self {
            Op::Pause => "pause",
            Op::Resume => "resume",
            Op::Restart => "restart",
            Op::RestartFailed => "restart failed parts of",
            Op::Delete => "delete",
        }
    }
}

impl Op {
    /// The state we expect to observe once Kafka Connect has processed the request.
    fn expect(self) -> Expect {
        match self {
            Op::Pause => Expect::Paused,
            Op::Resume => Expect::Running,
            Op::Restart | Op::RestartFailed => Expect::Restarted,
            Op::Delete => Expect::Gone,
        }
    }
}

/// Applies `op` to every name, continuing past failures and reporting them together.
async fn apply(client: Client, op: Op, names: Vec<String>) -> Result<(), ActionError> {
    let mut errors = Vec::new();
    let mut failed = Vec::new();
    for n in &names {
        let r = match op {
            Op::Pause => client.pause(n).await,
            Op::Resume => client.resume(n).await,
            Op::Restart => client.restart(n, false).await,
            Op::RestartFailed => client.restart(n, true).await,
            Op::Delete => client.delete(n).await,
        };
        if let Err(e) = r {
            errors.push(format!("{n}: {e:#}"));
            failed.push(n.clone());
        }
    }
    if errors.is_empty() { Ok(()) } else { Err(ActionError { msg: errors.join("\n"), failed: Some(failed) }) }
}

/// Asks the main loop to suspend the TUI and run the user's editor on `path`.
pub struct EditorRequest {
    pub name: String,
    pub path: PathBuf,
    original: Map<String, Value>,
}

pub enum Validation {
    Pending,
    Ok,
    Errors(Vec<String>),
    /// The validate endpoint could not be used; creating is still allowed.
    Unavailable(String),
}

pub struct ImportState {
    pub input: String,
    pub error: Option<String>,
}

pub enum PreviewSource {
    Form(FormState),
    Import(String),
    Edit,
}

pub struct Preview {
    pub id: u64,
    pub name: String,
    pub config: Map<String, Value>,
    pub scroll: u16,
    pub source: PreviewSource,
    /// `true` = update an existing connector (PUT), `false` = create (POST).
    pub update: bool,
    pub validation: Validation,
    /// The config as it is on the cluster now; set for updates so the preview can show a diff.
    pub original: Option<Map<String, Value>>,
    /// Whether the connector is a source; picks which replay heads-ups apply.
    pub is_source: bool,
}

pub struct AddCluster {
    pub input: String,
    pub error: Option<String>,
}

#[derive(Default)]
pub enum Overlay {
    #[default]
    None,
    Help,
    Error(String),
    Confirm(Confirm),
    Clusters(usize),
    AddCluster(AddCluster),
    Templates(usize),
    Form(Box<FormState>),
    Import(ImportState),
    Preview(Box<Preview>),
    Plugins(usize),
    /// Activity journal; the value is the scroll offset (0 = newest).
    Activity(usize),
}

pub struct App {
    pub config: Config,
    config_path: Option<PathBuf>,
    pub cluster: String,
    client: Option<Client>,
    pub conn: Conn,
    generation: u64,
    pub loading: bool,

    pub connectors: Vec<Connector>,
    pub visible: Vec<usize>,
    pub table: TableState,
    /// Connectors marked with Space for batch operations.
    pub marked: BTreeSet<String>,
    /// Plugins installed on the current cluster (e.g. which Debezium versions).
    pub plugins: Vec<Plugin>,

    pub query: String,
    pub searching: bool,
    pub type_filter: TypeFilter,
    pub state_filter: StateFilter,
    /// Only connectors whose worker (`host:port`) equals this; cycled with `w`.
    pub worker_filter: Option<String>,
    /// Only connectors using this plugin class; cycled with `x`.
    pub plugin_filter: Option<String>,
    pub sort: SortKey,
    pub sort_desc: bool,

    /// Search inside the Trace panel (`/` while it is focused); empty = no search.
    pub trace_query: String,
    pub trace_searching: bool,
    /// Width of the Trace panel at the last draw, so match jumps can count wrapped rows.
    pub trace_width: Cell<u16>,

    pub focus: Focus,
    pub panel: Panel,
    pub task_sel: usize,
    pub scroll: u16,

    pub overlay: Overlay,
    pub toast: Option<Toast>,

    pub auto_refresh: bool,
    interval: Duration,
    pub last_refresh: Option<Instant>,
    last_attempt: Instant,
    pending_select: Option<String>,
    preview_seq: u64,
    pub editor_request: Option<EditorRequest>,

    /// Journal of everything Tuizium did and observed; also appended to a file.
    pub log: ActivityLog,
    /// Requested changes still being verified against fresh snapshots.
    watches: Vec<Watch>,

    tx: UnboundedSender<Msg>,
    pub should_quit: bool,
    pub dirty: bool,

    /// What is on screen: the splash, the connection form, or the connector list.
    pub screen: Screen,
    /// Show the connection form after the splash (see `Config::ask_on_start`).
    ask_setup: bool,
}

/// Which full-screen view is active.
pub enum Screen {
    Splash(Instant),
    Setup(Box<SetupForm>),
    Main,
}

impl App {
    #[cfg(test)]
    pub fn new(
        config: Config,
        config_path: Option<PathBuf>,
        cluster: String,
        tx: UnboundedSender<Msg>,
    ) -> Self {
        Self::new_with(config, config_path, cluster, tx, true)
    }

    /// `connect = false` leaves the app disconnected (no request is sent) until a
    /// cluster is chosen, e.g. on the startup connection screen.
    pub fn new_with(
        config: Config,
        config_path: Option<PathBuf>,
        cluster: String,
        tx: UnboundedSender<Msg>,
        connect: bool,
    ) -> Self {
        let interval = config.refresh_interval();
        let mut app = Self {
            config,
            config_path,
            cluster: String::new(),
            client: None,
            conn: Conn::Connecting,
            generation: 0,
            loading: false,
            connectors: Vec::new(),
            visible: Vec::new(),
            table: TableState::default(),
            marked: BTreeSet::new(),
            plugins: Vec::new(),
            query: String::new(),
            searching: false,
            type_filter: TypeFilter::All,
            state_filter: StateFilter::All,
            worker_filter: None,
            plugin_filter: None,
            sort: SortKey::Name,
            sort_desc: false,
            trace_query: String::new(),
            trace_searching: false,
            trace_width: Cell::new(80),
            focus: Focus::List,
            panel: Panel::Overview,
            task_sel: 0,
            scroll: 0,
            overlay: Overlay::None,
            toast: None,
            auto_refresh: true,
            interval,
            last_refresh: None,
            last_attempt: Instant::now(),
            pending_select: None,
            preview_seq: 0,
            editor_request: None,
            log: ActivityLog::default(),
            watches: Vec::new(),
            tx,
            should_quit: false,
            dirty: true,
            screen: Screen::Main,
            ask_setup: false,
        };
        if connect {
            app.use_cluster(&cluster);
        } else {
            app.cluster = cluster;
        }
        app
    }

    /// Chooses the opening screens. Without `splash` and `ask`, connects right away.
    pub fn start_screens(&mut self, splash: bool, ask: bool) {
        self.ask_setup = ask;
        if splash {
            splash::ACTIVE.store(true, std::sync::atomic::Ordering::Relaxed);
            self.screen = Screen::Splash(Instant::now());
        } else {
            self.after_splash();
        }
        self.dirty = true;
    }

    fn after_splash(&mut self) {
        splash::ACTIVE.store(false, std::sync::atomic::Ordering::Relaxed);
        if self.ask_setup {
            let form = SetupForm::new(&self.config, &self.cluster);
            self.screen = Screen::Setup(Box::new(form));
        } else {
            self.screen = Screen::Main;
            let cluster = self.cluster.clone();
            self.use_cluster(&cluster);
        }
        self.dirty = true;
    }

    /// Handles a key on the splash or the connection form; `true` if it was consumed.
    fn on_screen_key(&mut self, key: KeyEvent) -> bool {
        match std::mem::replace(&mut self.screen, Screen::Main) {
            Screen::Main => false,
            // The splash waits for Enter; every other key leaves it on screen.
            Screen::Splash(started) => {
                match key.code {
                    KeyCode::Enter => self.after_splash(),
                    KeyCode::Esc => self.should_quit = true,
                    _ => self.screen = Screen::Splash(started),
                }
                true
            }
            Screen::Setup(mut form) => {
                match form.on_key(&self.config, key) {
                    setup::Outcome::Stay => self.screen = Screen::Setup(form),
                    setup::Outcome::Quit => self.should_quit = true,
                    setup::Outcome::Connect { name, cluster, save } => {
                        self.adopt_profile(&name, cluster, save);
                        self.screen = Screen::Main;
                        self.use_cluster(&name);
                        self.note(Severity::Info, &format!("connecting to profile {name}"));
                    }
                }
                true
            }
        }
    }

    /// Remembers the profile chosen on the connection form and makes it the default.
    fn adopt_profile(&mut self, name: &str, cluster: Cluster, save: bool) {
        let changed_default = self.config.default_cluster.as_deref() != Some(name);
        if save {
            self.config.clusters.insert(name.to_string(), cluster);
        }
        self.config.default_cluster = Some(name.to_string());
        if (save || changed_default)
            && let Some(path) = &self.config_path
            && let Err(e) = config::save(path, &self.config)
        {
            self.toast(format!("connected, but the profile could not be saved: {e:#}"), Level::Warn);
        }
    }

    /// Enables the persistent action log (not done in tests, which stay off disk).
    pub fn set_activity_file(&mut self, path: Option<PathBuf>) {
        self.log = ActivityLog::with_file(path);
        let cluster = self.cluster.clone();
        self.log.record(&cluster, Severity::Info, "session started");
    }

    fn note(&mut self, severity: Severity, text: &str) {
        let cluster = self.cluster.clone();
        self.log.record(&cluster, severity, text);
        self.dirty = true;
    }

    // ---- cluster / refresh ------------------------------------------------

    pub fn use_cluster(&mut self, name: &str) {
        self.generation += 1;
        if !self.watches.is_empty() {
            self.watches.clear();
            self.note(Severity::Warn, "cluster switched: pending verifications were dropped");
        }
        self.cluster = name.to_string();
        self.connectors.clear();
        self.visible.clear();
        self.marked.clear();
        self.plugins.clear();
        self.worker_filter = None;
        self.plugin_filter = None;
        self.trace_query.clear();
        self.trace_searching = false;
        self.table.select(None);
        self.last_refresh = None;
        self.loading = false;
        self.focus = Focus::List;
        self.conn = Conn::Connecting;
        let built = self
            .config
            .clusters
            .get(name)
            .ok_or_else(|| format!("unknown cluster {name:?}"))
            .and_then(|c| c.client().map_err(|e| format!("{e:#}")));
        match built {
            Ok(c) => {
                let insecure = c.sends_credentials_in_clear();
                self.client = Some(c);
                self.refresh();
                if insecure {
                    let msg = format!(
                        "{name}: credentials are sent over plain http to a non-local host; use https"
                    );
                    self.note(Severity::Warn, &msg);
                    self.toast(msg, Level::Warn);
                }
            }
            Err(e) => {
                self.client = None;
                self.conn = Conn::Failed(e);
            }
        }
        self.dirty = true;
    }

    pub fn refresh(&mut self) {
        let Some(client) = self.client.clone() else { return };
        if self.loading {
            return;
        }
        self.loading = true;
        self.last_attempt = Instant::now();
        let generation = self.generation;
        let tx = self.tx.clone();
        let want_plugins = self.plugins.is_empty();
        tokio::spawn(async move {
            let result = client.snapshot(want_plugins).await.map_err(|e| format!("{e:#}"));
            let _ = tx.send(Msg::Loaded { generation, result });
        });
        self.dirty = true;
    }

    fn run_action<F, Fut>(
        &mut self,
        label: String,
        select: Option<String>,
        watch: Vec<(String, Expect)>,
        f: F,
    ) where
        F: FnOnce(Client) -> Fut,
        Fut: Future<Output = Result<(), ActionError>> + Send + 'static,
    {
        let Some(client) = self.client.clone() else {
            self.error_overlay(format!("{label}: not connected to a cluster"));
            return;
        };
        self.toast(format!("{label}…"), Level::Info);
        self.note(Severity::Info, &format!("{label}: requested"));
        let fut = f(client);
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let result = fut.await;
            let _ = tx.send(Msg::ActionDone { label, select, result, watch });
        });
    }

    /// Judges every pending watch against the connectors of a *fresh* snapshot.
    fn evaluate_watches(&mut self) {
        let mut pending = Vec::new();
        for mut w in std::mem::take(&mut self.watches) {
            let c = self.connectors.iter().find(|c| c.name == w.name);
            let elapsed = w.started.elapsed();
            match activity::judge(&mut w, c, elapsed) {
                Some((sev, text)) => {
                    self.note(sev, &text);
                    self.toast(text, sev.into());
                }
                None => pending.push(w),
            }
        }
        self.watches = pending;
    }

    /// Gives up on watches that can no longer be verified (e.g. the cluster went away).
    fn expire_watches(&mut self) {
        let (expired, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut self.watches)
            .into_iter()
            .partition(|w| w.started.elapsed() >= activity::WATCH_TIMEOUT + Duration::from_secs(2));
        self.watches = keep;
        for w in expired {
            let text =
                format!("{}: could not verify the change — the cluster did not answer in time", w.name);
            self.note(Severity::Warn, &text);
            self.toast(text, Level::Warn);
        }
    }

    pub fn update(&mut self, msg: Msg) {
        match msg {
            Msg::Key(k) => self.on_key(k),
            Msg::Redraw => self.dirty = true,
            Msg::Frame => {
                if matches!(self.screen, Screen::Splash(_)) {
                    self.dirty = true; // animation frame; leaving is up to the user
                }
            }
            Msg::Quit => self.should_quit = true,
            Msg::Tick => {
                self.dirty = true; // keeps "refreshed Ns ago" and toasts current
                if let Some(t) = &self.toast {
                    let ttl = if t.level == Level::Info { 6 } else { 12 };
                    if t.at.elapsed() > Duration::from_secs(ttl) {
                        self.toast = None;
                    }
                }
                self.expire_watches();
                // While verifying a change, poll every second regardless of auto-refresh.
                let watching = !self.watches.is_empty();
                let due = if watching { Duration::from_secs(1) } else { self.interval };
                if (self.auto_refresh || watching) && !self.loading && self.last_attempt.elapsed() >= due {
                    self.refresh();
                }
            }
            Msg::Loaded { generation, result } => {
                if generation != self.generation {
                    return; // stale response from a previous cluster
                }
                self.loading = false;
                match result {
                    Ok(snap) => {
                        self.conn = Conn::Connected(snap.root.clone());
                        self.last_refresh = Some(Instant::now());
                        self.apply_snapshot(snap);
                    }
                    Err(e) => self.conn = Conn::Failed(e),
                }
                self.dirty = true;
            }
            Msg::Validated { id, result } => {
                if let Overlay::Preview(p) = &mut self.overlay
                    && p.id == id
                {
                    p.validation = match result {
                        Ok(errs) if errs.is_empty() => Validation::Ok,
                        Ok(errs) => Validation::Errors(errs),
                        Err(e) => Validation::Unavailable(e),
                    };
                }
                self.dirty = true;
            }
            Msg::ActionDone { label, select, result, watch } => {
                let failed = match result {
                    Ok(()) => {
                        let verifying = if watch.is_empty() { "" } else { " — verifying…" };
                        self.toast(format!("{label}: accepted by Kafka Connect{verifying}"), Level::Info);
                        self.note(Severity::Ok, &format!("{label}: accepted by Kafka Connect (HTTP 2xx)"));
                        if select.is_some() {
                            self.pending_select = select;
                        }
                        Vec::new()
                    }
                    Err(e) => {
                        self.toast(format!("{label} failed"), Level::Error);
                        for line in e.msg.lines() {
                            self.note(Severity::Error, &format!("{label} failed: {line}"));
                        }
                        self.error_overlay(format!("{label} failed:\n\n{}", e.msg));
                        // Everything not listed as failed (partial batch) is still verified.
                        e.failed.unwrap_or_else(|| watch.iter().map(|(n, _)| n.clone()).collect())
                    }
                };
                for (name, expect) in watch {
                    if !failed.contains(&name) {
                        self.watches.retain(|w| w.name != name);
                        self.watches.push(Watch::new(name, expect));
                    }
                }
                self.loading = false; // allow an immediate refresh
                self.refresh();
                self.dirty = true;
            }
        }
    }

    fn toast(&mut self, text: String, level: Level) {
        self.toast = Some(Toast { text, level, at: Instant::now() });
        self.dirty = true;
    }

    fn error_overlay(&mut self, text: String) {
        self.overlay = Overlay::Error(text);
        self.dirty = true;
    }

    // ---- list view --------------------------------------------------------

    fn apply_snapshot(&mut self, snap: Snapshot) {
        let keep = self.pending_select.take().or_else(|| self.selected_name());
        self.connectors = snap.connectors;
        if let Some(p) = snap.plugins {
            self.plugins = p;
        }
        let existing: BTreeSet<&str> = self.connectors.iter().map(|c| c.name.as_str()).collect();
        self.marked.retain(|n| existing.contains(n.as_str()));
        self.rebuild_view(keep);
        self.evaluate_watches();
    }

    pub fn rebuild_view(&mut self, keep: Option<String>) {
        let prev = self.selected_name();
        let q = self.query.to_lowercase();
        let mut idx: Vec<usize> = self
            .connectors
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                (q.is_empty() || c.name.to_lowercase().contains(&q))
                    && match self.type_filter {
                        TypeFilter::All => true,
                        TypeFilter::Source => c.kind == Kind::Source,
                        TypeFilter::Sink => c.kind == Kind::Sink,
                    }
                    && self.state_filter.matches(c)
                    && self.worker_filter.as_ref().is_none_or(|w| &c.worker_id == w)
                    && self.plugin_filter.as_ref().is_none_or(|p| &c.class == p)
            })
            .map(|(i, _)| i)
            .collect();
        let cs = &self.connectors;
        let key = self.sort;
        idx.sort_by(|&a, &b| {
            let (a, b) = (&cs[a], &cs[b]);
            match key {
                SortKey::Name => a.name.cmp(&b.name),
                SortKey::Type => a.kind.label().cmp(b.kind.label()).then_with(|| a.name.cmp(&b.name)),
                SortKey::State => a.state.cmp(&b.state).then_with(|| a.name.cmp(&b.name)),
                SortKey::Tasks => a
                    .running_tasks()
                    .cmp(&b.running_tasks())
                    .then_with(|| a.tasks.len().cmp(&b.tasks.len()))
                    .then_with(|| a.name.cmp(&b.name)),
                SortKey::Plugin => a.class.cmp(&b.class).then_with(|| a.name.cmp(&b.name)),
            }
        });
        if self.sort_desc {
            idx.reverse();
        }
        self.visible = idx;

        let want = keep.or_else(|| prev.clone());
        let pos =
            want.as_ref().and_then(|n| self.visible.iter().position(|&i| &self.connectors[i].name == n));
        let sel = match pos {
            Some(p) => Some(p),
            None if self.visible.is_empty() => None,
            None => Some(self.table.selected().unwrap_or(0).min(self.visible.len() - 1)),
        };
        self.table.select(sel);
        if self.selected_name() != prev {
            self.scroll = 0;
            self.task_sel = 0;
        }
        let ntasks = self.selected().map_or(0, |c| c.tasks.len());
        self.task_sel = self.task_sel.min(ntasks.saturating_sub(1));
        if self.selected().is_none() {
            self.focus = Focus::List;
        }
    }

    pub fn selected(&self) -> Option<&Connector> {
        let i = *self.visible.get(self.table.selected()?)?;
        self.connectors.get(i)
    }

    fn selected_name(&self) -> Option<String> {
        self.selected().map(|c| c.name.clone())
    }

    fn move_selection(&mut self, delta: isize) {
        if self.visible.is_empty() {
            return;
        }
        let cur = self.table.selected().unwrap_or(0) as isize;
        let next = (cur + delta).clamp(0, self.visible.len() as isize - 1) as usize;
        if Some(next) != self.table.selected() {
            self.table.select(Some(next));
            self.scroll = 0;
            self.task_sel = 0;
        }
    }

    // ---- input ------------------------------------------------------------

    fn on_key(&mut self, key: KeyEvent) {
        self.dirty = true;
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        if self.on_screen_key(key) {
            return;
        }
        if !matches!(self.overlay, Overlay::None) {
            let ov = std::mem::take(&mut self.overlay);
            self.overlay = self.on_overlay_key(ov, key);
            return;
        }
        if self.searching {
            self.on_search_key(key);
            return;
        }
        if self.trace_searching {
            self.on_trace_search_key(key);
            return;
        }
        self.on_main_key(key);
    }

    /// `true` while the Trace panel has the keyboard (so `/`, `n`, `N` and `y` act on the trace).
    fn in_trace(&self) -> bool {
        self.focus == Focus::Details && self.panel == Panel::Trace
    }

    fn on_trace_search_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.trace_query.clear();
                self.trace_searching = false;
            }
            KeyCode::Enter => self.trace_searching = false,
            KeyCode::Backspace => {
                self.trace_query.pop();
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => self.trace_query.push(c),
            _ => return,
        }
        self.jump_to_match(0, true);
    }

    /// Scrolls the Trace panel to the next (`dir > 0`) or previous (`dir < 0`) match; `dir == 0`
    /// keeps the current row if it already matches. Wraps around.
    fn jump_to_match(&mut self, dir: isize, include_current: bool) {
        let Some(c) = self.selected() else { return };
        let rows = trace_rows(c, self.trace_width.get() as usize);
        let hits: Vec<usize> = trace_matches(&rows, &self.trace_query);
        if hits.is_empty() {
            if !self.trace_query.is_empty() && !self.trace_searching {
                self.toast(format!("no match for \"{}\"", self.trace_query), Level::Info);
            }
            return;
        }
        let cur = self.scroll as usize;
        let target = if dir >= 0 {
            let from = if include_current { cur } else { cur + 1 };
            hits.iter().copied().find(|&h| h >= from).unwrap_or(hits[0])
        } else {
            hits.iter().rev().copied().find(|&h| h < cur).unwrap_or(hits[hits.len() - 1])
        };
        self.scroll = target.min(10_000) as u16;
    }

    /// Copies sanitised text (secrets masked, control characters removed) to the terminal clipboard.
    fn copy(&mut self, what: &str, text: String) {
        match crate::clip::copy(&crate::clip::sanitize(&text)) {
            Ok(()) => self.toast(
                format!("{what} sent to the clipboard (OSC 52; nothing happens if your terminal blocks it)"),
                Level::Info,
            ),
            Err(e) => self.toast(format!("cannot copy: {e}"), Level::Error),
        }
    }

    /// Text for `y` in the details view: the masked config on the Config panel, the masked traces
    /// on the Trace panel.
    fn copy_selection(&mut self) {
        let Some(c) = self.selected() else { return };
        let (what, text) = match self.panel {
            Panel::Config => (
                "config (secrets masked)",
                serde_json::to_string_pretty(&templates::masked(&c.config)).unwrap_or_default(),
            ),
            Panel::Trace => {
                let rows = trace_rows(c, usize::MAX);
                if rows.iter().all(|r| r.kind == RowKind::Empty) {
                    return self.toast("nothing to copy: no failures reported".into(), Level::Info);
                }
                ("trace (secrets masked)", rows.into_iter().map(|r| r.text).collect::<Vec<_>>().join("\n"))
            }
            _ => return self.toast("open the Config (3) or Trace (4) panel to copy".into(), Level::Info),
        };
        self.copy(what, text);
    }

    /// Distinct values of `key` over the current connectors, sorted.
    fn distinct(&self, key: impl Fn(&Connector) -> &str) -> Vec<String> {
        let set: BTreeSet<&str> = self.connectors.iter().map(&key).filter(|v| !v.is_empty()).collect();
        set.into_iter().map(str::to_string).collect()
    }

    /// Next value after `cur` in `values`, or `None` (= no filter) after the last one.
    fn cycle(cur: &Option<String>, values: &[String]) -> Option<String> {
        match cur.as_ref().and_then(|c| values.iter().position(|v| v == c)) {
            Some(i) => values.get(i + 1).cloned(),
            None => values.first().cloned(),
        }
    }

    /// Counts for the header: healthy, paused, failed, other.
    pub fn health(&self) -> Health {
        let mut h = Health::default();
        for c in &self.connectors {
            if c.is_failed() {
                h.failed += 1;
            } else if c.is_paused() {
                h.paused += 1;
            } else if c.state == "RUNNING" && c.tasks.iter().all(|t| t.state == "RUNNING") {
                h.healthy += 1;
            } else {
                h.other += 1;
            }
        }
        h
    }

    fn on_search_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.query.clear();
                self.searching = false;
            }
            KeyCode::Enter => self.searching = false,
            KeyCode::Backspace => {
                self.query.pop();
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => self.query.push(c),
            _ => return,
        }
        self.rebuild_view(None);
    }

    fn on_main_key(&mut self, key: KeyEvent) {
        let details = self.focus == Focus::Details;
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('?') | KeyCode::F(1) => self.overlay = Overlay::Help,
            KeyCode::Esc => {
                if details && !self.trace_query.is_empty() {
                    self.trace_query.clear();
                } else if details {
                    self.focus = Focus::List;
                } else if !self.marked.is_empty() {
                    self.marked.clear();
                } else if !self.query.is_empty()
                    || self.type_filter != TypeFilter::All
                    || self.state_filter != StateFilter::All
                    || self.worker_filter.is_some()
                    || self.plugin_filter.is_some()
                {
                    self.query.clear();
                    self.type_filter = TypeFilter::All;
                    self.state_filter = StateFilter::All;
                    self.worker_filter = None;
                    self.plugin_filter = None;
                    self.rebuild_view(None);
                }
            }
            KeyCode::Enter => {
                if self.selected().is_some() {
                    self.focus = Focus::Details;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => self.nav(1),
            KeyCode::Up | KeyCode::Char('k') => self.nav(-1),
            KeyCode::PageDown => self.nav(10),
            KeyCode::PageUp => self.nav(-10),
            KeyCode::Home | KeyCode::Char('g') => self.nav(-100_000),
            KeyCode::End | KeyCode::Char('G') => self.nav(100_000),
            KeyCode::Tab | KeyCode::Char(']') => self.cycle_panel(1),
            KeyCode::BackTab | KeyCode::Char('[') => self.cycle_panel(-1),
            KeyCode::Char(c @ '1'..='4') => self.set_panel(Panel::ALL[c as usize - '1' as usize]),
            KeyCode::Char('/') if self.in_trace() => {
                self.trace_searching = true;
                self.jump_to_match(0, true);
            }
            KeyCode::Char('/') => {
                self.focus = Focus::List;
                self.searching = true;
            }
            KeyCode::Char('n') if self.in_trace() && !self.trace_query.is_empty() => {
                self.jump_to_match(1, false)
            }
            KeyCode::Char('N') if self.in_trace() && !self.trace_query.is_empty() => {
                self.jump_to_match(-1, false)
            }
            KeyCode::Char('y') if details => self.copy_selection(),
            KeyCode::Char('w') => {
                let workers = self.distinct(|c| c.worker_id.as_str());
                self.worker_filter = Self::cycle(&self.worker_filter, &workers);
                self.rebuild_view(None);
            }
            KeyCode::Char('x') => {
                let classes = self.distinct(|c| c.class.as_str());
                self.plugin_filter = Self::cycle(&self.plugin_filter, &classes);
                self.rebuild_view(None);
            }
            KeyCode::Char('r') => {
                self.loading = false;
                self.refresh();
                self.toast("refreshing…".into(), Level::Info);
            }
            KeyCode::Char('a') => {
                self.auto_refresh = !self.auto_refresh;
                let s = if self.auto_refresh { "on" } else { "off" };
                self.toast(format!("auto refresh {s}"), Level::Info);
            }
            KeyCode::Char('f') => {
                self.type_filter = self.type_filter.next();
                self.rebuild_view(None);
            }
            KeyCode::Char('s') => {
                self.state_filter = self.state_filter.next();
                self.rebuild_view(None);
            }
            KeyCode::Char('o') => {
                self.sort = self.sort.next();
                self.rebuild_view(None);
            }
            KeyCode::Char('O') => {
                self.sort_desc = !self.sort_desc;
                self.rebuild_view(None);
            }
            KeyCode::Char(' ') => {
                if let Some(name) = self.selected_name() {
                    if !self.marked.remove(&name) {
                        self.marked.insert(name);
                    }
                    self.nav(1);
                }
            }
            KeyCode::Char('p') => self.batch(Op::Pause, self.targets()),
            KeyCode::Char('u') => self.batch(Op::Resume, self.targets()),
            KeyCode::Char('R') => self.restart_selected(),
            KeyCode::Char('F') => {
                let names = if self.marked.is_empty() {
                    self.connectors.iter().filter(|c| c.is_failed()).map(|c| c.name.clone()).collect()
                } else {
                    self.targets()
                };
                if names.is_empty() {
                    self.toast("no failed connectors".into(), Level::Info);
                } else {
                    self.batch(Op::RestartFailed, names);
                }
            }
            KeyCode::Char('d') => {
                let names = self.targets();
                if !names.is_empty() {
                    let what = if names.len() == 1 {
                        format!("\"{}\"", names[0])
                    } else {
                        format!("{} connectors:\n  {}", names.len(), names.join("\n  "))
                    };
                    self.overlay = Overlay::Confirm(Confirm {
                        title: "Delete".into(),
                        body: format!(
                            "Permanently delete {what}\nfrom cluster \"{}\"?\nTasks will be stopped and configs removed.",
                            self.cluster
                        ),
                        delete: names,
                    });
                }
            }
            KeyCode::Char('e') => self.request_edit(),
            KeyCode::Char('P') => self.overlay = Overlay::Plugins(0),
            KeyCode::Char('L') => self.overlay = Overlay::Activity(0),
            KeyCode::Char('n') => {
                self.overlay = Overlay::Import(ImportState { input: String::new(), error: None })
            }
            KeyCode::Char('t') => self.overlay = Overlay::Templates(0),
            KeyCode::Char('c') => {
                let sel = self.config.clusters.keys().position(|k| *k == self.cluster).unwrap_or(0);
                self.overlay = Overlay::Clusters(sel);
            }
            _ => {}
        }
    }

    fn restart_selected(&mut self) {
        let Some(c) = self.selected() else { return };
        let name = c.name.clone();
        if self.focus == Focus::Details && self.panel == Panel::Tasks {
            if let Some(t) = c.tasks.get(self.task_sel) {
                let id = t.id;
                let watch = vec![(name.clone(), Expect::Restarted)];
                self.run_action(format!("restart {name} task {id}"), None, watch, move |c| async move {
                    c.restart_task(&name, id).await.map_err(ActionError::from)
                });
            }
        } else {
            self.batch(Op::Restart, self.targets());
        }
    }

    /// Marked connectors if any, otherwise the selected one.
    fn targets(&self) -> Vec<String> {
        if self.marked.is_empty() {
            self.selected_name().into_iter().collect()
        } else {
            self.marked.iter().cloned().collect()
        }
    }

    fn batch(&mut self, op: Op, names: Vec<String>) {
        if names.is_empty() {
            return;
        }
        let label = match names.as_slice() {
            [one] => format!("{} {one}", op.verb()),
            many => format!("{} {} connectors", op.verb(), many.len()),
        };
        self.marked.clear();
        let watch = names.iter().map(|n| (n.clone(), op.expect())).collect();
        self.run_action(label, None, watch, move |c| apply(c, op, names));
    }

    // ---- edit in $EDITOR + validation --------------------------------------

    /// Writes the selected connector's config to a temp file and asks the main
    /// loop to suspend the TUI and open the user's editor on it.
    fn request_edit(&mut self) {
        let Some(c) = self.selected() else { return };
        let (name, config) = (c.name.clone(), c.config.clone());
        let safe: String = name.chars().filter(|ch| ch.is_ascii_alphanumeric()).take(32).collect();
        let text = serde_json::to_string_pretty(&config).unwrap_or_default();
        match write_private_temp(&format!("tuizium-{safe}"), &text) {
            Ok(path) => self.editor_request = Some(EditorRequest { name, path, original: config }),
            Err(e) => self.error_overlay(format!("cannot create a temporary file for the editor: {e}")),
        }
    }

    /// Called by the main loop after the editor exited.
    pub fn editor_finished(&mut self, req: EditorRequest, outcome: Result<(), String>) {
        self.dirty = true;
        let text = read_capped(&req.path, MAX_JSON_BYTES);
        let _ = std::fs::remove_file(&req.path);
        if let Err(e) = outcome {
            return self
                .error_overlay(format!("could not run the editor:\n\n{e}\n\nSet $VISUAL or $EDITOR."));
        }
        let parsed = text
            .map_err(|e| format!("cannot read edited file: {e}"))
            .and_then(|t| {
                serde_json::from_str::<Value>(&t)
                    .map_err(|e| format!("invalid JSON, nothing was changed: {e}"))
            })
            .and_then(|v| api::normalize_create(v).map_err(|e| e.to_string()));
        let (name, mut config) = match parsed {
            Ok(p) => p,
            Err(e) => return self.error_overlay(e),
        };
        if name != req.name {
            return self.error_overlay(format!(
                "the connector name cannot be changed (\"{}\" → \"{name}\"); Kafka Connect has no rename.",
                req.name
            ));
        }
        if config.values().any(|v| matches!(v.as_str(), Some("[hidden]" | "********"))) {
            return self.error_overlay(
                "The config contains masked values (\"[hidden]\" or \"********\"): saving would \
                 overwrite the real secret with that literal text.\n\nReplace them with the real value or a config-provider \
                 reference such as ${file:/path:key} / ${env:VAR}, then edit again."
                    .into(),
            );
        }
        let mut original = req.original;
        original.remove("name");
        config.remove("name");
        if config == original {
            return self.toast("no changes".into(), Level::Info);
        }
        let is_source = self.connectors.iter().any(|c| c.name == name && c.kind == Kind::Source);
        let mut overlay = self.open_preview(name, config, PreviewSource::Edit, true);
        if let Overlay::Preview(p) = &mut overlay {
            p.original = Some(original);
            p.is_source = is_source;
        }
        self.overlay = overlay;
    }

    fn open_preview(
        &mut self,
        name: String,
        config: Map<String, Value>,
        source: PreviewSource,
        update: bool,
    ) -> Overlay {
        self.preview_seq += 1;
        let id = self.preview_seq;
        let validation = match self.client.clone() {
            Some(client) => {
                let (tx, n, cfg) = (self.tx.clone(), name.clone(), config.clone());
                tokio::spawn(async move {
                    let result = client.validate(&n, &cfg).await.map_err(|e| format!("{e:#}"));
                    let _ = tx.send(Msg::Validated { id, result });
                });
                Validation::Pending
            }
            None => Validation::Unavailable("not connected".into()),
        };
        Overlay::Preview(Box::new(Preview {
            id,
            name,
            config,
            scroll: 0,
            source,
            update,
            validation,
            original: None,
            is_source: false,
        }))
    }

    fn nav(&mut self, delta: isize) {
        if self.focus == Focus::List {
            self.move_selection(delta);
            return;
        }
        if self.panel == Panel::Tasks {
            let n = self.selected().map_or(0, |c| c.tasks.len());
            if n > 0 {
                self.task_sel = (self.task_sel as isize + delta).clamp(0, n as isize - 1) as usize;
            }
        } else {
            self.scroll = (self.scroll as isize + delta).clamp(0, 10_000) as u16;
        }
    }

    fn set_panel(&mut self, p: Panel) {
        if self.panel != p {
            self.panel = p;
            self.scroll = 0;
        }
    }

    fn cycle_panel(&mut self, d: isize) {
        let n = Panel::ALL.len() as isize;
        let i = (self.panel.index() as isize + d).rem_euclid(n) as usize;
        self.set_panel(Panel::ALL[i]);
    }

    // ---- overlays ---------------------------------------------------------

    fn on_overlay_key(&mut self, ov: Overlay, key: KeyEvent) -> Overlay {
        let code = key.code;
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match ov {
            Overlay::None => Overlay::None,
            Overlay::Help => match code {
                KeyCode::Esc | KeyCode::Char('q' | '?') | KeyCode::Enter => Overlay::None,
                _ => Overlay::Help,
            },
            Overlay::Error(e) => match code {
                KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => Overlay::None,
                KeyCode::Char('y') => {
                    self.copy("error text", e.clone());
                    Overlay::Error(e)
                }
                _ => Overlay::Error(e),
            },
            Overlay::Confirm(c) => match code {
                KeyCode::Char('y' | 'Y') => {
                    self.batch(Op::Delete, c.delete);
                    Overlay::None
                }
                KeyCode::Esc | KeyCode::Char('n' | 'N' | 'q') => Overlay::None,
                _ => Overlay::Confirm(c),
            },
            Overlay::Clusters(sel) => self.on_clusters_key(sel, key),
            Overlay::AddCluster(mut a) => match code {
                KeyCode::Esc => Overlay::Clusters(0),
                KeyCode::Enter => match self.add_cluster(&a.input) {
                    Ok(()) => Overlay::Clusters(0),
                    Err(e) => {
                        a.error = Some(e);
                        Overlay::AddCluster(a)
                    }
                },
                KeyCode::Backspace => {
                    a.input.pop();
                    Overlay::AddCluster(a)
                }
                KeyCode::Char(c) if !ctrl => {
                    a.input.push(c);
                    Overlay::AddCluster(a)
                }
                _ => Overlay::AddCluster(a),
            },
            Overlay::Templates(sel) => match code {
                KeyCode::Esc | KeyCode::Char('q') => Overlay::None,
                KeyCode::Down | KeyCode::Char('j') => Overlay::Templates((sel + 1).min(TEMPLATES.len() - 1)),
                KeyCode::Up | KeyCode::Char('k') => Overlay::Templates(sel.saturating_sub(1)),
                KeyCode::Enter => {
                    let t = &TEMPLATES[sel];
                    if templates::availability(t, &self.plugins) == templates::Availability::Missing {
                        self.toast(
                            format!(
                                "\"{}\" needs {}, which is not installed on this cluster — press P to list installed plugins",
                                t.title,
                                t.class()
                            ),
                            Level::Error,
                        );
                        Overlay::Templates(sel)
                    } else {
                        Overlay::Form(Box::new(FormState::new(sel)))
                    }
                }
                _ => Overlay::Templates(sel),
            },
            Overlay::Form(mut f) => {
                let n = f.fields.len();
                match code {
                    KeyCode::Esc => return Overlay::Templates(f.template),
                    KeyCode::Down | KeyCode::Tab => f.selected = (f.selected + 1) % n,
                    KeyCode::Up | KeyCode::BackTab => f.selected = (f.selected + n - 1) % n,
                    KeyCode::Backspace => {
                        f.fields[f.selected].value.pop();
                    }
                    KeyCode::Char('u') if ctrl => f.fields[f.selected].value.clear(),
                    KeyCode::Char(c) if !ctrl => f.fields[f.selected].value.push(c),
                    KeyCode::Enter => match f.build() {
                        Ok((name, config)) => {
                            return self.open_preview(name, config, PreviewSource::Form(*f), false);
                        }
                        Err(e) => f.error = Some(e),
                    },
                    _ => {}
                }
                Overlay::Form(f)
            }
            Overlay::Import(mut i) => match code {
                KeyCode::Esc => Overlay::None,
                KeyCode::Enter => match load_json(&i.input) {
                    Ok((name, config)) => {
                        self.open_preview(name, config, PreviewSource::Import(i.input), false)
                    }
                    Err(e) => {
                        i.error = Some(e);
                        Overlay::Import(i)
                    }
                },
                KeyCode::Backspace => {
                    i.input.pop();
                    i.error = None;
                    Overlay::Import(i)
                }
                KeyCode::Char('u') if ctrl => {
                    i.input.clear();
                    Overlay::Import(i)
                }
                KeyCode::Char(c) if !ctrl => {
                    i.input.push(c);
                    i.error = None;
                    Overlay::Import(i)
                }
                _ => Overlay::Import(i),
            },
            Overlay::Preview(mut p) => match code {
                KeyCode::Esc => match p.source {
                    PreviewSource::Form(f) => Overlay::Form(Box::new(f)),
                    PreviewSource::Import(input) => Overlay::Import(ImportState { input, error: None }),
                    PreviewSource::Edit => Overlay::None,
                },
                KeyCode::Down | KeyCode::Char('j') => {
                    p.scroll = p.scroll.saturating_add(1);
                    Overlay::Preview(p)
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    p.scroll = p.scroll.saturating_sub(1);
                    Overlay::Preview(p)
                }
                KeyCode::Enter | KeyCode::Char('y') => match p.validation {
                    Validation::Pending => {
                        self.toast("still validating…".into(), Level::Info);
                        Overlay::Preview(p)
                    }
                    Validation::Errors(_) => {
                        self.toast("fix the validation errors first (Esc to go back)".into(), Level::Error);
                        Overlay::Preview(p)
                    }
                    Validation::Ok | Validation::Unavailable(_) => {
                        let Preview { name, mut config, update, .. } = *p;
                        let n = name.clone();
                        // A paused connector legitimately stays paused after a config update.
                        let paused = self.connectors.iter().any(|c| c.name == name && c.is_paused());
                        let expect = if update { Expect::Running } else { Expect::Created };
                        let watch = if paused { vec![] } else { vec![(name.clone(), expect)] };
                        if update {
                            config.insert("name".into(), Value::String(n.clone()));
                            self.run_action(
                                format!("update {name}"),
                                Some(name),
                                watch,
                                move |c| async move {
                                    c.update_config(&n, &config).await.map_err(ActionError::from)
                                },
                            );
                        } else {
                            self.run_action(format!("create {name}"), Some(name), watch, move |c| async move {
                                c.create(&n, &config).await.map_err(ActionError::from)
                            });
                        }
                        Overlay::None
                    }
                },
                _ => Overlay::Preview(p),
            },
            Overlay::Plugins(sel) => match code {
                KeyCode::Esc | KeyCode::Char('q' | 'P') => Overlay::None,
                KeyCode::Down | KeyCode::Char('j') => {
                    Overlay::Plugins((sel + 1).min(self.plugins.len().saturating_sub(1)))
                }
                KeyCode::Up | KeyCode::Char('k') => Overlay::Plugins(sel.saturating_sub(1)),
                _ => Overlay::Plugins(sel),
            },
            Overlay::Activity(off) => {
                let max = self.log.entries.len().saturating_sub(1);
                match code {
                    KeyCode::Esc | KeyCode::Char('q' | 'L') => Overlay::None,
                    KeyCode::Down | KeyCode::Char('j') => Overlay::Activity((off + 1).min(max)),
                    KeyCode::Up | KeyCode::Char('k') => Overlay::Activity(off.saturating_sub(1)),
                    KeyCode::PageDown => Overlay::Activity((off + 10).min(max)),
                    KeyCode::PageUp => Overlay::Activity(off.saturating_sub(10)),
                    KeyCode::Home | KeyCode::Char('g') => Overlay::Activity(0),
                    KeyCode::End | KeyCode::Char('G') => Overlay::Activity(max),
                    _ => Overlay::Activity(off.min(max)),
                }
            }
        }
    }

    fn on_clusters_key(&mut self, sel: usize, key: KeyEvent) -> Overlay {
        let names: Vec<String> = self.config.clusters.keys().cloned().collect();
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => Overlay::None,
            KeyCode::Down | KeyCode::Char('j') => {
                Overlay::Clusters((sel + 1).min(names.len().saturating_sub(1)))
            }
            KeyCode::Up | KeyCode::Char('k') => Overlay::Clusters(sel.saturating_sub(1)),
            KeyCode::Char('a') => Overlay::AddCluster(AddCluster { input: String::new(), error: None }),
            KeyCode::Char(c @ '1'..='9') if (c as usize - '1' as usize) < names.len() => {
                let name = names[c as usize - '1' as usize].clone();
                self.use_cluster(&name);
                Overlay::None
            }
            KeyCode::Enter => {
                if let Some(name) = names.get(sel).cloned() {
                    self.use_cluster(&name);
                }
                Overlay::None
            }
            _ => Overlay::Clusters(sel),
        }
    }

    /// Input format: `<name> <url> [username] [password_env]`.
    fn add_cluster(&mut self, input: &str) -> Result<(), String> {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.len() < 2 {
            return Err("usage: <name> <url> [username password_env]".into());
        }
        if parts.len() > 4 {
            return Err("too many fields; usage: <name> <url> [username password_env]".into());
        }
        if self.config.clusters.contains_key(parts[0]) {
            return Err(format!(
                "a profile named {:?} already exists; edit the config file to change it",
                parts[0]
            ));
        }
        let cluster = Cluster {
            url: parts[1].to_string(),
            username: parts.get(2).map(|s| s.to_string()),
            password_env: parts.get(3).map(|s| s.to_string()),
            ..Default::default()
        };
        cluster.client().map(|_| ()).or_else(|e| {
            // A missing env var shouldn't stop saving the profile.
            let msg = format!("{e:#}");
            if msg.contains("environment variable") { Ok(()) } else { Err(msg) }
        })?;
        self.config.clusters.insert(parts[0].to_string(), cluster);
        if let Some(path) = &self.config_path {
            config::save(path, &self.config).map_err(|e| format!("saved in memory only: {e:#}"))?;
        }
        Ok(())
    }
}

/// Connector counts shown in the header.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Health {
    pub healthy: usize,
    pub paused: usize,
    pub failed: usize,
    pub other: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowKind {
    Heading,
    Line,
    /// "No failures reported" placeholder; never copied.
    Empty,
}

/// One screen row of the Trace panel (already masked, sanitised and wrapped).
#[derive(Clone, Debug)]
pub struct TraceRow {
    pub text: String,
    pub kind: RowKind,
}

/// The Trace panel as rows of at most `width` characters. Wrapping is done here (not by the
/// widget) so that a row index is exactly a scroll offset, which search relies on.
pub fn trace_rows(c: &Connector, width: usize) -> Vec<TraceRow> {
    let width = width.max(1);
    let mut rows = Vec::new();
    let mut section = |title: String, trace: &str| {
        rows.push(TraceRow { text: title, kind: RowKind::Heading });
        for line in templates::mask_text(trace).lines() {
            let line: String = line
                .chars()
                .map(|ch| if ch == '\t' { ' ' } else { ch })
                .filter(|ch| !ch.is_control())
                .collect();
            let chars: Vec<char> = line.chars().collect();
            if chars.is_empty() {
                rows.push(TraceRow { text: String::new(), kind: RowKind::Line });
            }
            for chunk in chars.chunks(width) {
                rows.push(TraceRow { text: chunk.iter().collect(), kind: RowKind::Line });
            }
        }
        rows.push(TraceRow { text: String::new(), kind: RowKind::Line });
    };
    if let Some(t) = &c.trace {
        section("connector".into(), t);
    }
    for t in &c.tasks {
        if let Some(tr) = &t.trace {
            section(format!("task {}", t.id), tr);
        }
    }
    if rows.is_empty() {
        rows.push(TraceRow { text: "No failures reported.".into(), kind: RowKind::Empty });
    }
    rows
}

/// Indices of rows containing `query` (case-insensitive); none for an empty query.
pub fn trace_matches(rows: &[TraceRow], query: &str) -> Vec<usize> {
    if query.is_empty() {
        return Vec::new();
    }
    let q = query.to_lowercase();
    rows.iter()
        .enumerate()
        .filter(|(_, r)| r.kind != RowKind::Empty && r.text.to_lowercase().contains(&q))
        .map(|(i, _)| i)
        .collect()
}

/// Reads a connector definition from a file path, or inline JSON if the input
/// starts with `{`.
/// On Windows, characters typed with AltGr (`\`, `{`, `}`, `[` on many layouts)
/// arrive as Ctrl+Alt+char. Treat them as plain characters so text fields accept them.
pub fn normalize_altgr(mut key: KeyEvent) -> KeyEvent {
    let altgr = KeyModifiers::CONTROL | KeyModifiers::ALT;
    if matches!(key.code, KeyCode::Char(_)) && key.modifiers.contains(altgr) {
        key.modifiers.remove(altgr);
    }
    key
}

pub fn load_json(input: &str) -> Result<(String, Map<String, Value>), String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("enter a path to a .json file (or paste inline JSON)".into());
    }
    let text = if input.starts_with('{') {
        input.to_string()
    } else {
        let path = match input.strip_prefix("~/").and_then(|rest| dirs::home_dir().map(|h| h.join(rest))) {
            Some(p) => p,
            None => PathBuf::from(input.trim_matches('"')),
        };
        read_capped(&path, MAX_JSON_BYTES).map_err(|e| format!("cannot read {}: {e}", path.display()))?
    };
    if text.len() as u64 > MAX_JSON_BYTES {
        return Err("input is too large".into());
    }
    let v: Value = serde_json::from_str(&text).map_err(|e| format!("invalid JSON: {e}"))?;
    api::normalize_create(v).map_err(|e| e.to_string())
}

/// Reads at most `max` bytes of text, so a huge file or an endless special file
/// (e.g. `/dev/zero`) cannot exhaust memory.
fn read_capped(path: &std::path::Path, max: u64) -> std::io::Result<String> {
    use std::io::Read;
    let mut buf = Vec::new();
    std::fs::File::open(path)?.take(max + 1).read_to_end(&mut buf)?;
    if buf.len() as u64 > max {
        return Err(std::io::Error::other(format!("file is larger than {} MiB", max >> 20)));
    }
    String::from_utf8(buf).map_err(|_| std::io::Error::other("file is not valid UTF-8"))
}

/// Creates a new file (`O_EXCL`: never follows a pre-planted symlink or reuses a
/// file), readable by the current user only on Unix, and writes `text` into it.
fn write_private_temp(prefix: &str, text: &str) -> std::io::Result<PathBuf> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir();
    let mut last = None;
    for _ in 0..8 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let path = dir.join(format!(
            "{prefix}-{}-{nanos:x}-{}.json",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        match opts.open(&path) {
            Ok(mut f) => {
                if let Err(e) = f.write_all(text.as_bytes()) {
                    drop(f);
                    let _ = std::fs::remove_file(&path);
                    return Err(e);
                }
                return Ok(path);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other("no free temporary file name")))
}

pub fn preview_json(name: &str, config: &Map<String, Value>) -> String {
    let v = serde_json::json!({ "name": name, "config": templates::masked(config) });
    serde_json::to_string_pretty(&v).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{Snapshot, Task};
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn altgr_characters_become_plain_characters() {
        let altgr = KeyEvent::new(KeyCode::Char('\\'), KeyModifiers::CONTROL | KeyModifiers::ALT);
        assert_eq!(normalize_altgr(altgr).modifiers, KeyModifiers::NONE);
        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert_eq!(normalize_altgr(ctrl_c).modifiers, KeyModifiers::CONTROL);
    }

    fn conn(name: &str, kind: Kind, state: &str, tasks: &[&str]) -> Connector {
        Connector {
            name: name.into(),
            kind,
            state: state.into(),
            worker_id: "w:8083".into(),
            class: format!("x.{name}Connector"),
            config: Map::new(),
            tasks: tasks
                .iter()
                .enumerate()
                .map(|(i, s)| Task { id: i as i64, state: (*s).into(), worker_id: "w".into(), trace: None })
                .collect(),
            trace: None,
        }
    }

    fn app() -> (App, tokio::sync::mpsc::UnboundedReceiver<Msg>) {
        let (tx, rx) = unbounded_channel();
        let mut cfg = Config::with_local_default();
        cfg.clusters.get_mut("local").unwrap().url = "http://127.0.0.1:1".into();
        let mut app = App::new(cfg, None, "local".into(), tx);
        let generation = app.generation;
        app.update(Msg::Loaded {
            generation,
            result: Ok(Snapshot {
                root: None,
                plugins: None,
                connectors: vec![
                    conn("alpha", Kind::Source, "RUNNING", &["RUNNING"]),
                    conn("beta", Kind::Sink, "RUNNING", &["RUNNING", "FAILED"]),
                    conn("gamma", Kind::Sink, "PAUSED", &["PAUSED"]),
                ],
            }),
        });
        (app, rx)
    }

    fn names(app: &App) -> Vec<&str> {
        app.visible.iter().map(|&i| app.connectors[i].name.as_str()).collect()
    }

    #[tokio::test]
    async fn filters_and_search() {
        let (mut app, _rx) = app();
        assert_eq!(names(&app), ["alpha", "beta", "gamma"]);
        app.type_filter = TypeFilter::Sink;
        app.rebuild_view(None);
        assert_eq!(names(&app), ["beta", "gamma"]);
        app.state_filter = StateFilter::Failed;
        app.rebuild_view(None);
        assert_eq!(names(&app), ["beta"]);
        app.type_filter = TypeFilter::All;
        app.state_filter = StateFilter::All;
        app.query = "AMM".into();
        app.rebuild_view(None);
        assert_eq!(names(&app), ["gamma"]);
    }

    #[tokio::test]
    async fn sorting_and_selection_follows_name() {
        let (mut app, _rx) = app();
        app.table.select(Some(1));
        app.sort_desc = true;
        app.rebuild_view(None);
        assert_eq!(names(&app), ["gamma", "beta", "alpha"]);
        assert_eq!(app.selected().unwrap().name, "beta");
    }

    #[tokio::test]
    async fn stale_responses_are_ignored() {
        let (mut app, _rx) = app();
        let before = app.connectors.len();
        app.update(Msg::Loaded { generation: 0, result: Ok(Snapshot::default()) });
        assert_eq!(app.connectors.len(), before);
    }

    #[tokio::test]
    async fn delete_requires_confirmation() {
        let (mut app, _rx) = app();
        app.on_key(KeyEvent::from(KeyCode::Char('d')));
        assert!(matches!(app.overlay, Overlay::Confirm(_)));
        app.on_key(KeyEvent::from(KeyCode::Esc));
        assert!(matches!(app.overlay, Overlay::None));
    }

    #[tokio::test]
    async fn connection_failure_is_shown_not_fatal() {
        let (mut app, _rx) = app();
        let generation = app.generation;
        app.update(Msg::Loaded { generation, result: Err("boom".into()) });
        assert!(matches!(app.conn, Conn::Failed(_)));
        assert_eq!(app.connectors.len(), 3); // stale data stays visible
    }

    #[tokio::test]
    async fn space_marks_and_esc_clears() {
        let (mut app, _rx) = app();
        app.on_key(KeyEvent::from(KeyCode::Char(' ')));
        app.on_key(KeyEvent::from(KeyCode::Char(' ')));
        assert_eq!(app.targets(), ["alpha", "beta"]);
        assert_eq!(app.selected().unwrap().name, "gamma");
        app.on_key(KeyEvent::from(KeyCode::Esc));
        assert_eq!(app.targets(), ["gamma"]);
    }

    fn edit_req(app: &App, original: &[(&str, &str)]) -> EditorRequest {
        let path = std::env::temp_dir().join(format!(
            "tuizium-test-{}-{}.json",
            std::process::id(),
            app.preview_seq
        ));
        EditorRequest {
            name: "alpha".into(),
            path,
            original: original.iter().map(|(k, v)| (k.to_string(), Value::String(v.to_string()))).collect(),
        }
    }

    #[tokio::test]
    async fn edit_flow_guards_and_preview() {
        let (mut app, _rx) = app();
        let cases = [
            (r#"{"name":"alpha","a":"2"}"#, "preview"),
            (r#"{"name":"alpha","a":"1"}"#, "none"), // unchanged
            (r#"{"name":"renamed","a":"2"}"#, "error"),
            (r#"{"name":"alpha","password":"[hidden]"}"#, "error"),
            (r#"{"name":"alpha","password":"********"}"#, "error"), // our own display mask must never be saved
            ("{oops", "error"),
        ];
        for (text, want) in cases {
            app.overlay = Overlay::None;
            let req = edit_req(&app, &[("a", "1")]);
            std::fs::write(&req.path, text).unwrap();
            app.editor_finished(req, Ok(()));
            let got = match &app.overlay {
                Overlay::Preview(p) => {
                    assert!(
                        p.update && matches!(p.validation, Validation::Pending | Validation::Unavailable(_))
                    );
                    "preview"
                }
                Overlay::Error(_) => "error",
                Overlay::None => "none",
                _ => "other",
            };
            assert_eq!(got, want, "input {text}");
        }
    }

    fn press(app: &mut App, c: char) {
        app.on_key(KeyEvent::from(KeyCode::Char(c)));
    }

    #[tokio::test]
    async fn worker_and_plugin_filters_cycle_and_esc_clears_them() {
        let (mut app, _rx) = app();
        app.connectors[1].worker_id = "w2:8083".into();
        app.rebuild_view(None);
        press(&mut app, 'w');
        assert_eq!(app.worker_filter.as_deref(), Some("w2:8083"));
        assert_eq!(names(&app), ["beta"]);
        press(&mut app, 'w');
        assert_eq!(app.worker_filter.as_deref(), Some("w:8083"));
        assert_eq!(names(&app), ["alpha", "gamma"]);
        press(&mut app, 'w');
        assert_eq!(app.worker_filter, None);
        assert_eq!(names(&app), ["alpha", "beta", "gamma"]);

        press(&mut app, 'x');
        assert_eq!(app.plugin_filter.as_deref(), Some("x.alphaConnector"));
        assert_eq!(names(&app), ["alpha"]);
        press(&mut app, 'w'); // first worker is w2 (beta's), and alpha is not on it
        assert!(names(&app).is_empty(), "filters combine with AND");
        app.on_key(KeyEvent::from(KeyCode::Esc));
        assert!(app.plugin_filter.is_none() && app.worker_filter.is_none());
        assert_eq!(names(&app), ["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn header_counts_come_from_the_snapshot() {
        let (app, _rx) = app();
        assert_eq!(app.health(), Health { healthy: 1, paused: 1, failed: 1, other: 0 });
    }

    #[tokio::test]
    async fn trace_search_jumps_between_matches_and_n_stays_new_connector_otherwise() {
        let (mut app, _rx) = app();
        app.connectors[1].tasks[1].trace = Some("Boom: one\n at A\n at B\nCaused by: two\n at A".into());
        app.rebuild_view(Some("beta".into()));
        app.focus = Focus::Details;
        app.panel = Panel::Trace;

        press(&mut app, '/');
        assert!(app.trace_searching);
        for ch in "at a".chars() {
            press(&mut app, ch);
        }
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(!app.trace_searching && app.trace_query == "at a");
        assert_eq!(app.scroll, 2, "first match: row 0 is the heading, row 1 'Boom'");
        press(&mut app, 'n');
        assert_eq!(app.scroll, 5);
        press(&mut app, 'n');
        assert_eq!(app.scroll, 2, "wraps around");
        press(&mut app, 'N');
        assert_eq!(app.scroll, 5, "wraps backwards");

        app.on_key(KeyEvent::from(KeyCode::Esc));
        assert!(
            app.trace_query.is_empty() && app.focus == Focus::Details,
            "first Esc only clears the search"
        );
        press(&mut app, 'n');
        assert!(matches!(app.overlay, Overlay::Import(_)), "with no search, n is still 'new connector'");
    }

    #[test]
    fn trace_rows_wrap_mask_and_strip_control_characters() {
        let mut c = conn("c", Kind::Sink, "FAILED", &[]);
        c.trace = Some("password=hunter2\u{1b}[31m x\n\tabcdefghij".into());
        let rows = trace_rows(&c, 4);
        let all: String = rows.iter().map(|r| r.text.as_str()).collect();
        assert!(!all.contains("hunter2") && !all.contains('\u{1b}'), "{all}");
        assert!(rows.iter().filter(|r| r.kind == RowKind::Line).all(|r| r.text.chars().count() <= 4));
        assert_eq!(rows[0].kind, RowKind::Heading);
        let healthy = trace_rows(&conn("ok", Kind::Sink, "RUNNING", &["RUNNING"]), 40);
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].kind, RowKind::Empty);
        assert!(trace_matches(&healthy, "failures").is_empty(), "the placeholder is not a match");
    }

    #[tokio::test]
    async fn update_preview_carries_the_running_config_for_the_diff() {
        let (mut app, _rx) = app();
        app.connectors[1].config.insert("topics".into(), Value::String("t1".into()));
        app.rebuild_view(Some("beta".into()));
        let mut req = edit_req(&app, &[("topics", "t1")]);
        req.name = "beta".into();
        std::fs::write(&req.path, r#"{"name":"beta","topics":"t1,t2"}"#).unwrap();
        app.editor_finished(req, Ok(()));
        let Overlay::Preview(p) = &app.overlay else { panic!("expected a preview") };
        assert_eq!(p.original.as_ref().and_then(|o| o.get("topics")).and_then(Value::as_str), Some("t1"));
        assert!(!p.is_source, "beta is a sink");
        let changes = crate::diff::diff(p.original.as_ref().unwrap(), &p.config);
        assert_eq!(crate::diff::replay_warnings(&changes, p.is_source).len(), 1);
    }

    #[tokio::test]
    async fn editor_file_holds_the_real_secret_not_the_display_mask() {
        let (mut app, _rx) = app();
        app.connectors[0].config.insert("database.password".into(), Value::String("real-pw".into()));
        app.table.select(Some(0));
        app.request_edit();
        let req = app.editor_request.take().expect("editor requested");
        let text = std::fs::read_to_string(&req.path).unwrap();
        let _ = std::fs::remove_file(&req.path);
        assert!(text.contains("real-pw") && !text.contains("********"), "{text}");
        assert_eq!(req.original["database.password"], "real-pw");
    }

    #[tokio::test]
    async fn template_whose_plugin_is_not_installed_is_refused_with_a_clear_message() {
        let (mut app, _rx) = app();
        let pick = |title: &str| TEMPLATES.iter().position(|t| t.title == title).unwrap();
        app.plugins = vec![crate::api::Plugin {
            class: "io.debezium.connector.postgresql.PostgresConnector".into(),
            kind: "source".into(),
            version: "3".into(),
        }];
        app.overlay = Overlay::Templates(pick("Confluent JDBC Sink"));
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(matches!(app.overlay, Overlay::Templates(_)), "must not open the form");
        let t = app.toast.as_ref().expect("explanation");
        assert!(t.level == Level::Error && t.text.contains("io.confluent.connect.jdbc.JdbcSinkConnector"));
        assert!(t.text.contains("not installed"), "{}", t.text);
        app.overlay = Overlay::Templates(pick("Debezium PostgreSQL Source"));
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(matches!(app.overlay, Overlay::Form(_)));
    }

    #[tokio::test]
    async fn validation_errors_block_saving() {
        let (mut app, _rx) = app();
        app.overlay = app.open_preview("alpha".into(), Map::new(), PreviewSource::Edit, true);
        let Overlay::Preview(p) = &app.overlay else { panic!() };
        let id = p.id;
        app.update(Msg::Validated { id: id + 99, result: Ok(vec!["ignored".into()]) }); // stale id
        assert!(
            matches!(&app.overlay, Overlay::Preview(p) if !matches!(p.validation, Validation::Errors(_)))
        );
        app.update(Msg::Validated { id, result: Ok(vec!["topics: missing".into()]) });
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(matches!(&app.overlay, Overlay::Preview(p) if matches!(p.validation, Validation::Errors(_))));
        app.update(Msg::Validated { id, result: Ok(vec![]) });
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(matches!(app.overlay, Overlay::None));
    }

    #[test]
    fn loads_inline_and_flat_json() {
        let (n, c) = load_json(r#"{"name":"x","connector.class":"A"}"#).unwrap();
        assert_eq!((n.as_str(), c.len()), ("x", 1));
        assert!(load_json("").is_err());
        assert!(load_json("/definitely/not/here.json").is_err());
        assert!(load_json("{broken").is_err());
    }

    // ---- end-to-end against a stateful fake Kafka Connect -----------------

    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    type Rx = tokio::sync::mpsc::UnboundedReceiver<Msg>;

    #[derive(Default)]
    struct Fake {
        requests: Vec<String>,
        paused: bool,
        deleted: bool,
        /// Number of list calls answered since a restart was requested.
        restart_polls: Option<u32>,
        /// Report RESTARTING for this many polls after a restart.
        restarting_polls: u32,
        /// Task 0 of "c1" stays FAILED forever.
        stuck_failed: bool,
        /// List calls answered since "new1" was created; it only shows up from the 3rd call on.
        create_polls: Option<u32>,
    }

    fn json_conn(name: &str, state: &str, task: &str) -> String {
        format!(
            r#""{name}":{{"info":{{"name":"{name}","type":"sink","config":{{"connector.class":"x.SinkConnector"}}}},"status":{{"name":"{name}","type":"sink","connector":{{"state":"{state}","worker_id":"w:8083"}},"tasks":[{{"id":0,"state":"{task}","worker_id":"w:8083"}}]}}}}"#
        )
    }

    async fn fake_connect() -> (String, Arc<Mutex<Fake>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let fake = Arc::new(Mutex::new(Fake { restarting_polls: 1, ..Default::default() }));
        let state = fake.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else { break };
                let state = state.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16384];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    let line = req.lines().next().unwrap_or("").to_string();
                    let (status, body) = {
                        let mut f = state.lock().unwrap();
                        f.requests.push(line.clone());
                        let mut parts = line.split_whitespace();
                        let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
                        match (method, path.split('?').next().unwrap_or("")) {
                            ("GET", "/") => (200, r#"{"version":"3.9.0","commit":"x"}"#.to_string()),
                            ("GET", "/connector-plugins") => (200, "[]".to_string()),
                            ("GET", "/connectors") => {
                                let mut c1 = ("RUNNING", if f.stuck_failed { "FAILED" } else { "RUNNING" });
                                if f.paused {
                                    c1 = ("PAUSED", "PAUSED");
                                }
                                if let Some(p) = f.restart_polls {
                                    if p < f.restarting_polls {
                                        c1 = ("RESTARTING", "RESTARTING");
                                    }
                                    f.restart_polls = Some(p + 1);
                                }
                                let mut all = Vec::new();
                                if let Some(p) = f.create_polls {
                                    if p >= 2 {
                                        all.push(json_conn("new1", "RUNNING", "RUNNING"));
                                    }
                                    f.create_polls = Some(p + 1);
                                }
                                if !f.deleted {
                                    all.push(json_conn("c1", c1.0, c1.1));
                                }
                                all.push(json_conn("c2", "RUNNING", "RUNNING"));
                                (200, format!("{{{}}}", all.join(",")))
                            }
                            ("POST", "/connectors") => {
                                f.create_polls = Some(0);
                                (201, r#"{"name":"new1"}"#.to_string())
                            }
                            ("PUT", "/connectors/c1/pause") => {
                                f.paused = true;
                                (202, String::new())
                            }
                            ("PUT", "/connectors/c1/resume") => {
                                f.paused = false;
                                (202, String::new())
                            }
                            ("PUT", "/connectors/c2/resume") => {
                                (500, r#"{"error_code":500,"message":"worker exploded"}"#.to_string())
                            }
                            ("POST", "/connectors/c1/restart") => {
                                f.restart_polls = Some(0);
                                (202, String::new())
                            }
                            ("DELETE", "/connectors/c1") => {
                                f.deleted = true;
                                (204, String::new())
                            }
                            _ => (404, r#"{"error_code":404,"message":"nope"}"#.to_string()),
                        }
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        (url, fake)
    }

    async fn live_app(url: &str) -> (App, Rx) {
        let (tx, mut rx) = unbounded_channel();
        let mut cfg = Config::with_local_default();
        cfg.clusters.get_mut("local").unwrap().url = url.into();
        let mut app = App::new(cfg, None, "local".into(), tx);
        pump(&mut app, &mut rx, |a| a.connectors.len() == 2).await;
        (app, rx)
    }

    /// Delivers async results to the app and ticks it (like the real event loop)
    /// until `until` holds.
    async fn pump(app: &mut App, rx: &mut Rx, until: impl Fn(&App) -> bool) {
        for _ in 0..400 {
            while let Ok(m) = rx.try_recv() {
                app.update(m);
            }
            if until(app) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            app.update(Msg::Tick);
        }
        panic!("condition not reached; journal: {:#?}", app.log.entries);
    }

    #[tokio::test]
    async fn created_connector_that_appears_late_is_confirmed_without_false_warnings() {
        let (url, _fake) = fake_connect().await;
        let (mut app, mut rx) = live_app(&url).await;
        let mut cfg = Map::new();
        cfg.insert("connector.class".into(), Value::String("x.NewConnector".into()));
        app.overlay = app.open_preview("new1".into(), cfg, PreviewSource::Edit, false);
        let Overlay::Preview(p) = &app.overlay else { panic!("no preview") };
        let id = p.id;
        app.update(Msg::Validated { id, result: Ok(vec![]) });
        app.on_key(KeyEvent::from(KeyCode::Enter));
        pump(&mut app, &mut rx, |a| !a.watches.is_empty()).await;
        pump(&mut app, &mut rx, |a| a.watches.is_empty()).await;
        let j = journal(&app);
        assert!(!j.contains("not listed"), "{j}");
        let last = app.log.last().unwrap();
        assert_eq!(last.severity, Severity::Ok, "{j}");
        assert!(last.text.contains("new1") && last.text.contains("created"), "{}", last.text);
    }

    #[tokio::test]
    async fn created_connector_that_never_appears_is_reported_after_the_timeout() {
        let (url, _fake) = fake_connect().await;
        let (mut app, mut rx) = live_app(&url).await;
        app.run_action(
            "create ghost".into(),
            Some("ghost".into()),
            vec![("ghost".into(), Expect::Created)],
            |_| async { Ok(()) },
        );
        pump(&mut app, &mut rx, |a| !a.watches.is_empty()).await;
        for w in &mut app.watches {
            w.started -= activity::WATCH_TIMEOUT;
        }
        pump(&mut app, &mut rx, |a| a.watches.is_empty()).await;
        let last = app.log.last().unwrap();
        assert_eq!(last.severity, Severity::Warn);
        assert!(last.text.contains("ghost") && last.text.contains("not listed"), "{}", last.text);
    }

    fn journal(app: &App) -> String {
        app.log.entries.iter().map(|e| e.text.clone()).collect::<Vec<_>>().join("\n")
    }

    #[tokio::test]
    async fn restart_is_requested_and_the_transition_is_confirmed_in_the_journal() {
        let (url, fake) = fake_connect().await;
        let (mut app, mut rx) = live_app(&url).await;
        app.on_key(KeyEvent::from(KeyCode::Char('R'))); // selected = c1
        pump(&mut app, &mut rx, |a| journal(a).contains("restart confirmed")).await;
        let j = journal(&app);
        assert!(j.contains("restart c1: requested") && j.contains("accepted by Kafka Connect"), "{j}");
        assert!(j.contains("RESTARTING → RUNNING"), "{j}");
        assert!(fake.lock().unwrap().requests.iter().any(|r| r.starts_with("POST /connectors/c1/restart")));
        assert!(app.watches.is_empty());
        assert_eq!(app.log.last().unwrap().severity, Severity::Ok);
    }

    #[tokio::test]
    async fn instant_restart_is_not_over_claimed() {
        let (url, fake) = fake_connect().await;
        fake.lock().unwrap().restarting_polls = 0; // the fake never shows RESTARTING
        let (mut app, mut rx) = live_app(&url).await;
        app.on_key(KeyEvent::from(KeyCode::Char('R')));
        pump(&mut app, &mut rx, |a| journal(a).contains("accepted, healthy again")).await;
        assert!(journal(&app).contains("no RESTARTING state was seen"));
        assert!(!journal(&app).contains("restart confirmed"));
    }

    #[tokio::test]
    async fn pause_then_resume_are_verified() {
        let (url, _fake) = fake_connect().await;
        let (mut app, mut rx) = live_app(&url).await;
        app.on_key(KeyEvent::from(KeyCode::Char('p')));
        pump(&mut app, &mut rx, |a| journal(a).contains("confirmed PAUSED")).await;
        assert_eq!(app.selected().unwrap().state, "PAUSED");
        app.on_key(KeyEvent::from(KeyCode::Char('u')));
        pump(&mut app, &mut rx, |a| journal(a).contains("confirmed RUNNING")).await;
    }

    #[tokio::test]
    async fn restart_of_a_connector_that_stays_failed_ends_as_an_error() {
        let (url, fake) = fake_connect().await;
        {
            let mut f = fake.lock().unwrap();
            f.stuck_failed = true;
            f.restarting_polls = 0; // the task never even leaves FAILED
        }
        let (mut app, mut rx) = live_app(&url).await;
        app.on_key(KeyEvent::from(KeyCode::Char('R')));
        pump(&mut app, &mut rx, |a| !a.watches.is_empty()).await;
        // Fast-forward the clock instead of waiting out the real timeout.
        for w in &mut app.watches {
            w.started -= activity::WATCH_TIMEOUT;
        }
        pump(&mut app, &mut rx, |a| a.watches.is_empty()).await;
        let last = app.log.last().unwrap();
        assert_eq!(last.severity, Severity::Error, "{}", journal(&app));
        assert!(last.text.contains("not healthy"), "{}", last.text);
        assert!(matches!(&app.toast, Some(t) if t.level == Level::Error));
    }

    #[tokio::test]
    async fn partial_batch_failure_reports_it_but_still_verifies_the_rest() {
        let (url, fake) = fake_connect().await;
        let (mut app, mut rx) = live_app(&url).await;
        fake.lock().unwrap().paused = true;
        app.on_key(KeyEvent::from(KeyCode::Char(' ')));
        app.on_key(KeyEvent::from(KeyCode::Char(' '))); // c1 and c2 marked
        app.on_key(KeyEvent::from(KeyCode::Char('u'))); // c2 resume fails with 500
        pump(&mut app, &mut rx, |a| matches!(a.overlay, Overlay::Error(_))).await;
        let Overlay::Error(text) = &app.overlay else { unreachable!() };
        assert!(text.contains("c2") && text.contains("worker exploded"), "{text}");
        assert!(!text.contains("c1:"), "c1 succeeded and must not be listed as failed: {text}");
        pump(&mut app, &mut rx, |a| journal(a).contains("c1: confirmed RUNNING")).await;
        assert!(journal(&app).contains("worker exploded"));
        assert!(app.watches.iter().all(|w| w.name != "c2"));
    }

    #[tokio::test]
    async fn delete_is_confirmed_when_the_connector_disappears() {
        let (url, _fake) = fake_connect().await;
        let (mut app, mut rx) = live_app(&url).await;
        app.on_key(KeyEvent::from(KeyCode::Char('d')));
        app.on_key(KeyEvent::from(KeyCode::Char('y')));
        pump(&mut app, &mut rx, |a| journal(a).contains("deleted — no longer listed")).await;
        assert_eq!(app.connectors.len(), 1);
    }

    #[tokio::test]
    async fn unreachable_cluster_gives_up_verifying_instead_of_claiming_success() {
        let (url, _fake) = fake_connect().await;
        let (mut app, _rx) = live_app(&url).await;
        // The cluster goes away; the watch must expire with a warning, not a false OK.
        app.client =
            Some(Client::new("http://127.0.0.1:1", api::Auth::None, Duration::from_secs(1)).unwrap());
        let mut w = Watch::new("c1".into(), Expect::Restarted);
        w.started -= activity::WATCH_TIMEOUT + Duration::from_secs(3);
        app.watches = vec![w];
        app.update(Msg::Tick);
        assert!(app.watches.is_empty());
        assert_eq!(app.log.last().unwrap().severity, Severity::Warn);
        assert!(app.log.last().unwrap().text.contains("could not verify"));
    }

    #[tokio::test]
    async fn switching_cluster_drops_pending_verifications() {
        let (url, _fake) = fake_connect().await;
        let (mut app, _rx) = live_app(&url).await;
        app.watches.push(Watch::new("c1".into(), Expect::Restarted));
        app.use_cluster("local");
        assert!(app.watches.is_empty());
        assert!(journal(&app).contains("pending verifications were dropped"));
    }

    #[tokio::test]
    async fn activity_overlay_opens_scrolls_and_closes() {
        let (mut app, _rx) = app();
        for i in 0..30 {
            app.note(Severity::Info, &format!("entry {i}"));
        }
        app.on_key(KeyEvent::from(KeyCode::Char('L')));
        assert!(matches!(app.overlay, Overlay::Activity(0)));
        for key in [KeyCode::Char('j'), KeyCode::PageDown, KeyCode::End, KeyCode::Down, KeyCode::Char('x')] {
            app.on_key(KeyEvent::from(key));
        }
        assert!(matches!(app.overlay, Overlay::Activity(29)), "clamped to the last entry");
        app.on_key(KeyEvent::from(KeyCode::Home));
        assert!(matches!(app.overlay, Overlay::Activity(0)));
        app.on_key(KeyEvent::from(KeyCode::Esc));
        assert!(matches!(app.overlay, Overlay::None));
    }

    // ---- security & edge cases ---------------------------------------------

    #[test]
    fn editor_temp_file_is_exclusive_and_private() {
        let a = write_private_temp("tuizium-t", "{}").unwrap();
        let b = write_private_temp("tuizium-t", "{}").unwrap();
        assert_ne!(a, b, "two files requested at the same instant must not collide");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&a).unwrap().permissions().mode() & 0o077,
                0,
                "no group/other access"
            );
        }
        assert_eq!(std::fs::read_to_string(&a).unwrap(), "{}");
        let _ = (std::fs::remove_file(a), std::fs::remove_file(b));
    }

    #[test]
    fn capped_file_reads_refuse_oversized_and_non_utf8_input() {
        let dir = std::env::temp_dir().join(format!("tuizium-cap-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let big = dir.join("big.json");
        std::fs::write(&big, vec![b' '; 2048]).unwrap();
        assert!(read_capped(&big, 1024).unwrap_err().to_string().contains("larger than"));
        assert!(read_capped(&big, 4096).is_ok());
        let bin = dir.join("bin.json");
        std::fs::write(&bin, [0xff, 0xfe, 0x00]).unwrap();
        assert!(read_capped(&bin, 1024).unwrap_err().to_string().contains("UTF-8"));
        assert!(read_capped(&dir.join("missing.json"), 1024).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn add_cluster_rejects_credentials_duplicates_and_garbage() {
        let (mut app, _rx) = app();
        assert!(app.add_cluster("").is_err());
        assert!(app.add_cluster("only-name").is_err());
        let e = app.add_cluster("p http://admin:hunter2@host:8083").unwrap_err();
        assert!(e.contains("credentials") && !e.contains("hunter2"), "{e}");
        assert!(!app.config.clusters.contains_key("p"));
        assert!(app.add_cluster("p ftp://host").is_err());
        assert!(app.add_cluster("local http://other:8083").unwrap_err().contains("already exists"));
        assert!(app.add_cluster("p http://h:1 u pw extra").is_err());
        assert!(
            app.add_cluster("p http://h:1 u TUIZIUM_UNSET_ENV_VAR").is_ok(),
            "a missing env var must not block saving"
        );
        assert!(app.config.clusters.contains_key("p"));
    }

    #[tokio::test]
    async fn plaintext_credentials_to_remote_hosts_are_flagged() {
        // SAFETY: test-only variable with a unique name, set before any thread reads it.
        unsafe { std::env::set_var("TUIZIUM_TEST_TOKEN_REMOTE", "t") };
        let (tx, _rx) = unbounded_channel();
        let mut cfg = Config::with_local_default();
        cfg.clusters.insert(
            "remote".into(),
            Cluster {
                url: "http://connect.example.com:8083".into(),
                token_env: Some("TUIZIUM_TEST_TOKEN_REMOTE".into()),
                ..Default::default()
            },
        );
        let app = App::new(cfg, None, "remote".into(), tx);
        assert!(journal(&app).contains("plain http"), "{}", journal(&app));
        assert!(matches!(&app.toast, Some(t) if t.level == Level::Warn));
    }

    #[tokio::test]
    async fn unusable_profiles_fail_visibly_not_by_panicking() {
        let (tx, _rx) = unbounded_channel();
        let mut cfg = Config::with_local_default();
        cfg.clusters.insert(
            "broken".into(),
            Cluster {
                url: "http://h".into(),
                token_env: Some("TUIZIUM_DEFINITELY_UNSET".into()),
                ..Default::default()
            },
        );
        let mut app = App::new(cfg, None, "broken".into(), tx);
        assert!(matches!(&app.conn, Conn::Failed(e) if e.contains("TUIZIUM_DEFINITELY_UNSET")));
        // Every action on a profile without a client is a polite error, not a crash.
        for key in ['R', 'p', 'u', 'F', 'r', 'e', 'd'] {
            app.on_key(KeyEvent::from(KeyCode::Char(key)));
            app.overlay = Overlay::None;
        }
        app.use_cluster("does-not-exist");
        assert!(matches!(&app.conn, Conn::Failed(e) if e.contains("unknown cluster")));
    }

    #[tokio::test]
    async fn empty_and_hostile_cluster_state_never_panics() {
        let (mut app, _rx) = app();
        let generation = app.generation;
        app.update(Msg::Loaded { generation, result: Ok(Snapshot::default()) });
        for key in "jkgGRFpudeP \n\tL1234[]/aofs?".chars() {
            app.on_key(KeyEvent::from(KeyCode::Char(key)));
            app.editor_request = None;
            app.overlay = Overlay::None;
            app.searching = false;
        }
        for code in
            [KeyCode::PageDown, KeyCode::PageUp, KeyCode::Home, KeyCode::End, KeyCode::Esc, KeyCode::F(1)]
        {
            app.on_key(KeyEvent::from(code));
            app.overlay = Overlay::None;
        }
        // One connector with a hostile name, state and worker and no tasks.
        let mut c = conn("x\u{1b}[2J\u{202e}evil\r\n", Kind::Unknown, "\u{7}WEIRD", &[]);
        c.worker_id = "\u{1b}]0;pwned\u{7}".into();
        app.update(Msg::Loaded {
            generation,
            result: Ok(Snapshot { root: None, plugins: None, connectors: vec![c] }),
        });
        assert_eq!(app.connectors.len(), 1);
        app.on_key(KeyEvent::from(KeyCode::Enter));
        app.on_key(KeyEvent::from(KeyCode::Char('2')));
        app.on_key(KeyEvent::from(KeyCode::Char('j')));
        assert_eq!(app.task_sel, 0);
    }

    #[tokio::test]
    async fn selection_survives_shrinking_task_lists_and_vanishing_connectors() {
        let (mut app, _rx) = app();
        let generation = app.generation;
        app.on_key(KeyEvent::from(KeyCode::Char('j'))); // beta (2 tasks)
        app.on_key(KeyEvent::from(KeyCode::Enter));
        app.on_key(KeyEvent::from(KeyCode::Char('2')));
        app.on_key(KeyEvent::from(KeyCode::Char('j')));
        assert_eq!(app.task_sel, 1);
        app.update(Msg::Loaded {
            generation,
            result: Ok(Snapshot {
                connectors: vec![conn("beta", Kind::Sink, "RUNNING", &["RUNNING"])],
                ..Default::default()
            }),
        });
        assert_eq!(app.task_sel, 0);
        assert_eq!(app.selected().unwrap().name, "beta");
        app.update(Msg::Loaded { generation, result: Ok(Snapshot::default()) });
        assert!(app.selected().is_none());
        assert_eq!(app.focus, Focus::List, "details focus is dropped when nothing is selected");
        app.on_key(KeyEvent::from(KeyCode::Char('R'))); // no-op on an empty list
    }

    #[tokio::test]
    async fn marks_for_vanished_connectors_are_dropped_before_a_batch() {
        let (mut app, _rx) = app();
        let generation = app.generation;
        app.on_key(KeyEvent::from(KeyCode::Char(' ')));
        app.on_key(KeyEvent::from(KeyCode::Char(' ')));
        assert_eq!(app.marked.len(), 2);
        app.update(Msg::Loaded {
            generation,
            result: Ok(Snapshot {
                connectors: vec![conn("gamma", Kind::Sink, "PAUSED", &["PAUSED"])],
                ..Default::default()
            }),
        });
        assert!(app.marked.is_empty());
        assert_eq!(app.targets(), ["gamma"]);
    }

    #[test]
    fn import_rejects_hostile_or_oversized_json() {
        for input in ["[]", "null", "123", r#"{"name":"..","connector.class":"X"}"#, "{\"name\":\"a\u{7}\"}"]
        {
            assert!(load_json(input).is_err(), "{input}");
        }
        let huge = format!("{{\"name\":\"a\",\"x\":\"{}\"}}", "y".repeat(6 * 1024 * 1024));
        assert!(load_json(&huge).is_err());
    }

    // ---- opening screens: splash → connection form → connector list ----------

    fn deferred_app(path: Option<PathBuf>) -> (App, Rx) {
        let (tx, rx) = unbounded_channel();
        let mut cfg = Config::with_local_default();
        cfg.clusters.get_mut("local").unwrap().url = "http://127.0.0.1:1".into();
        (App::new_with(cfg, path, "local".into(), tx, false), rx)
    }

    fn type_text(app: &mut App, text: &str) {
        for ch in text.chars() {
            app.on_key(KeyEvent::from(KeyCode::Char(ch)));
        }
    }

    #[tokio::test]
    async fn nothing_connects_before_the_user_has_chosen() {
        let (mut app, mut rx) = deferred_app(None);
        assert!(app.client.is_none(), "no client, so no request can have been sent");
        app.start_screens(true, true);
        assert!(matches!(app.screen, Screen::Splash(_)));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(rx.try_recv().is_err(), "no network traffic results while the splash is up");
        assert!(app.client.is_none());
    }

    #[tokio::test]
    async fn only_enter_ends_the_splash() {
        let (mut app, _rx) = deferred_app(None);
        app.start_screens(true, true);
        for code in [
            KeyCode::Char('x'),
            KeyCode::Char(' '),
            KeyCode::Tab,
            KeyCode::Down,
            KeyCode::Backspace,
            KeyCode::F(1),
        ] {
            app.on_key(KeyEvent::from(code));
            assert!(matches!(app.screen, Screen::Splash(_)), "{code:?} must not leave the splash");
        }
        // Time passing never moves on by itself either.
        for _ in 0..5 {
            app.update(Msg::Frame);
            app.update(Msg::Tick);
        }
        app.screen = Screen::Splash(Instant::now() - Duration::from_secs(3600));
        app.update(Msg::Frame);
        assert!(matches!(app.screen, Screen::Splash(_)));
        assert!(app.client.is_none());
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(matches!(app.screen, Screen::Setup(_)));
        assert!(
            !splash::ACTIVE.load(std::sync::atomic::Ordering::Relaxed),
            "the frame ticker is told to stop"
        );
        app.update(Msg::Frame); // late frames are harmless
        assert!(matches!(app.screen, Screen::Setup(_)));
    }

    #[tokio::test]
    async fn enter_on_the_splash_goes_straight_to_the_list_when_not_asking() {
        let (mut app, _rx) = deferred_app(None);
        app.start_screens(true, false);
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(matches!(app.screen, Screen::Main));
        assert!(app.client.is_some());
    }

    #[tokio::test]
    async fn esc_quits_from_the_splash() {
        let (mut app, _rx) = deferred_app(None);
        app.start_screens(true, true);
        app.on_key(KeyEvent::from(KeyCode::Esc));
        assert!(app.should_quit);
    }

    #[tokio::test]
    async fn ctrl_c_and_esc_quit_from_the_opening_screens() {
        let (mut app, _rx) = deferred_app(None);
        app.start_screens(true, true);
        app.on_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert!(app.should_quit);

        let (mut app, _rx) = deferred_app(None);
        app.start_screens(false, true);
        app.on_key(KeyEvent::from(KeyCode::Esc));
        assert!(app.should_quit);
        assert!(app.client.is_none());
    }

    #[tokio::test]
    async fn without_splash_or_questions_it_connects_immediately() {
        let (mut app, _rx) = deferred_app(None);
        app.start_screens(false, false);
        assert!(matches!(app.screen, Screen::Main));
        assert!(app.client.is_some());
        assert!(matches!(app.conn, Conn::Connecting | Conn::Failed(_)));
    }

    #[tokio::test]
    async fn entering_a_url_connects_and_remembers_the_profile() {
        let dir = std::env::temp_dir().join(format!("tuizium-flow-{}", std::process::id()));
        let path = dir.join("config.toml");
        let (url, _fake) = fake_connect().await;
        let (mut app, mut rx) = deferred_app(Some(path.clone()));
        app.start_screens(false, true);
        let Screen::Setup(form) = &mut app.screen else { panic!("expected the connection form") };
        form.selected = setup::NAME;
        app.on_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        type_text(&mut app, "dev");
        app.on_key(KeyEvent::from(KeyCode::Tab));
        app.on_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        type_text(&mut app, &url);
        app.on_key(KeyEvent::from(KeyCode::Enter));

        assert!(matches!(app.screen, Screen::Main));
        assert_eq!(app.cluster, "dev");
        assert!(app.client.is_some());
        pump(&mut app, &mut rx, |a| a.connectors.len() == 2).await;
        assert!(matches!(app.conn, Conn::Connected(_)));

        // Persisted for next time, without any secret material.
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(saved.contains("default_cluster = \"dev\"") && saved.contains(&url), "{saved}");
        let reloaded = config::load(Some(&path)).unwrap();
        assert_eq!(reloaded.initial_cluster().as_deref(), Some("dev"));
        assert_eq!(reloaded.clusters["dev"].url, url);
        assert!(journal(&app).contains("connecting to profile dev"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn picking_an_existing_profile_unchanged_does_not_rewrite_the_config() {
        let dir = std::env::temp_dir().join(format!("tuizium-flow2-{}", std::process::id()));
        let path = dir.join("config.toml");
        let (mut app, _rx) = deferred_app(Some(path.clone()));
        app.config.default_cluster = Some("local".into());
        app.start_screens(false, true);
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(matches!(app.screen, Screen::Main));
        assert!(!path.exists(), "nothing changed, so nothing is written");
        assert!(app.client.is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn invalid_input_keeps_the_form_open_and_sends_nothing() {
        let (mut app, mut rx) = deferred_app(None);
        app.start_screens(false, true);
        let Screen::Setup(form) = &mut app.screen else { panic!() };
        form.selected = setup::URL;
        app.on_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        type_text(&mut app, "http://admin:hunter2@host:8083");
        app.on_key(KeyEvent::from(KeyCode::Enter));
        let Screen::Setup(form) = &app.screen else { panic!("must stay on the form") };
        let err = form.error.clone().unwrap();
        assert!(err.contains("credentials") && !err.contains("hunter2"), "{err}");
        assert!(app.client.is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn an_unwritable_config_does_not_block_connecting() {
        let file = std::env::temp_dir().join(format!("tuizium-flow3-{}", std::process::id()));
        std::fs::write(&file, "x").unwrap(); // a file where a directory is needed
        let (mut app, _rx) = deferred_app(Some(file.join("config.toml")));
        app.start_screens(false, true);
        let Screen::Setup(form) = &mut app.screen else { panic!() };
        form.selected = setup::URL;
        type_text(&mut app, "9"); // edit => must be saved
        app.on_key(KeyEvent::from(KeyCode::Enter));
        assert!(matches!(app.screen, Screen::Main));
        assert!(app.client.is_some());
        assert!(
            matches!(&app.toast, Some(t) if t.level == Level::Warn && t.text.contains("could not be saved"))
        );
        let _ = std::fs::remove_file(&file);
    }
}
