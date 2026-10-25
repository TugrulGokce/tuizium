mod activity;
mod api;
mod app;
mod cli;
mod clip;
mod config;
mod debezium;
mod diff;
mod setup;
mod splash;
mod templates;
mod ui;

use std::path::Path;
use std::process::{Command, ExitCode};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{self, Event, KeyEventKind};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use std::io::IsTerminal;

use app::{App, EditorRequest, Msg};
use config::{Cluster, Config};

const HELP: &str = "\
Tuizium - a fast terminal interface for Debezium and Kafka Connect

USAGE:
    tuizium [OPTIONS]                       open the terminal UI
    tuizium [OPTIONS] <COMMAND> [ARGS]      run one command and exit (for scripts)

OPTIONS:
    -c, --cluster <NAME>   Use the given cluster profile
    -u, --url <URL>        Use an ad-hoc Kafka Connect URL (no auth)
        --config <PATH>    Use a specific config file
        --no-splash        Skip the opening animation (UI only)
        --read-only        Refuse every change (also per profile: read_only = true)
    -h, --help             Print help
    -V, --version          Print version

COMMANDS (plain text or --output json; no colours, no prompts):
    list [--state S] [--type T]          List connectors (S: running|paused|failed|unassigned, T: source|sink)
    status <name>                        One connector: state, tasks, masked config and traces
    pause <name>... [--wait]             Pause connectors
    resume <name>... [--wait]            Resume connectors (also resumes stopped ones)
    stop <name>... [--wait]              Stop connectors (Kafka Connect 3.5+; tasks are removed)
    restart <name>... [--only-failed] [--wait]
                                         Restart connectors and their tasks
    create <file|-> [--dry-run] [--wait]   Create a connector from a JSON file or stdin
    update <file|-> [--dry-run] [--wait]   Replace a connector's config (shows the diff first)

COMMAND OPTIONS:
    -o, --output <table|json>   Output format (default table)
    --wait                      Poll up to 15 s and report whether the change really happened
    --dry-run                   create/update: validate and show what would change; send nothing

EXIT CODES:
    0 success   1 error (cannot connect, HTTP error, bad input)   2 usage error
    3 request accepted but the connector is unhealthy / the change was not confirmed

ENVIRONMENT:
    TUIZIUM_CONFIG         Config file path override
    TUIZIUM_NO_SPLASH      Skip the opening animation
    TUIZIUM_LOG            Log filter (e.g. debug); logs go to a file, never the terminal

FILES:
    <state dir>/tuizium/actions.log   Journal of every action and its verified outcome (press L in the app)
";

fn init_logging() -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let filter = std::env::var("TUIZIUM_LOG").ok()?;
    let dir = dirs::state_dir().or_else(dirs::data_local_dir)?.join("tuizium");
    std::fs::create_dir_all(&dir).ok()?;
    let (writer, guard) =
        tracing_appender::non_blocking(tracing_appender::rolling::never(dir, "tuizium.log"));
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .init();
    Some(guard)
}

#[tokio::main]
async fn main() -> Result<ExitCode> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match cli::parse(&argv) {
        Ok(cli::Parsed::Help) => {
            print!("{HELP}");
            return Ok(ExitCode::SUCCESS);
        }
        Ok(cli::Parsed::Version) => {
            println!("tuizium {}", env!("CARGO_PKG_VERSION"));
            return Ok(ExitCode::SUCCESS);
        }
        Ok(cli::Parsed::Cmd(global, command)) => {
            let _log_guard = init_logging();
            return Ok(ExitCode::from(cli::run(global, command).await as u8));
        }
        Ok(cli::Parsed::Tui(global)) => global,
        Err(e) => {
            eprintln!("error: {e}\n\nRun `tuizium --help` for usage.");
            return Ok(ExitCode::from(cli::EXIT_USAGE as u8));
        }
    };
    let _log_guard = init_logging();

    let path = args.config.or_else(config::config_path);
    let mut cfg = config::load(path.as_ref())?;
    let mut cluster = args.cluster.clone().or_else(|| cfg.initial_cluster());
    let adhoc = args.url.is_some();
    if let Some(url) = args.url {
        cfg =
            Config { clusters: [("adhoc".to_string(), Cluster { url, ..Default::default() })].into(), ..cfg };
        cluster = Some("adhoc".into());
    }
    let Some(cluster) = cluster else { bail!("no cluster configured") };
    if !cfg.clusters.contains_key(&cluster) {
        bail!(
            "unknown cluster {cluster:?}; configured: {}",
            cfg.clusters.keys().cloned().collect::<Vec<_>>().join(", ")
        );
    }

    // A TUI writes escape sequences and reads key events; with a pipe or file on either side that is
    // useless (and would dump control codes into the file), so say so and stop.
    if !(std::io::stdin().is_terminal() && std::io::stdout().is_terminal()) {
        bail!(
            "tuizium needs an interactive terminal (stdin and stdout must be a TTY); run it directly in a terminal"
        );
    }

    // Asking where Kafka Connect lives is skipped when the cluster was named on the command line.
    let ask = cfg.ask_on_start() && args.cluster.is_none() && !adhoc;
    let splash = !args.no_splash && std::env::var_os("TUIZIUM_NO_SPLASH").is_none();

    let (tx, rx) = mpsc::unbounded_channel();
    // Created before the terminal switches modes so errors print normally.
    // Nothing connects until the connection screen (or the splash) hands over.
    let mut app = App::new_with(cfg, path, cluster, tx.clone(), false);
    app.set_read_only(args.read_only);
    app.set_activity_file(activity::default_path());
    app.start_screens(splash, ask);
    spawn_background(tx.clone());
    if splash {
        spawn_frames(tx);
    }

    // `init` enters raw mode + alt screen and installs a panic hook that restores the terminal.
    let mut terminal = ratatui::init();
    let result = run(&mut terminal, &mut app, rx).await;
    ratatui::restore();
    result.map(|()| ExitCode::SUCCESS)
}

fn spawn_background(tx: UnboundedSender<Msg>) {
    let input_tx = tx.clone();
    std::thread::spawn(move || {
        loop {
            // Stay off the console while an external editor owns it.
            if EDITING.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
            match event::poll(Duration::from_millis(100)) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_) => break,
            }
            let Ok(ev) = event::read() else { break };
            let msg = match ev {
                Event::Key(k) if k.kind != KeyEventKind::Release => Msg::Key(app::normalize_altgr(k)),
                Event::Resize(..) => Msg::Redraw,
                _ => continue,
            };
            if input_tx.send(msg).is_err() {
                break;
            }
        }
    });
    let tick_tx = tx.clone();
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(1));
        loop {
            t.tick().await;
            if tick_tx.send(Msg::Tick).is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = tx.send(Msg::Quit);
        }
    });
}

/// Drives the splash animation for as long as the splash is on screen.
fn spawn_frames(tx: UnboundedSender<Msg>) {
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_millis(50));
        while splash::ACTIVE.load(Ordering::Relaxed) {
            t.tick().await;
            if tx.send(Msg::Frame).is_err() {
                break;
            }
        }
    });
}

static EDITING: AtomicBool = AtomicBool::new(false);

/// Suspends the TUI, runs `$VISUAL`/`$EDITOR` on the file, then resumes.
fn run_editor(terminal: &mut DefaultTerminal, app: &mut App, req: EditorRequest) {
    EDITING.store(true, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(150)); // let the input thread notice
    ratatui::restore();
    let outcome = spawn_editor(&req.path);
    *terminal = ratatui::init();
    EDITING.store(false, Ordering::SeqCst);
    app.editor_finished(req, outcome);
}

fn spawn_editor(path: &Path) -> Result<(), String> {
    let cmd = ["VISUAL", "EDITOR"]
        .iter()
        .find_map(|v| std::env::var(v).ok().filter(|s| !s.trim().is_empty()))
        .unwrap_or_else(|| if cfg!(windows) { "notepad".into() } else { "vi".into() });
    let mut parts = cmd.split_whitespace();
    let program = parts.next().unwrap_or("vi");
    let status = if cfg!(windows) {
        // `cmd /C` lets .cmd/.bat shims such as VS Code's `code` resolve.
        Command::new("cmd").arg("/C").arg(program).args(parts).arg(path).status()
    } else {
        Command::new(program).args(parts).arg(path).status()
    };
    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => Err(format!("{cmd:?} exited with {s}")),
        Err(e) => Err(format!("cannot start {cmd:?}: {e}")),
    }
}

async fn run(terminal: &mut DefaultTerminal, app: &mut App, mut rx: UnboundedReceiver<Msg>) -> Result<()> {
    while !app.should_quit {
        if let Some(req) = app.editor_request.take() {
            run_editor(terminal, app, req);
        }
        if app.dirty {
            app.dirty = false;
            terminal.draw(|f| ui::draw(f, app))?;
        }
        let Some(msg) = rx.recv().await else { break };
        app.update(msg);
        while let Ok(msg) = rx.try_recv() {
            app.update(msg);
        }
    }
    Ok(())
}
