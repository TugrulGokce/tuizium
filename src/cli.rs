//! Non-interactive subcommands (`tuizium list`, `status`, `pause`, ...), for scripts and CI.
//!
//! Plain text or `--output json` on stdout, errors on stderr, never any ANSI escape codes.
//! Exit codes: 0 success, 1 error (cannot connect, HTTP error, bad input), 2 usage error,
//! 3 the request went through but the connector is unhealthy / the change was not confirmed.
//! Secrets are masked in everything printed, exactly as in the TUI.

use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use serde_json::{Map, Value, json};

use crate::activity::{self, Expect, Severity, Watch};
use crate::api::{Client, Connector, Kind};
use crate::config::{self, Cluster};
use crate::{clip, diff, templates};

pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 1;
pub const EXIT_USAGE: i32 = 2;
pub const EXIT_UNHEALTHY: i32 = 3;

/// Largest connector definition read from a file or stdin.
const MAX_INPUT_BYTES: u64 = 5 * 1024 * 1024;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Global {
    pub cluster: Option<String>,
    pub url: Option<String>,
    pub config: Option<PathBuf>,
    pub no_splash: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Output {
    Table,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateSel {
    Running,
    Paused,
    Failed,
    Unassigned,
}

impl StateSel {
    fn matches(self, c: &Connector) -> bool {
        match self {
            StateSel::Running => c.state == "RUNNING" && !c.is_failed(),
            StateSel::Paused => c.is_paused(),
            StateSel::Failed => c.is_failed(),
            StateSel::Unassigned => c.state == "UNASSIGNED",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    List { state: Option<StateSel>, kind: Option<Kind>, output: Output },
    Status { name: String, output: Output },
    Pause { names: Vec<String>, wait: bool, output: Output },
    Resume { names: Vec<String>, wait: bool, output: Output },
    Restart { names: Vec<String>, only_failed: bool, wait: bool, output: Output },
    Create { file: String, dry_run: bool, wait: bool, output: Output },
    Update { file: String, dry_run: bool, wait: bool, output: Output },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Parsed {
    Help,
    Version,
    Tui(Global),
    Cmd(Global, Command),
}

#[derive(Default)]
struct Opts {
    output: Option<Output>,
    state: Option<StateSel>,
    kind: Option<Kind>,
    only_failed: bool,
    wait: bool,
    dry_run: bool,
    /// Names of the command-only flags that were given, to reject them on the wrong command.
    given: Vec<&'static str>,
}

const COMMANDS: &str = "list, status, pause, resume, restart, create, update";

/// Parses everything after the program name. `Err` is a usage error (exit code 2).
pub fn parse(args: &[String]) -> Result<Parsed, String> {
    let mut global = Global::default();
    let mut opts = Opts::default();
    let mut positional: Vec<String> = Vec::new();
    let mut it = args.iter().cloned();
    while let Some(raw) = it.next() {
        // `--flag=value` is accepted for every flag that takes a value.
        let (flag, mut inline) = match raw.split_once('=') {
            Some((f, v)) if f.starts_with("--") => (f.to_string(), Some(v.to_string())),
            _ => (raw.clone(), None),
        };
        let mut value = |name: &str| -> Result<String, String> {
            inline.take().or_else(|| it.next()).ok_or_else(|| format!("{name} requires a value"))
        };
        match flag.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "-V" | "--version" => return Ok(Parsed::Version),
            "-c" | "--cluster" => global.cluster = Some(value("--cluster")?),
            "-u" | "--url" => {
                let v = value("--url")?;
                // An empty shell variable makes the next word (often the subcommand) the value.
                if !(v.starts_with("http://") || v.starts_with("https://")) {
                    return Err(format!(
                        "--url expects an http:// or https:// address, got {v:?} (is a shell variable empty?)"
                    ));
                }
                global.url = Some(v);
            }
            "--config" => global.config = Some(value("--config")?.into()),
            "--no-splash" => global.no_splash = true,
            "-o" | "--output" => {
                opts.given.push("--output");
                opts.output = Some(match value("--output")?.as_str() {
                    "table" | "text" => Output::Table,
                    "json" => Output::Json,
                    other => return Err(format!("--output must be table or json, not {other:?}")),
                });
            }
            "--state" => {
                opts.given.push("--state");
                opts.state = Some(match value("--state")?.to_ascii_lowercase().as_str() {
                    "running" => StateSel::Running,
                    "paused" => StateSel::Paused,
                    "failed" => StateSel::Failed,
                    "unassigned" => StateSel::Unassigned,
                    other => {
                        return Err(format!(
                            "--state must be running, paused, failed or unassigned, not {other:?}"
                        ));
                    }
                });
            }
            "--type" => {
                opts.given.push("--type");
                opts.kind = Some(match value("--type")?.to_ascii_lowercase().as_str() {
                    "source" => Kind::Source,
                    "sink" => Kind::Sink,
                    other => return Err(format!("--type must be source or sink, not {other:?}")),
                });
            }
            "--only-failed" => {
                opts.given.push("--only-failed");
                opts.only_failed = true;
            }
            "--wait" => {
                opts.given.push("--wait");
                opts.wait = true;
            }
            "--dry-run" => {
                opts.given.push("--dry-run");
                opts.dry_run = true;
            }
            f if f.starts_with('-') && f != "-" => return Err(format!("unknown argument {raw:?}")),
            _ => positional.push(raw),
        }
        if inline.is_some() {
            return Err(format!("{flag} does not take a value"));
        }
    }

    let mut pos = positional.into_iter();
    let Some(cmd) = pos.next() else {
        if let Some(f) = opts.given.first() {
            return Err(format!("{f} needs a subcommand ({COMMANDS})"));
        }
        return Ok(Parsed::Tui(global));
    };
    let rest: Vec<String> = pos.collect();
    let allowed: &[&str] = match cmd.as_str() {
        "list" => &["--output", "--state", "--type"],
        "status" => &["--output"],
        "pause" | "resume" => &["--output", "--wait"],
        "restart" => &["--output", "--wait", "--only-failed"],
        "create" | "update" => &["--output", "--wait", "--dry-run"],
        other => return Err(format!("unknown command {other:?} (expected one of: {COMMANDS})")),
    };
    if let Some(bad) = opts.given.iter().find(|g| !allowed.contains(g)) {
        return Err(format!("{bad} is not valid for `{cmd}`"));
    }
    let need = |n: usize, usage: &str| -> Result<(), String> {
        if rest.len() == n { Ok(()) } else { Err(format!("usage: tuizium {usage}")) }
    };
    let at_least_one = |usage: &str| -> Result<Vec<String>, String> {
        if rest.is_empty() { Err(format!("usage: tuizium {usage}")) } else { Ok(rest.clone()) }
    };
    let output = opts.output.unwrap_or(Output::Table);
    let command = match cmd.as_str() {
        "list" => {
            need(0, "list [--state S] [--type T] [--output json]")?;
            Command::List { state: opts.state, kind: opts.kind, output }
        }
        "status" => {
            need(1, "status <name> [--output json]")?;
            Command::Status { name: rest[0].clone(), output }
        }
        "pause" => {
            Command::Pause { names: at_least_one("pause <name>... [--wait]")?, wait: opts.wait, output }
        }
        "resume" => {
            Command::Resume { names: at_least_one("resume <name>... [--wait]")?, wait: opts.wait, output }
        }
        "restart" => Command::Restart {
            names: at_least_one("restart <name>... [--only-failed] [--wait]")?,
            only_failed: opts.only_failed,
            wait: opts.wait,
            output,
        },
        "create" => {
            need(1, "create <file|-> [--dry-run] [--wait]")?;
            Command::Create { file: rest[0].clone(), dry_run: opts.dry_run, wait: opts.wait, output }
        }
        _ => {
            need(1, "update <file|-> [--dry-run] [--wait]")?;
            Command::Update { file: rest[0].clone(), dry_run: opts.dry_run, wait: opts.wait, output }
        }
    };
    Ok(Parsed::Cmd(global, command))
}

// ---- running ---------------------------------------------------------------------------

/// Runs one subcommand and returns the process exit code. Output goes to stdout, problems to stderr.
pub async fn run(global: Global, command: Command) -> i32 {
    match execute(global, command).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {}", clip::sanitize(&format!("{e:#}")));
            EXIT_ERROR
        }
    }
}

fn connect(g: &Global) -> Result<(String, Client)> {
    let (name, cluster) = if let Some(url) = &g.url {
        ("adhoc".to_string(), Cluster { url: url.clone(), ..Default::default() })
    } else {
        let path = g.config.clone().or_else(config::config_path);
        let cfg = config::load(path.as_ref())?;
        let name = g
            .cluster
            .clone()
            .or_else(|| cfg.initial_cluster())
            .ok_or_else(|| anyhow!("no cluster configured; use --url or --cluster"))?;
        let Some(cluster) = cfg.clusters.get(&name).cloned() else {
            bail!(
                "unknown cluster {name:?}; configured: {}",
                cfg.clusters.keys().cloned().collect::<Vec<_>>().join(", ")
            );
        };
        (name, cluster)
    };
    Ok((name, cluster.client()?))
}

fn healthy(c: &Connector) -> bool {
    c.state == "RUNNING" && c.tasks.iter().all(|t| t.state == "RUNNING")
}

fn kind_name(k: Kind) -> &'static str {
    match k {
        Kind::Source => "source",
        Kind::Sink => "sink",
        Kind::Unknown => "unknown",
    }
}

/// Text that may contain server-supplied content: secrets masked, control characters removed.
fn safe(s: &str) -> String {
    clip::sanitize(s)
}

fn connector_json(c: &Connector, detailed: bool) -> Value {
    let tasks: Vec<Value> = c
        .tasks
        .iter()
        .map(|t| {
            let mut v = json!({ "id": t.id, "state": t.state, "worker_id": t.worker_id });
            if let (true, Some(tr)) = (detailed, &t.trace) {
                v["trace"] = Value::String(safe(tr));
            }
            v
        })
        .collect();
    let mut v = json!({
        "name": c.name,
        "type": kind_name(c.kind),
        "state": c.state,
        "worker_id": c.worker_id,
        "plugin": c.class,
        "healthy": healthy(c),
        "failed": c.is_failed(),
        "tasks": { "running": c.running_tasks(), "total": c.tasks.len(), "list": tasks },
    });
    if detailed {
        if let Some(tr) = &c.trace {
            v["trace"] = Value::String(safe(tr));
        }
        v["config"] = Value::Object(templates::masked(&c.config));
    }
    v
}

fn print_json(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
}

async fn execute(global: Global, command: Command) -> Result<i32> {
    let (cluster, client) = connect(&global)?;
    match command {
        Command::List { state, kind, output } => list(&cluster, &client, state, kind, output).await,
        Command::Status { name, output } => status(&cluster, &client, &name, output).await,
        Command::Pause { names, wait, output } => {
            mutate(&cluster, &client, "pause", names, Expect::Paused, wait, output, |c, n| {
                Box::pin(async move { c.pause(&n).await })
            })
            .await
        }
        Command::Resume { names, wait, output } => {
            mutate(&cluster, &client, "resume", names, Expect::Running, wait, output, |c, n| {
                Box::pin(async move { c.resume(&n).await })
            })
            .await
        }
        Command::Restart { names, only_failed, wait, output } => {
            mutate(&cluster, &client, "restart", names, Expect::Restarted, wait, output, move |c, n| {
                Box::pin(async move { c.restart(&n, only_failed).await })
            })
            .await
        }
        Command::Create { file, dry_run, wait, output } => {
            apply_definition(&cluster, &client, &file, false, dry_run, wait, output).await
        }
        Command::Update { file, dry_run, wait, output } => {
            apply_definition(&cluster, &client, &file, true, dry_run, wait, output).await
        }
    }
}

async fn list(
    cluster: &str,
    client: &Client,
    state: Option<StateSel>,
    kind: Option<Kind>,
    output: Output,
) -> Result<i32> {
    let mut all = client.list().await?;
    all.retain(|c| state.is_none_or(|s| s.matches(c)) && kind.is_none_or(|k| c.kind == k));
    match output {
        Output::Json => print_json(&json!({
            "cluster": cluster,
            "connectors": all.iter().map(|c| connector_json(c, false)).collect::<Vec<_>>(),
        })),
        Output::Table => {
            let rows: Vec<[String; 5]> = all
                .iter()
                .map(|c| {
                    [
                        safe(&c.name),
                        c.kind.label().to_string(),
                        // A RUNNING connector with a failed task must not look healthy in a table.
                        if c.is_failed() && c.state != "FAILED" {
                            format!("{} (task failed)", safe(&c.state))
                        } else {
                            safe(&c.state)
                        },
                        format!("{}/{}", c.running_tasks(), c.tasks.len()),
                        safe(&c.class),
                    ]
                })
                .collect();
            print_table(["NAME", "TYPE", "STATE", "TASKS", "PLUGIN"], &rows);
        }
    }
    Ok(EXIT_OK)
}

fn print_table(head: [&str; 5], rows: &[[String; 5]]) {
    let mut widths = head.map(|h| h.chars().count());
    for r in rows {
        for (w, cell) in widths.iter_mut().zip(r) {
            *w = (*w).max(cell.chars().count());
        }
    }
    let line = |cells: [&str; 5]| {
        let mut out = String::new();
        for (i, (cell, w)) in cells.iter().zip(widths).enumerate() {
            if i + 1 == cells.len() {
                out.push_str(cell);
            } else {
                out.push_str(&format!("{cell:<w$}  "));
            }
        }
        println!("{}", out.trim_end());
    };
    line(head);
    for r in rows {
        line([&r[0], &r[1], &r[2], &r[3], &r[4]]);
    }
}

async fn status(cluster: &str, client: &Client, name: &str, output: Output) -> Result<i32> {
    crate::api::check_name(name)?;
    let all = client.list().await?;
    let Some(c) = all.iter().find(|c| c.name == name) else {
        bail!("connector {name:?} not found on cluster {cluster:?}");
    };
    match output {
        Output::Json => print_json(&json!({ "cluster": cluster, "connector": connector_json(c, true) })),
        Output::Table => {
            println!("name     {}", safe(&c.name));
            println!("cluster  {cluster}");
            println!("type     {}", c.kind.label());
            println!("state    {}", safe(&c.state));
            println!("worker   {}", safe(&c.worker_id));
            println!("plugin   {}", safe(&c.class));
            println!("tasks    {}/{} running", c.running_tasks(), c.tasks.len());
            for t in &c.tasks {
                println!("  task {:<3} {:<12} {}", t.id, safe(&t.state), safe(&t.worker_id));
            }
            if let Some(tr) = &c.trace {
                println!("\nconnector trace:\n{}", safe(tr));
            }
            for t in &c.tasks {
                if let Some(tr) = &t.trace {
                    println!("\ntask {} trace:\n{}", t.id, safe(tr));
                }
            }
        }
    }
    Ok(if healthy(c) { EXIT_OK } else { EXIT_UNHEALTHY })
}

// ---- verification ----------------------------------------------------------------------

struct Verdict {
    name: String,
    /// `confirmed`, `not_confirmed` or `not_checked`.
    status: &'static str,
    detail: String,
}

impl Verdict {
    fn ok(&self) -> bool {
        self.status != "not_confirmed"
    }
}

/// Polls the cluster until every watch has a verdict or the watch timeout passes.
async fn verify(client: &Client, watches: Vec<(String, Expect)>) -> Vec<Verdict> {
    let mut pending: Vec<Watch> = watches.into_iter().map(|(n, e)| Watch::new(n, e)).collect();
    let mut done: Vec<Verdict> = Vec::new();
    let start = Instant::now();
    let mut last_err: Option<String> = None;
    while !pending.is_empty() && start.elapsed() <= activity::WATCH_TIMEOUT + Duration::from_secs(1) {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let list = match client.list().await {
            Ok(l) => l,
            Err(e) => {
                last_err = Some(format!("{e:#}"));
                continue;
            }
        };
        let elapsed = start.elapsed();
        pending.retain_mut(|w| {
            let c = list.iter().find(|c| c.name == w.name);
            match activity::judge(w, c, elapsed) {
                Some((sev, detail)) => {
                    let status = if matches!(sev, Severity::Ok | Severity::Info) {
                        "confirmed"
                    } else {
                        "not_confirmed"
                    };
                    done.push(Verdict { name: w.name.clone(), status, detail: safe(&detail) });
                    false
                }
                None => true,
            }
        });
    }
    for w in pending {
        let why = last_err.clone().map(|e| format!(" (last error: {})", safe(&e))).unwrap_or_default();
        done.push(Verdict {
            name: w.name,
            status: "not_confirmed",
            detail: format!("no verdict within {}s{why}", activity::WATCH_TIMEOUT.as_secs()),
        });
    }
    done
}

type Call = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;

#[allow(clippy::too_many_arguments)]
async fn mutate(
    cluster: &str,
    client: &Client,
    verb: &str,
    names: Vec<String>,
    expect: Expect,
    wait: bool,
    output: Output,
    call: impl Fn(Client, String) -> Call,
) -> Result<i32> {
    let mut results: Vec<Value> = Vec::new();
    let mut accepted: Vec<String> = Vec::new();
    let mut errors = 0;
    for n in &names {
        match call(client.clone(), n.clone()).await {
            Ok(()) => accepted.push(n.clone()),
            Err(e) => {
                errors += 1;
                results.push(json!({ "name": n, "accepted": false, "error": safe(&format!("{e:#}")) }));
            }
        }
    }
    let verdicts = if wait && !accepted.is_empty() {
        verify(client, accepted.iter().map(|n| (n.clone(), expect)).collect()).await
    } else {
        Vec::new()
    };
    for n in &accepted {
        let (status, detail) = match verdicts.iter().find(|v| &v.name == n) {
            Some(v) => (v.status, v.detail.clone()),
            None => {
                ("not_checked", "accepted by Kafka Connect; the change was not verified (use --wait)".into())
            }
        };
        results.push(json!({ "name": n, "accepted": true, "verified": status, "detail": detail }));
    }
    let unconfirmed = verdicts.iter().any(|v| !v.ok());
    match output {
        Output::Json => print_json(&json!({ "cluster": cluster, "action": verb, "results": results })),
        Output::Table => {
            for r in &results {
                let n = r["name"].as_str().unwrap_or("");
                if r["accepted"] == false {
                    println!("{verb} {n}: FAILED — {}", r["error"].as_str().unwrap_or(""));
                } else {
                    let detail = r["detail"].as_str().unwrap_or("");
                    // The judge's sentence starts with the connector name, which is already on this line.
                    let detail = detail.strip_prefix(&format!("{n}: ")).unwrap_or(detail);
                    println!("{verb} {n}: {} — {detail}", r["verified"].as_str().unwrap_or(""));
                }
            }
        }
    }
    Ok(if errors > 0 {
        EXIT_ERROR
    } else if unconfirmed {
        EXIT_UNHEALTHY
    } else {
        EXIT_OK
    })
}

// ---- create / update --------------------------------------------------------------------

fn read_definition(file: &str) -> Result<(String, Map<String, Value>)> {
    let text = if file == "-" {
        let mut buf = Vec::new();
        std::io::stdin().take(MAX_INPUT_BYTES + 1).read_to_end(&mut buf)?;
        if buf.len() as u64 > MAX_INPUT_BYTES {
            bail!("stdin input is larger than {} MiB", MAX_INPUT_BYTES >> 20);
        }
        String::from_utf8(buf).map_err(|_| anyhow!("stdin is not valid UTF-8"))?
    } else {
        // A path, never inline JSON: this is for scripts, where a typo must not become a request.
        if file.trim_start().starts_with('{') {
            bail!("expected a file path or - (stdin), not inline JSON");
        }
        file.to_string()
    };
    crate::app::load_json(&text).map_err(|e| anyhow!(e))
}

#[allow(clippy::too_many_arguments)]
async fn apply_definition(
    cluster: &str,
    client: &Client,
    file: &str,
    update: bool,
    dry_run: bool,
    wait: bool,
    output: Output,
) -> Result<i32> {
    let (name, mut config) = read_definition(file)?;
    let existing = client.list().await?;
    let current = existing.iter().find(|c| c.name == name);
    let verb = if update { "update" } else { "create" };
    match (update, current) {
        (true, None) => bail!("cannot update: connector {name:?} does not exist on {cluster:?} (use create)"),
        (false, Some(_)) => {
            bail!("cannot create: connector {name:?} already exists on {cluster:?} (use update)")
        }
        _ => {}
    }

    let changes = current.map(|c| {
        let mut old = c.config.clone();
        old.remove("name");
        let mut new = config.clone();
        new.remove("name");
        diff::diff(&old, &new)
    });
    let is_source = current.is_some_and(|c| c.kind == Kind::Source);
    let warnings = changes.as_deref().map(|c| diff::replay_warnings(c, is_source)).unwrap_or_default();
    let unchanged = changes.as_ref().is_some_and(Vec::is_empty);

    let validation = client.validate(&name, &config).await;
    let (valid, problems, validation_note) = match &validation {
        Ok(p) => (Some(p.is_empty()), p.clone(), None),
        Err(e) => (None, Vec::new(), Some(safe(&format!("{e:#}")))),
    };

    let blocked = !problems.is_empty();
    let apply = !dry_run && !blocked && !unchanged;
    let mut verified: Vec<Verdict> = Vec::new();
    if apply {
        config.insert("name".into(), Value::String(name.clone()));
        if update {
            client.update_config(&name, &config).await?;
        } else {
            client.create(&name, &config).await?;
        }
        if wait {
            // A paused connector legitimately stays paused after a config update.
            let paused = current.is_some_and(Connector::is_paused);
            let expect = if update { Expect::Running } else { Expect::Created };
            if !paused {
                verified = verify(client, vec![(name.clone(), expect)]).await;
            }
        }
    }
    let unconfirmed = verified.iter().any(|v| !v.ok());

    match output {
        Output::Json => print_json(&json!({
            "cluster": cluster,
            "action": verb,
            "name": name,
            "dry_run": dry_run,
            "applied": apply,
            "unchanged": unchanged,
            "valid": valid,
            "validation_errors": problems.iter().map(|p| safe(p)).collect::<Vec<_>>(),
            "validation_unavailable": validation_note,
            "changes": changes.as_deref().map(|c| diff::lines(c).into_iter()
                .map(|(sign, text)| json!({ "sign": sign.to_string(), "text": text })).collect::<Vec<_>>()),
            "warnings": warnings.iter().map(|(k, n)| json!({ "key": k, "note": n })).collect::<Vec<_>>(),
            "verified": verified.first().map(|v| json!({ "status": v.status, "detail": v.detail })),
            "config": Value::Object(templates::masked(&config)),
        })),
        Output::Table => {
            println!("{verb} {name} on {cluster}{}", if dry_run { " (dry run, nothing sent)" } else { "" });
            match &changes {
                Some(c) if c.is_empty() => println!("no changes against the running config"),
                Some(c) => {
                    println!("changes ({}):", c.len());
                    for (sign, text) in diff::lines(c) {
                        println!("  {sign} {text}");
                    }
                }
                None => println!("{}", crate::app::preview_json(&name, &config)),
            }
            for (key, note) in &warnings {
                println!("warning: {key}: {note}");
            }
            if let Some(n) = &validation_note {
                println!("validation unavailable: {n}");
            } else if blocked {
                println!("validation FAILED:");
                for p in &problems {
                    println!("  {}", safe(p));
                }
            } else {
                println!("validation: ok");
            }
            if apply {
                println!("sent to Kafka Connect");
                for v in &verified {
                    let detail = v.detail.strip_prefix(&format!("{}: ", v.name)).unwrap_or(&v.detail);
                    println!("{}: {} — {detail}", v.name, v.status);
                }
                if verified.is_empty() {
                    println!("not verified (use --wait to confirm the connector becomes healthy)");
                }
            } else if !dry_run && unchanged {
                println!("nothing to do");
            }
        }
    }
    Ok(if blocked {
        EXIT_ERROR
    } else if unconfirmed {
        EXIT_UNHEALTHY
    } else {
        EXIT_OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(args: &[&str]) -> Result<Parsed, String> {
        parse(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn no_subcommand_starts_the_tui_with_global_flags() {
        let Parsed::Tui(g) =
            p(&["--no-splash", "-u", "http://h:1", "-c", "prod", "--config=/x.toml"]).unwrap()
        else {
            panic!()
        };
        assert_eq!(g.url.as_deref(), Some("http://h:1"));
        assert_eq!(g.cluster.as_deref(), Some("prod"));
        assert_eq!(g.config, Some(PathBuf::from("/x.toml")));
        assert!(g.no_splash);
        assert_eq!(p(&["--help"]).unwrap(), Parsed::Help);
        assert_eq!(p(&["-V"]).unwrap(), Parsed::Version);
    }

    #[test]
    fn parses_subcommands_and_flags_in_any_order() {
        let Parsed::Cmd(g, c) =
            p(&["list", "--url", "http://h", "--state", "FAILED", "--type=sink", "-o", "json"]).unwrap()
        else {
            panic!()
        };
        assert_eq!(g.url.as_deref(), Some("http://h"));
        assert_eq!(
            c,
            Command::List { state: Some(StateSel::Failed), kind: Some(Kind::Sink), output: Output::Json }
        );
        let Parsed::Cmd(_, c) = p(&["restart", "a", "b", "--only-failed", "--wait"]).unwrap() else {
            panic!()
        };
        assert_eq!(
            c,
            Command::Restart {
                names: vec!["a".into(), "b".into()],
                only_failed: true,
                wait: true,
                output: Output::Table
            }
        );
        let Parsed::Cmd(_, c) = p(&["update", "--dry-run", "-"]).unwrap() else { panic!() };
        assert_eq!(
            c,
            Command::Update { file: "-".into(), dry_run: true, wait: false, output: Output::Table }
        );
    }

    #[test]
    fn usage_errors_are_specific() {
        for (args, needle) in [
            (vec!["frobnicate"], "unknown command"),
            (vec!["list", "extra"], "usage: tuizium list"),
            (vec!["status"], "usage: tuizium status"),
            (vec!["pause"], "usage: tuizium pause"),
            (vec!["list", "--dry-run"], "not valid for `list`"),
            (vec!["pause", "a", "--only-failed"], "not valid for `pause`"),
            (vec!["--output", "json"], "needs a subcommand"),
            (vec!["list", "--output", "xml"], "table or json"),
            (vec!["list", "--state", "weird"], "--state must be"),
            (vec!["--bogus"], "unknown argument"),
            (vec!["--url"], "requires a value"),
            (vec!["--url", "list", "--state", "failed"], "--url expects an http://"),
            (vec!["--no-splash=1"], "does not take a value"),
        ] {
            let e = p(&args).unwrap_err();
            assert!(e.contains(needle), "{args:?}: {e}");
        }
    }

    #[test]
    fn inline_json_is_rejected_as_a_file_argument() {
        let e = read_definition(r#"{"name":"x","connector.class":"A"}"#).unwrap_err().to_string();
        assert!(e.contains("not inline JSON"), "{e}");
    }

    #[test]
    fn connector_json_masks_secrets_and_only_details_carry_traces() {
        use crate::api::Task;
        let mut config = Map::new();
        config.insert("database.password".into(), "hunter2".into());
        config.insert("connector.class".into(), "x.Y".into());
        let c = Connector {
            name: "c".into(),
            kind: Kind::Source,
            state: "RUNNING".into(),
            worker_id: "w:1".into(),
            class: "x.Y".into(),
            config,
            tasks: vec![Task {
                id: 0,
                state: "FAILED".into(),
                worker_id: "w:1".into(),
                trace: Some("boom jdbc:x?password=hunter2\u{1b}[31m".into()),
            }],
            trace: None,
        };
        let short = connector_json(&c, false).to_string();
        assert!(!short.contains("trace") && !short.contains("config"));
        let long = connector_json(&c, true).to_string();
        assert!(!long.contains("hunter2") && !long.contains("\\u001b"), "{long}");
        assert!(long.contains("\"healthy\":false") && long.contains("\"failed\":true"));
    }
}
