//! End-to-end tests of the non-interactive subcommands against a small fake Kafka Connect.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    /// name -> (connector state, task state, config JSON)
    connectors: BTreeMap<String, (String, String, String)>,
    requests: Vec<String>,
}

struct Fake {
    url: String,
    state: Arc<Mutex<State>>,
}

impl Fake {
    fn requests(&self) -> Vec<String> {
        self.state.lock().unwrap().requests.clone()
    }
}

fn connector_json(name: &str, state: &str, task: &str, config: &str, trace: bool) -> String {
    let t = if trace {
        r#","trace":"java.lang.Boom: jdbc:x?user=u&password=hunter2\n\tat Foo.bar(Foo.java:1)""#
    } else {
        ""
    };
    format!(
        r#""{name}":{{"info":{{"name":"{name}","type":"sink","config":{config}}},"status":{{"name":"{name}","type":"sink","connector":{{"state":"{state}","worker_id":"w:8083"}},"tasks":[{{"id":0,"state":"{task}","worker_id":"w:8083"{t}}}]}}}}"#
    )
}

fn fake_connect() -> Fake {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(State::default()));
    {
        let mut s = state.lock().unwrap();
        let cfg =
            r#"{"connector.class":"x.Sink","topics":"t1","tasks.max":"1","database.password":"hunter2"}"#;
        s.connectors.insert("a".into(), ("RUNNING".into(), "RUNNING".into(), cfg.into()));
        s.connectors.insert("f".into(), ("RUNNING".into(), "FAILED".into(), cfg.into()));
        s.connectors.insert("p".into(), ("PAUSED".into(), "PAUSED".into(), cfg.into()));
    }
    let shared = state.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            let state = shared.clone();
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let (head_end, body_len) = loop {
                    let n = s.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..p]).to_ascii_lowercase();
                        let len = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        break (p + 4, len);
                    }
                };
                while buf.len() < head_end + body_len {
                    let n = s.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let text = String::from_utf8_lossy(&buf).to_string();
                let line = text.lines().next().unwrap_or("").to_string();
                let body = text[head_end.min(text.len())..].to_string();
                let mut parts = line.split_whitespace();
                let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
                let path = target.split('?').next().unwrap_or("").to_string();
                let mut st = state.lock().unwrap();
                st.requests.push(format!("{method} {path}"));
                let (code, reply) = match (method, path.as_str()) {
                    ("GET", "/connectors") => {
                        let all: Vec<String> = st
                            .connectors
                            .iter()
                            .map(|(n, (c, t, cfg))| connector_json(n, c, t, cfg, t == "FAILED"))
                            .collect();
                        (200, format!("{{{}}}", all.join(",")))
                    }
                    ("PUT", p) if p.ends_with("/pause") => {
                        let name = p.trim_start_matches("/connectors/").trim_end_matches("/pause");
                        match st.connectors.get_mut(name) {
                            Some(c) => {
                                c.0 = "PAUSED".into();
                                c.1 = "PAUSED".into();
                                (202, String::new())
                            }
                            None => (404, r#"{"error_code":404,"message":"Connector not found"}"#.into()),
                        }
                    }
                    ("PUT", p) if p.contains("/config/validate") => {
                        if body.contains(r#""bad""#) {
                            let r = r#"{"configs":[{"value":{"name":"topics","errors":["Missing required configuration \"topics\""]}}]}"#;
                            (200, r.into())
                        } else {
                            (200, r#"{"configs":[]}"#.into())
                        }
                    }
                    ("PUT", p) if p.ends_with("/config") => {
                        let name =
                            p.trim_start_matches("/connectors/").trim_end_matches("/config").to_string();
                        if let Some(c) = st.connectors.get_mut(&name) {
                            c.2 = body.clone();
                        }
                        (200, "{}".into())
                    }
                    ("POST", "/connectors") => {
                        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                        let name = v["name"].as_str().unwrap().to_string();
                        st.connectors
                            .insert(name, ("RUNNING".into(), "RUNNING".into(), v["config"].to_string()));
                        (201, "{}".into())
                    }
                    _ => (404, r#"{"error_code":404,"message":"nope"}"#.into()),
                };
                drop(st);
                let resp = format!(
                    "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                let _ = s.write_all(resp.as_bytes());
            });
        }
    });
    Fake { url, state }
}

struct Out {
    code: i32,
    stdout: String,
    stderr: String,
}

fn tuizium(fake: &Fake, args: &[&str]) -> Out {
    let o = Command::new(env!("CARGO_BIN_EXE_tuizium"))
        .args(["--url", &fake.url])
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("run tuizium");
    Out {
        code: o.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
    }
}

fn temp_json(tag: &str, json: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("tuizium-cli-test-{}-{tag}.json", std::process::id()));
    std::fs::write(&p, json).unwrap();
    p
}

fn no_escape_codes(o: &Out) {
    assert!(!o.stdout.contains('\x1b') && !o.stderr.contains('\x1b'), "escape codes in output");
}

#[test]
fn list_table_and_json() {
    let f = fake_connect();
    let o = tuizium(&f, &["list"]);
    assert_eq!(o.code, 0, "{}", o.stderr);
    no_escape_codes(&o);
    assert!(o.stdout.starts_with("NAME"), "{}", o.stdout);
    assert!(o.stdout.contains("PAUSED") && o.stdout.contains("x.Sink"));
    let failed_row = o.stdout.lines().find(|l| l.starts_with("f ")).expect("row for f");
    assert!(
        failed_row.contains("RUNNING (task failed)"),
        "a failed task must show in the table: {failed_row}"
    );

    let o = tuizium(&f, &["list", "--output", "json"]);
    let v: serde_json::Value = serde_json::from_str(&o.stdout).unwrap();
    assert_eq!(v["cluster"], "adhoc");
    assert_eq!(v["connectors"].as_array().unwrap().len(), 3);
    assert_eq!(v["connectors"][0]["name"], "a");
    assert_eq!(v["connectors"][0]["healthy"], true);
    assert_eq!(v["connectors"][1]["failed"], true);
    assert!(!o.stdout.contains("hunter2"), "list must not print config or traces");

    let o = tuizium(&f, &["list", "--state", "failed", "-o", "json"]);
    let v: serde_json::Value = serde_json::from_str(&o.stdout).unwrap();
    assert_eq!(v["connectors"].as_array().unwrap().len(), 1);
    assert_eq!(v["connectors"][0]["name"], "f");
}

#[test]
fn status_reports_health_in_the_exit_code_and_masks_secrets() {
    let f = fake_connect();
    let ok = tuizium(&f, &["status", "a"]);
    assert_eq!(ok.code, 0, "{}", ok.stderr);

    let bad = tuizium(&f, &["status", "f", "--output", "json"]);
    assert_eq!(bad.code, 3);
    no_escape_codes(&bad);
    assert!(!bad.stdout.contains("hunter2"), "{}", bad.stdout);
    let v: serde_json::Value = serde_json::from_str(&bad.stdout).unwrap();
    assert!(v["connector"]["tasks"]["list"][0]["trace"].as_str().unwrap().contains("Boom"));
    assert_eq!(v["connector"]["config"]["database.password"], "********");

    let text = tuizium(&f, &["status", "f"]);
    assert_eq!(text.code, 3);
    assert!(!text.stdout.contains("hunter2") && text.stdout.contains("Boom"));

    let missing = tuizium(&f, &["status", "nope"]);
    assert_eq!(missing.code, 1);
    assert!(missing.stderr.contains("not found"), "{}", missing.stderr);
}

#[test]
fn pause_reports_unverified_then_confirmed_with_wait() {
    let f = fake_connect();
    let o = tuizium(&f, &["pause", "a"]);
    assert_eq!(o.code, 0, "{}", o.stderr);
    assert!(o.stdout.contains("not_checked") && o.stdout.contains("not verified"), "{}", o.stdout);

    let f = fake_connect();
    let o = tuizium(&f, &["pause", "a", "--wait", "--output", "json"]);
    assert_eq!(o.code, 0, "{} / {}", o.stdout, o.stderr);
    let v: serde_json::Value = serde_json::from_str(&o.stdout).unwrap();
    assert_eq!(v["results"][0]["verified"], "confirmed");
    assert!(f.requests().contains(&"PUT /connectors/a/pause".to_string()));

    let o = tuizium(&f, &["pause", "nope", "a"]);
    assert_eq!(o.code, 1, "a failing name makes the whole command fail");
    assert!(
        o.stdout.contains("FAILED") && o.stdout.contains("not found") || o.stdout.contains("404"),
        "{}",
        o.stdout
    );
}

#[test]
fn update_dry_run_sends_nothing_and_shows_a_masked_diff_with_heads_up() {
    let f = fake_connect();
    let file = temp_json(
        "upd1",
        r#"{"name":"a","config":{"connector.class":"x.Sink","topics":"t1,t2","tasks.max":"2","database.password":"new-pass"}}"#,
    );
    let o = tuizium(&f, &["update", file.to_str().unwrap(), "--dry-run"]);
    assert_eq!(o.code, 0, "{}", o.stderr);
    no_escape_codes(&o);
    assert!(o.stdout.contains("dry run"), "{}", o.stdout);
    assert!(o.stdout.contains("~ topics: t1  →  t1,t2"), "{}", o.stdout);
    assert!(o.stdout.contains("warning: topics:"), "{}", o.stdout);
    assert!(!o.stdout.contains("new-pass") && !o.stdout.contains("hunter2"), "{}", o.stdout);
    assert!(
        !f.requests().iter().any(|r| r.starts_with("PUT /connectors/a/config")),
        "dry run must not update: {:?}",
        f.requests()
    );

    let o = tuizium(&f, &["update", file.to_str().unwrap()]);
    assert_eq!(o.code, 0, "{}", o.stderr);
    assert!(o.stdout.contains("sent to Kafka Connect"));
    assert!(f.requests().contains(&"PUT /connectors/a/config".to_string()));
    let _ = std::fs::remove_file(file);
}

#[test]
fn update_refuses_unknown_connectors_invalid_configs_and_noops() {
    let f = fake_connect();
    let ghost = temp_json("upd2", r#"{"name":"ghost","config":{"connector.class":"x.Sink"}}"#);
    let o = tuizium(&f, &["update", ghost.to_str().unwrap()]);
    assert_eq!(o.code, 1);
    assert!(o.stderr.contains("does not exist"), "{}", o.stderr);

    let invalid = temp_json("upd3", r#"{"name":"a","config":{"connector.class":"x.Sink","bad":"1"}}"#);
    let o = tuizium(&f, &["update", invalid.to_str().unwrap()]);
    assert_eq!(o.code, 1);
    assert!(o.stdout.contains("validation FAILED") && o.stdout.contains("Missing required"), "{}", o.stdout);
    assert!(!f.requests().iter().any(|r| r.contains("/config") && r.starts_with("PUT /connectors/")));

    let same = temp_json(
        "upd4",
        r#"{"name":"a","config":{"connector.class":"x.Sink","topics":"t1","tasks.max":"1","database.password":"hunter2"}}"#,
    );
    let o = tuizium(&f, &["update", same.to_str().unwrap()]);
    assert_eq!(o.code, 0);
    assert!(o.stdout.contains("no changes"), "{}", o.stdout);
    assert!(!f.requests().iter().any(|r| r == "PUT /connectors/a/config"));
    for p in [ghost, invalid, same] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn create_dry_run_and_real_create() {
    let f = fake_connect();
    let new = temp_json(
        "crt1",
        r#"{"name":"fresh","config":{"connector.class":"x.Sink","topics":"t","database.password":"s3cret"}}"#,
    );
    let o = tuizium(&f, &["create", new.to_str().unwrap(), "--dry-run", "--output", "json"]);
    assert_eq!(o.code, 0, "{} / {}", o.stdout, o.stderr);
    assert!(!o.stdout.contains("s3cret"), "{}", o.stdout);
    let v: serde_json::Value = serde_json::from_str(&o.stdout).unwrap();
    assert_eq!(
        (v["dry_run"].clone(), v["applied"].clone(), v["valid"].clone()),
        (true.into(), false.into(), true.into())
    );
    assert!(!f.requests().contains(&"POST /connectors".to_string()));

    let o = tuizium(&f, &["create", new.to_str().unwrap(), "--wait"]);
    assert_eq!(o.code, 0, "{} / {}", o.stdout, o.stderr);
    assert!(f.requests().contains(&"POST /connectors".to_string()));
    assert!(o.stdout.contains("fresh: confirmed"), "{}", o.stdout);

    let o = tuizium(&f, &["create", new.to_str().unwrap()]);
    assert_eq!(o.code, 1);
    assert!(o.stderr.contains("already exists"), "{}", o.stderr);

    let o = tuizium(&f, &["create", r#"{"name":"x","connector.class":"A"}"#]);
    assert_eq!(o.code, 1);
    assert!(o.stderr.contains("not inline JSON"), "{}", o.stderr);
    let _ = std::fs::remove_file(new);
}

#[test]
fn usage_errors_and_unreachable_clusters_have_distinct_exit_codes() {
    let f = fake_connect();
    let o = tuizium(&f, &["frobnicate"]);
    assert_eq!(o.code, 2);
    assert!(o.stderr.contains("unknown command"));
    let o = tuizium(&f, &["list", "--dry-run"]);
    assert_eq!(o.code, 2);

    let o = Command::new(env!("CARGO_BIN_EXE_tuizium"))
        .args(["--url", "http://127.0.0.1:9", "list"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("cannot connect"));
}
