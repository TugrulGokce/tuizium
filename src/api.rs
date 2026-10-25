//! Thin async client for the Kafka Connect REST API
//! (<https://kafka.apache.org/documentation/#connect_rest>).

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use reqwest::{RequestBuilder, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Auth {
    None,
    Basic { user: String, password: String },
    Bearer(String),
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: Url,
    auth: Auth,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RootInfo {
    #[serde(default)]
    pub version: String,
}

impl RootInfo {
    /// `(major, minor)` parsed from the `GET /` version string, e.g. `3.9.0` or `3.5.1-SNAPSHOT`.
    pub fn version_pair(&self) -> Option<(u32, u32)> {
        let mut parts = self.version.trim().split('.');
        let major = parts.next()?.parse().ok()?;
        let minor =
            parts.next()?.chars().take_while(char::is_ascii_digit).collect::<String>().parse().ok()?;
        Some((major, minor))
    }

    /// Whether `PUT /connectors/{name}/stop` exists (added in Kafka Connect 3.5, KIP-875).
    /// An unknown version counts as unsupported rather than guessing.
    pub fn supports_stop(&self) -> bool {
        self.version_pair().is_some_and(|v| v >= (3, 5))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Source,
    Sink,
    Unknown,
}

impl Kind {
    pub fn label(self) -> &'static str {
        match self {
            Kind::Source => "SOURCE",
            Kind::Sink => "SINK",
            Kind::Unknown => "?",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Task {
    pub id: i64,
    pub state: String,
    pub worker_id: String,
    pub trace: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Connector {
    pub name: String,
    pub kind: Kind,
    pub state: String,
    pub worker_id: String,
    pub class: String,
    pub config: Map<String, Value>,
    pub tasks: Vec<Task>,
    pub trace: Option<String>,
}

impl Connector {
    pub fn running_tasks(&self) -> usize {
        self.tasks.iter().filter(|t| t.state == "RUNNING").count()
    }

    pub fn is_failed(&self) -> bool {
        self.state == "FAILED" || self.tasks.iter().any(|t| t.state == "FAILED")
    }

    pub fn is_paused(&self) -> bool {
        matches!(self.state.as_str(), "PAUSED" | "STOPPED")
    }
}

#[derive(Debug, Default)]
pub struct Snapshot {
    pub root: Option<RootInfo>,
    pub connectors: Vec<Connector>,
    /// Installed connector plugins; `None` when not requested or unavailable.
    pub plugins: Option<Vec<Plugin>>,
}

/// A connector plugin installed on the Connect workers (`GET /connector-plugins`).
#[derive(Clone, Debug, Deserialize)]
pub struct Plugin {
    pub class: String,
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub version: String,
}

impl Plugin {
    pub fn is_debezium(&self) -> bool {
        self.class.starts_with("io.debezium.")
    }
}

/// Extracts `field: message` lines from a `.../config/validate` response.
pub fn parse_validation(body: &str) -> Result<Vec<String>> {
    let v: Value = serde_json::from_str(body).map_err(|e| anyhow!("unexpected validation response: {e}"))?;
    let mut out = Vec::new();
    for cfg in v.get("configs").and_then(Value::as_array).into_iter().flatten() {
        let value = &cfg["value"];
        let field = value.get("name").and_then(Value::as_str).unwrap_or("?");
        for e in value.get("errors").and_then(Value::as_array).into_iter().flatten() {
            if let Some(e) = e.as_str() {
                out.push(format!("{field}: {e}"));
            }
        }
    }
    Ok(out)
}

// ---- wire models -------------------------------------------------------

#[derive(Deserialize)]
struct Expanded {
    status: Option<RawStatus>,
    info: Option<RawInfo>,
}

#[derive(Deserialize)]
struct RawStatus {
    connector: RawConnState,
    #[serde(default)]
    tasks: Vec<RawTask>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct RawConnState {
    #[serde(default)]
    state: String,
    worker_id: Option<String>,
    trace: Option<String>,
}

#[derive(Deserialize)]
struct RawTask {
    id: i64,
    #[serde(default)]
    state: String,
    worker_id: Option<String>,
    trace: Option<String>,
}

#[derive(Deserialize)]
struct RawInfo {
    #[serde(default)]
    config: Map<String, Value>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

fn parse_kind(s: Option<&str>, class: &str) -> Kind {
    match s.map(str::to_ascii_lowercase).as_deref() {
        Some("source") => Kind::Source,
        Some("sink") => Kind::Sink,
        _ if class.contains("Source") => Kind::Source,
        _ if class.contains("Sink") => Kind::Sink,
        _ => Kind::Unknown,
    }
}

/// Parses the body of `GET /connectors?expand=status&expand=info`.
pub fn parse_connectors(body: &str) -> Result<Vec<Connector>> {
    let raw: BTreeMap<String, Expanded> =
        serde_json::from_str(body).map_err(|e| anyhow!("unexpected response from /connectors: {e}"))?;
    Ok(raw
        .into_iter()
        .map(|(name, e)| {
            let config = e.info.as_ref().map(|i| i.config.clone()).unwrap_or_default();
            let class = config.get("connector.class").and_then(Value::as_str).unwrap_or("").to_string();
            let kind_str = e
                .status
                .as_ref()
                .and_then(|s| s.kind.clone())
                .or_else(|| e.info.as_ref().and_then(|i| i.kind.clone()));
            let kind = parse_kind(kind_str.as_deref(), &class);
            let (state, worker_id, trace, tasks) = match e.status {
                Some(s) => (
                    s.connector.state,
                    s.connector.worker_id.unwrap_or_default(),
                    s.connector.trace,
                    s.tasks
                        .into_iter()
                        .map(|t| Task {
                            id: t.id,
                            state: t.state,
                            worker_id: t.worker_id.unwrap_or_default(),
                            trace: t.trace,
                        })
                        .collect(),
                ),
                None => ("UNKNOWN".into(), String::new(), None, Vec::new()),
            };
            Connector { name, kind, state, worker_id, class, config, tasks, trace }
        })
        .collect())
}

/// Accepts either `{"name": .., "config": {..}}` or a flat config map that
/// contains a `name` key, and returns `(name, config)`.
pub fn normalize_create(v: Value) -> Result<(String, Map<String, Value>)> {
    let Value::Object(mut obj) = v else {
        bail!("connector JSON must be an object");
    };
    if let Some(Value::Object(cfg)) = obj.get("config") {
        let cfg = cfg.clone();
        let name = obj
            .get("name")
            .and_then(Value::as_str)
            .or_else(|| cfg.get("name").and_then(Value::as_str))
            .map(str::to_string);
        return match name {
            Some(n) if !n.trim().is_empty() => {
                check_name(&n)?;
                Ok((n, cfg))
            }
            _ => bail!("missing connector \"name\""),
        };
    }
    match obj.get("name").and_then(Value::as_str) {
        Some(n) if !n.trim().is_empty() => {
            check_name(n)?;
            let n = n.to_string();
            obj.remove("name");
            Ok((n, obj))
        }
        _ => bail!(
            "missing connector \"name\" (expected {{\"name\":..,\"config\":{{..}}}} or a flat config with \"name\")"
        ),
    }
}

fn describe_transport(e: &reqwest::Error, base: &Url) -> String {
    if e.is_timeout() {
        format!("request to {base} timed out")
    } else if e.is_connect() {
        format!("cannot connect to {base} (is Kafka Connect running and reachable?)")
    } else if e.is_decode() {
        format!("invalid response from {base}: {e}")
    } else {
        format!("request to {base} failed: {e}")
    }
}

/// Upper bound for any response body; a misbehaving or hostile endpoint must not
/// be able to exhaust memory.
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Error bodies (often whole HTML pages from proxies) are cut to this many characters.
const MAX_ERROR_CHARS: usize = 500;
const MAX_REDIRECTS: usize = 5;

fn clip(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(max).collect::<String>())
    }
}

/// Only follow a redirect that stays on the same host/port and never downgrades
/// from https to http; this keeps `Authorization` from being re-sent elsewhere.
fn safe_redirect(from: &Url, to: &Url) -> bool {
    let upgrade =
        from.scheme() == "http" && to.scheme() == "https" && from.port().is_none() && to.port().is_none();
    from.host_str() == to.host_str()
        && (upgrade || from.port_or_known_default() == to.port_or_known_default())
        && !(from.scheme() == "https" && to.scheme() == "http")
}

/// Names are interpolated into URL paths (percent-encoded), but `.`/`..` segments
/// would still be normalised away by URL parsing and could address other endpoints.
pub fn check_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("connector name is empty");
    }
    if name == "." || name == ".." {
        bail!("{name:?} is not a valid connector name");
    }
    if name.chars().any(char::is_control) {
        bail!("connector name must not contain control characters");
    }
    if name.len() > 255 {
        bail!("connector name is longer than 255 bytes");
    }
    Ok(())
}

fn describe_http(status: StatusCode, body: &str) -> String {
    let msg = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| body.trim().to_string());
    let msg = clip(&crate::templates::mask_text(&msg), MAX_ERROR_CHARS);
    let hint = match status {
        StatusCode::UNAUTHORIZED => " — check credentials (password_env / token_env)",
        StatusCode::FORBIDDEN => " — the account is not allowed to do this",
        StatusCode::NOT_FOUND => " — not found (was it deleted?)",
        StatusCode::CONFLICT => {
            " — conflict: it may already exist or a rebalance is in progress; retry shortly"
        }
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::BAD_GATEWAY => " — worker unavailable, retry shortly",
        _ => "",
    };
    if msg.is_empty() {
        format!("HTTP {}{hint}", status.as_u16())
    } else {
        format!("HTTP {}: {msg}{hint}", status.as_u16())
    }
}

impl Client {
    pub fn new(base_url: &str, auth: Auth, timeout: Duration) -> Result<Self> {
        let mut base = Url::parse(base_url).map_err(|e| anyhow!("invalid cluster url {base_url:?}: {e}"))?;
        if !matches!(base.scheme(), "http" | "https") {
            bail!("cluster url must start with http:// or https://");
        }
        if base.cannot_be_a_base() {
            bail!("invalid cluster url {base_url:?}");
        }
        // Credentials belong in the config/env, never in the URL (which gets saved and displayed).
        if !base.username().is_empty() || base.password().is_some() {
            bail!("the cluster url must not contain credentials; use username + password_env, or token_env");
        }
        base.set_query(None);
        base.set_fragment(None);
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(Duration::from_secs(5))
            .user_agent(concat!("tuizium/", env!("CARGO_PKG_VERSION")))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= MAX_REDIRECTS {
                    attempt.error("too many redirects")
                } else if attempt.previous().last().is_some_and(|prev| safe_redirect(prev, attempt.url())) {
                    attempt.follow()
                } else {
                    attempt.stop()
                }
            }))
            .build()?;
        Ok(Self { http, base, auth })
    }

    /// `true` when credentials would travel over plain http to a non-local host.
    pub fn sends_credentials_in_clear(&self) -> bool {
        let host = self.base.host_str().unwrap_or("");
        let local = host.eq_ignore_ascii_case("localhost")
            || host.trim_matches(['[', ']']).parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
        self.auth != Auth::None && self.base.scheme() == "http" && !local
    }

    fn url(&self, segs: &[&str]) -> Url {
        let mut u = self.base.clone();
        if let Ok(mut p) = u.path_segments_mut() {
            p.pop_if_empty();
            p.extend(segs);
        }
        u
    }

    async fn send(&self, rb: RequestBuilder) -> Result<String> {
        let rb = match &self.auth {
            Auth::None => rb,
            Auth::Basic { user, password } => rb.basic_auth(user, Some(password)),
            Auth::Bearer(t) => rb.bearer_auth(t),
        };
        let mut resp = rb.send().await.map_err(|e| anyhow!(describe_transport(&e, &self.base)))?;
        let status = resp.status();
        if resp.content_length().is_some_and(|n| n > MAX_BODY_BYTES as u64) {
            bail!(
                "response from {} is larger than {} MiB; refusing to read it",
                self.base,
                MAX_BODY_BYTES >> 20
            );
        }
        let mut buf: Vec<u8> = Vec::new();
        loop {
            match resp.chunk().await {
                Ok(Some(chunk)) => {
                    if buf.len() + chunk.len() > MAX_BODY_BYTES {
                        bail!("response from {} exceeded {} MiB; aborted", self.base, MAX_BODY_BYTES >> 20);
                    }
                    buf.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                // A truncated error body is still worth reporting; a truncated success is not.
                Err(_) if !status.is_success() => break,
                Err(e) => bail!(describe_transport(&e, &self.base)),
            }
        }
        let body = String::from_utf8_lossy(&buf).into_owned();
        if status.is_success() {
            Ok(body)
        } else {
            tracing::warn!(%status, body = %clip(&body, MAX_ERROR_CHARS), "kafka connect returned an error");
            Err(anyhow!(describe_http(status, &body)))
        }
    }

    pub async fn root(&self) -> Result<RootInfo> {
        let body = self.send(self.http.get(self.url(&[]))).await?;
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn list(&self) -> Result<Vec<Connector>> {
        let rb = self.http.get(self.url(&["connectors"])).query(&[("expand", "status"), ("expand", "info")]);
        parse_connectors(&self.send(rb).await?)
    }

    pub async fn plugins(&self) -> Result<Vec<Plugin>> {
        let body = self.send(self.http.get(self.url(&["connector-plugins"]))).await?;
        serde_json::from_str(&body).map_err(|e| anyhow!("unexpected response from /connector-plugins: {e}"))
    }

    pub async fn snapshot(&self, want_plugins: bool) -> Result<Snapshot> {
        let plugins = async { if want_plugins { self.plugins().await.ok() } else { None } };
        let (root, list, plugins) = tokio::join!(self.root(), self.list(), plugins);
        Ok(Snapshot { root: root.ok(), connectors: list?, plugins })
    }

    /// `PUT /connector-plugins/{class}/config/validate`; returns the list of problems.
    pub async fn validate(&self, name: &str, config: &Map<String, Value>) -> Result<Vec<String>> {
        let class = config
            .get("connector.class")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("connector.class is required"))?;
        let mut cfg = config.clone();
        cfg.entry("name").or_insert_with(|| Value::String(name.into()));
        let rb = self.http.put(self.url(&["connector-plugins", class, "config", "validate"])).json(&cfg);
        parse_validation(&self.send(rb).await?)
    }

    /// `PUT /connectors/{name}/config` — updates an existing connector's configuration.
    pub async fn update_config(&self, name: &str, config: &Map<String, Value>) -> Result<()> {
        check_name(name)?;
        self.send(self.http.put(self.url(&["connectors", name, "config"])).json(config)).await?;
        Ok(())
    }

    pub async fn pause(&self, name: &str) -> Result<()> {
        check_name(name)?;
        self.send(self.http.put(self.url(&["connectors", name, "pause"]))).await?;
        Ok(())
    }

    /// `PUT /connectors/{name}/stop` (Connect 3.5+): stops the connector and removes its tasks.
    pub async fn stop(&self, name: &str) -> Result<()> {
        check_name(name)?;
        self.send(self.http.put(self.url(&["connectors", name, "stop"]))).await?;
        Ok(())
    }

    pub async fn resume(&self, name: &str) -> Result<()> {
        check_name(name)?;
        self.send(self.http.put(self.url(&["connectors", name, "resume"]))).await?;
        Ok(())
    }

    /// Restarts the connector and all of its tasks.
    pub async fn restart(&self, name: &str, only_failed: bool) -> Result<()> {
        check_name(name)?;
        let only_failed = if only_failed { "true" } else { "false" };
        let rb = self
            .http
            .post(self.url(&["connectors", name, "restart"]))
            .query(&[("includeTasks", "true"), ("onlyFailed", only_failed)]);
        self.send(rb).await?;
        Ok(())
    }

    pub async fn restart_task(&self, name: &str, id: i64) -> Result<()> {
        check_name(name)?;
        let id = id.to_string();
        self.send(self.http.post(self.url(&["connectors", name, "tasks", &id, "restart"]))).await?;
        Ok(())
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        check_name(name)?;
        self.send(self.http.delete(self.url(&["connectors", name]))).await?;
        Ok(())
    }

    pub async fn create(&self, name: &str, config: &Map<String, Value>) -> Result<()> {
        check_name(name)?;
        let body = serde_json::json!({ "name": name, "config": config });
        self.send(self.http.post(self.url(&["connectors"])).json(&body)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const LIST: &str = r#"{
      "pg-orders": {
        "info": {"name":"pg-orders","type":"source","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","tasks.max":"1"},"tasks":[{"connector":"pg-orders","task":0}]},
        "status": {"name":"pg-orders","connector":{"state":"RUNNING","worker_id":"w1:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"w1:8083"}],"type":"source"}
      },
      "es-sink": {
        "info": {"name":"es-sink","type":"sink","config":{"connector.class":"io.confluent.connect.elasticsearch.ElasticsearchSinkConnector"},"tasks":[]},
        "status": {"name":"es-sink","connector":{"state":"RUNNING","worker_id":"w2:8083"},"tasks":[{"id":0,"state":"FAILED","worker_id":"w2:8083","trace":"java.lang.Boom\n\tat x"},{"id":1,"state":"RUNNING","worker_id":"w2:8083"}],"type":"sink"}
      }
    }"#;

    #[test]
    fn parses_expanded_listing() {
        let list = parse_connectors(LIST).unwrap();
        assert_eq!(list.len(), 2);
        let es = &list[0];
        assert_eq!(es.name, "es-sink");
        assert_eq!(es.kind, Kind::Sink);
        assert_eq!(es.running_tasks(), 1);
        assert!(es.is_failed());
        assert!(es.tasks[0].trace.as_deref().unwrap().starts_with("java.lang.Boom"));
        let pg = &list[1];
        assert_eq!(pg.kind, Kind::Source);
        assert!(pg.class.ends_with("PostgresConnector"));
        assert!(!pg.is_failed());
    }

    #[test]
    fn normalizes_create_payloads() {
        let (n, c) = normalize_create(serde_json::json!({"name":"a","config":{"x":"1"}})).unwrap();
        assert_eq!((n.as_str(), c.len()), ("a", 1));
        let (n, c) = normalize_create(serde_json::json!({"name":"b","connector.class":"X"})).unwrap();
        assert_eq!(n, "b");
        assert!(!c.contains_key("name"));
        assert!(normalize_create(serde_json::json!({"connector.class":"X"})).is_err());
        assert!(normalize_create(serde_json::json!([1])).is_err());
    }

    /// Minimal one-shot-per-connection HTTP server that records request heads.
    async fn mock(status: u16, body: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else { break };
                let seen = seen2.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    let text = String::from_utf8_lossy(&buf[..n]).to_string();
                    seen.lock().unwrap().push(text);
                    let resp = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}"), seen)
    }

    fn client(url: &str, auth: Auth) -> Client {
        Client::new(url, auth, Duration::from_secs(5)).unwrap()
    }

    #[tokio::test]
    async fn actions_hit_official_endpoints() {
        let (url, seen) = mock(202, "").await;
        let c = client(&url, Auth::Basic { user: "u".into(), password: "p".into() });
        c.pause("my conn").await.unwrap();
        c.resume("a").await.unwrap();
        c.restart("a", false).await.unwrap();
        c.restart_task("a", 3).await.unwrap();
        c.delete("a").await.unwrap();
        c.restart("b", true).await.unwrap();
        c.update_config("a", &Map::new()).await.unwrap();
        c.stop("a").await.unwrap();
        let heads: Vec<String> =
            seen.lock().unwrap().iter().map(|r| r.lines().next().unwrap().to_string()).collect();
        assert_eq!(heads[0], "PUT /connectors/my%20conn/pause HTTP/1.1");
        assert_eq!(heads[1], "PUT /connectors/a/resume HTTP/1.1");
        assert_eq!(heads[2], "POST /connectors/a/restart?includeTasks=true&onlyFailed=false HTTP/1.1");
        assert_eq!(heads[3], "POST /connectors/a/tasks/3/restart HTTP/1.1");
        assert_eq!(heads[4], "DELETE /connectors/a HTTP/1.1");
        assert_eq!(heads[5], "POST /connectors/b/restart?includeTasks=true&onlyFailed=true HTTP/1.1");
        assert_eq!(heads[6], "PUT /connectors/a/config HTTP/1.1");
        assert_eq!(heads[7], "PUT /connectors/a/stop HTTP/1.1");
        assert!(seen.lock().unwrap()[0].to_ascii_lowercase().contains("authorization: basic"));
    }

    #[test]
    fn stop_capability_follows_connect_version() {
        let root = |v: &str| RootInfo { version: v.into() };
        assert!(root("3.9.0").supports_stop());
        assert!(root("3.5.0").supports_stop());
        assert!(root("4.0.1").supports_stop());
        assert!(root("3.10.2-SNAPSHOT").supports_stop());
        assert!(!root("3.4.1").supports_stop());
        assert!(!root("2.8.0").supports_stop());
        assert!(!root("").supports_stop());
        assert!(!root("weird").supports_stop());
    }

    const VALIDATION: &str = r#"{"name":"X","error_count":1,"groups":[],"configs":[
        {"definition":{"name":"topics"},"value":{"name":"topics","value":null,"errors":["Missing required configuration \"topics\""]}},
        {"definition":{"name":"tasks.max"},"value":{"name":"tasks.max","value":"1","errors":[]}}]}"#;

    #[test]
    fn parses_validation_errors() {
        let errs = parse_validation(VALIDATION).unwrap();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].starts_with("topics: Missing required"));
        assert!(parse_validation("not json").is_err());
    }

    #[tokio::test]
    async fn validate_uses_plugin_endpoint() {
        let (url, seen) = mock(200, VALIDATION).await;
        let mut cfg = Map::new();
        cfg.insert("connector.class".into(), "io.x.Sink".into());
        let errs = client(&url, Auth::None).validate("n", &cfg).await.unwrap();
        assert_eq!(errs.len(), 1);
        let req = seen.lock().unwrap()[0].clone();
        assert!(req.starts_with("PUT /connector-plugins/io.x.Sink/config/validate HTTP/1.1"), "{req}");
        assert!(req.contains(r#""name":"n""#));
        assert!(client(&url, Auth::None).validate("n", &Map::new()).await.is_err());
    }

    #[tokio::test]
    async fn lists_plugins() {
        let body = r#"[{"class":"io.debezium.connector.postgresql.PostgresConnector","type":"source","version":"3.0.0.Final"},{"class":"x.Y","type":"sink"}]"#;
        let (url, _) = mock(200, body).await;
        let p = client(&url, Auth::None).plugins().await.unwrap();
        assert_eq!(p.len(), 2);
        assert!(p[0].is_debezium() && p[0].version == "3.0.0.Final");
        assert!(!p[1].is_debezium() && p[1].version.is_empty());
    }

    #[tokio::test]
    async fn create_posts_name_and_config() {
        let (url, seen) = mock(201, "{}").await;
        let c = client(&url, Auth::Bearer("tok".into()));
        let mut cfg = Map::new();
        cfg.insert("connector.class".into(), "X".into());
        c.create("n", &cfg).await.unwrap();
        let req = seen.lock().unwrap()[0].clone();
        assert!(req.starts_with("POST /connectors HTTP/1.1"));
        assert!(req.to_ascii_lowercase().contains("authorization: bearer tok"));
        assert!(req.contains(r#""name":"n""#) && req.contains(r#""connector.class":"X""#));
    }

    #[tokio::test]
    async fn surfaces_api_errors() {
        let (url, _) = mock(409, r#"{"error_code":409,"message":"Connector n already exists"}"#).await;
        let err = client(&url, Auth::None).create("n", &Map::new()).await.unwrap_err();
        let s = err.to_string();
        assert!(s.contains("409") && s.contains("already exists"), "{s}");
    }

    #[tokio::test]
    async fn unreachable_endpoint_is_reported() {
        let err = client("http://127.0.0.1:1", Auth::None).root().await.unwrap_err();
        assert!(err.to_string().contains("cannot connect"), "{err}");
    }

    /// Like `mock`, but replies with fixed raw bytes (headers included).
    async fn raw_mock(response: Vec<u8>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else { break };
                let (seen, response) = (seen2.clone(), response.clone());
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    seen.lock().unwrap().push(String::from_utf8_lossy(&buf[..n]).to_string());
                    let _ = s.write_all(&response).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        (format!("http://{addr}"), seen)
    }

    #[test]
    fn rejects_credentials_in_url_and_strips_query() {
        let err =
            Client::new("http://admin:secret@host:8083", Auth::None, Duration::from_secs(1)).err().unwrap();
        assert!(err.to_string().contains("must not contain credentials"));
        assert!(!err.to_string().contains("secret"), "the password must not be echoed back");
        assert!(Client::new("http://admin@host:8083", Auth::None, Duration::from_secs(1)).is_err());
        let c = Client::new("http://host:8083/base/?x=1#frag", Auth::None, Duration::from_secs(1)).unwrap();
        assert_eq!(c.url(&["connectors"]).as_str(), "http://host:8083/base/connectors");
    }

    #[test]
    fn connector_names_are_validated() {
        for bad in ["", "   ", ".", "..", "a\nb", "a\x1b[2Jb", &"x".repeat(256)] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
        for ok in ["a", "my conn", "ünï-cødé", "a/b", "a.b", "..a", "a%2Fb"] {
            assert!(check_name(ok).is_ok(), "{ok:?}");
        }
        assert!(normalize_create(serde_json::json!({"name":"..","connector.class":"X"})).is_err());
        assert!(normalize_create(serde_json::json!({"name":"a\u{7}","config":{}})).is_err());
    }

    #[tokio::test]
    async fn dot_segment_names_never_reach_the_network() {
        let (url, seen) = mock(202, "").await;
        let c = client(&url, Auth::None);
        assert!(c.delete("..").await.is_err());
        assert!(c.restart(".", false).await.is_err());
        assert!(c.pause("").await.is_err());
        assert!(seen.lock().unwrap().is_empty());
        // Slashes and percent signs in a name stay inside one encoded path segment.
        c.delete("a/b%2Fc").await.unwrap();
        let head = seen.lock().unwrap()[0].lines().next().unwrap().to_string();
        assert_eq!(head, "DELETE /connectors/a%2Fb%252Fc HTTP/1.1");
    }

    #[test]
    fn redirects_stay_on_host_and_never_downgrade() {
        let u = |s: &str| Url::parse(s).unwrap();
        assert!(safe_redirect(&u("http://h:8083/a"), &u("http://h:8083/b")));
        assert!(safe_redirect(&u("http://h/a"), &u("https://h/a")));
        assert!(!safe_redirect(&u("https://h/a"), &u("http://h/a")));
        assert!(!safe_redirect(&u("http://h:8083/a"), &u("http://evil:8083/a")));
        assert!(!safe_redirect(&u("http://h:8083/a"), &u("http://h:9999/a")));
    }

    #[tokio::test]
    async fn cross_host_redirect_is_not_followed_and_credentials_are_not_forwarded() {
        let (target, target_seen) = mock(200, "{}").await;
        let location = target.replace("127.0.0.1", "localhost");
        let resp = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}/connectors\r\nContent-Length: 0\r\n\r\n"
        );
        let (url, _) = raw_mock(resp.into_bytes()).await;
        let c = client(&url, Auth::Bearer("topsecret".into()));
        let err = c.list().await.unwrap_err();
        assert!(err.to_string().contains("307"), "{err}");
        assert!(target_seen.lock().unwrap().is_empty(), "the redirect target must not be contacted");
    }

    #[tokio::test]
    async fn oversized_declared_body_is_refused() {
        let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", MAX_BODY_BYTES + 1);
        let (url, _) = raw_mock(head.into_bytes()).await;
        let err = client(&url, Auth::None).root().await.unwrap_err();
        assert!(err.to_string().contains("larger than"), "{err}");
    }

    #[tokio::test]
    async fn truncated_success_body_is_an_error_but_truncated_error_body_is_reported() {
        // Declares 100 bytes, sends 5, then closes.
        let (url, _) = raw_mock(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nhello".to_vec()).await;
        assert!(client(&url, Auth::None).root().await.is_err());
        let (url, _) = raw_mock(b"HTTP/1.1 500 Oops\r\nContent-Length: 100\r\n\r\nboom".to_vec()).await;
        let err = client(&url, Auth::None).root().await.unwrap_err();
        assert!(err.to_string().contains("500"), "{err}");
    }

    #[tokio::test]
    async fn huge_error_pages_and_invalid_utf8_are_contained() {
        let html = format!("<html>{}</html>", "x".repeat(50_000));
        let resp = format!("HTTP/1.1 502 Bad Gateway\r\nContent-Length: {}\r\n\r\n{html}", html.len());
        let (url, _) = raw_mock(resp.into_bytes()).await;
        let err = client(&url, Auth::None).root().await.unwrap_err().to_string();
        assert!(err.len() < 800, "error text must be bounded, was {}", err.len());
        assert!(err.contains("502") && err.contains("worker unavailable"));

        let body = b"{\"version\":\"\xff\xfe3.9\"}";
        let mut resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
        resp.extend_from_slice(body);
        let (url, _) = raw_mock(resp).await;
        let root = client(&url, Auth::None).root().await.unwrap();
        assert!(root.version.contains("3.9"));
    }

    #[test]
    fn warns_only_for_remote_plaintext_auth() {
        let t = Duration::from_secs(1);
        let basic = || Auth::Bearer("t".into());
        let cases = [
            ("http://connect.example.com:8083", basic(), true),
            ("https://connect.example.com", basic(), false),
            ("http://localhost:8083", basic(), false),
            ("http://127.0.0.1:8083", basic(), false),
            ("http://[::1]:8083", basic(), false),
            ("http://connect.example.com:8083", Auth::None, false),
        ];
        for (url, auth, want) in cases {
            assert_eq!(Client::new(url, auth, t).unwrap().sends_credentials_in_clear(), want, "{url}");
        }
    }

    #[test]
    fn malformed_server_payloads_do_not_panic() {
        for body in [
            "",
            "null",
            "[]",
            "{}",
            r#"{"a":null}"#,
            r#"{"a":{"status":{"connector":{}}}}"#,
            r#"{"a":{"status":{"connector":{"state":"X"},"tasks":[{"state":"RUNNING"}]}}}"#,
        ] {
            let _ = parse_connectors(body);
        }
        assert!(parse_connectors("{}").unwrap().is_empty());
        for body in [
            "",
            "[]",
            r#"{"configs":null}"#,
            r#"{"configs":[{"value":null}]}"#,
            r#"{"configs":[{"value":{"errors":[1,null,"x"]}}]}"#,
        ] {
            let _ = parse_validation(body);
        }
        assert_eq!(parse_validation(r#"{"configs":[{"value":{"errors":[1,null,"x"]}}]}"#).unwrap(), ["?: x"]);
    }

    #[test]
    fn error_bodies_do_not_echo_password_parameters() {
        let body = r#"{"error_code":500,"message":"bad url jdbc:postgresql://h/db?user=u&password=Hunter2&ssl=true"}"#;
        let msg = describe_http(StatusCode::INTERNAL_SERVER_ERROR, body);
        assert!(
            !msg.contains("Hunter2") && msg.contains("password=********") && msg.contains("ssl=true"),
            "{msg}"
        );
    }
}
