//! User configuration. Secrets are never stored: profiles only reference the
//! *names* of environment variables that hold passwords or tokens.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

use crate::api::{Auth, Client};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Cluster {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Name of the env var holding the basic-auth password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_env: Option<String>,
    /// Name of the env var holding a bearer token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Config {
    /// Cluster selected at startup (defaults to the first by name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_cluster: Option<String>,
    /// Auto refresh interval in seconds (default 5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_secs: Option<u64>,
    /// Show the connection screen on every start (default true). Set to false to
    /// connect straight to `default_cluster`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask_on_start: Option<bool>,
    #[serde(default)]
    pub clusters: BTreeMap<String, Cluster>,
}

impl Config {
    pub fn with_local_default() -> Self {
        let mut c = Config::default();
        c.clusters
            .insert("local".into(), Cluster { url: "http://localhost:8083".into(), ..Default::default() });
        c
    }

    pub fn ask_on_start(&self) -> bool {
        self.ask_on_start.unwrap_or(true)
    }

    pub fn refresh_interval(&self) -> Duration {
        Duration::from_secs(self.refresh_secs.unwrap_or(5).max(1))
    }

    pub fn initial_cluster(&self) -> Option<String> {
        self.default_cluster
            .clone()
            .filter(|n| self.clusters.contains_key(n))
            .or_else(|| self.clusters.keys().next().cloned())
    }
}

pub fn config_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("TUIZIUM_CONFIG") {
        return Some(PathBuf::from(p));
    }
    dirs::config_dir().map(|d| d.join("tuizium").join("config.toml"))
}

/// Loads the config; if the file doesn't exist, writes a default one.
pub fn load(path: Option<&PathBuf>) -> Result<Config> {
    let Some(path) = path else { return Ok(Config::with_local_default()) };
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let cfg: Config =
                toml::from_str(&text).with_context(|| format!("invalid config {}", path.display()))?;
            Ok(if cfg.clusters.is_empty() { Config::with_local_default() } else { cfg })
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let cfg = Config::with_local_default();
            let _ = save(path, &cfg); // best effort
            Ok(cfg)
        }
        Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
    }
}

pub fn save(path: &PathBuf, cfg: &Config) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, toml::to_string_pretty(cfg)?)?;
    Ok(())
}

fn env_secret(var: &str) -> Result<String> {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(anyhow!("environment variable {var} is not set")),
    }
}

impl Cluster {
    pub fn auth(&self) -> Result<Auth> {
        if let Some(var) = &self.token_env {
            return Ok(Auth::Bearer(env_secret(var)?));
        }
        match (&self.username, &self.password_env) {
            (Some(user), Some(var)) => Ok(Auth::Basic { user: user.clone(), password: env_secret(var)? }),
            (Some(_), None) => Err(anyhow!("username is set but password_env is missing")),
            _ => Ok(Auth::None),
        }
    }

    pub fn client(&self) -> Result<Client> {
        Client::new(&self.url, self.auth()?, Duration::from_secs(self.timeout_secs.unwrap_or(10)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_config() {
        let cfg: Config = toml::from_str(
            r#"
            refresh_secs = 3
            [clusters.local]
            url = "http://localhost:8083"
            [clusters.prod]
            url = "https://connect.example.com"
            username = "admin"
            password_env = "TUIZIUM_TEST_PW"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.clusters.len(), 2);
        assert_eq!(cfg.initial_cluster().as_deref(), Some("local"));
        assert_eq!(cfg.refresh_interval(), Duration::from_secs(3));
    }

    #[test]
    fn secrets_come_from_env_only() {
        let c = Cluster {
            url: "http://x".into(),
            username: Some("u".into()),
            password_env: Some("TUIZIUM_TEST_PW_MISSING".into()),
            ..Default::default()
        };
        assert!(c.auth().unwrap_err().to_string().contains("TUIZIUM_TEST_PW_MISSING"));
        let plain = Cluster { url: "http://x".into(), ..Default::default() };
        assert_eq!(plain.auth().unwrap(), Auth::None);
        // Serialized form never contains secret material, only variable names.
        let s = toml::to_string_pretty(&Config::with_local_default()).unwrap();
        assert!(!s.contains("password ="));
    }

    #[test]
    fn rejects_bad_urls() {
        let c = Cluster { url: "ftp://nope".into(), ..Default::default() };
        assert!(c.client().is_err());
    }
}
