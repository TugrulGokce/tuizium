//! Built-in connector templates and the editable form state used to fill them.

use serde_json::{Map, Value};

use crate::api::Plugin;

pub struct FieldDef {
    /// Config key, or the special key `name` for the connector name.
    pub key: &'static str,
    pub label: &'static str,
    pub default: &'static str,
    pub secret: bool,
}

pub struct Template {
    pub title: &'static str,
    pub description: &'static str,
    /// Fixed config entries that aren't edited in the form.
    pub fixed: &'static [(&'static str, &'static str)],
    pub fields: &'static [FieldDef],
}

const fn f(key: &'static str, label: &'static str, default: &'static str) -> FieldDef {
    FieldDef { key, label, default, secret: false }
}

const fn secret(key: &'static str, label: &'static str) -> FieldDef {
    FieldDef { key, label, default: "", secret: true }
}

pub const TEMPLATES: &[Template] = &[
    Template {
        title: "Debezium PostgreSQL Source",
        description: "CDC from PostgreSQL using the pgoutput logical decoding plugin",
        fixed: &[
            ("connector.class", "io.debezium.connector.postgresql.PostgresConnector"),
            ("plugin.name", "pgoutput"),
        ],
        fields: &[
            f("name", "Connector name", "pg-source"),
            f("database.hostname", "Database host", "localhost"),
            f("database.port", "Database port", "5432"),
            f("database.user", "Database user", "postgres"),
            secret("database.password", "Database password"),
            f("database.dbname", "Database name", "postgres"),
            f("topic.prefix", "Topic prefix", "pg"),
            f("schema.include.list", "Schemas (comma separated)", "public"),
            f("table.include.list", "Tables (empty = all)", ""),
            f("slot.name", "Replication slot", "tuizium_slot"),
            f("tasks.max", "Max tasks", "1"),
        ],
    },
    Template {
        title: "Debezium MySQL Source",
        description: "CDC from MySQL / MariaDB binlog",
        fixed: &[("connector.class", "io.debezium.connector.mysql.MySqlConnector")],
        fields: &[
            f("name", "Connector name", "mysql-source"),
            f("database.hostname", "Database host", "localhost"),
            f("database.port", "Database port", "3306"),
            f("database.user", "Database user", "debezium"),
            secret("database.password", "Database password"),
            f("database.server.id", "Unique server id", "184054"),
            f("topic.prefix", "Topic prefix", "mysql"),
            f("database.include.list", "Databases (empty = all)", ""),
            f("table.include.list", "Tables (empty = all)", ""),
            f("schema.history.internal.kafka.bootstrap.servers", "Kafka bootstrap servers", "kafka:9092"),
            f("schema.history.internal.kafka.topic", "Schema history topic", "schema-changes.mysql"),
            f("tasks.max", "Max tasks", "1"),
        ],
    },
    Template {
        title: "Debezium JDBC Sink",
        description: "Write Kafka topics to a relational database (Debezium JDBC sink)",
        fixed: &[
            ("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector"),
            ("insert.mode", "upsert"),
            ("primary.key.mode", "record_key"),
            ("schema.evolution", "basic"),
        ],
        fields: &[
            f("name", "Connector name", "jdbc-sink"),
            f("topics", "Topics (comma separated)", ""),
            f("connection.url", "JDBC URL", "jdbc:postgresql://localhost:5432/postgres"),
            f("connection.username", "Database user", "postgres"),
            secret("connection.password", "Database password"),
            f("tasks.max", "Max tasks", "1"),
        ],
    },
    Template {
        title: "Confluent JDBC Sink",
        description: "Write Kafka topics to a relational database (Confluent JDBC sink, must be installed)",
        fixed: &[
            ("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector"),
            ("insert.mode", "upsert"),
            ("pk.mode", "record_key"),
            ("auto.create", "true"),
            ("auto.evolve", "true"),
        ],
        fields: &[
            f("name", "Connector name", "jdbc-sink"),
            f("topics", "Topics (comma separated)", ""),
            f("connection.url", "JDBC URL", "jdbc:postgresql://localhost:5432/postgres"),
            f("connection.user", "Database user", "postgres"),
            secret("connection.password", "Database password"),
            f("tasks.max", "Max tasks", "1"),
        ],
    },
];

impl Template {
    /// The `connector.class` this template creates.
    pub fn class(&self) -> &'static str {
        self.fixed.iter().find(|(k, _)| *k == "connector.class").map_or("", |(_, v)| v)
    }
}

/// Whether the cluster reported the plugin a template needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Availability {
    Installed,
    Missing,
    /// The plugin list is not known (yet), so nothing is claimed.
    Unknown,
}

pub fn availability(t: &Template, plugins: &[Plugin]) -> Availability {
    if plugins.is_empty() {
        Availability::Unknown
    } else if plugins.iter().any(|p| p.class == t.class()) {
        Availability::Installed
    } else {
        Availability::Missing
    }
}

#[derive(Clone, Debug)]
pub struct FieldState {
    pub key: &'static str,
    pub label: &'static str,
    pub value: String,
    pub secret: bool,
}

#[derive(Clone, Debug)]
pub struct FormState {
    pub template: usize,
    pub fields: Vec<FieldState>,
    pub selected: usize,
    pub error: Option<String>,
}

impl FormState {
    pub fn new(template: usize) -> Self {
        let fields = TEMPLATES[template]
            .fields
            .iter()
            .map(|d| FieldState {
                key: d.key,
                label: d.label,
                value: d.default.to_string(),
                secret: d.secret,
            })
            .collect();
        Self { template, fields, selected: 0, error: None }
    }

    /// Builds `(name, config)` from the current field values.
    pub fn build(&self) -> Result<(String, Map<String, Value>), String> {
        let t = &TEMPLATES[self.template];
        let mut cfg = Map::new();
        for (k, v) in t.fixed {
            cfg.insert((*k).into(), Value::String((*v).into()));
        }
        let mut name = String::new();
        for fld in &self.fields {
            let v = fld.value.trim();
            if fld.key == "name" {
                name = v.to_string();
            } else if !v.is_empty() {
                cfg.insert(fld.key.into(), Value::String(v.to_string()));
            }
        }
        if name.is_empty() {
            return Err("connector name is required".into());
        }
        if t.fields.iter().any(|f| f.key == "topics") && !cfg.contains_key("topics") {
            return Err("at least one topic is required".into());
        }
        Ok((name, cfg))
    }
}

/// Key-name fragments that mark a config value as a secret.
const SECRET_KEYS: [&str; 9] =
    ["password", "passwd", "secret", "token", "credential", "api.key", "apikey", "private.key", "jaas"];
/// URL / connection-string parameters whose value is a secret (`...?password=x&ssl=true`).
const SECRET_PARAMS: [&str; 5] = ["password=", "passwd=", "pwd=", "secret=", "token="];
const MASK: &str = "********";

fn is_secret_key(key: &str) -> bool {
    let lk = key.to_ascii_lowercase();
    SECRET_KEYS.iter().any(|s| lk.contains(s))
}

/// Replaces the value of secret-looking `name=value` parameters inside a string.
fn mask_params(s: &str) -> String {
    let lower = s.to_ascii_lowercase();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        match SECRET_PARAMS.iter().find(|p| lower[i..].starts_with(*p)) {
            Some(p) => {
                let value_start = i + p.len();
                let end = s[value_start..]
                    .find(['&', ';', ' ', '"', '\'', '\n', '\r', '\t'])
                    .map_or(s.len(), |n| value_start + n);
                out.push_str(&s[i..value_start]);
                out.push_str(MASK);
                i = end;
            }
            None => {
                let ch = s[i..].chars().next().unwrap_or(' ');
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    out
}

/// Returns a copy of the config with secret-looking values masked, for display.
/// Config-provider references such as `${env:VAR}` are not secrets and stay visible.
pub fn masked(cfg: &Map<String, Value>) -> Map<String, Value> {
    cfg.iter()
        .map(|(k, v)| {
            let reference = v.as_str().is_some_and(|s| s.starts_with("${"));
            let v = if reference {
                v.clone()
            } else if is_secret_key(k) {
                Value::String(MASK.into())
            } else if let Some(s) = v.as_str() {
                Value::String(mask_params(s))
            } else {
                v.clone()
            };
            (k.clone(), v)
        })
        .collect()
}

/// Masks `name=value` secrets inside one displayed string (e.g. a JDBC URL).
pub fn mask_text(s: &str) -> String {
    mask_params(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_template_builds_with_defaults() {
        for (i, t) in TEMPLATES.iter().enumerate() {
            let mut form = FormState::new(i);
            if t.fields.iter().any(|f| f.key == "topics") {
                form.fields.iter_mut().find(|f| f.key == "topics").unwrap().value = "orders".into();
            }
            let (name, cfg) = form.build().unwrap_or_else(|e| panic!("{}: {e}", t.title));
            assert!(!name.is_empty());
            assert!(cfg.contains_key("connector.class"));
            assert!(!cfg.contains_key("name"));
        }
    }

    #[test]
    fn empty_optional_fields_are_omitted_and_name_required() {
        let mut form = FormState::new(0);
        assert!(!form.build().unwrap().1.contains_key("table.include.list"));
        form.fields[0].value = "  ".into();
        assert!(form.build().is_err());
    }

    #[test]
    fn jdbc_sink_requires_topics() {
        assert!(FormState::new(2).build().is_err());
    }

    #[test]
    fn masks_secrets_for_preview() {
        let mut form = FormState::new(0);
        form.fields.iter_mut().find(|f| f.secret).unwrap().value = "hunter2".into();
        let (_, cfg) = form.build().unwrap();
        assert_eq!(cfg["database.password"], "hunter2");
        assert_eq!(masked(&cfg)["database.password"], "********");
        assert_eq!(masked(&cfg)["database.user"], "postgres");
    }

    #[test]
    fn masks_more_secret_keys_and_url_parameters() {
        let mut cfg = Map::new();
        for (k, v) in [
            ("api.key", "k1"),
            ("sasl.jaas.config", "org.x.Login required password=\"p\";"),
            ("connection.url", "jdbc:postgresql://h:5432/db?user=u&password=hunter2&ssl=true"),
            ("database.hostname", "db.internal"),
            ("pw.ref", "${env:DB_PW}"),
            ("database.password", "${file:/run/secrets:pw}"),
        ] {
            cfg.insert(k.into(), Value::String(v.into()));
        }
        let m = masked(&cfg);
        assert_eq!(m["api.key"], "********");
        assert_eq!(m["sasl.jaas.config"], "********");
        let url = m["connection.url"].as_str().unwrap();
        assert!(
            !url.contains("hunter2") && url.contains("password=********") && url.contains("&ssl=true"),
            "{url}"
        );
        assert_eq!(m["database.hostname"], "db.internal");
        assert_eq!(
            m["database.password"], "${file:/run/secrets:pw}",
            "config-provider references stay visible"
        );
    }

    fn plugin(class: &str) -> Plugin {
        Plugin { class: class.into(), kind: "sink".into(), version: "1".into() }
    }

    fn idx(title: &str) -> usize {
        TEMPLATES.iter().position(|t| t.title == title).unwrap_or_else(|| panic!("no template {title}"))
    }

    #[test]
    fn jdbc_sink_has_separate_debezium_and_confluent_templates() {
        let build = |title: &str| {
            let mut form = FormState::new(idx(title));
            form.fields.iter_mut().find(|f| f.key == "topics").unwrap().value = "orders".into();
            form.build().unwrap().1
        };
        let d = build("Debezium JDBC Sink");
        assert_eq!(d["connector.class"], "io.debezium.connector.jdbc.JdbcSinkConnector");
        assert!(d.contains_key("connection.username") && !d.contains_key("connection.user"), "{d:?}");
        assert_eq!(d["primary.key.mode"], "record_key");
        assert_eq!(d["schema.evolution"], "basic");
        let c = build("Confluent JDBC Sink");
        assert_eq!(c["connector.class"], "io.confluent.connect.jdbc.JdbcSinkConnector");
        assert!(c.contains_key("connection.user") && c.contains_key("pk.mode"), "{c:?}");
        for t in ["Debezium JDBC Sink", "Confluent JDBC Sink"] {
            assert!(FormState::new(idx(t)).build().is_err(), "{t} needs a topic");
        }
    }

    #[test]
    fn availability_follows_the_plugins_the_cluster_reports() {
        let pg = &TEMPLATES[idx("Debezium PostgreSQL Source")];
        let confluent = &TEMPLATES[idx("Confluent JDBC Sink")];
        let installed = [plugin("io.debezium.connector.postgresql.PostgresConnector")];
        assert_eq!(availability(pg, &installed), Availability::Installed);
        assert_eq!(availability(confluent, &installed), Availability::Missing);
        assert_eq!(availability(confluent, &[]), Availability::Unknown, "no plugin list = do not block");
    }
}
