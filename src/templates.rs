//! Built-in connector templates and the editable form state used to fill them.

use serde_json::{Map, Value};

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
        title: "JDBC Sink",
        description: "Write Kafka topics to a relational database (Confluent JDBC sink)",
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
        if t.title == "JDBC Sink" && !cfg.contains_key("topics") {
            return Err("at least one topic is required".into());
        }
        Ok((name, cfg))
    }
}

/// Returns a copy of the config with secret-looking values masked, for display.
pub fn masked(cfg: &Map<String, Value>) -> Map<String, Value> {
    cfg.iter()
        .map(|(k, v)| {
            let lk = k.to_ascii_lowercase();
            let sensitive = ["password", "secret", "token", "credential"].iter().any(|s| lk.contains(s));
            let v = if sensitive && !v.as_str().is_some_and(|s| s.starts_with("${")) {
                Value::String("********".into())
            } else {
                v.clone()
            };
            (k.clone(), v)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_template_builds_with_defaults() {
        for (i, t) in TEMPLATES.iter().enumerate() {
            let mut form = FormState::new(i);
            if t.title == "JDBC Sink" {
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
}
