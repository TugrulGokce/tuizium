//! What a Debezium connector's *configuration* says about capture filters, snapshot mode and
//! replication objects. Everything here is read from the config returned by the Connect REST API:
//! it is the configured intent, never runtime state (Tuizium does not look at the database, the
//! replication slot or snapshot progress).

use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Family {
    Postgres,
    MySql,
    SqlServer,
    Oracle,
    Mongo,
    OtherSource,
    JdbcSink,
    MongoSink,
}

/// One line of the view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub key: &'static str,
    /// `None`: the key is not in the config.
    pub value: Option<String>,
    /// Number of comma-separated entries, for include/exclude lists.
    pub entries: Option<usize>,
}

fn family(class: &str) -> Option<Family> {
    let rest = class.strip_prefix("io.debezium.connector.")?;
    let module = rest.split('.').next().unwrap_or("");
    let sink = class.ends_with("SinkConnector");
    Some(match (module, sink) {
        ("jdbc", _) => Family::JdbcSink,
        ("mongodb", true) => Family::MongoSink,
        (_, true) => return None,
        ("postgresql", _) => Family::Postgres,
        ("mysql" | "mariadb", _) => Family::MySql,
        ("sqlserver", _) => Family::SqlServer,
        ("oracle", _) => Family::Oracle,
        ("mongodb", _) => Family::Mongo,
        _ => Family::OtherSource,
    })
}

/// Keys per family: `(key, shown even when absent)`. Keys the Overview already prints
/// (`topic.prefix`, `database.hostname`, `topics`, `connection.url`) are left out here.
fn spec(f: Family) -> &'static [(&'static str, bool)] {
    match f {
        Family::Postgres => &[
            ("database.dbname", false),
            ("schema.include.list", false),
            ("schema.exclude.list", false),
            ("table.include.list", false),
            ("table.exclude.list", false),
            ("column.include.list", false),
            ("column.exclude.list", false),
            ("snapshot.mode", true),
            ("slot.name", true),
            ("plugin.name", false),
            ("publication.name", true),
            ("publication.autocreate.mode", false),
        ],
        Family::MySql => &[
            ("database.include.list", false),
            ("database.exclude.list", false),
            ("table.include.list", false),
            ("table.exclude.list", false),
            ("column.include.list", false),
            ("column.exclude.list", false),
            ("snapshot.mode", true),
            ("database.server.id", false),
            ("schema.history.internal.kafka.topic", false),
        ],
        Family::SqlServer => &[
            ("database.names", false),
            ("schema.include.list", false),
            ("table.include.list", false),
            ("table.exclude.list", false),
            ("column.include.list", false),
            ("column.exclude.list", false),
            ("snapshot.mode", true),
        ],
        Family::Oracle => &[
            ("database.dbname", false),
            ("schema.include.list", false),
            ("schema.exclude.list", false),
            ("table.include.list", false),
            ("table.exclude.list", false),
            ("column.include.list", false),
            ("column.exclude.list", false),
            ("snapshot.mode", true),
            ("log.mining.strategy", false),
        ],
        Family::Mongo => &[
            ("mongodb.connection.string", false),
            ("topic.prefix", false),
            ("database.include.list", false),
            ("database.exclude.list", false),
            ("collection.include.list", false),
            ("collection.exclude.list", false),
            ("field.exclude.list", false),
            ("snapshot.mode", true),
            ("capture.mode", false),
        ],
        Family::OtherSource => &[
            ("table.include.list", false),
            ("table.exclude.list", false),
            ("column.include.list", false),
            ("column.exclude.list", false),
            ("snapshot.mode", true),
        ],
        Family::JdbcSink => &[
            ("topics.regex", false),
            ("insert.mode", false),
            ("delete.enabled", false),
            ("primary.key.mode", false),
            ("primary.key.fields", false),
            ("schema.evolution", false),
            ("table.name.format", false),
        ],
        Family::MongoSink => &[
            ("connection.string", false),
            ("topics.regex", false),
            ("sink.database", false),
            ("collection.name.format", false),
        ],
    }
}

fn is_list(key: &str) -> bool {
    key.ends_with(".list") || key.ends_with(".names") || key.ends_with(".fields")
}

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The configured Debezium settings of a connector, or `None` when it is not a Debezium
/// connector (or a Debezium class we have no field list for). Values are masked.
pub fn view(class: &str, config: &Map<String, Value>) -> Option<Vec<Row>> {
    let fam = family(class)?;
    let config = crate::templates::masked(config);
    let rows = spec(fam)
        .iter()
        .filter_map(|&(key, always)| {
            let value =
                config.get(key).filter(|v| !v.is_null()).map(text).map(|v| crate::templates::mask_text(&v));
            if value.is_none() && !always {
                return None;
            }
            let entries = value
                .as_deref()
                .filter(|_| is_list(key))
                .map(|v| v.split(',').filter(|s| !s.trim().is_empty()).count());
            Some(Row { key, value, entries })
        })
        .collect();
    Some(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    const PG: &str = "io.debezium.connector.postgresql.PostgresConnector";

    #[test]
    fn postgres_shows_filters_snapshot_slot_and_publication_from_config() {
        let rows = view(
            PG,
            &cfg(json!({
                "table.include.list": "public.a, public.b",
                "snapshot.mode": "initial",
                "slot.name": "s1",
                "publication.name": "p1",
                "database.password": "hunter2",
            })),
        )
        .unwrap();
        let get = |k: &str| rows.iter().find(|r| r.key == k).unwrap();
        assert_eq!(get("table.include.list").entries, Some(2));
        assert_eq!(get("snapshot.mode").value.as_deref(), Some("initial"));
        assert_eq!(get("slot.name").value.as_deref(), Some("s1"));
        assert_eq!(get("publication.name").value.as_deref(), Some("p1"));
        assert!(rows.iter().all(|r| r.value.as_deref() != Some("hunter2")));
    }

    #[test]
    fn important_keys_are_listed_as_unset_but_optional_ones_are_not() {
        let rows = view(PG, &cfg(json!({}))).unwrap();
        let keys: Vec<_> = rows.iter().map(|r| r.key).collect();
        assert_eq!(keys, ["snapshot.mode", "slot.name", "publication.name"]);
        assert!(rows.iter().all(|r| r.value.is_none()));
    }

    #[test]
    fn families_are_told_apart_by_class() {
        assert_eq!(family("io.debezium.connector.mysql.MySqlConnector"), Some(Family::MySql));
        assert_eq!(family("io.debezium.connector.mariadb.MariaDbConnector"), Some(Family::MySql));
        assert_eq!(family("io.debezium.connector.mongodb.MongoDbConnector"), Some(Family::Mongo));
        assert_eq!(family("io.debezium.connector.mongodb.MongoDbSinkConnector"), Some(Family::MongoSink));
        assert_eq!(family("io.debezium.connector.jdbc.JdbcSinkConnector"), Some(Family::JdbcSink));
        assert_eq!(family("io.debezium.connector.db2.Db2Connector"), Some(Family::OtherSource));
        assert_eq!(family("io.debezium.connector.unknown.FooSinkConnector"), None);
        assert_eq!(family("io.confluent.connect.jdbc.JdbcSinkConnector"), None);
        assert_eq!(family("org.apache.kafka.connect.mirror.MirrorSourceConnector"), None);
    }

    #[test]
    fn mongo_connection_string_credentials_are_masked() {
        let rows = view(
            "io.debezium.connector.mongodb.MongoDbConnector",
            &cfg(json!({ "mongodb.connection.string": "mongodb://u:secretpw@host:27017/?replicaSet=rs0" })),
        )
        .unwrap();
        let v = rows.iter().find(|r| r.key == "mongodb.connection.string").unwrap().value.clone().unwrap();
        assert!(!v.contains("secretpw"), "{v}");
    }

    #[test]
    fn non_string_values_and_nulls_are_handled() {
        let rows = view(
            "io.debezium.connector.jdbc.JdbcSinkConnector",
            &cfg(json!({ "delete.enabled": true, "insert.mode": null })),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value.as_deref(), Some("true"));
    }
}
