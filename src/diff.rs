//! Old-vs-new connector config comparison, shown before an update is saved.
//!
//! Values are masked with the same rules as every other display, and the "heads-up" notes are
//! heuristics keyed on *which settings changed*: Tuizium cannot know what a connector will do with
//! its stored offsets, so the wording always says "may".

use serde_json::{Map, Value};

use crate::templates;

#[derive(Clone, Debug, PartialEq)]
pub enum Change {
    Added { key: String, new: Value },
    Removed { key: String, old: Value },
    Changed { key: String, old: Value, new: Value },
}

impl Change {
    pub fn key(&self) -> &str {
        match self {
            Change::Added { key, .. } | Change::Removed { key, .. } | Change::Changed { key, .. } => key,
        }
    }
}

/// Differences between two configs, sorted by key. The `name` entry is ignored: it cannot change.
pub fn diff(old: &Map<String, Value>, new: &Map<String, Value>) -> Vec<Change> {
    let mut keys: Vec<&String> = old.keys().chain(new.keys()).filter(|k| *k != "name").collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|k| match (old.get(k), new.get(k)) {
            (None, Some(n)) => Some(Change::Added { key: k.clone(), new: n.clone() }),
            (Some(o), None) => Some(Change::Removed { key: k.clone(), old: o.clone() }),
            (Some(o), Some(n)) if o != n => {
                Some(Change::Changed { key: k.clone(), old: o.clone(), new: n.clone() })
            }
            _ => None,
        })
        .collect()
}

/// One value as shown to the user: strings plain, everything else compact JSON, secrets masked.
fn show(key: &str, v: &Value) -> String {
    let one: Map<String, Value> = std::iter::once((key.to_string(), v.clone())).collect();
    match templates::masked(&one).remove(key) {
        Some(Value::String(s)) => s,
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// Display lines for the diff: `(sign, text)` where sign is `+`, `-` or `~`.
pub fn lines(changes: &[Change]) -> Vec<(char, String)> {
    changes
        .iter()
        .map(|c| match c {
            Change::Added { key, new } => ('+', format!("{key} = {}", show(key, new))),
            Change::Removed { key, old } => ('-', format!("{key} = {}", show(key, old))),
            Change::Changed { key, old, new } => {
                let (o, n) = (show(key, old), show(key, new));
                if o == n {
                    ('~', format!("{key}: value changed (hidden, it is a secret)"))
                } else {
                    ('~', format!("{key}: {o}  →  {n}"))
                }
            }
        })
        .collect()
}

/// Settings whose change may make the connector re-read data, lose its position, or break.
/// Returns `(key, note)` pairs, one per changed risky key.
pub fn replay_warnings(changes: &[Change], is_source: bool) -> Vec<(String, &'static str)> {
    changes.iter().filter_map(|c| risk(c.key(), is_source).map(|note| (c.key().to_string(), note))).collect()
}

fn risk(key: &str, is_source: bool) -> Option<&'static str> {
    let source = is_source;
    Some(match key {
        "connector.class" => "different connector implementation: stored offsets may not be usable",
        "topic.prefix" | "database.server.name" if source => {
            "changes topic names and the key stored offsets are filed under: may trigger a new snapshot, and consumers will see new topics"
        }
        "slot.name" | "publication.name" if source => {
            "replication slot/publication changed: may lose changes made in between or force a new snapshot"
        }
        "database.hostname" | "database.dbname" | "database.port" | "mongodb.connection.string" if source => {
            "points at a different database or server: stored offsets/slot may not match it"
        }
        "database.server.id" if source => {
            "MySQL server id must be unique among replicas: a clash disconnects others"
        }
        "schema.history.internal.kafka.topic" | "database.history.kafka.topic" if source => {
            "schema history topic changed: the old history is no longer read and the connector may fail to start"
        }
        "table.include.list"
        | "table.exclude.list"
        | "schema.include.list"
        | "schema.exclude.list"
        | "database.include.list"
        | "database.exclude.list"
        | "collection.include.list"
        | "collection.exclude.list"
            if source =>
        {
            "capture scope changed: newly included tables may be snapshotted (depends on snapshot.mode); removed ones stop producing events"
        }
        "snapshot.mode" if source => {
            "snapshot behaviour changed: takes effect at the next start and may re-read existing data"
        }
        k if source && k.starts_with("snapshot.") => {
            "snapshot setting changed: takes effect at the next snapshot and may re-read existing data"
        }
        "topics" | "topics.regex" if !source => {
            "topics changed: newly read topics start from the consumer offset reset policy (Connect default: earliest = full replay)"
        }
        "consumer.override.group.id" | "consumer.group.id" | "group.id" if !source => {
            "consumer group changed: the sink starts with no committed offsets and may replay from the reset policy"
        }
        "consumer.override.auto.offset.reset" if !source => {
            "offset reset policy changed: matters for any topic without committed offsets"
        }
        "insert.mode" | "primary.key.mode" | "primary.key.fields" | "delete.enabled"
        | "table.name.format"
            if !source =>
        {
            "write semantics changed: new events are written differently, existing rows are not rewritten; table.name.format may target a different table"
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn reports_added_removed_and_changed_sorted_and_skips_name() {
        let old = map(json!({"name": "a", "b": "1", "c": "2", "same": "x"}));
        let new = map(json!({"name": "b", "b": "9", "d": "4", "same": "x"}));
        let d = diff(&old, &new);
        assert_eq!(
            d,
            vec![
                Change::Changed { key: "b".into(), old: json!("1"), new: json!("9") },
                Change::Removed { key: "c".into(), old: json!("2") },
                Change::Added { key: "d".into(), new: json!("4") },
            ]
        );
        assert!(diff(&old, &old).is_empty());
    }

    #[test]
    fn secrets_are_masked_in_the_diff() {
        let old = map(json!({"database.password": "old-secret", "x": "1"}));
        let new = map(json!({"database.password": "new-secret", "x": "1"}));
        let text: String = lines(&diff(&old, &new)).iter().map(|(_, t)| t.clone()).collect();
        assert!(!text.contains("old-secret") && !text.contains("new-secret"), "{text}");
        assert!(text.contains("database.password"));
        assert!(text.contains("hidden"));
        let added = map(json!({"database.password": "p4ss"}));
        let text: String = lines(&diff(&Map::new(), &added)).iter().map(|(_, t)| t.clone()).collect();
        assert!(!text.contains("p4ss"), "{text}");
    }

    #[test]
    fn jdbc_url_password_parameter_is_masked() {
        let old = map(json!({"connection.url": "jdbc:postgresql://h/db?user=u&password=hunter2"}));
        let new = map(json!({"connection.url": "jdbc:postgresql://h2/db?user=u&password=hunter3"}));
        let text: String = lines(&diff(&old, &new)).iter().map(|(_, t)| t.clone()).collect();
        assert!(!text.contains("hunter"), "{text}");
    }

    #[test]
    fn replay_warnings_depend_on_connector_kind() {
        let old = map(json!({"table.include.list": "a", "topics": "t", "tasks.max": "1"}));
        let new = map(json!({"table.include.list": "a,b", "topics": "t,u", "tasks.max": "2"}));
        let d = diff(&old, &new);
        let src: Vec<_> = replay_warnings(&d, true).into_iter().map(|(k, _)| k).collect();
        let snk: Vec<_> = replay_warnings(&d, false).into_iter().map(|(k, _)| k).collect();
        assert_eq!(src, vec!["table.include.list"]);
        assert_eq!(snk, vec!["topics"]);
        // tasks.max alone is not risky
        let d = diff(&map(json!({"tasks.max": "1"})), &map(json!({"tasks.max": "2"})));
        assert!(replay_warnings(&d, true).is_empty() && replay_warnings(&d, false).is_empty());
    }
}
