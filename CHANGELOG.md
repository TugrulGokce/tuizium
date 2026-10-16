# Changelog

## 1.0.0

First public release.

- Terminal UI (Rust + Ratatui) for the Kafka Connect REST API; works with Debezium and other connectors.
- Connector table with search, type/state filters, sorting, auto refresh; details with overview, tasks, config and failure traces.
- Pause, resume, restart (connector or task), restart failed, delete (confirmed), batch operations on marked connectors.
- Create connectors from a JSON file, inline JSON, or templates (Debezium PostgreSQL, Debezium MySQL, JDBC Sink); edit configs in `$EDITOR` with validation.
- Installed plugin list (`P`) and an activity log (`L`) that records what was actually observed after each action.
- Cluster profiles with credentials read from environment variables; security hardening of the HTTP client.
- CLI flags: `-c/--cluster`, `-u/--url`, `--config`, `--no-splash`, `-h`, `-V`.

Known limitations: no failover between workers; per-task restart verification judges the whole connector;
no non-interactive CLI subcommands.
