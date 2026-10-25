# Changelog

Versions 1.0.1, 1.1.0 and 1.2.0 were developed one after the other and are committed together as 1.2.0, so
1.0.1 and 1.1.0 were never released on their own. Tests use a fake Kafka Connect; 1.0.1 was also checked by hand against a local
Connect 3.9.0, but 1.1.0 and 1.2.0 have not yet been run against a real cluster or in a real terminal, and nothing has been run
against a multi-worker cluster.

## 1.3.0 — Debezium depth, REST only (roadmap v1.3)

Tests use a fake Kafka Connect (140 tests). Also checked against the single-worker playground (Connect 3.9.0, Debezium 3.0.8):
- CLI: `stop`/`resume --wait`, `create`/`update --dry-run`, `create --wait`, the `--read-only` refusal, malformed input (bad JSON, `../` and `/` in names,
  empty `--url`, unreachable server), and a canary secret that never appeared in any output.
- UI, driven in a Windows pseudo-terminal (ConPTY, not a real terminal emulator): the Debezium view on PostgreSQL, MongoDB and MySQL sources and its absence on
  a non-Debezium connector, `S` then `L` (journal shows confirmed STOPPED), `u`, the `READ-ONLY` badge and refused keys, refusal to remove the active profile, `?`, Ctrl+C.
- `TUIZIUM_LOG=debug` log, CLI and UI: no password, plain or base64 (Basic auth), in the log file.
Not checked: a multi-worker cluster, Linux/macOS, a real terminal emulator, profile removal beyond the unit test, OSC 52 delivery.

- **Stop** (`S`, `tuizium stop <name>... [--wait]`): `PUT /connectors/{name}/stop`. Needs Kafka Connect 3.5 or newer; the version comes
  from `GET /`, and an unknown or older version refuses with a message (use pause instead) rather than sending the request.
  The journal confirms `STOPPED` only when the connector reports it. `u` / `resume` brings a stopped connector back.
- **Read-only mode**: `--read-only`, or `read_only = true` in a profile. Every key that changes a cluster (`p u S R F d e n t`) and every
  changing command (`pause resume stop restart create update`) is refused; `list`, `status` and `--dry-run` still work. The header shows `READ-ONLY`.
  The refusal is also enforced at the single place all UI changes pass through, not only at the keys.
- **Debezium view** in the Overview panel for Debezium connectors: include/exclude lists (with entry counts), `snapshot.mode`,
  `slot.name`, `publication.name` and family-specific settings for PostgreSQL, MySQL/MariaDB, SQL Server, Oracle, MongoDB, other sources and the Debezium JDBC / MongoDB sinks.
  It is read from the connector config only and says so on screen: Tuizium does not read the database, the replication slot or snapshot progress.
  `snapshot.mode`, `slot.name` and `publication.name` show "not set in config" when absent.
- **Profile removal**: `c`, then `d` on a profile, then `y`. The profile in use and the last remaining profile cannot be removed; environment variables are never touched.
- **Fix (security):** a password inside URL user-info (`mongodb://user:password@host/db`, `jdbc:...://user:password@host`) was shown in clear in the
  Overview, Config panel and CLI output. It is now masked everywhere other secrets are. This also affected 1.2.0 and earlier.
- **Dependencies:** `rustls` 0.23.45 → 0.23.46 (lockfile only) for RUSTSEC-2026-0346 to -0349 (TLS secrets in `Debug` output, ECH, Brotli
  certificate decompression, ServerHello downgrade). `cargo audit` reports no advisories afterwards; `cargo deny` passes advisories, licenses, bans and sources.

## 1.2.0 — scripting (roadmap v1.2)

Non-interactive subcommands, for scripts and CI. No terminal is needed; output is plain text or JSON, never ANSI colour.

- `tuizium list [--state running|paused|failed|unassigned] [--type source|sink]`
- `tuizium status <name>`: state, tasks, workers, masked config and masked failure traces
- `tuizium pause|resume <name>...` and `tuizium restart <name>... [--only-failed]`
- `tuizium create <file|->` and `tuizium update <file|->`: read a connector definition from a file or stdin
  (not inline JSON, so a typo cannot turn into a request)
- `-o/--output table|json` on every command. JSON has a stable shape: `{"cluster": .., "connectors": [..]}` for `list`,
  `{"cluster": .., "connector": {..}}` for `status`, `{"cluster": .., "action": .., "results": [..]}` for pause/resume/restart.
- `--wait`: after a change, poll up to 15 s and report `confirmed` or `not_confirmed` using the same judge as the UI journal
  (so a restart is only "confirmed" after a transition was seen, or reported honestly as "healthy again, finished between polls").
  Without `--wait` the command says the change was accepted and **not verified**; it never claims more than Kafka Connect's `202`.
- `--dry-run` (create/update): validate against the connector plugin, print the diff and warnings, send nothing.
- Exit codes: `0` success, `1` error (cannot connect, HTTP error, bad input, validation failed), `2` usage error,
  `3` request accepted but the connector is unhealthy (`status`) or the change was not confirmed (`--wait`).
- Safety: secrets are masked in all output; server-supplied text has control characters removed; `update` refuses a connector that
  does not exist and `create` one that does; neither sends anything when validation fails; an `update` that changes nothing sends nothing.
  `delete` is deliberately not available outside the UI.
- Cluster selection is the same as for the UI: `--url`, `--cluster`, `--config`, else `default_cluster` or the first profile.
- Changed: argument errors now exit with code 2 (they were 1). `--url` must start with `http://` or `https://`, so an empty shell variable gives a clear error instead of swallowing the next argument.
- `list` marks a connector whose state is RUNNING but has a failed task as `RUNNING (task failed)`.
- Known limit: `--wait` after `update` can report `confirmed` early, because the connector is already RUNNING before the new config has produced any effect.
- Fix (UI, found in manual testing): on Windows, characters typed with AltGr (`\`, `{`, `}`, `[` on Turkish and many other layouts) arrive as Ctrl+Alt+character and
  were dropped by every text field, so a Windows path pasted into the `n` box lost its backslashes. They are now treated as plain characters.
- Tests: 7 end-to-end tests (`tests/cli.rs`) run the real binary against a small fake Kafka Connect; argument parsing is unit-tested.

## 1.1.0 — safety and usability (roadmap v1.1)

- **Diff before saving an edit (B4).** After `e` the preview opens with "Changes against the running config": added, removed and
  changed settings (`+`, `-`, `~`), then the full new config. Secrets stay masked, including secret values inside URLs; a changed secret is
  reported as "value changed (hidden)".
- **Heads-up for risky changes.** When a changed setting may make a connector re-read data, lose its position or break (source: `connector.class`,
  `topic.prefix`, `slot.name`, `publication.name`, host/database, `snapshot.*`, include/exclude lists, schema-history topic, `database.server.id`;
  sink: `topics`, `topics.regex`, consumer group and offset-reset overrides, `insert.mode`/`primary.key.*`/`delete.enabled`/`table.name.format`)
  the preview lists them. This is a guess from the setting names, not something Tuizium can observe, and the screen says so.
- **Header totals.** `● n ok  ‖ n paused  ✖ n failed  ○ n other`, computed from the last snapshot only; drawn from 110 columns.
- **Filters by worker (`w`) and connector plugin (`x`).** They cycle through the values present on the cluster, combine (AND) with the type/state/search
  filters, show as tags in the list title and are cleared by `Esc`.
- **Search in the Trace panel.** `/` types a query, `Enter` keeps it, `n`/`N` jump to the next/previous matching row (wrapping around), matches are
  highlighted, `Esc` clears. Rows are wrapped by Tuizium itself, so a match always scrolls to the right place. Without a search, `n` is still "new connector".
- **Copy (`y`).** In the Config or Trace panel it copies the config (masked) or the trace (masked, control characters removed); in an error box it copies the
  error text. It uses the terminal's OSC 52 clipboard sequence: no clipboard dependency, but Tuizium cannot tell whether the terminal accepted it.
- **Narrow terminals (B5).** The plugin, type and tasks columns are dropped in that order; NAME and STATE stay, and below 34 columns only a state symbol is kept.
- **Delete dialog** already named the target cluster; it is now pinned by a test.
- Fix: secret masking inside a string stopped only at `& ; space quote`, so `password=...` followed by a newline swallowed the next line. It now also stops at
  newline, carriage return and tab.
- Tests: new unit tests for the diff and warnings, filters, trace search and wrapping, header counts, narrow layouts, and the masked diff preview.

## 1.0.1 — fixes from the v1.0.0 audit

- Secrets are masked in the Config and Overview panels, in traces and in server error messages (previously the Config panel showed connector passwords in clear). Editing is unchanged and refuses to save masked placeholders.
- Creating a connector is now verified correctly: the journal waits for it to appear and reports `created — confirmed RUNNING`, instead of a false "not listed on the cluster any more" warning.
- The JDBC sink template is split into "Debezium JDBC Sink" and "Confluent JDBC Sink". A template whose plugin is not installed on the cluster is marked and refused with an explanation.
- Without an interactive terminal (stdin/stdout piped or redirected) the UI prints an error and exits with code 1 instead of writing escape sequences into the output.

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
crates.io publishing is not done yet.
