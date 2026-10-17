# Tuizium v1.0.0 audit results

Audit of the released v1.0.0 against `TUIZIUM_AUDIT_CHECKLIST.md` (Stage 2). Report only: no application code was changed during the audit.

## 1. Audit environment

| | |
|---|---|
| Date | 2026-10-17 |
| Commit / tag | `78c26f1` (`v1.0.0`) |
| Binary | `tuizium 1.0.0`, release build from that tree (Windows) |
| Rust | rustc 1.92.0 locally; CI uses current stable (1.99) |
| OS | Windows 11 Pro 10.0.26300 |
| Terminal | Pseudo-terminal harness (pywinpty/ConPTY, screens read with `pyte`). Not a real terminal emulator such as Windows Terminal. |
| Kafka Connect | 3.9.0 (Kafka 3.9.0, KRaft), disposable stack `examples/local-connect`, REST on `127.0.0.1:8083` |
| Plugins | Debezium 3.0.8.Final: JDBC sink, MongoDB sink, PostgreSQL/MySQL/MariaDB/SQL Server/Oracle/Db2/MongoDB/others (13), plus Kafka MirrorMaker connectors 3.9.0 |
| Fixtures | `src-ok` (PostgreSQL source), `snk-ok`, `snk-multi` (3 tasks), `snk-badpass` (RUNNING, 2 FAILED tasks), `snk-failedtask` (RUNNING, 0 tasks), `snk-paused`, `snk-stopped`; 110 temporary MirrorHeartbeat connectors for T21 (deleted afterwards). No UNASSIGNED fixture. |
| Other | A local fake HTTP server returning 401 on port 18099 for T16 (stopped afterwards). All destructive tests ran against the disposable stack only. |

Result legend as in the checklist. Optional (P2) features that v1.0 does not build are marked `N/A`.

## 2. Summary

Checklist items (168):

| Result | Total | P0 | P1 | P2 |
|---|---|---|---|---|
| PASS | 68 | 51 | 15 | 2 |
| FAIL | 26 | 8 | 18 | 0 |
| PARTIAL | 52 | 31 | 20 | 1 |
| N/A | 22 | 2 | 2 | 18 |
| NOT TESTED | 0 | 0 | 0 | 0 |
| **All** | 168 | 92 | 55 | 21 |

Scenarios T01-T26: PASS 16, FAIL 1, PARTIAL 6, N/A 1, NOT TESTED 2.

Reading the numbers: `N/A` means an optional (P2) feature v1.0 does not build, or an item with nothing to check (10.3, 13.3); `FAIL` on P1 items is mostly a feature that is not built yet, not a defect. The defects are listed in section 4.

## 3. Evidence matrix


### 0. Audit setup

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 0.1 | P0 | PASS | This report, section 1. |
| 0.2 | P0 | PASS | Entry `src/main.rs` (arg parser, event loop), `app.rs` state/input/actions, `ui.rs` rendering, `api.rs` REST client, `config.rs`, `setup.rs`, `splash.rs`, `activity.rs`, `templates.rs`. No CLI subcommand module exists. |
| 0.3 | P0 | PARTIAL | README key table and `?` help overlay agree. Code has extra keys not in README: `g`/`G`, `[`/`]`, `x` (`app.rs:927-931`, `app.rs:1905`). |
| 0.4 | P0 | PASS | `examples/local-connect` brought up from scratch twice; Connect 3.9.0 with Debezium 3.0.8 PostgreSQL source and JDBC sink. |
| 0.5 | P0 | PARTIAL | Created: healthy source/sink, RUNNING connector with 2 FAILED tasks, PAUSED, STOPPED, RUNNING with 0 tasks, 3-task. UNASSIGNED could not be produced. |
| 0.6 | P0 | PASS | Throwaway containers and dummy credentials only. |
| 0.7 | P0 | PASS | Tuizium only calls the Connect REST API; README says so. |

### 1. Startup, clusters, authentication

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 1.1 | P0 | PASS | Ratatui TUI starts with no browser/web server. |
| 1.2 | P0 | PASS | `--help`, `--version` (`tuizium 1.0.0`); `--bogus` and `-c nope` exit 1 with message on stderr and nothing on stdout. |
| 1.3 | P0 | PARTIAL | URL configurable via profile/`--url`; tested with `http://localhost:8083`. Base URL with a path prefix not tested. |
| 1.4 | P0 | PARTIAL | Timeout (`request to … timed out`, observed with `docker pause`) and connect failure (`cannot connect to …`) differ (`api.rs` `describe_transport`). Refused and DNS failure share one message. Invalid URL and URL with credentials are rejected with a clear message. |
| 1.5 | P0 | PASS | T02/T18: dead endpoint and mid-session API freeze; UI stayed responsive. |
| 1.6 | P0 | PARTIAL | Request timeout (profile `timeout_secs`, 5 s connect). No retry/backoff. Fixed refresh interval measured at about one `GET /connectors?expand=...` per 5 s. |
| 1.7 | P0 | PARTIAL | rustls default verification; no `danger_accept_invalid_*` anywhere in `src/`. No custom-CA option. No live HTTPS test. |
| 1.8 | P0 | PARTIAL | `username` + `password_env`, `token_env` implemented and unit-tested. Live authenticated success not tested; a real 401 was shown correctly. |
| 1.9 | P0 | PARTIAL | Secret never appeared in screen, journal or errors in tests. `TUIZIUM_LOG=debug` file contents not inspected. Connector config display leaks secrets (see 3.5). |
| 1.10 | P1 | PARTIAL | Add/select/edit profiles exist (`c`, `a`, setup screen). No profile removal found in `config.rs`/`setup.rs`. |
| 1.11 | P1 | PASS | Generation counter drops stale snapshots (`app.rs:553`, `:693`); test `stale_responses_are_ignored`. Not exercised live. |
| 1.12 | P1 | PASS | Header shows `Kafka Connect 3.9.0` from `GET /`. |
| 1.13 | P1 | PASS | `P` lists class, type, version (13 Debezium plugins on the test stack). |
| 1.14 | P2 | PARTIAL | Bearer token via `token_env`. No custom headers/proxy option. |
| 1.15 | P2 | N/A | Not implemented (optional); README documents manual port-forward/ssh. |

### 2. Connector list

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 2.1 | P0 | PASS | 7/7 and 117/117 connectors listed. |
| 2.2 | P0 | PASS | SOURCE/SINK and full class shown in table and Overview. |
| 2.3 | P0 | PASS | `✖ RUNNING 0/2` for RUNNING connector with 2 FAILED tasks; Overview `⚠ 2 failed task(s)`. |
| 2.4 | P0 | PARTIAL | RUNNING, PAUSED, STOPPED, FAILED verified. UNASSIGNED/unknown not seen live; code has `UNASSIGNED` filter (`app.rs:159`) and `UNKNOWN` fallback. |
| 2.5 | P0 | PASS | `0/0` (STOPPED), `0/1`, `3/3` match the API. |
| 2.6 | P0 | PARTIAL | Substring search works (`badpass`, `hb-05` -> 10/117). Case handling not tested. |
| 2.7 | P0 | PASS | `f`/`s` combine; `Esc` clears. |
| 2.8 | P0 | PASS | `refreshed Ns ago` indicator; `⟳` while loading. Manual `r` not driven separately. |
| 2.9 | P0 | PARTIAL | Selection kept across auto refresh; about 1 request/5 s. Flicker cannot be judged from a terminal emulator. |
| 2.10 | P0 | PARTIAL | Unreachable cluster: actionable error. Empty cluster (T03) not tested. |
| 2.11 | P1 | PARTIAL | `o`/`O` cycle name/type/state; task-health sort not verified. |
| 2.12 | P1 | FAIL | Header only shows `Connectors n/m`; no healthy/degraded/failed/paused totals. |
| 2.13 | P1 | PASS | Selected row stays after refresh. |
| 2.14 | P1 | PASS | 117 connectors: first render 0.45 s; 300 `j` presses + `G` in 0.31 s; idle CPU 0.06 s per 20 s. |
| 2.15 | P1 | PARTIAL | Type and state filters combine; no plugin/worker filter. |
| 2.16 | P2 | N/A | Not implemented (optional). |

### 3. Details and configuration

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 3.1 | P0 | PASS | `Enter` opens details. |
| 3.2 | P0 | PASS | Name/class/type/state/worker/tasks match `GET /connectors/{n}/status`. |
| 3.3 | P0 | PASS | Tasks panel (ID, state, worker, error) and Trace panel per task. |
| 3.4 | P0 | PASS | Config panel shows full JSON, scrollable. No syntax highlighting. |
| 3.5 | P0 | FAIL | Config panel prints `"connection.password": "WRONG-SECRET-123"` and `"database.password": "postgres"` in clear. Connect 3.9 returns them raw; `templates::masked` (`templates.rs:149`) is used only in create/update previews. |
| 3.6 | P0 | PARTIAL | Multiline traces render; terminal escape sanitising is unit-tested. Very long values, null fields and unusual Unicode not tested live. |
| 3.7 | P1 | FAIL | No export/copy of config. |
| 3.8 | P1 | PARTIAL | Source Overview shows `topic.prefix`, `database.hostname`; Config panel shows `table.include.list`. No snapshot-mode/slot/publication fields highlighted. |
| 3.9 | P1 | PASS | Sink `topics` shown in Overview. |
| 3.10 | P1 | PASS | `Plugin ver.` shown separately from config. |
| 3.11 | P2 | N/A | Not implemented (optional). |

### 4. Operations

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 4.1 | P0 | PASS | Live create via `n` and `t` (`POST /connectors`, body `{name, config}`). |
| 4.2 | P0 | PASS | Live `e` edit: `PUT /connectors/snk-ok/config`; server `tasks.max` changed 1 to 2. |
| 4.3 | P0 | PARTIAL | Delete dialog names the connector and needs `y`; `n`/`Esc` cancel verified. Full-name typing not required; dialog does not show cluster. |
| 4.4 | P0 | PASS | Live pause then resume; states `PAUSED` then `RUNNING` verified via API. |
| 4.5 | P0 | PASS | Live restart; `POST …/restart?includeTasks=true&onlyFailed=false`; connector `RUNNING` after. |
| 4.6 | P0 | PASS | Live `R` in Tasks panel accepted (`POST …/tasks/0/restart`). Recovery not observable: the test task fails permanently. |
| 4.7 | P0 | PARTIAL | Live: 409 (duplicate), 500 (unknown class), 401 (fake server). 400/403/404/503 only via `describe_http` mapping and unit tests. |
| 4.8 | P0 | PARTIAL | Pause/resume/restart journal "accepted … verifying" then outcome. Create verification is wrong (see B2). |
| 4.9 | P0 | FAIL | No version/capability check; unsupported operations are not disabled or explained. |
| 4.10 | P0 | FAIL | Update preview shows only the new config: no old config, no diff, no data-replay warning. |
| 4.11 | P1 | FAIL | No stop action (`PUT …/stop`); STOPPED connectors are shown, `u` is the only way back. |
| 4.12 | P1 | PARTIAL | `F` implemented (`onlyFailed=true`) and unit-tested; not run live. |
| 4.13 | P1 | PARTIAL | Marks + batch `p/u/R/d/F`, delete asks confirmation, results go to the journal. Not run live. |
| 4.14 | P1 | FAIL | Preview only, no diff. |
| 4.15 | P1 | FAIL | No export before delete. |
| 4.16 | P2 | N/A | Not implemented (optional). |
| 4.17 | P2 | PASS | `L` journal and `actions.log` (rotated at 1 MB, mode 0600 on Unix); no secrets observed. |

### 5. Creation and templates

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 5.1 | P0 | PASS | `n` and `t` flows in the TUI. |
| 5.2 | P0 | FAIL | `t` always lists PostgreSQL, MySQL, JDBC Sink; nothing checks installed plugins. See B3. |
| 5.3 | P0 | PASS | `P` shows type and version. |
| 5.4 | P0 | PASS | Live: PostgreSQL template validated ("configuration is valid"), created, connector RUNNING. |
| 5.5 | P0 | FAIL | JDBC Sink template uses `io.confluent.connect.jdbc.JdbcSinkConnector` (`templates.rs:74`), not installed on the documented local stack (validate returns HTTP 500 class not found). Not usable there. |
| 5.6 | P0 | PASS | Live: file path import created `zz-good`. |
| 5.7 | P0 | PASS | Full final JSON preview, secrets masked, endpoint shown. |
| 5.8 | P0 | PASS | Live: validate endpoint; 4 field-level errors listed for bad `tasks.max`/missing topics. |
| 5.9 | P0 | PARTIAL | 409 and unknown-class errors are clear. Unknown class goes to `POST` (HTTP 500) because validation cannot run. |
| 5.10 | P1 | PARTIAL | Template form has labelled fields and defaults; required/optional markers not verified. |
| 5.11 | P1 | PASS | Live `$EDITOR` round trip returned to the TUI cleanly; failing editor shows a dialog. |
| 5.12 | P1 | PARTIAL | MySQL yes; SQL Server and MongoDB no. |
| 5.13 | P1 | FAIL | No Elasticsearch/S3 templates. |
| 5.14 | P1 | FAIL | No duplicate-connector action. |
| 5.15 | P1 | FAIL | No custom template files (JSON import exists). |
| 5.16 | P1 | FAIL | After create the journal shows a false warning and never confirms creation. See B2. |
| 5.17 | P2 | N/A | Not implemented (optional). |

### 6. Errors and diagnosis

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 6.1 | P0 | PASS | Connector state vs per-task state separated in table, Overview, Tasks and Trace. |
| 6.2 | P0 | PARTIAL | Full traces scroll; no in-trace search. |
| 6.3 | P0 | FAIL | No copy/export of errors. |
| 6.4 | P0 | PASS | Connectivity error in header (`✖ request … timed out`); state column shows connector health. |
| 6.5 | P0 | PARTIAL | Journal separates "accepted" from observed outcome; known limit: task restart judged on the whole connector. |
| 6.6 | P1 | FAIL | No doctor view. |
| 6.7 | P1 | PARTIAL | Hints only for HTTP 401/403/404/409/502/503. |
| 6.8 | P1 | PASS | No log feature; README says logs stay in worker logs. |
| 6.9 | P1 | PARTIAL | Journal records Tuizium actions and verified outcomes, not passive state changes; makes no history claims. |
| 6.10 | P2 | N/A | Not implemented (optional). |
| 6.11 | P2 | PASS | No lag/throughput/snapshot metrics are shown anywhere. |

### 7. Debezium-specific

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 7.1 | P1 | PARTIAL | Debezium detected by class prefix `io.debezium` (`api.rs:102`); PostgreSQL class verified, others not. |
| 7.2 | P1 | PARTIAL | `table.include.list` visible in Config; no dedicated filter view. |
| 7.3 | P1 | FAIL | Snapshot mode not surfaced. |
| 7.4 | P1 | PARTIAL | `topic.prefix` shown; slot/publication not highlighted. |
| 7.5 | P1 | FAIL | No field explanations. |
| 7.6 | P1 | FAIL | No signal support. |
| 7.7 | P2 | N/A | Not implemented (optional). |
| 7.8 | P2 | N/A | Not implemented (optional). |
| 7.9 | P2 | N/A | Not implemented (optional). |
| 7.10 | P2 | N/A | Not implemented (optional). |

### 8. Kafka-adjacent integrations

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 8.1 | P1 | PASS | Explicit `topics` shown; nothing is inferred. |
| 8.2 | P1 | PASS | No topic discovery/lag/sample claims. |
| 8.3 | P2 | N/A | Not implemented (optional integration). |
| 8.4 | P2 | N/A | Not implemented (optional integration). |
| 8.5 | P2 | N/A | Not implemented (optional integration). |
| 8.6 | P2 | N/A | Not implemented (optional integration). |

### 9. Terminal UX

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 9.1 | P0 | PASS | j/k, arrows, Enter, Esc, `?`, `/`, `q` all verified. |
| 9.2 | P0 | PASS | Footer changes between list and details focus. |
| 9.3 | P0 | PASS | `▶` row marker; panel titles. |
| 9.4 | P0 | PASS | Typing in search/prompts is not intercepted by shortcuts (verified with letters). See B7 for characters not tested. |
| 9.5 | P0 | PASS | `Connecting…`, `⟳`, `refreshed Ns ago`, `✖` error, `⚠` warning, stale data kept on failure. |
| 9.6 | P0 | PARTIAL | Symbols accompany colour (`● ✖ ‖ ⚠`). Light-background legibility not tested. |
| 9.7 | P0 | PARTIAL | Resize 130 -> 60 -> 40 -> 20 columns: no panic. At 40 columns the NAME column disappears (B5). Only a ConPTY harness was used, not real terminal emulators. |
| 9.8 | P0 | PASS | After `q`, Ctrl+C and a failing editor: alt-screen left, cursor restored. Panic path not tested (release profile uses `panic = "abort"`). |
| 9.9 | P0 | PASS | UI stayed responsive for 14 s while the API did not answer. |
| 9.10 | P0 | PARTIAL | Selection preserved; flashing not assessable. |
| 9.11 | P1 | N/A | No command palette. |
| 9.12 | P1 | PARTIAL | `?` overlay and footer hints exist; help is not searchable. |
| 9.13 | P1 | N/A | No mouse support. |
| 9.14 | P1 | PARTIAL | Class column shrinks; full values via Config panel. NAME column is dropped first at narrow width. |
| 9.15 | P1 | PARTIAL | Last profile saved; filters/layout not saved. |
| 9.16 | P2 | N/A | Not implemented (optional). |

### 10. Non-interactive CLI

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 10.1 | P0 | FAIL | No non-interactive subcommands. Documented as a known limitation in CHANGELOG. |
| 10.2 | P0 | PASS | `--help` lists only implemented flags. |
| 10.3 | P0 | N/A | README advertises no scripted commands. |
| 10.4 | P0 | PASS | Argument errors: exit 1, message on stderr, stdout empty; `--help` exit 0. |
| 10.5 | P0 | PASS | No secrets in any CLI output seen. |
| 10.6 | P1 | PARTIAL | `-c/--cluster` and `--config` exist; no `--timeout`/`--output`. |
| 10.7 | P1 | FAIL | No JSON output. |
| 10.8 | P1 | FAIL | No `--dry-run`. |
| 10.9 | P1 | FAIL | No shell completions. |
| 10.10 | P1 | FAIL | With stdout redirected the TUI still starts and writes ANSI sequences (B6). `--help` output has none. |
| 10.11 | P2 | N/A | Not implemented (optional). |

### 11. Security and configuration

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 11.1 | P0 | PASS | Tracked files and git history scanned: no secrets or private addresses; example passwords are throwaway values labelled as such. |
| 11.2 | P0 | FAIL | Display unmasked (3.5); even the preview mask only matches keys containing password/secret/token/credential. |
| 11.3 | P0 | PARTIAL | Journal and editor temp file use mode 0600 on Unix. `config.toml` is written without explicit permissions (it holds env-var names, no secrets). |
| 11.4 | P0 | PARTIAL | Code inspection only; no live HTTPS test. |
| 11.5 | P0 | PARTIAL | Create dialog names the cluster ("on adhoc"); delete dialog shows the connector but not the cluster. |
| 11.6 | P0 | PARTIAL | 401 test leaked nothing. Server error bodies and traces are shown verbatim (clipped to 500 chars), so server-side secrets could appear. |
| 11.7 | P1 | PARTIAL | `--config`, `TUIZIUM_CONFIG`, profile fields documented; precedence between them is not stated. |
| 11.8 | P1 | PASS | Credentials never on the command line; `--url` with embedded credentials is rejected. |
| 11.9 | P1 | FAIL | No read-only mode. |
| 11.10 | P2 | N/A | Not implemented (optional). |

### 12. Performance and tests

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 12.1 | P0 | PASS | See 2.14. |
| 12.2 | P0 | PASS | Idle CPU 0.06 s per 20 s with 117 connectors; `refresh_secs` configurable. |
| 12.3 | P0 | PARTIAL | Stale responses are dropped; in-flight requests are not cancelled; Ctrl+C exits within 1.5 s. |
| 12.4 | P0 | PASS | Non-JSON response shown as `unexpected response from /connectors: expected value at line …`; absent fields use serde defaults. |
| 12.5 | P0 | PASS | 86 unit tests cover parsing, payload building, masking, filtering, sanitising. |
| 12.6 | P0 | PARTIAL | Fake-server tests exist; no automated test against a real Connect (done manually here). |
| 12.7 | P0 | PASS | fmt/clippy/test/release local (rustc 1.92); CI green on `78c26f1` (Rust stable, 3 OS). |
| 12.8 | P1 | PARTIAL | TestBackend tests for splash and app key handling; no full-screen snapshots. |
| 12.9 | P1 | PASS | Out-of-order responses and stale validation ids are tested (`app.rs:1550`, `:1635`). |
| 12.10 | P1 | PARTIAL | Unit tests run on 3 OS in CI; manual terminal checks on Windows only. |
| 12.11 | P2 | N/A | No benchmarks (optional). |

### 13. v1.0 release verification

| # | Prio | Result | Evidence / notes |
|---|---|---|---|
| 13.1 | P0 | PARTIAL | `cargo install --path .` verified and Windows release asset verified; Linux/macOS binaries not run; no screenshot in README. |
| 13.2 | P0 | PASS | README separates prebuilt binaries from source builds. |
| 13.3 | P0 | N/A | README has no screenshots. |
| 13.4 | P0 | PASS | README states Windows hand-tested, others CI-built; release has the 5 targets listed. |
| 13.5 | P0 | PASS | Release workflow success, 5 archives + checksums; Windows zip downloaded, checksum matched, `--version` OK. |
| 13.6 | P0 | PARTIAL | LICENSE, `.gitignore`, CHANGELOG present; GitHub release notes are auto-generated only ("Full Changelog" link). |
| 13.7 | P0 | PASS | History scan clean; junk files with a private IP were removed before the first commit. |
| 13.8 | P0 | PARTIAL | Clean checkout install and Windows asset start verified; full first-run flow from the release asset not run. |
| 13.9 | P1 | PASS | `examples/local-connect` reproducible; CONTRIBUTING present. |
| 13.10 | P1 | PASS | README states the project is independent of Debezium/Apache Kafka. |

### Scenarios T01-T26

| ID | Result | Observed |
|---|---|---|
| T01 | PASS | Connected, `Kafka Connect 3.9.0` shown. |
| T02 | PASS | `cannot connect to http://127.0.0.1:9/ (is Kafka Connect …`; no crash. |
| T03 | NOT TESTED | Needs an empty Connect cluster; not set up. |
| T04 | PASS | Source and sinks listed with correct types. |
| T05 | PASS | `snk-badpass`: `✖ RUNNING 0/2`, traces in Trace panel. |
| T06 | PARTIAL | 3-task connector `3/3` and 2-task all-FAILED verified; mixed task states not produced. |
| T07 | PASS | Search/filter preserved across auto refresh. |
| T08 | PASS | Pause/resume reconciled, verified through the API. |
| T09 | PARTIAL | Correct endpoint and acceptance; recovery not observable (task fails permanently). |
| T10 | PASS | Created from JSON file; listed. |
| T11 | PARTIAL | PostgreSQL template passes; JDBC Sink template targets a plugin not on the local stack. |
| T12 | PASS | HTTP 500 "Failed to find any class …" shown, no false success. |
| T13 | PASS | Four field-level validation errors shown before create. |
| T14 | PASS | PUT payload correct; preview shows new config only (no diff). |
| T15 | PASS | Confirmation required; list refreshed. |
| T16 | PASS | Real 401: `HTTP 401: Unauthorized … check credentials`; secret absent from screen, stream and journal. |
| T17 | PARTIAL | 409 on duplicate create shown explicitly; rebalance 409 not reproduced. |
| T18 | PASS | API frozen 14 s: error in header, stale data kept, recovered on resume. |
| T19 | PARTIAL | No panic down to 20x6; NAME column drops at 40 columns. |
| T20 | PARTIAL | Ctrl+C restored the terminal at idle; not tested during an in-flight request. |
| T21 | PASS | 117 connectors responsive, bounded API load. |
| T22 | FAIL | Password visible in the Config panel; masked in previews, journal and errors. |
| T23 | NOT TESTED | Live cluster switching not run (unit-tested generation counter). |
| T24 | N/A | No JSON CLI output exists. |
| T25 | PASS | No metrics shown at all. |
| T26 | PASS | Base TUI works with no optional integration. |

## 4. Top release risks and bugs

| # | Severity | Module | Finding |
|---|---|---|---|
| B1 | High (P0) | `ui.rs` / details Config panel | **Secrets are shown in clear.** Kafka Connect returns connector config unmasked, and the Config panel prints it as-is (`"connection.password": "WRONG-SECRET-123"`). Repro: open any connector with a password, press `3`. Only create/update previews mask values (`templates.rs:149`), and that mask only matches key names containing password/secret/token/credential. Screenshots and screen sharing leak credentials. |
| B2 | Medium (P1) | `activity.rs` `judge` (`:175`), create watch in `app.rs` | **Creation is not verified correctly.** After a successful create (JSON file or template) the journal says `<name>: not listed on the cluster any more` as a warning, and never confirms the connector is running, because the first snapshot after `POST` can still lack it. Repro: `n`, a valid JSON file, Enter, Enter; press `L`. The connector does appear a few seconds later. |
| B3 | Medium (P0/P1) | `templates.rs`, README | **Templates are offered whether or not the plugin exists, and the JDBC Sink template does not match the documented stack.** The template uses `io.confluent.connect.jdbc.JdbcSinkConnector`; the local stack in `examples/local-connect` ships Debezium's JDBC sink, so validating it returns HTTP 500 class not found. The PostgreSQL template works. |
| B4 | Medium (P0/P1) | `app.rs` edit flow | **No old-config diff or data-replay warning** before `PUT /connectors/{name}/config`; the preview shows only the new config. |
| B5 | Low (P1) | `ui.rs` | At 40 columns the NAME column is dropped first, so rows cannot be told apart. No panic down to 20x6. |
| B6 | Low (P1) | `main.rs` | With stdout redirected (no TTY) the TUI still starts and writes ANSI sequences. It should refuse with a message. |
| B7 | **Unverified**, possibly high on Windows | input handling | In the ConPTY harness the characters `i`, `{`, `}` and `\` never reached text prompts (`/` search, `n` prompt). Plain Windows console reads in the same harness received them, and the code has no special handling of those keys, so this is most likely a harness/Turkish-keyboard (AltGr) artefact. It must be checked by hand on a real terminal: type `i`, `{`, `}`, `\` into the `/` search and the `n` prompt. |
| B8 | Low | various | No retry/backoff; no profile removal; no read-only mode; config file written without explicit permissions (holds env-var names only). |

## 5. Untested or blocked

- **T03 empty cluster:** needs a Connect cluster with no connectors. Reproduce: bring up the stack, do not create connectors, run `tuizium --url http://localhost:8083`.
- **T23 live cluster switching** (`c`): only the generation-counter unit test ran.
- **UNASSIGNED / mixed task states / rebalance 409:** could not be produced on a single-worker stack. A multi-worker cluster is needed.
- **HTTPS, custom CA, basic/bearer authentication success:** no TLS or authenticated Connect available; only code inspection and unit tests.
- **Linux and macOS:** binaries built by CI, never run. Only Windows was exercised.
- **Real terminals and light backgrounds:** only the ConPTY harness was used.
- **Panic path** and the claim in the README that the terminal is restored on panic: not tested (release profile uses `panic = "abort"`).
- **Batch operations (`Space` marks) and `F` restart-failed:** unit-tested, not run live.
- **`TUIZIUM_LOG=debug` file contents:** not inspected for secrets.
- **Characters `i { } \` in text prompts:** see B7.

## 6. Roadmap after v1.0

Smallest useful slices, in order.

**v1.0.1 (fixes, no new features)**
1. B1: mask secret-looking values in the Config panel by default (same key rule as the previews, plus a broader key list); explicit reveal key.
2. B2: for `create`, treat "not listed yet" as "keep watching" and report `created, RUNNING (n/n tasks)` when observed.
3. B3: make the JDBC Sink template use the plugin that is installed (offer Debezium and Confluent variants) or hide templates whose plugin is missing; align the README local stack.
4. B7: confirm on a real Turkish keyboard; fix input handling if it is real.

**v1.1 (safety and usability)**
*Status 2026-10-17: v1.0.1 fixes (items 1-4 except B7), v1.1 (items 5-7) and v1.2 (item 8) are implemented and tested with a fake Kafka Connect only, and are committed together as crate version 1.2.0 (not pushed, not tagged). Not done: B7 (needs the user's Turkish keyboard), v1.3 (item 9), the live log panel and the optional integrations (items 10-11). Not verified: a run against a real cluster (the git-excluded `playground/` stack was written but never started), the new keys in a real terminal, OSC 52 delivery, Linux/macOS.*

5. Old-vs-new diff and replay warning before update (B4) — **done** (the replay note is a heuristic on setting names and says so); target cluster in the delete dialog — **was already there**, now tested.
6. Refuse to start without a TTY (B6) — done in v1.0.1; narrow-width column fix (B5) — **done** (plugin, type, tasks dropped in turn; below 34 columns only a state symbol).
7. Health totals in the header (shown from 110 columns); plugin/worker filters (`x`/`w`); in-trace search (`/`, `n`, `N`); copy sanitised error/config/trace (`y`, OSC 52, delivery cannot be confirmed) — **done**.

**v1.2 (scripting)**
8. Non-interactive subcommands `list`, `status`, `pause`, `resume`, `restart` with `--output json`, exit codes and no ANSI; `--dry-run` for create/update — **done**, plus `create`/`update` themselves and `--wait` verification. `delete` is intentionally UI-only.

**v1.3 (Debezium depth, REST only)**
9. Debezium views for capture filters, snapshot mode, slot/publication from config, with a clear "configured, not runtime" label; stop action and capability check by Connect version; read-only mode; profile removal.

**Later (optional integrations, clearly separated)**
10. Kafka client, Schema Registry, JMX/Prometheus metrics with explicit provenance. Never show lag or throughput without such a source.
11. **Live worker log panel (planned, not started; explicitly out of v1.0.1).** A separate `Logs` panel in the Ratatui UI that follows Kafka Connect worker logs in real time. The Connect REST API does not serve worker logs, so this needs an external log source (Docker, Kubernetes or another configured source), and the panel must never look usable while no source is configured.
    - Real-time streaming, with a visible label of the active log source (type and target).
    - ERROR / WARN / INFO level filters, text search, and auto-scroll that pauses when the user scrolls up.
    - Filter by connector/task, only where log lines can be correlated to them (otherwise say so).
    - Mask sensitive values in log lines with the same rules as the Config panel and traces.
    - Handle connection loss and reconnect with backoff, showing the stream state (connected / reconnecting / stopped).
    - Reading runs off the UI thread (bounded buffer), so a log flood never blocks the TUI.
    - No log source configured: the panel shows a "not configured" state with setup hints, not an empty live view.

## 7. No false claims

- Items marked PASS were executed, or are backed by code plus a named unit test where stated. Nothing unexecuted is marked PASS.
- No CDC lag, throughput or snapshot-progress value is shown by Tuizium or claimed here.
- Capabilities of the Connect REST API are kept apart from optional integrations (Kafka client, Schema Registry, JMX, source DB, worker logs), none of which Tuizium implements.
