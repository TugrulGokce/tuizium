<p align="center">
  <img src="assets/logo.png" alt="Tuizium logo" width="200">
</p>

# Tuizium

**A fast terminal interface for Debezium and Kafka Connect.**

Inspect, monitor and operate Kafka Connect connectors from your terminal — no more `curl`.
Tuizium is an independent community project; it is **not affiliated with Debezium or Apache Kafka**.
Keyboard-driven, lightweight, in the spirit of k9s / lazydocker / htop. Works with Debezium and any
other Kafka Connect connector. Every action uses the official
[Kafka Connect REST API](https://kafka.apache.org/documentation/#connect_rest).

## Features

- Multiple cluster profiles, switch with `c`; basic auth and bearer tokens (secrets via env vars only)
- Connector table: name, type, state, tasks (running/total), plugin; search, type/state filters, sorting,
  manual + automatic refresh, failed connectors highlighted
- Details: overview, task states, config JSON, failure traces
- Actions: pause, resume, restart (connector or single task), delete (confirmed), create from JSON file / inline JSON
- Templates (Debezium PostgreSQL, Debezium MySQL, JDBC Sink) with editable fields, masked JSON preview and confirmation
- Never blocks the UI on network calls; handles timeouts and unreachable clusters; restores the terminal on exit/panic

## What does Tuizium talk to?

Debezium is not a separate server here: it is a set of **connector plugins running inside Kafka Connect**
(`io.debezium.connector.postgresql.PostgresConnector`, ...). Tuizium therefore talks only to the
**Kafka Connect REST API** (default port `8083`) — never to Kafka brokers, databases or Debezium directly.

```
Tuizium ──HTTP(S)──▶ Kafka Connect REST (:8083) ──▶ workers running Debezium/JDBC/... connectors
                                                    └──▶ Kafka brokers, source/sink databases
```

- One Connect **cluster** = one REST URL. Any worker of the cluster answers for the whole cluster
  (the *Worker* field in the details panel shows which worker runs each connector/task), so you point
  Tuizium at one worker, or at a load balancer / Kubernetes service in front of them.
- **Several Debezium/Connect deployments** (dev, staging, prod, per-team): add one profile per Connect
  cluster and switch with `c`. Use `P` to see which Debezium versions each cluster has installed.
- Network access: the machine running Tuizium must reach the REST port (VPN, `kubectl port-forward
  svc/<connect> 8083:8083`, `ssh -L 8083:connect-host:8083 jump-host`, or an ingress). If the REST API is
  protected, use `username` + `password_env` (basic) or `token_env` (bearer).
- **Debezium Server** (the standalone, non-Connect runtime) has no Connect REST API and is not supported.

## Prerequisites

- A running **Kafka Connect** with its REST API reachable from your machine (default `http://localhost:8083`).
  No local Kafka Connect yet? See [Try it locally](#try-it-locally).
- To build from source: a recent stable Rust toolchain (edition 2024). Prebuilt binaries need nothing else.
- A terminal with colour support. Tested on Windows 11 (Windows Terminal/ConPTY); Linux and macOS binaries are built by CI but not yet tested by hand.

## Install

**Prebuilt binary** — from the [GitHub Releases](../../releases) page download the archive for your platform
(`tuizium-v1.0.0-<target>.tar.gz` for Linux/macOS, `.zip` for Windows) and its `.sha256` file, verify, extract, and put `tuizium` on your `PATH`:

```sh
# Linux / macOS
sha256sum -c tuizium-v1.0.0-x86_64-unknown-linux-gnu.tar.gz.sha256   # macOS: shasum -a 256 -c ...
tar xzf tuizium-v1.0.0-x86_64-unknown-linux-gnu.tar.gz
install -m 755 tuizium-v1.0.0-x86_64-unknown-linux-gnu/tuizium ~/.local/bin/
```

```powershell
# Windows (PowerShell)
Expand-Archive tuizium-v1.0.0-x86_64-pc-windows-msvc.zip .
# then move tuizium-v1.0.0-x86_64-pc-windows-msvc\tuizium.exe to a folder on your PATH
```

**From source:**

```sh
git clone <repository-url> tuizium && cd tuizium
cargo install --path .
```

crates.io publishing is not done yet.

## Usage

```sh
tuizium                       # opening animation, then asks where Kafka Connect / Debezium lives
tuizium --cluster prod        # start on a profile, skipping the question
tuizium --url http://host:8083
tuizium --no-splash           # skip the opening screen (or set TUIZIUM_NO_SPLASH)
```

On start the logo ripples above a steady TUIZIUM wordmark until you press **Enter** (Esc quits), then Tuizium asks for the **Kafka Connect REST API URL**
(the address Debezium is reached at, usually port 8083) with the last profile prefilled — press Enter to accept it.
`←/→` on the name field switches between saved profiles; a new or edited profile is saved to the config. Only the *names*
of password/token environment variables are asked for, never the secrets. Set `ask_on_start = false` in the config to
connect straight to `default_cluster` instead.

### Configuration

Located at `~/.config/tuizium/config.toml` (Linux), `~/Library/Application Support/tuizium/config.toml`
(macOS) or `%APPDATA%\tuizium\config.toml` (Windows). Override with `--config` or `TUIZIUM_CONFIG`.

```toml
default_cluster = "local"
refresh_secs = 5
ask_on_start = true                      # false = skip the connection screen

[clusters.local]
url = "http://localhost:8083"

[clusters.prod]
url = "https://connect.example.com"
username = "admin"
password_env = "PROD_CONNECT_PASSWORD"   # name of an env var — passwords are never stored in the file

[clusters.cloud]
url = "https://connect.example.org"
token_env = "CLOUD_CONNECT_TOKEN"        # bearer token from an env var
timeout_secs = 15
```

You can also add profiles inside the app (`c`, then `a`). Set `TUIZIUM_LOG=debug` to write diagnostics to a log
file (`tuizium.log` in the OS state/data directory); logs never go to the terminal.

### Keys

| Key | Action |
|---|---|
| `j`/`k`, arrows | Navigate (scroll in details) |
| `/` | Search by name |
| `Enter` | Focus connector details |
| `Esc` | Back / clear filters |
| `Tab`, `1`–`4` | Details panel: Overview, Tasks, Config, Trace |
| `r` / `a` | Refresh / toggle auto refresh |
| `f` / `s` | Cycle type / state filter |
| `o` / `O` | Cycle sort column / reverse |
| `Space` | Mark connector for batch operations (`Esc` clears marks) |
| `p` / `u` | Pause / resume (all marked, or the selected one) |
| `R` | Restart connector(s) (restart selected task in the Tasks panel) |
| `F` | Restart only failed connectors/tasks (marked, or every failed one) |
| `e` | Edit the config in `$VISUAL`/`$EDITOR`, validate, preview, save (`PUT /connectors/{name}/config`) |
| `P` | Installed plugins and versions (Debezium highlighted) |
| `L` | Activity log: every action, whether Kafka Connect accepted it, and what was actually observed afterwards |
| `d` | Delete (asks for confirmation, `y`) |
| `n` | New connector from a JSON file path or inline JSON |
| `t` | New connector from a template |
| `c` | Switch cluster (`a` adds a profile) |
| `?` | Help |
| `q`, `Ctrl+C` | Quit |

Custom JSON accepts either `{"name": "...", "config": {...}}` or a flat config map containing `"name"`.

## Try it locally

[`examples/local-connect`](examples/local-connect) is a disposable Docker Compose stack (Kafka, Postgres, Kafka Connect with
the Debezium PostgreSQL source and JDBC sink plugins). Its REST API is published on `127.0.0.1:8083` only.

```sh
docker compose -f examples/local-connect/docker-compose.yml up -d --build
curl -s localhost:8083/connector-plugins | head -c 200     # wait until this answers (about a minute)
tuizium --url http://localhost:8083
```

Create connectors with `n`/`t` in Tuizium (the Debezium PostgreSQL template uses host `postgres`, port `5432`,
user/password `postgres`, database `inventory`, table `public.customers`). Tear down with
`docker compose -f examples/local-connect/docker-compose.yml down -v`. The passwords in that file are throwaway values.

## Troubleshooting

- **`cannot connect to http://…`** — Kafka Connect is not reachable: check the URL/port, VPN or port-forward, and that
  `curl <url>/` answers.
- **401/403** — the REST API needs credentials: set `username` + `password_env` (or `token_env`) in the profile and export that variable.
- **A template or connector fails with "class not found"/plugin errors** — the plugin is not installed on the Connect worker; press `P` to see what is.
- **Config rejected on save** — the validation message from Kafka Connect is shown; fix the listed fields.
- **Garbled screen** — resize the terminal or press `r`; Tuizium needs a UTF-8, true-colour capable terminal.
- Set `TUIZIUM_LOG=debug` for diagnostics in the log file.

## Did my pause / resume / restart actually happen?

Kafka Connect answers these requests with a bare `202 Accepted` and does the work asynchronously; its REST API has no
event log. So after every action Tuizium polls once a second for up to 15 s and records the outcome (press `L`):

```
14:32:05 · restart sink-postgres: requested
14:32:05 ✔ restart sink-postgres: accepted by Kafka Connect (HTTP 2xx)
14:32:07 ✔ sink-postgres: restart confirmed (RESTARTING → RUNNING, RUNNING (1/1 tasks running))
```

If a restart finishes between two polls, the journal says so instead of claiming a confirmed restart. A connector that
is still unhealthy after 15 s is reported as an error. The journal is also appended to `actions.log` in the user state
directory (`%LOCALAPPDATA%\tuizium` on Windows, `~/.local/state/tuizium` on Linux; rotated at 1 MB). The connector's own
logs stay in the Connect worker logs (`docker logs <connect>`).

## Security notes

- Secrets are never stored: profiles hold the *names* of environment variables. URLs containing credentials are rejected.
- A warning is shown when credentials would be sent over plain `http` to a non-local host.
- Redirects are only followed on the same host and never from https to http; response bodies are capped at 32 MiB.
- The editor temp file is created exclusively (never through a symlink), private to your user on Unix, and deleted afterwards.
- Text from the server is rendered through ratatui, which drops control characters, so a hostile connector name or stack
  trace cannot inject terminal escape sequences. Journal lines are sanitized, so they cannot forge log entries.
- Connector names `.`/`..` and names with control characters are refused before any request is made.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo build --release
```

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [CHANGELOG](CHANGELOG.md). A sample config is in [`examples/config.toml`](examples/config.toml).

Layout: `api.rs` (REST client + models), `app.rs` (state, input, async actions), `ui.rs` (rendering),
`templates.rs`, `config.rs`.

Releases are built by GitHub Actions when a `v*` tag is pushed.

## License

MIT — see [LICENSE](LICENSE).
