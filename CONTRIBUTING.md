# Contributing

Tuizium is an independent community project, not affiliated with Debezium or Apache Kafka.

1. Fork, create a branch, make a focused change.
2. Run the same checks as CI before opening a pull request:
   ```sh
   cargo fmt --all -- --check
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test --all-targets --all-features
   cargo build --release
   ```
3. To try changes against a real Kafka Connect, use the disposable stack in
   [`examples/local-connect`](examples/local-connect) (see the README).

Never include real hostnames, passwords or tokens in issues, tests or examples.
