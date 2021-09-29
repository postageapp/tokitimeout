# Tokitimeout

This is a series of server examples to explore the behavior of Tokio under
different timeout mechanisms.

## Usage

There is a single self-contained client/server program with command-line
arguments that can be explained via:

```shell
cargo run --release -- --help
```

By default it sets up a local server, then runs some client testing
to it to produce a performance report.
