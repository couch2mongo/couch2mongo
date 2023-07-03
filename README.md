# CouchDB to MongoDB Streaming Replicator

This simple Rust application replicates data from a CouchDB database to a MongoDB database. It is designed to be run as
a Docker container but can also be run as a standalone application.

## Requirements

You need both Rust Stable and Nightly!

```bash
rustup update stable
rustup toolchain install nightly
```

If you want to use the DynamoDB Sequence Store, you'll need to have
v2 installed. You can download it from 
[here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html).

## Building

```bash
cargo build
```

## Running

```bash
cargo run -- --help
```

See `config.toml` for an example configuration file.
