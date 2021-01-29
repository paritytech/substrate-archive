<div align="center">

# Substrate Archive

### Blockchain Indexing Engine

[Install the CLI](#install-the-cli) • [Documentation](#documentation) • [Contributing](#contributing) • [FAQ](#faq)

![Rust](https://github.com/paritytech/substrate-archive/workflows/Rust/badge.svg)
<a href="https://matrix.to/#/!roCGBGBArdcqwsdeXc:matrix.parity.io?via=matrix.parity.io&via=matrix.org&via=web3.foundation">
![Matrix](https://img.shields.io/badge/Matrix-archive%20chatroom-blue)
</a>
</div>

Run alongside a substrate-backed chain to index all Blocks, State, and Extrinsic data into PostgreSQL.

![](https://i.imgur.com/1eOkKvo.gif)

## Usage

The schema for the PostgreSQL database is described in the PDF File at the root of this directory.

Examples for how to use substrate-archive are in the [`examples/`](https://github.com/paritytech/substrate-archive/tree/master/examples) directory

## Prerequisites

Extended requirements list found in the [wiki](https://github.com/paritytech/substrate-archive/wiki/)

- depending on the chain you want to index, ~60GB free space
- PostgreSQL with a database ready for lots of new data
- Substrate-based Blockchain running with RocksDB as the backend
- Substrate-based Blockchain running under `--pruning=archive`

## Install The CLI

### The CLI

The CLI is an easier way to get started with substrate-archive. It provides a batteries-included binary, so that you don't have to write any rust code. All thats required is setting up a PostgreSQL DB, and modifying a config file. More information in the [wiki](https://github.com/paritytech/substrate-archive/wiki)

#### The Node-Template CLI

The node-template CLI (in /bin/node-template-archive) is provided as an example of implementing substrate-archive for your chain.

### Quick Start

```bash
git clone https://github.com/paritytech/substrate-archive.git
# Set up the databases
source ./substrate-archive/scripts/up.sh # Run ./scripts/down.sh to drop the database
cd substrate-archive/bin/polkadot-archive/
# Start the normal polkadot node with `pruning` set to `archive`
polkadot --chain=polkadot --pruning=archive
# Start up the substrate-archive node. `chain` can be one of `polkadot`, `kusama`, or `westend`.
cargo run --release --  -c test_conf.toml --chain=polkadot
```

You can access the help dialog via `cargo run --release -- --help`. Note that `up` and `down` scripts are meant for convenience and are not meant to be complete. Look in the [wiki](https://github.com/paritytech/substrate-archive/wiki) for more information about the database setup.

## [FAQ](https://github.com/paritytech/substrate-archive/wiki/0.\)-FAQ)

## Contributing

Contributors are welcome!

Read the [Doc](https://github.com/paritytech/substrate-archive/blob/master/CONTRIBUTING.md)

## Documentation

You can build the documentation for this crate by running `cargo doc`. 
More Docs [here](https://github.com/paritytech/substrate-archive/wiki)

## Troubleshooting

### Archive fails to start with a `too many open files` error.

Because of the way a [RocksDB Secondary Instance](https://github.com/facebook/rocksdb/wiki/Secondary-instance) works, it requires that all the files of the primary instance remain open in the secondary instance. This could trigger the error on linux, but simply requires that you raise the max open files limit (`ulimit`):

- With Docker: `$ docker run --ulimit nofile=90000:90000 <image-tag>`
- For Current Shell Session: `ulimit -a 90000`
- Permanantly Per-User
  - Edit `/etc/security/limits.conf`

    ```
    # /etc/security/limits.conf
    *           hard    nofile      4096
    *           soft    nofile      1024
    some_usr    hard    nofile      90000
    some_usr    soft    nofile      90000
    insipx      hard    nofile      90000
    insipx      soft    nofile      90000
    root        hard    nofile      90000
    root        soft    nofile      90000
    ```

For macOS and Linux, a warning message will be raised on the startup when there is a low fd sources limit in the current system, but Windows won't have such a low fd limit warning.

## Contact

You can contact us at:
 - matrix: #substrate-archive:matrix.parity.io
 - email: andrew.plaza@parity.io

[contribution]: CONTRIBUTING.md
