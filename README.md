<div align="center">

# Substrate Archive
### Blockchain Indexing Engine

[Install the CLI](#install-the-cli) • [Documentation](#documentation) • [Contributing](#contributing) 

![Rust](https://github.com/paritytech/substrate-archive/workflows/Rust/badge.svg)

</div>

Run alongside a substrate-backed chain to index all Blocks, State, and Extrinsic data into PostgreSQL.

# Usage
The schema for the PostgreSQL database is described in the PDF File at the root of this directory.

Examples for how to use substrate-archive are in the [`examples/`](https://github.com/paritytech/substrate-archive/tree/master/archive/examples) directory

# Prerequisites 
Extended requirements list found in the [wiki](https://github.com/paritytech/substrate-archive/wiki/)
- PostgreSQL with the required schema: `schema/archive.sql`
- Substrate-based Blockchain running with RocksDB as the backend
- Substrate-based Blockchain running under `--pruning=archive`

# Install The CLI

## The CLI
The CLI is an easier way to get started with substrate-archive. It provides a batteries-included binary, so that you don't have to write any rust code. All thats required is setting up a PostgreSQL DB, and modifying a config file. More information in the [wiki](https://github.com/paritytech/substrate-archive/wiki)

## Install

`git clone https://github.com/paritytech/substrate-archive.git`

`cd substrate-archive/kusama-archive/`

`cargo build --release`

run with `./../target/release/kusama-archive`

You can copy the binary file `kusama-archive` anywhere you want ie `~/.local/share/bin/`. Or, instead of `cargo build --release` just run `cargo install --path .`

# Contributing
Contributors are welcome!

Read the [Doc](https://github.com/paritytech/substrate-archive/blob/master/CONTRIBUTING.md) 

# Documentation

You can build the documentation for this crate by running `cargo doc` in the `archive` directory.
More Docs [here]( https://github.com/paritytech/substrate-archive/wiki)


# Contact

You can contact me on
 - matrix @aplaza:matrix.parity.io
 - email: andrew.plaza@parity.io
 - keybase: https://keybase.io/insi

[contribution]: CONTRIBUTING.md
