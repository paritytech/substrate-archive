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
- depending on the chain you want to index, ~60GB free space
- PostgreSQL with a database ready for lots of new data
- Substrate-based Blockchain running with RocksDB as the backend
- Substrate-based Blockchain running under `--pruning=archive`

# Install The CLI

## The CLI
The CLI is an easier way to get started with substrate-archive. It provides a batteries-included binary, so that you don't have to write any rust code. All thats required is setting up a PostgreSQL DB, and modifying a config file. More information in the [wiki](https://github.com/paritytech/substrate-archive/wiki)

## Install

- `git clone https://github.com/paritytech/substrate-archive.git`
- `cd substrate-archive/polkadot-archive/`
- `cargo build --release`
- startup your polkadot, kusama or westend node `./polkadot --chain=polkadot --pruning=archive`
- run the archive with `./../target/release/polkadot-archive --config "your-config-file.toml" --chain=polkadot` where 'chain' is one of polkadot, kusama or westend, depending on which chain you are running
- You can copy the binary file `polkadot-archive` anywhere you want ie `~/.local/share/bin/`. Or, instead of `cargo build --release` just run `cargo install --path .`

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
