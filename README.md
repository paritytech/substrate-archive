<div align="center">

# Substrate Archive

[Install the CLI](#install-the-cli) • [Documentation] • [Contributing](#contributing) 

![Rust](https://github.com/paritytech/substrate-archive/workflows/Rust/badge.svg)

</div>

Run alongside a substrate-backed chain to index all Blocks, State, and Extrinsic data into PostgreSQL.

# Example Usage
```rust
use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use substrate_archive::{backend, init, chain_traits::{HeaderBackend as _}, twox_128, StorageKey};

pub fn main() {
    let db =
        backend::open_database::<Block>("/home/insipx/.local/share/polkadot/chains/ksmcc4/db", 8192).unwrap();

    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let client =
        backend::client::<Block, ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor, _>(
            db, spec,
        )
        .unwrap();

    let info = client.info();
    println!("{:?}", info);

    // create the keys we want to query storage for
    let system_key = twox_128(b"System").to_vec();
    let accounts_key = twox_128(b"Account").to_vec();

    let mut keys = Vec::new();
   
    let mut system_accounts = system_key.clone();
    system_accounts.extend(accounts_key);
    
    keys.push(StorageKey(system_accounts));
   
    // run until we want to exit (Ctrl-C)
    futures::executor::block_on(init::<ksm_runtime::Runtime, _>(
        client,
        "ws://127.0.0.1:9944".to_string(),
        keys,
    ))
    .unwrap()
}
``` 

The schema for the PostgreSQL database is described in the PDF File at the root of this directory

# Prerequisites 
Extended requirements list found in the [wiki](https://github.com/paritytech/substrate-archive/wiki/1.)-Requirements)
- PostgreSQL with the required schema: `schema/archive.sql`
- Substrate-based Blockchain running with Rocksdb as the backend

# Install The CLI
`git clone https://github.com/paritytech/substrate-archive.git`

`cd substrate-archive/kusama-archive/`

`cargo build --release`

run with `./../target/release/kusama-archive`

# Contributing
Read the [Doc](https://github.com/paritytech/substrate-archive/blob/master/CONTRIBUTING.md) 


[documentation]: https://github.com/paritytech/substrate-archive/wiki
[contribution]: CONTRIBUTING.md
