<div align="center">

# Substrate Archive

[Install the CLI](#install) • [Documentation] • [Contributing](#contributing) 

</div>
Run this alongside the substrate client to sync all historical TxData. Indexes all Blocks, State, and Extrinsic date from a running node.

Example Usage:
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
more fleshed-out example can be found in `archive/examples/simple.rs`. You can run it by entering the `archive` directory
and running `cargo run --example simple --features logging` 

The schema for the PostgreSQL database is described in the PDF File at the root of this directory

### Requirements
Extended requirements list found in the [wiki](https://github.com/paritytech/substrate-archive/wiki/Requirements)
- PostgreSQL
- Substrate-based Blockchain running with Rocksdb as the backend



### How to use
- need a Postgres Database with the URL exposed as an environment variable under the name `DATABASE_URL`
- Create the tables by running `./schema/archive.sql` inside Postgres

Create the database
```
sudo -u postgres createdb -O $(whoami) archive
psql -d postgres://$(whoami):password@localhost/archive -f ./schema/archive.sql
```

##### Required Dependencies:
Ubuntu: `postgresql`, `postgresql-contrib`, `libpq-dev`
Fedora: `postgresql`, `postgresql-contrib`, `postgresql-devel`




[documentation]: https://github.com/paritytech/substrate-archive/wiki
[contribution]: CONTRIBUTION.md
