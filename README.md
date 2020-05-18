# Substrate Archive Db

Run this alongside the substrate client to sync all historical TxData. Indexes all Blocks, State, and Extrinsic date from a running node.

##### Archive is pre-0.3, use at your own risk

Requires a substrate node running with a RocksDb Database instance

Example Usage:
```rust
use polkadot_service::kusama_runtime as real_ksm_runtime;
use sc_service::config::DatabaseConfig;
use sp_blockchain::HeaderBackend as _;
use sc_client_api::backend::StorageProvider;
use sp_storage::StorageKey;
use sp_runtime::generic::BlockId;

use subxt::KusamaRuntime;
use substrate_archive::{backend, init, NotSignedBlock};


// type Block = NotSignedBlock<KusamaRuntime>;
type Block = NotSignedBlock;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info);
	
    // connect to the rocksdb database
    let db = backend::open_database::<Block>(
        "/home/insipx/.local/share/polkadot/chains/ksmcc3/db",
        4096,
    )
    .unwrap();
    let conf = DatabaseConfig::Custom(db);
	
    // get the kusama chain-spec from `polkadot-service`
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let client = backend::client::<
        KusamaRuntime,
        real_ksm_runtime::RuntimeApi,
        polkadot_service::KusamaExecutor,
        _,
    >(conf, spec)
    .unwrap();

    let info = client.info();
    println!("{:?}", info);

    init::<KusamaRuntime, _>(client, "ws://127.0.0.1:9944".to_string())
        .unwrap();
}
```


The schema for the PostgreSQL database is described in the Pdf File at the root of this directory

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


