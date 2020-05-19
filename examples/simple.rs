//! A simple example

use polkadot_service::kusama_runtime as real_ksm_runtime;
use sc_client_api::backend::StorageProvider;
use sc_service::config::DatabaseConfig; // integrate this into Archive Proper
use sp_blockchain::HeaderBackend as _;
use sp_runtime::generic::BlockId;
use sp_storage::StorageKey;
use substrate_archive::{backend, init, NotSignedBlock};
use subxt::KusamaRuntime;

// type Block = NotSignedBlock<KusamaRuntime>;
type Block = NotSignedBlock;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info);

    // FIXME Database and spec initialization can be done in the lib with a convenience func
    let db = backend::open_database::<Block>(
        "/home/insipx/.local/share/polkadot/chains/ksmcc3/db",
        4096,
    )
    .unwrap();
    let conf = DatabaseConfig::Custom(db);

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

    init::<KusamaRuntime, _>(client, "ws://127.0.0.1:9944".to_string()).unwrap();
}
