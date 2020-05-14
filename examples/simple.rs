//! A simple example

use desub::decoder::Decoder;
use desub_extras::polkadot::PolkadotTypes;
use polkadot_service::kusama_runtime as real_ksm_runtime;
use sc_service::config::DatabaseConfig; // integrate this into Archive Proper
use substrate_archive::{backend, init, NotSignedBlock};
use subxt::KusamaRuntime;
use sp_blockchain::HeaderBackend as _;

// type Block = NotSignedBlock<KusamaRuntime>;
type Block = NotSignedBlock;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Debug);
    let types = PolkadotTypes::new().unwrap();
    let decoder = Decoder::new(types, "kusama");

    // FIXME Database and spec initialization can be done in the lib with a convenience func
    let db =
        backend::open_database::<Block>("/home/insipx/.local/share/polkadot/chains/ksmcc3/db/", 1024).unwrap();
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

    init::<KusamaRuntime, PolkadotTypes, _>(decoder, client, "ws://127.0.0.1:9944".to_string()).unwrap();
}
