//! A simple example

use desub::decoder::Decoder;
use desub_extras::polkadot::PolkadotTypes;
use polkadot_service::kusama_runtime as real_ksm_runtime;
use sc_service::config::DatabaseConfig; // integrate this into Archive Proper
use substrate_archive::{backend, init, NotSignedBlock};
use subxt::KusamaRuntime;
// use std::sync::Arc;

type Block = NotSignedBlock<KusamaRuntime>;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Debug);
    let types = PolkadotTypes::new().unwrap();
    let decoder = Decoder::new(types, "kusama");

    let db =
        backend::open_database::<Block>("~/.local/share/polkadot/chains/ksmcc3/db/", 1024).unwrap();
    let conf = DatabaseConfig::Custom(db);
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let client = backend::client::<
        KusamaRuntime,
        real_ksm_runtime::RuntimeApi,
        polkadot_service::KusamaExecutor,
        _,
    >(conf, spec)
    .unwrap();

    init::<KusamaRuntime, PolkadotTypes>(decoder, "ws://127.0.0.1:9944".to_string()).unwrap();
}
