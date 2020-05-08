//! A simple example

use desub::decoder::Decoder;
use desub_extras::polkadot::PolkadotTypes;
use std::sync::Arc;
use substrate_archive::init;
use substrate_archive::rpc::Rpc;
use subxt::KusamaRuntime;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Debug);
    let types = PolkadotTypes::new().unwrap();
    let decoder = Decoder::new(types, "kusama");
    init::<KusamaRuntime, PolkadotTypes>(decoder, "ws://127.0.0.1:9944".to_string()).unwrap();
}
