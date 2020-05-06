//! A simple example

use substrate_archive::init;
use substrate_archive::rpc::Rpc;
use desub::decoder::Decoder;
use desub_extras::polkadot::PolkadotTypes;
use subxt::KusamaRuntime;
use std::sync::Arc;


pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Debug);
    let types = PolkadotTypes::new().unwrap();
    let decoder = Decoder::new(types, "kusama");
    init::<KusamaRuntime, PolkadotTypes>(decoder, "ws://127.0.0.1:9944".to_string()).unwrap();
}