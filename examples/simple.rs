use substrate_archive::init;
use substrate_archive::rpc::Rpc;
use subxt::KusamaRuntime;
use std::sync::Arc;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Debug);
    let client = async_std::task::block_on(
        subxt::ClientBuilder::<KusamaRuntime>::new()
        .set_url("ws://127.0.0.1:9944")
        .build()
    ).unwrap();
    let rpc = Arc::new(Rpc::<KusamaRuntime>::new(client));
    init(rpc);
}