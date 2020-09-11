//! A simple example

use polkadot_service::{kusama_runtime::RuntimeApi, Block, KusamaExecutor};
use substrate_archive::{Archive, ArchiveBuilder};

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Info);

    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();

    // export DATABASE_URL="postgres://postgres:123@localhost:5432/kusama_db"
    let db_url = std::env::var("DATABASE_URL").unwrap();
    // export CHAIN_DATA_DB="/home/insipx/.local/share/polkadot/chains/ksmcc3/db"
    let chain_backend_url = std::env::var("CHAIN_DATA_DB").unwrap();

    let mut archive = ArchiveBuilder::<Block, RuntimeApi, KusamaExecutor>::default()
        .block_workers(2)
        .wasm_pages(512)
        .cache_size(128)
        .pg_url(db_url)
        .chain_data_db(chain_backend_url)
        .chain_spec(Box::new(spec))
        .build()
        .unwrap();

    archive.drive().unwrap();
    futures::executor::block_on(archive.block_until_stopped());
}
