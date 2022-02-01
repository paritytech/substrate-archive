//! A simple example

use polkadot_service::{kusama_runtime::RuntimeApi, Block};
use substrate_archive::{Archive, ArchiveBuilder, SecondaryRocksDb};

pub fn main() {
	// get spec/runtime from node library
	let spec = polkadot_service::chain_spec::kusama_config().unwrap();

	// export DATABASE_URL="postgres://postgres:123@localhost:5432/kusama_db"
	let db_url = std::env::var("DATABASE_URL").unwrap();
	// export CHAIN_DATA_DB="/home/insipx/.local/share/polkadot/chains/ksmcc3/db"
	let chain_backend_url = std::env::var("CHAIN_DATA_DB").unwrap();

	let mut archive = ArchiveBuilder::<Block, RuntimeApi, SecondaryRocksDb>::default()
		.chain_spec(Box::new(spec))
		.chain_data_path(chain_backend_url)
		.pg_url(db_url)
		.cache_size(128)
		.block_workers(2)
		.wasm_pages(512)
		.build()
		.unwrap();

	archive.drive().unwrap();
	futures::executor::block_on(archive.block_until_stopped());
}
