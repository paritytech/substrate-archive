use substrate_archive::{database::models::BlockModel, native_executor_instance};
use anyhow::Error;
use polkadot_service::{polkadot_runtime as dot_rt, Block};
use std::{fs::File, sync::Arc};
use substrate_archive_backend::{
	runtime_api, ExecutionMethod, ReadOnlyBackend, ReadOnlyDb, RuntimeConfig, SecondaryRocksDb, TArchiveClient,
};

native_executor_instance!(
	pub PolkadotExecutor,
	dot_rt::api::dispatch,
	dot_rt::native_version,
	sp_io::SubstrateHostFunctions,
);

pub fn get_dot_runtime_api(
	block_workers: usize,
	wasm_pages: u64,
) -> Result<
	(
		TArchiveClient<Block, dot_rt::RuntimeApi, PolkadotExecutor, SecondaryRocksDb>,
		Arc<ReadOnlyBackend<Block, SecondaryRocksDb>>,
	),
	Error,
> {
	let chain_path = std::env::var("CHAIN_DATA_DB").expect("must have database as variable CHAIN_DATA_DB");
	let mut tmp_path = tempfile::tempdir()?.into_path();
	tmp_path.push("rocksdb_secondary");
	let db = Arc::new(SecondaryRocksDb::open_database(chain_path.as_str(), 128, tmp_path).unwrap());
	let config = RuntimeConfig {
		exec_method: ExecutionMethod::Compiled,
		wasm_runtime_overrides: None,
		wasm_pages: wasm_pages.into(),
		block_workers,
	};
	let backend = Arc::new(ReadOnlyBackend::new(db.clone(), false));
	runtime_api(db, config).map(|o| (o, backend)).map_err(Into::into)
}
