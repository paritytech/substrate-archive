use substrate_archive::{database::models::BlockModel, native_executor_instance};
use anyhow::Error;
use paste::paste;
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

type PolkadotClient = TArchiveClient<Block, dot_rt::RuntimeApi, PolkadotExecutor, SecondaryRocksDb>;
type PolkadotBackend = Arc<ReadOnlyBackend<Block, SecondaryRocksDb>>;

pub fn get_dot_runtime_api(
	block_workers: usize,
	wasm_pages: u64,
) -> Result<(PolkadotClient, PolkadotBackend), Error> {
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


fn csv_to_block(file: &str, has_header: bool) -> Result<Vec<BlockModel>, Error> {
	let blocks = File::open(file)?;
	let mut rdr = csv::ReaderBuilder::new().has_headers(has_header).from_reader(blocks);
	let mut blocks = Vec::new();
	for line in rdr.records() {
		let line = line?;
		let id: i32 = line.get(0).unwrap().parse()?;
		let parent_hash: Vec<u8> = hex::decode(line.get(1).unwrap().strip_prefix("\\x").unwrap())?;
		let hash: Vec<u8> = hex::decode(line.get(2).unwrap().strip_prefix("\\x").unwrap())?;
		let block_num: i32 = line.get(3).unwrap().parse()?;
		let state_root: Vec<u8> = hex::decode(line.get(4).unwrap().strip_prefix("\\x").unwrap())?;
		let extrinsics_root: Vec<u8> = hex::decode(line.get(5).unwrap().strip_prefix("\\x").unwrap())?;
		let digest = hex::decode(line.get(6).unwrap().strip_prefix("\\x").unwrap())?;
		let ext: Vec<u8> = hex::decode(line.get(7).unwrap().strip_prefix("\\x").unwrap())?;
		let spec: i32 = line.get(8).unwrap().parse()?;
		let block = BlockModel { id, parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec };
		blocks.push(block);
	}
	Ok(blocks)
}

/// Creates a function from `version` and `file` such that it returns a Vector of `BlockModel` from the
/// passed in CSV file of substrate blocks. The function will be named `blocks_${version}`, EX: `blocks_v28`
/// This macro may only be used locally in this file
macro_rules! decl_block_data {
	(
		$(
			[$version:expr, $file:expr]
		)*
	) => {
		$(
			paste! {
				pub fn [<blocks_ $version>]() -> Result<Vec<BlockModel>, Error> {
					println!("{:?}", std::env::current_dir().unwrap());
					csv_to_block($file, true)
				}
			}
		)*
	};
}

decl_block_data! {
	["v25", "./test-common/test_data/blocks_v25.csv" ]
	["v26", "./test-common/test_data/blocks_v26.csv" ]
	["v27", "./test-common/test_data/blocks_v27.csv" ]
	["v28", "../test-common/test_data/blocks_v28.csv"]
	["v29", "../test-common/test_data/blocks_v29.csv"]
	["v30", "../test-common/test_data/blocks_v30.csv"]
}
