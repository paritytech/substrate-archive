// Copyright 2018-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

use anyhow::{anyhow, Context, Result};
use polkadot_service::kusama_runtime as ksm_rt;
use polkadot_service::polkadot_runtime as dot_rt;
use polkadot_service::westend_runtime as westend_rt;
use polkadot_service::{Block, KusamaExecutor, PolkadotExecutor, WestendExecutor};
use sc_chain_spec::ChainSpec;
use substrate_archive::{Archive, ArchiveBuilder};
use substrate_archive_common::ReadOnlyDB;

use crate::config::Config;

pub fn run_archive<D: ReadOnlyDB + 'static>(config: Config) -> Result<Box<dyn Archive<Block, D>>> {
	let mut db_path = if let Some(p) = config.polkadot_path() {
		p
	} else {
		let path = std::env::var("CHAIN_DATA_DB").expect("CHAIN_DATA_DB must be set.");
		std::path::PathBuf::from(path)
	};

	let spec = get_spec(config.cli().chain.as_str())?;

	let last_path_part =
		db_path.file_name().context("Polkadot path not valid")?.to_str().context("could not convert path to string")?;

	match last_path_part {
		"polkadot" => db_path.push(format!("chains/{}/db", spec.id())),
		"chains" => db_path.push(format!("{}/db", spec.id())),
		_ => return Err(anyhow!("invalid path {}", db_path.as_path().display())),
	}

	let db_path = db_path.as_path().to_str().context("could not convert rocksdb path to str")?.to_string();

	match config.cli().chain.to_ascii_lowercase().as_str() {
		"kusama" | "ksm" => {
			let archive = ArchiveBuilder::<Block, ksm_rt::RuntimeApi, KusamaExecutor, D>::default()
				.chain_spec(spec)
				.chain_data_path(Some(db_path))
				.pg_url(config.psql_conf().map(|u| u.url()))
				.cache_size(config.cache_size())
				.block_workers(config.block_workers())
				.wasm_pages(config.wasm_pages())
				.max_block_load(config.max_block_load())
				.build()?;
			Ok(Box::new(archive))
		}
		"westend" | "wnd" => {
			let archive = ArchiveBuilder::<Block, westend_rt::RuntimeApi, WestendExecutor, D>::default()
				.chain_spec(spec)
				.chain_data_path(Some(db_path))
				.pg_url(config.psql_conf().map(|u| u.url()))
				.cache_size(config.cache_size())
				.block_workers(config.block_workers())
				.wasm_pages(config.wasm_pages())
				.max_block_load(config.max_block_load())
				.build()?;
			Ok(Box::new(archive))
		}
		"polkadot" | "dot" => {
			let archive = ArchiveBuilder::<Block, dot_rt::RuntimeApi, PolkadotExecutor, D>::default()
				.chain_spec(spec)
				.chain_data_path(Some(db_path))
				.pg_url(config.psql_conf().map(|u| u.url()))
				.cache_size(config.cache_size())
				.block_workers(config.block_workers())
				.wasm_pages(config.wasm_pages())
				.max_block_load(config.max_block_load())
				.build()?;
			Ok(Box::new(archive))
		}
		c => Err(anyhow!("unknown chain {}", c)),
	}
}

fn get_spec(chain: &str) -> Result<Box<dyn ChainSpec>> {
	match chain.to_ascii_lowercase().as_str() {
		"kusama" | "ksm" => {
			let spec = polkadot_service::chain_spec::kusama_config().unwrap();
			Ok(Box::new(spec) as Box<dyn ChainSpec>)
		}
		"westend" | "wnd" => {
			let spec = polkadot_service::chain_spec::westend_config().unwrap();
			Ok(Box::new(spec) as Box<dyn ChainSpec>)
		}
		"polkadot" | "dot" => {
			let spec = polkadot_service::chain_spec::polkadot_config().unwrap();
			Ok(Box::new(spec) as Box<dyn ChainSpec>)
		}
		c => Err(anyhow!("unknown chain {}", c)),
	}
}
