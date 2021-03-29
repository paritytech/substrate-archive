// Copyright 2018-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of // MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

mod cli_opts;

use std::sync::{
	atomic::{AtomicBool, Ordering},
	Arc,
};

use anyhow::{anyhow, Result};
use polkadot_service::kusama_runtime as ksm_rt;
use polkadot_service::polkadot_runtime as dot_rt;
use polkadot_service::westend_runtime as wnd_rt;
use polkadot_service::Block;
use substrate_archive::{
	native_executor_instance, Archive, ArchiveBuilder, ArchiveConfig, ReadOnlyDb, SecondaryRocksDb,
};

native_executor_instance!(
	pub PolkadotExecutor,
	dot_rt::api::dispatch,
	dot_rt::native_version,
	sp_io::SubstrateHostFunctions,
);

native_executor_instance!(
	pub KusamaExecutor,
	ksm_rt::api::dispatch,
	ksm_rt::native_version,
	sp_io::SubstrateHostFunctions,
);

native_executor_instance!(
	pub WestendExecutor,
	wnd_rt::api::dispatch,
	wnd_rt::native_version,
	sp_io::SubstrateHostFunctions,
);

pub fn main() -> Result<()> {
	let cli = cli_opts::CliOpts::init();
	let config = cli.parse()?;

	let mut archive = run_archive::<SecondaryRocksDb>(&cli.chain_spec, config)?;
	archive.drive()?;
	let running = Arc::new(AtomicBool::new(true));
	let r = running.clone();

	ctrlc::set_handler(move || {
		r.store(false, Ordering::SeqCst);
	})
	.expect("Error setting Ctrl-C handler");
	while running.load(Ordering::SeqCst) {}
	archive.boxed_shutdown()?;

	Ok(())
}

fn run_archive<D: ReadOnlyDb + 'static>(
	chain_spec: &str,
	config: Option<ArchiveConfig>,
) -> Result<Box<dyn Archive<Block, D>>> {
	match chain_spec.to_ascii_lowercase().as_str() {
		"kusama" | "ksm" => {
			let spec = polkadot_service::chain_spec::kusama_config().map_err(|err| anyhow!("{}", err))?;
			let archive = ArchiveBuilder::<Block, wnd_rt::RuntimeApi, KusamaExecutor, D>::with_config(config)
				.chain_spec(Box::new(spec))
				.build()?;
			Ok(Box::new(archive))
		}
		"westend" | "wnd" => {
			let spec = polkadot_service::chain_spec::westend_config().map_err(|err| anyhow!("{}", err))?;
			let archive = ArchiveBuilder::<Block, wnd_rt::RuntimeApi, WestendExecutor, D>::with_config(config)
				.chain_spec(Box::new(spec))
				.build()?;
			Ok(Box::new(archive))
		}
		"polkadot" | "dot" => {
			let spec = polkadot_service::chain_spec::polkadot_config().map_err(|err| anyhow!("{}", err))?;
			let archive = ArchiveBuilder::<Block, dot_rt::RuntimeApi, PolkadotExecutor, D>::with_config(config)
				.chain_spec(Box::new(spec))
				.build()?;
			Ok(Box::new(archive))
		}
		c => Err(anyhow!("unknown chain {}", c)),
	}
}
