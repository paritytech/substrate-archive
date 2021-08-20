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

use node_template_runtime::{opaque::Block, RuntimeApi};

use substrate_archive::{Archive, ArchiveBuilder, SecondaryRocksDb};

fn main() -> anyhow::Result<()> {
	let cli = cli_opts::CliOpts::init();
	let config = cli.parse()?;

	let mut archive = ArchiveBuilder::<Block, RuntimeApi, SecondaryRocksDb>::with_config(config)
		.chain_spec(Box::new(cli.chain_spec))
		.build()?;
	archive.drive()?;

	let running = Arc::new(AtomicBool::new(true));
	let r = running.clone();

	ctrlc::set_handler(move || {
		r.store(false, Ordering::SeqCst);
	})
	.expect("Error setting Ctrl-C handler");
	while running.load(Ordering::SeqCst) {}
	archive.shutdown()?;
	Ok(())
}
