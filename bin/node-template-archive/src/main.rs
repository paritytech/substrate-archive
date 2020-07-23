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

mod archive;
mod cli_opts;
mod config;

use anyhow::Result;

#[tokio::main]
pub async fn main() -> Result<()> {
    let config = config::Config::new()?;
    substrate_archive::init_logger(config.cli().log_level, log::LevelFilter::Debug);

    let archive = archive::run_archive(config.clone()).await?;
    ctrlc().await?;
    Ok(())
}

async fn ctrlc() -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen on ctrlc");
    println!("\nShutting down ...");
    Ok(())
}
