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
mod queries;

use anyhow::Result;
use futures::future::FutureExt;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::PgPool;
use std::time::Duration;

pub fn main() -> Result<()> {
    let config = config::Config::new()?;
    substrate_archive::init_logger(config.cli().log_level, log::LevelFilter::Debug);

    let archive = archive::run_archive(config.clone())?;

    let url = config.psql_conf().url();
    let mut runtime = tokio::runtime::Builder::new().basic_scheduler().build()?;
    let pool = runtime.block_on(PgPool::builder().max_size(2).build(url.as_str()))?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ])
            .template("{spinner:.blue} {msg}"),
    );

    let (tx, mut rx) = futures::channel::oneshot::channel();

    // don't run ticker for higher levels
    if config.cli().log_level == log::Level::Error || config.cli().log_level == log::Level::Warn {
        runtime.spawn(async move {
            loop {
                let indexed_blocks: Option<u32> = queries::block_count(&pool).await.ok();
                let max = queries::max_block(&pool).await.ok();
                let (indexed_blocks, max) = match (indexed_blocks, max) {
                    (Some(a), Some(b)) => (a, b),
                    _ => {
                        timer::Delay::new(Duration::from_millis(160)).await;
                        continue;
                    }
                };
                let msg = format!("Indexed {}/{} blocks", indexed_blocks, max + 1,);
                pb.set_message(msg.as_str());
                match rx.try_recv() {
                    Ok(v) => {
                        if v.is_some() {
                            std::process::exit(1);
                        }
                    }
                    Err(_) => {
                        log::error!("Sender dropped, exiting!");
                        std::process::exit(1);
                    }
                }
                timer::Delay::new(Duration::from_millis(80)).await;
            }
        });
    }

    let ctrlc = async_ctrlc::CtrlC::new().expect("Couldn't create ctrlc handler");
    println!("Waiting on ctrlc...");
    runtime.block_on(ctrlc.then(|_| async {
        // kill main loop
        tx.send(1).unwrap();
        println!("\nShutting down ...");
        archive.shutdown().await;
    }));
    runtime.shutdown_timeout(Duration::from_millis(125));
    Ok(())
}
