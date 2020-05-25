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

mod cli_opts;
mod config;
mod archive;
mod queries;

use anyhow::Result;
use sqlx::PgPool;
use substrate_archive::{chain_traits::UsageProvider as _};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use futures::future::FutureExt;
use std::{thread, time::Duration};

pub fn main() -> Result<()> {
    
    let config = config::Config::new()?;
    substrate_archive::init_logger(config.cli().log_level, log::LevelFilter::Info);
    
    //let handle = async_std::task::spawn(archive::run_archive(config.clone()));
    let (client, archive) = archive::run_archive(config.clone())?;
    
    let pool = if let Some(url) = config.psql_url() {
        async_std::task::block_on(PgPool::builder()
            .max_size(2)
            .build(url))?
    } else {
        log::warn!("No url passed on initialization, using environment variable");
        async_std::task::block_on(
            PgPool::builder()
                .max_size(2)
                .build(&std::env::var("DATABASE_URL")?)
        )?
    };
    
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
        ]).template("{spinner:.blue} {msg}"),
    );
    
    async_std::task::spawn(async move {
        loop {
            let indexed_blocks: Option<u32> = queries::block_count(&pool).await.ok();
            let indexed_storage = queries::get_max_storage(&pool).await.ok();
            let max = queries::max_block(&pool).await.ok();
            let (in_blocks, in_storg, max) = match (indexed_blocks, indexed_storage, max) {
                (Some(a), Some(b), Some(c)) => {
                    (a, b, c)
                },
                _ => {
                    async_std::task::sleep(Duration::from_millis(120)).await;
                    continue;
                }
            };
            let msg = format!("Indexed {}/{} blocks, {}/{} storage", in_blocks, max, in_storg.0, max);
            pb.set_message(msg.as_str());
            async_std::task::sleep(Duration::from_millis(65)).await;
        }
    });
        
    let ctrlc = async_ctrlc::CtrlC::new().expect("Couldn't create ctrlc handler");
    println!("Waiting on ctrlc");
    async_std::task::block_on(    
        ctrlc.then(|_| async {
            println!("\nShutting down ...");
            archive.shutdown().await.unwrap();
        })
    );

    Ok(())
}
