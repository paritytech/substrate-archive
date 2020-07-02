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
    match run() {
        Ok(_) => (),
        Err(e) => log::error!("{}", e.to_string()),
    };
    Ok(())
}

fn run() -> Result<()> {
    let config = config::Config::new()?;
    substrate_archive::init_logger(config.cli().log_level, log::LevelFilter::Info);

    let mut rt = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(2)
        .max_threads(4)
        .thread_name("subst-arch")
        .build()?;

    archive::run_archive(config.clone(), rt.handle())?;

    if config.cli().log_num == 0 {
        rt.spawn(progress(config.clone()));
    }

    rt.block_on(ctrlc())?;
    Ok(())
}

async fn ctrlc() -> Result<()> {
    let ctrlc = async_ctrlc::CtrlC::new().expect("Couldn't create ctrlc handler");
    ctrlc
        .then(|_| async {
            println!("\nShutting down ...");
        })
        .await;
    Ok(())
}

async fn progress(config: config::Config) -> Result<()> {
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

    let url = config.psql_conf().url();
    let pool = PgPool::builder().max_size(2).build(url.as_str()).await?;

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
        timer::Delay::new(Duration::from_millis(80)).await;
    }
}
