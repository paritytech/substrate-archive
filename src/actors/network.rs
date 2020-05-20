// Copyright 2017-2019 Parity Technologies (UK) Ltd.
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

//! Network Actors
//! These aggregate data for child actors to work with
//! they mostly wait on network IO

use crate::{
    backend::ChainAccess,
    rpc::Rpc,
    types::{Block, NotSignedBlock, Substrate, SubstrateBlock},
};
use bastion::prelude::*;
use sc_client_api::BlockBackend;
use sp_runtime::generic::BlockId;
use sp_runtime::traits::{Block as _, Header as _};
use std::sync::Arc;
use subxt::system::System;
use sqlx::PgConnection;

use super::scheduler::{Algorithm, Scheduler};

/// Subscribe to new blocks via RPC
/// this is a worker that never stops
pub fn actor<T, C>(
    client: Arc<C>,
    pool: sqlx::Pool<PgConnection>,
    url: String,
) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::BlockNumber: Into<u32>,
{
    let meta_workers = super::metadata::actor::<T, _>(url.clone(), pool, client.clone())
        .expect("Couldn't start metadata workers");
    // actor which produces work in the form of collecting blocks
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let meta_workers = meta_workers.clone();
            let url: String = url.clone();
            let client = client.clone();
            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &meta_workers);
                let rpc = super::connect::<T>(url.as_str()).await;
                let mut subscription = rpc
                    .subscribe_finalized_blocks()
                    .await
                    .expect("Subscription failed");
                while let head = subscription.next().await {
                    let block = client
                        .block(&BlockId::Number((*head.number()).into()))
                        .map_err(|e| log::error!("{:?}", e))
                        .unwrap();

                    if let Some(b) = block {
                        log::trace!("{:?}", b);
                        sched.ask_next(b).unwrap().await?;
                    } else {
                        log::warn!("Block does not exist!");
                    }
                }
                Bastion::stop();
                Ok(())
            }
        })
    })
}
