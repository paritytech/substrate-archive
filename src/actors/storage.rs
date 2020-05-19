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

//! Indexes storage deltas

use bastion::prelude::*;
use subxt::system::System;
use sqlx::PgConnection;
use std::{sync::Arc, time::Duration};
use sp_blockchain::HeaderBackend;
use sp_storage::{StorageKey, StorageData, StorageChangeSet};
use primitive_types::H256;
use super::scheduler::{Algorithm, Scheduler};
use crate::{types::*, queries, backend::ChainAccess};

pub fn actor<T, C>(db_workers: ChildrenRef, client: Arc<C>, url: String, pool: sqlx::Pool<PgConnection>) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::Hash: From<H256>
{

    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let workers = db_workers.clone();
            let url = url.clone();
            let pool = pool.clone();
            let client = client.clone();
            async move {
                loop {
                    let rpc = super::connect::<T>(url.as_str()).await;
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                    let (max_storage_num, storage_hash) = queries::get_max_storage(&pool)
                        .await
                        .unwrap_or((1, client.hash(1).expect("Couldn't get hash").unwrap().as_ref().to_vec()));
                    let (max_block, block_hash) = match queries::get_max_block_num(&pool).await {
                        Err(e) => {
                            log::error!("{:?}", e);
                            // we might just not have anything in the database yet
                            async_std::task::sleep(Duration::from_secs(1)).await;
                            continue;
                        },
                        Ok(v) => v
                    };
                    // we've already collected storage up to most recent block
                    if max_storage_num == max_block {
                        async_std::task::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                    log::info!("storage num: {:?}, storage hash {:?}", max_storage_num, storage_hash);
                    log::info!("block num: {:?}, block hash {:?}", max_block, block_hash);
                    let storage_hash = H256::from_slice(storage_hash.as_slice());
                    let max_block_hash = H256::from_slice(block_hash.as_slice());
                    log::info!("Querying storage");
                    let change_set = rpc.query_storage(Vec::new(), T::Hash::from(storage_hash), Some(T::Hash::from(max_block_hash)))
                                        .await
                                        .expect("Querying storage failed");
                    log::info!("Change set: {:?}", change_set);
                    let storage = change_set.into_iter().map(|change| {
                        let num = client.number(H256::from_slice(change.block.as_ref())).expect("Couldn't get block number for hash");
                        let block = change.block;
                        if let Some(num) = num {
                            change
                                .changes
                                .into_iter()
                                .filter_map(|(k, d)| {
                                    if d.is_some() {
                                        Some((k, d))
                                    } else {
                                        None
                                    }
                                })
                                .map(|c| {
                                    Storage::new(block, num, c.0, c.1.expect("checked in filter"))
                                }).collect::<Vec<Storage<T>>>()
                        } else {
                            log::error!("Block doesn't exist!");
                            Vec::new()
                        }
                    }).flatten().collect::<Vec<Storage<T>>>();
                    log::info!("Storage: {:?}", storage);
                    let answer = sched.ask_next(storage).unwrap().await.expect("Couldn't send storage to transformers");
                    log::debug!("{:?}", answer);
                }
                Ok(())
            }
        })
    })
}
