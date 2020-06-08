// Copyright 2019-2020 Parity Technologies (UK) Ltd.
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

use crate::actors::{
    self,
    scheduler::{Algorithm, Scheduler},
    workers,
};
use crate::{
    backend::{ApiAccess, BlockExecutor, BlockChanges},
    error::Error as ArchiveError,
    types::{NotSignedBlock, Substrate, System, Block},
};
use bastion::prelude::*;
use sc_client_db::Backend;
use sc_client_api::backend;
use sp_api::{ConstructRuntimeApi, ApiExt};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT};
use std::sync::Arc;

use sqlx::PgConnection;

pub const REDUNDANCY: usize = 10;

// TODO: should probably make this into some kind of job queue
// in which two actors work together

// shouldn't need defer_storage and collect_storage any longer

pub fn actor<T, Runtime, ClientApi>(
    client: Arc<ClientApi>,
    backend: Arc<Backend<NotSignedBlock<T>>>,
    pool: sqlx::Pool<PgConnection>,
) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    // weird how this trait works
    Runtime: ConstructRuntimeApi<NotSignedBlock<T>, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<NotSignedBlock<T>, Error = sp_blockchain::Error> 
        + ApiExt<NotSignedBlock<T>, StateBackend = backend::StateBackendFor<Backend<NotSignedBlock<T>>, NotSignedBlock<T>>>,
    ClientApi: ApiAccess<NotSignedBlock<T>, Backend<NotSignedBlock<T>>, Runtime> + 'static,
   <T as System>::BlockNumber: Into<u32>,
{
    let db = crate::database::Database::new(&pool)?;
    let db_workers = workers::db::<T>(db)?;
   
    Bastion::children(|children: Children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let db_workers = db_workers.clone();
                let pool = pool.clone();
                let client = client.clone();
                let backend = backend.clone();
                async move {
                    let mut max_storage: u32 = 0;
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                    sched.add_worker("db", &db_workers);
                    match entry::<T, Runtime, _>(&mut sched, &client, &backend).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e)
                    };
                    Ok(())
                }
            })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate full storage workers"))
    
}

async fn entry<T, Runtime, ClientApi>(
    sched: &mut Scheduler<'_>,
    client: &Arc<ClientApi>,
    backend: &Arc<Backend<NotSignedBlock<T>>>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    Runtime: ConstructRuntimeApi<NotSignedBlock<T>, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<NotSignedBlock<T>, Error = sp_blockchain::Error> 
        + ApiExt<NotSignedBlock<T>, StateBackend = backend::StateBackendFor<Backend<NotSignedBlock<T>>, NotSignedBlock<T>>>,
    ClientApi: ApiAccess<NotSignedBlock<T>, Backend<NotSignedBlock<T>>, Runtime> + 'static,
   <T as System>::BlockNumber: Into<u32>,
{
    let ctx = sched.context();
    'main: loop {
        msg! {
            ctx.recv().await.expect("Could Not Receive"),
            block: Block<T> => {
                log::info!("Got Block!");
                let runtime_api = client.runtime_api();
                let now = std::time::Instant::now();
                let executor = BlockExecutor::new(runtime_api, backend.clone(), &block.inner().block).unwrap();
                let changes = executor.block_into_storage()?.storage_changes;
                let elapsed = now.elapsed();
                log::info!("Took {} seconds, {} milli-seconds to get storage for block", elapsed.as_secs(), elapsed.as_millis());
                let main_changes = changes.main_storage_changes;
                let (key, value) = &main_changes[0];
                let key = hex::encode(key);
                if value.is_some() {
                    let val = hex::encode(value.as_ref().unwrap());
                    log::info!("Key: {}, Val: {}", key, val);
                } else {
                    log::info!("Key: {}, Val: None", key) ;
                }
            };
            blocks: Vec<Block<T>> => {
                log::info!("Got Multiple Blocks!");
                for block in blocks.iter() {
                    let runtime_api = client.runtime_api();
                    let executor = BlockExecutor::new(runtime_api, backend.clone(), &block.inner().block).unwrap();
                    let changes = executor.block_into_storage()?.storage_changes;
                }
            };
            ref broadcast: super::Broadcast => {
                match broadcast {
                    _ => break 'main,
                }
            };
            e: _ => log::warn!("Received unknown data {:?}", e);
        }
    }
    Ok(())
}
