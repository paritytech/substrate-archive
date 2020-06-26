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

//! Executes blocks to get storage changes without traversing changes trie
//! Contains a ThreadedBlockExecutor that executes blocks concurrently
//! according to a work-stealing scheduler

use crate::{
    backend::{ApiAccess, ReadOnlyBackend as Backend},
    error::Error as ArchiveError,
    types::*,
};
use crossbeam::channel;
use frame_system::Trait as System;
use hashbrown::HashSet;
use sc_client_api::backend;
use sp_api::{ApiExt, ApiRef, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header, NumberFor},
};
use sp_storage::{StorageData, StorageKey as StorageKeyWrapper};
use std::{sync::Arc, thread::JoinHandle};

pub type StorageKey = Vec<u8>;
pub type StorageValue = Vec<u8>;
pub type StorageCollection = Vec<(StorageKey, Option<StorageValue>)>;
pub type ChildStorageCollection = Vec<(StorageKey, StorageCollection)>;

pub enum BlockData<Block: BlockT> {
    Batch(Vec<Block>),
    Single(Block),
    Stop,
}

/// A layer between ThreadedBlockExecutor and whatever else that contains
/// channels for sending new work and receiving storage changes
/// Works in it's own thread
pub struct BlockBroker<Block: BlockT> {
    /// channel for sending blocks to be executed on a threadpool
    pub work: channel::Sender<BlockData<Block>>,
    /// results once execution is finished
    pub results: channel::Receiver<BlockChanges<Block>>,
    /// handle to join threadpool back to main thread
    /// only one thread may own this handle
    /// any clones will make the handle `None`
    handle: Option<JoinHandle<()>>,
}

impl<Block: BlockT> Clone for BlockBroker<Block> {
    fn clone(&self) -> Self {
        Self {
            work: self.work.clone(),
            results: self.results.clone(),
            // only one thread may own a handle
            handle: None,
        }
    }
}

impl<Block: BlockT> BlockBroker<Block>
where
    NumberFor<Block>: Into<u32>,
{
    pub fn stop(mut self) -> Result<(), ArchiveError> {
        self.work.send(BlockData::Stop)?;
        std::thread::sleep(std::time::Duration::from_millis(250));
        self.handle.take().map(JoinHandle::join);
        Ok(())
    }
}

/// Executor that sends blocks to a thread pool for execution
pub struct ThreadedBlockExecutor<Block: BlockT> {
    /// the threadpool
    pool: rayon::ThreadPool,
    /// Entries that have been inserted (avoids inserting duplicates)
    inserted: HashSet<Block::Hash>,
}

impl<Block: BlockT> ThreadedBlockExecutor<Block>
where
    NumberFor<Block>: Into<u32>,
{
    /// create a new instance of the executor
    pub fn new<Runtime, ClientApi>(
        num_threads: Option<usize>,
        client: Arc<ClientApi>,
        backend: Arc<Backend<Block>>,
    ) -> Result<BlockBroker<Block>, ArchiveError>
    where
        Runtime: ConstructRuntimeApi<Block, ClientApi> + Send + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
            + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
        ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    {
        // channel pair for sending and receiving BlockChanges
        let (sender, receiver) = channel::unbounded();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.unwrap_or(0))
            .thread_name(|i| format!("block-executor-{}", i))
            .build()?;

        let (tx, handle) = Self::scheduler_loop::<Runtime, ClientApi>(
            Self {
                pool,
                inserted: HashSet::new(),
            },
            backend,
            client,
            sender,
        )?;

        Ok(BlockBroker {
            work: tx,
            results: receiver,
            handle: Some(handle),
        })
    }

    fn scheduler_loop<Runtime, ClientApi>(
        mut exec: ThreadedBlockExecutor<Block>,
        backend: Arc<Backend<Block>>,
        client: Arc<ClientApi>,
        sender: channel::Sender<BlockChanges<Block>>,
    ) -> Result<(channel::Sender<BlockData<Block>>, JoinHandle<()>), ArchiveError>
    where
        Runtime: ConstructRuntimeApi<Block, ClientApi> + Send + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
            + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
        ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    {
        let (tx, rx) = channel::unbounded();
        let handle = std::thread::spawn(move || loop {
            let data = rx.recv();
            match data.unwrap() {
                BlockData::Batch(v) => exec
                    .add_vec_task::<Runtime, _>(v, client.clone(), backend.clone(), sender.clone())
                    .unwrap(),
                BlockData::Single(v) => exec
                    .add_task::<Runtime, _>(v, client.clone(), backend.clone(), sender.clone())
                    .unwrap(),
                BlockData::Stop => break,
            }
        });
        Ok((tx, handle))
    }

    fn work<Runtime, ClientApi>(
        block: Block,
        client: Arc<ClientApi>,
        backend: Arc<Backend<Block>>,
        sender: channel::Sender<BlockChanges<Block>>,
    ) -> Result<(), ArchiveError>
    where
        Runtime: ConstructRuntimeApi<Block, ClientApi> + Send + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
            + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
        ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    {
        let api = client.runtime_api();

        // don't execute genesis block
        if *block.header().parent_hash() == Default::default() {
            return Ok(());
        }

        log::debug!(
            "Executing Block: {}:{}, version {}",
            block.header().hash(),
            block.header().number(),
            client
                .runtime_version_at(&BlockId::Hash(block.header().hash()))?
                .spec_version,
        );

        let block = BlockExecutor::new(api, backend, block)?.block_into_storage()?;

        sender.send(block).map_err(Into::into)
    }

    /// push some work to the global pool
    pub fn add_task<Runtime, ClientApi>(
        &mut self,
        block: Block,
        client: Arc<ClientApi>,
        backend: Arc<Backend<Block>>,
        sender: channel::Sender<BlockChanges<Block>>,
    ) -> Result<(), ArchiveError>
    where
        Runtime: ConstructRuntimeApi<Block, ClientApi> + Send + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
            + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
        ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    {
        if self.inserted.contains(&block.hash()) {
            return Ok(());
        } else {
            self.inserted.insert(block.hash());
            self.pool.spawn_fifo(move || {
                match Self::work::<Runtime, _>(block.clone(), client, backend, sender) {
                    Ok(_) => (),
                    Err(e) => log::error!("{:?}", e),
                }
            });
        }
        Ok(())
    }

    pub fn add_vec_task<Runtime, ClientApi>(
        &mut self,
        blocks: Vec<Block>,
        client: Arc<ClientApi>,
        backend: Arc<Backend<Block>>,
        sender: channel::Sender<BlockChanges<Block>>,
    ) -> Result<(), ArchiveError>
    where
        Runtime: ConstructRuntimeApi<Block, ClientApi> + Send + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
            + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
        ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    {
        let to_insert = blocks
            .into_iter()
            .filter(|b| !self.inserted.contains(&b.hash()))
            .collect::<Vec<_>>();

        if to_insert.len() > 0 {
            for block in to_insert.into_iter() {
                self.inserted.insert(block.hash());
                let client = client.clone();
                let backend = backend.clone();
                let sender = sender.clone();
                self.pool.spawn_fifo(move || {
                    match Self::work::<Runtime, _>(block.clone(), client, backend, sender) {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    }
                });
            }
        }
        Ok(())
    }
}

/// Storage Changes that occur as a result of a block's executions
#[derive(Debug)]
pub struct BlockChanges<Block: BlockT> {
    /// In memory array of storage values.
    pub storage_changes: StorageCollection,
    /// In memory arrays of storage values for multiple child tries.
    pub child_storage: ChildStorageCollection,
    /// Hash of the block these changes come from
    pub block_hash: Block::Hash,
    pub block_num: NumberFor<Block>,
}

impl<T> From<BlockChanges<NotSignedBlock<T>>> for Storage<T>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    fn from(changes: BlockChanges<NotSignedBlock<T>>) -> Storage<T> {
        let hash = changes.block_hash;
        let num: u32 = changes.block_num.into();

        Storage::new(
            hash,
            num,
            false,
            changes
                .storage_changes
                .into_iter()
                .map(|s| (StorageKeyWrapper(s.0), s.1.map(|d| StorageData(d))))
                .collect::<Vec<(StorageKeyWrapper, Option<StorageData>)>>(),
        )
    }
}

pub struct BlockExecutor<'a, Block, Api, B>
where
    Block: BlockT,
    Api: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
    B: backend::Backend<Block>,
{
    api: ApiRef<'a, Api>,
    backend: Arc<B>,
    block: Block,
    id: BlockId<Block>,
}

impl<'a, Block, Api, B> BlockExecutor<'a, Block, Api, B>
where
    Block: BlockT,
    // Api: ProvideRuntimeApi<Block>,
    Api: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
    B: backend::Backend<Block>,
{
    pub fn new(api: ApiRef<'a, Api>, backend: Arc<B>, block: Block) -> Result<Self, ArchiveError> {
        let header = block.header();
        let parent_hash = header.parent_hash();
        let id = BlockId::Hash(*parent_hash);

        Ok(Self {
            api,
            backend,
            block,
            id,
        })
    }

    pub fn block_into_storage(self) -> Result<BlockChanges<Block>, ArchiveError> {
        let header = (&self.block).header();
        let parent_hash = header.parent_hash().clone();
        let hash = header.hash();
        let num = header.number().clone();

        let state = self.backend.state_at(self.id)?;

        // FIXME: For some reason, wasm runtime calculates a different number of digest items
        // then what we have in the block
        // We don't do anything with consensus
        // so digest isn't very important (we don't currently index digest items anyway)
        // popping a digest item has no effect on storage changes afaik
        let (mut header, ext) = self.block.deconstruct();
        header.digest_mut().pop();
        let block = Block::new(header, ext);

        self.api.execute_block(&self.id, block)?;
        let storage_changes = self.api.into_storage_changes(&state, None, parent_hash)?;

        Ok(BlockChanges {
            storage_changes: storage_changes.main_storage_changes,
            child_storage: storage_changes.child_storage_changes,
            block_hash: hash,
            block_num: num,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::test_util;
    use crate::simple_db::SimpleDb;
    use polkadot_service::Block;
    use sp_blockchain::HeaderBackend;
    //use sp_runtime::traits::BlockBackend;
    use sc_client_api::BlockBackend;
    use sp_api::ProvideRuntimeApi;

    pub const DB_STR: &str = "/home/insipx/.local/share/polkadot/chains/ksmcc3/db";
    #[test]
    fn should_create_new() {
        let full_client = test_util::backend(DB_STR);
        let id = BlockId::Number(50970);
        // let header = full_client.header(id).unwrap().expect("No such block exists!");
        let block = full_client.block(&id).unwrap().block;
        let client = test_util::client(DB_STR);
        let backend = Arc::new(test_util::backend(DB_STR));

        let api = client.runtime_api();
        let executor = BlockExecutor::new(api, backend, block);
    }

    #[test]
    fn should_execute_a_block() {
        pretty_env_logger::try_init();
        let backend = Arc::new(test_util::backend(DB_STR));
        let client = test_util::client(DB_STR);
        // let id = BlockId::Number(50970);
        // let id = BlockId::Number(1_730_000);
        let id = BlockId::Number(1_990_123);
        let block = backend.block(&id).unwrap().block;

        println!("Starting the API!");

        let api = client.runtime_api();
        let time = std::time::Instant::now();
        let executor = BlockExecutor::new(api, backend, block).unwrap();
        match executor.block_into_storage() {
            Ok(result) => {
                let elapsed = time.elapsed();
                println!("{:?}", result);
                println!(
                    "Took {} seconds, {} milli-seconds, {} micro-seconds, {} nano-sconds",
                    elapsed.as_secs(),
                    elapsed.as_millis(),
                    elapsed.as_micros(),
                    elapsed.as_nanos()
                );
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }

    #[test]
    // #[ignore]
    fn should_execute_blocks_concurrently() {
        pretty_env_logger::try_init();

        let client = test_util::client(DB_STR);
        let db = SimpleDb::new(std::path::PathBuf::from(
            "/home/insipx/projects/parity/substrate-archive-api/archive/test_data/10K_BLOCKS.bin",
        ))
        .unwrap();
        let blocks: Vec<Block> = db.get().unwrap();
        // 650 000   --  659 999
        println!("Min: {:?}, Max: {:?}", blocks[0], blocks[blocks.len() - 1]);

        let backend = Arc::new(test_util::backend(DB_STR));
        println!("Got {} blocks", blocks.len());
        // should push to global queue before starting execution
        let mut executor = ThreadedBlockExecutor::new(1, Some(8_000_000), client, backend);
        executor.push_vec_to_queue(blocks).unwrap();
        executor.join();
    }
}
