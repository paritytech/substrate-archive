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
use crossbeam::deque::{Injector, /*Steal,*/ Stealer, Worker};
use frame_system::Trait as System;
use sc_client_api::backend;
// use sc_client_db::Backend;
use crossbeam::channel;
use hashbrown::HashSet;
use sp_api::{ApiExt, ApiRef, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, HashFor, Header, NumberFor},
};
use sp_storage::{StorageData, StorageKey as StorageKeyWrapper};
use std::{
    iter::{self, FromIterator},
    marker::PhantomData,
    sync::{Arc, Mutex},
};
use threadpool::ThreadPool;

pub type StorageKey = Vec<u8>;
pub type StorageValue = Vec<u8>;
pub type StorageCollection = Vec<(StorageKey, Option<StorageValue>)>;
pub type ChildStorageCollection = Vec<(StorageKey, StorageCollection)>;

const POOL_NAME: &str = "block-executor-worker";

pub enum BlockData<Block: BlockT> {
    Batch(Vec<Block>),
    Single(Block),
    Stop,
}

pub struct ExecutorContext<Block: BlockT> {
    pub work: channel::Sender<BlockData<Block>>,
    pub results: channel::Receiver<BlockChanges<Block>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl<Block: BlockT> Clone for ExecutorContext<Block> {
    fn clone(&self) -> Self {
        Self {
            work: self.work.clone(),
            results: self.results.clone(),
            // only one thread may own a handle
            handle: None,
        }
    }
}

impl<Block: BlockT> ExecutorContext<Block>
where
    NumberFor<Block>: Into<u32>,
{
    pub fn from_executor(executor: ThreadedBlockExecutor<Block>) -> Self {
        let results = executor.receiver().clone();
        let (work, handle) = Self::scheduler_loop(executor);

        Self {
            results,
            work,
            handle: Some(handle),
        }
    }

    fn scheduler_loop(
        mut executor: ThreadedBlockExecutor<Block>,
    ) -> (
        channel::Sender<BlockData<Block>>,
        std::thread::JoinHandle<()>,
    ) {
        let (tx, rx) = channel::unbounded();
        let handle = std::thread::spawn(move || loop {
            let data = rx.recv();
            match data.unwrap() {
                BlockData::Batch(v) => executor.push_vec_to_queue(v).unwrap(),
                BlockData::Single(v) => executor.push_to_queue(v).unwrap(),
                Stop => {
                    executor.join();
                    break;
                }
            }
        });
        (tx, handle)
    }

    pub fn stop(mut self) -> Result<(), ArchiveError> {
        self.work.send(BlockData::Stop);
        std::thread::sleep(std::time::Duration::from_millis(250));
        self.handle.take().map(std::thread::JoinHandle::join);
        Ok(())
    }
}

/// A worker on the threadpool that has
/// blocks on the queue to execute
pub struct BlockWorker<Block, Runtime, ClientApi>
where
    Block: BlockT,
    Runtime: ConstructRuntimeApi<Block, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
    ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
{
    /// Local queue of tasks
    local: Worker<Block>,
    /// Other threads we can steal work from
    stealers: Arc<Vec<Stealer<Block>>>,
    /// Global queue of tasks
    global: Arc<Injector<Block>>,
    /// The Substrate API Client
    client: Arc<ClientApi>,
    /// The Substrate Disk Backend (RocksDB)
    backend: Arc<Backend<Block>>,
    sender: channel::Sender<BlockChanges<Block>>,
    _marker: PhantomData<Runtime>,
}

impl<Block, Runtime, ClientApi> BlockWorker<Block, Runtime, ClientApi>
where
    Block: BlockT,
    Runtime: ConstructRuntimeApi<Block, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
    ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
{
    /// create a new worker
    fn init_worker(
        stealers: Arc<Vec<Stealer<Block>>>,
        global: Arc<Injector<Block>>,
        client: Arc<ClientApi>,
        backend: Arc<Backend<Block>>,
        sender: channel::Sender<BlockChanges<Block>>,
    ) -> Self {
        let worker = Worker::new_fifo();
        Self {
            local: worker,
            stealers,
            global,
            client,
            backend,
            sender,
            _marker: PhantomData,
        }
    }

    /// Start the worker loop
    fn start_worker(self) -> Result<(), ArchiveError> {
        loop {
            let block = if let Some(b) = self.find_work() {
                b
            } else {
                std::thread::sleep(std::time::Duration::from_millis(5));
                continue;
            };

            let api = self.client.runtime_api();
            let num = *block.header().number();
            let block = BlockExecutor::new(api, self.backend.clone(), block)?;
            let block = match block.block_into_storage() {
                Ok(v) => v,
                Err(e) => {
                    log::info!("problematic block number: {}", num);
                    log::error!("{:?}", e);
                    return Err(e);
                }
            };

            match self.sender.send(block) {
                Ok(_) => (),
                Err(e) => {
                    log::error!("Could not send storage changes because {}", e);
                }
            }
            // TODO: Park os thread
            // currently the OS thread just sleeps
            // we should park it and wait  for other work
            // or just stop the thread, and spawn more workers when more work comes in
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(())
    }

    /// this will try to find work from the local queue first,
    /// then from the global queue
    /// then try to steal from other threads
    /// On start, the worker should always find work from the global queue
    fn find_work(&self) -> Option<Block> {
        // Pop a task from the local queue, if not empty.
        self.local.pop().or_else(|| {
            // Otherwise, we need to look for a task elsewhere.
            iter::repeat_with(|| {
                // Try stealing a batch of tasks from the global queue.
                self.global
                    .steal_batch_and_pop(&self.local)
                    // Or try stealing a task from one of the other threads.
                    .or_else(|| self.stealers.iter().map(|s| s.steal()).collect())
            })
            // Loop while no task was stolen and any steal operation needs to be retried.
            .find(|s| !s.is_retry())
            // Extract the stolen task, if there is one.
            .and_then(|s| s.success())
        })
    }

    /// get the thief for this worker
    fn own_stealer(&self) -> Stealer<Block> {
        self.local.stealer()
    }
}

/// Builder for the BlockWorker
/// must have stealers, global queue, client, and backend specified
///
///# Examples
///```
/// let worker = BlockWorkerBuilder::<Block, Runtime, ClientApi>::default()
///     .global(..)
///     .client(..)
///     .backend(..)
///     .build()
///```
struct BlockWorkerBuilder<Block, Runtime, ClientApi>
where
    Block: BlockT,
    Runtime: ConstructRuntimeApi<Block, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
    ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    NumberFor<Block>: Into<u32>,
{
    /// Local queue of tasks
    pub local: Worker<Block>,
    /// Other threads we can steal work from
    stealers: Option<Arc<Vec<Stealer<Block>>>>,
    /// Global queue of tasks
    global: Option<Arc<Injector<Block>>>,
    /// The Substrate API Client
    client: Option<Arc<ClientApi>>,
    /// The Substrate Disk Backend (RocksDB)
    backend: Option<Arc<Backend<Block>>>,
    sender: Option<channel::Sender<BlockChanges<Block>>>,
    _marker: PhantomData<Runtime>,
}

impl<Block, Runtime, ClientApi> Default for BlockWorkerBuilder<Block, Runtime, ClientApi>
where
    Block: BlockT,
    Runtime: ConstructRuntimeApi<Block, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
    ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    NumberFor<Block>: Into<u32>,
{
    fn default() -> Self {
        Self {
            local: Worker::new_fifo(),
            stealers: None,
            global: None,
            client: None,
            backend: None,
            sender: None,
            _marker: PhantomData,
        }
    }
}

impl<Block, Runtime, ClientApi> BlockWorkerBuilder<Block, Runtime, ClientApi>
where
    Block: BlockT,
    Runtime: ConstructRuntimeApi<Block, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
    ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    NumberFor<Block>: Into<u32>,
{
    fn stealers(mut self, stealers: Arc<Vec<Stealer<Block>>>) -> Self {
        self.stealers = Some(stealers);
        self
    }

    fn global(mut self, injector: Arc<Injector<Block>>) -> Self {
        self.global = Some(injector);
        self
    }

    fn client(mut self, client: Arc<ClientApi>) -> Self {
        self.client = Some(client);
        self
    }

    fn backend(mut self, backend: Arc<Backend<Block>>) -> Self {
        self.backend = Some(backend);
        self
    }

    fn sender(mut self, sender: channel::Sender<BlockChanges<Block>>) -> Self {
        self.sender = Some(sender);
        self
    }

    fn build(self) -> BlockWorker<Block, Runtime, ClientApi> {
        BlockWorker {
            local: self.local,
            stealers: self.stealers.expect("Stealers are required"),
            global: self.global.expect("Global queue is required"),
            client: self.client.expect("Runtime Api Client is required"),
            backend: self.backend.expect("Disk Backend is required"),
            sender: self.sender.expect("Sender is required"),
            _marker: PhantomData,
        }
    }
}

/// Executor that sends blocks to a thread pool for execution
pub struct ThreadedBlockExecutor<Block: BlockT> {
    /// the workers that execute blocks
    worker_count: usize,
    /// Other threads from which work can be stolen
    stealers: Arc<Vec<Stealer<Block>>>,
    /// global queue that keeps unexecuteed blocks
    global_queue: Arc<Injector<Block>>,
    /// the threadpool
    pool: Arc<Mutex<ThreadPool>>,
    /// Entries that have been inserted (avoids inserting duplicates)
    inserted: HashSet<Block::Hash>,
    receiver: channel::Receiver<BlockChanges<Block>>,
}

impl<Block: BlockT> ThreadedBlockExecutor<Block>
where
    NumberFor<Block>: Into<u32>,
{
    /// create a new instance of the executor
    pub fn new<Runtime, ClientApi>(
        pool_multiplier: usize,
        stack_size: Option<usize>,
        client: Arc<ClientApi>,
        backend: Arc<Backend<Block>>,
    ) -> Self
    where
        Runtime: ConstructRuntimeApi<Block, ClientApi> + Send + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
            + ApiExt<Block, StateBackend = backend::StateBackendFor<Backend<Block>, Block>>,
        ClientApi: ApiAccess<Block, Backend<Block>, Runtime> + 'static,
    {
        let worker_count = num_cpus::get() * pool_multiplier;
        let stack_size = stack_size.unwrap_or(8_000_000);
        let pool = threadpool::Builder::new()
            .num_threads(worker_count)
            .thread_name(POOL_NAME.to_string())
            .thread_stack_size(stack_size) // 8 MB
            .build();
        log::info!("Starting {} workers", worker_count);

        let global_queue = Arc::new(Injector::<Block>::new());

        let mut stealers = Vec::new();
        let mut workers = Vec::new();

        for _ in 0..worker_count {
            workers.push(BlockWorkerBuilder::<Block, Runtime, ClientApi>::default())
        }

        for worker in workers.iter() {
            stealers.push(worker.local.stealer())
        }

        let stealers = Arc::new(stealers);
        let (sender, receiver) = channel::unbounded();

        let workers = workers
            .into_iter()
            .map(|w| {
                w.stealers(stealers.clone())
                    .global(global_queue.clone())
                    .client(client.clone())
                    .backend(backend.clone())
                    .sender(sender.clone())
                    .build()
            })
            .collect::<Vec<BlockWorker<Block, Runtime, ClientApi>>>();

        // having more workers than max count will cause a deadlock
        assert_eq!(workers.len(), pool.max_count());
        for worker in workers.into_iter() {
            pool.execute(move || match worker.start_worker() {
                Ok(_) => log::info!("Worker Finished"),
                Err(e) => {
                    log::error!("{:?}", e);
                    panic!("Worker should be able to find work");
                }
            });
        }
        let pool = Arc::new(Mutex::new(pool));
        Self {
            worker_count,
            global_queue,
            stealers,
            pool,
            receiver,
            inserted: HashSet::new(),
        }
    }

    /// push some work to the global pool
    pub fn push_to_queue(&mut self, block: Block) -> Result<(), ArchiveError> {
        if self.inserted.contains(&block.hash()) {
            return Ok(());
        } else {
            self.inserted.insert(block.hash());
            self.global_queue.push(block)
        }
        Ok(())
    }

    pub fn push_vec_to_queue(&mut self, blocks: Vec<Block>) -> Result<(), ArchiveError> {
        let to_insert = blocks
            .into_iter()
            .filter(|b| !self.inserted.contains(&b.hash()))
            .collect::<Vec<_>>();

        if to_insert.len() > 0 {
            for block in to_insert.into_iter() {
                self.inserted.insert(block.hash());
                self.global_queue.push(block)
            }
        }
        Ok(())
    }

    /// return how many workers are actively executing blocks
    pub fn active(&self) -> Result<usize, ArchiveError> {
        Ok(self
            .pool
            .lock()
            .map_err(|_| ArchiveError::from("Could not obtain mutex"))?
            .active_count())
    }

    /// wait for the workers to finish
    pub fn join(&self) -> Result<(), ArchiveError> {
        self.pool
            .lock()
            .map_err(|_| ArchiveError::from("Could not obtain mutex"))?
            .join();
        Ok(())
    }

    /// get the receiving end for this channel
    pub fn receiver(&self) -> &channel::Receiver<BlockChanges<Block>> {
        &self.receiver
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
        executor.push_vec_to_queue(blocks);
        executor.join();
    }
}
