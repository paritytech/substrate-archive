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

//! Spawning of all tasks happens in this module
//! Nowhere else is anything ever spawned

use desub::{
    decoder::{Decoder, Metadata},
    TypeDetective,
};
use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    future, Future, StreamExt,
};
use log::*;
use runtime_version::RuntimeVersion;
use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};
use subxt::system::System;

use crate::{
    blocks_archive::BlocksArchive,
    database::Database,
    error::Error as ArchiveError,
    rpc::Rpc,
    types::{BatchData, ChainInfo, Data, Substrate},
};

// with the hopeful and long-anticipated release of async-await
pub struct Archive<T: Substrate + Send + Sync, P: TypeDetective> {
    rpc: Arc<Rpc<T>>,
    // database that holds types of runtime P
    db: Arc<Database<P>>,
    /// queue of hashes to be fetched from other threads
    queue: Arc<RwLock<HashSet<T::Hash>>>,
}

impl<T, P> Archive<T, P>
where
    T: Substrate + Send + Sync,
    P: TypeDetective + Send + Sync,
{
    pub fn new(decoder: Decoder<P>, rpc: subxt::Client<T>) -> Result<Self, ArchiveError> {
        let rpc = Rpc::<T>::new(rpc);
        let db = Database::new(decoder)?;
        let (rpc, db) = (Arc::new(rpc), Arc::new(db));
        let queue = Arc::new(RwLock::new(HashSet::new()));
        Ok(Self { rpc, db, queue })
    }

    /// run as a single-threaded app
    pub fn run(self) -> Result<(), ArchiveError>
    where
        <T as System>::BlockNumber: Into<u32>,
    {
        let (sender, receiver) = mpsc::unbounded();
        let data_in = Self::handle_data(receiver, self.db.clone(), self.rpc.clone());
        let blocks = Self::blocks(self.rpc.clone(), sender.clone());
        futures::executor::block_on(future::join(data_in, blocks));
        log::info!("All Done!");
        Ok(())
    }

    pub fn parts(
        self,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<Output = ()>,
            impl Future<Output = Result<(), ArchiveError>>,
            impl Future<Output = Result<(), ArchiveError>>,
        ),
        ArchiveError,
    >
    where
        <T as System>::BlockNumber: Into<u32>,
        <T as System>::BlockNumber: Into<u32>,
        <T as System>::BlockNumber: From<u32>,
    {
        let (sender, receiver) = mpsc::unbounded();
        let (sender_batch, receiver_batch) = mpsc::unbounded();

        let blocks_archive =
            BlocksArchive::new(self.queue.clone(), self.rpc.clone(), self.db.clone())?;
        let blocks_archive = blocks_archive.run(sender_batch);
        let blocks = Self::blocks(self.rpc.clone(), sender.clone());

        let data_in = Self::handle_data(receiver, self.db.clone(), self.rpc.clone());
        let batch_handler =
            Self::handle_batch_data(receiver_batch, self.db.clone(), self.rpc.clone());

        Ok((data_in, blocks, blocks_archive, batch_handler))
    }

    async fn blocks(rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>) {
        log::info!("Spawning block subscription");
        match rpc.subscribe_blocks(sender).await {
            Ok(_) => (),
            Err(e) => error!("{:?}", e),
        };
    }

    async fn handle_data(
        mut receiver: UnboundedReceiver<Data<T>>,
        db: Arc<Database<P>>,
        rpc: Arc<Rpc<T>>,
    ) where
        <T as System>::BlockNumber: Into<u32>,
    {
        log::info!("Spawning data");
        while let Some(data) = receiver.next().await {
            match data {
                d => {
                    let db = db.clone();
                    let (meta, version) = match Self::version_info(rpc.clone(), &d).await {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!("{:?}", e);
                            panic!("Internal Error");
                        }
                    };
                    // make sure metadata version is registered for decoding anything
                    db.register_version(meta, version.spec_version).unwrap();
                    db.insert(d, Some(version.spec_version)).unwrap();
                }
            }
        }
    }

    async fn version_info<H: ChainInfo<T>>(
        rpc: Arc<Rpc<T>>,
        block: &H,
    ) -> Result<(Metadata, RuntimeVersion), ArchiveError> {
        let hash = block.get_hash();
        let meta = rpc.metadata(Some(&hash)).await?;
        let version = rpc.version(Some(&hash)).await?;
        Ok((meta, version))
    }

    async fn handle_batch_data(
        mut receiver: UnboundedReceiver<BatchData<T>>,
        db: Arc<Database<P>>,
        rpc: Arc<Rpc<T>>,
    ) -> Result<(), ArchiveError>
    where
        <T as System>::BlockNumber: Into<u32>,
    {
        log::info!("Initializing batch data handler");
        while let Some(data) = receiver.next().await {
            match data {
                d => {
                    db.insert(d, None)?;
                }
            }
        }
        Ok(())
    }
}
