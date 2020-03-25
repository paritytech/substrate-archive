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

use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    future, FutureExt, StreamExt, TryFutureExt,
};
use log::*;
use runtime_primitives::traits::Header;
use desub::TypeDetective;

use std::sync::Arc;

use crate::{
    database::Database,
    error::Error as ArchiveError,
    rpc::Rpc,
    types::{BatchBlock, Data, Substrate},
};

// with the hopeful and long-anticipated release of async-await
pub struct Archive<T: Substrate + Send + Sync, P: TypeDetective> {
    rpc: Arc<Rpc<T>>,
    // database that holds types of runtime P
    db: Arc<Database<P>>,
}

impl<T, P> Archive<T, P>
where
    T: Substrate + Send + Sync,
    P: TypeDetective
{
    pub fn new(decoder: P) -> Result<Self, ArchiveError> {
        // let rpc = runtime.block_on(Rpc::<T>::new(url::Url::parse("ws://127.0.0.1:9944")?))?;
        let rpc = Rpc::<T>::new("ws:://127.0.0.1:9944")?;
        let db = Database::new(decoder)?;
        let (rpc, db) = (Arc::new(rpc), Arc::new(db));
        log::debug!("METADATA: {}", rpc.metadata());
        log::debug!("KEYS: {:?}", rpc.keys());
        // log::debug!("PROPERTIES: {:?}", rpc.properties());
        Ok(Self { rpc, db })
    }

    pub fn run(mut self) -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        let data_in = Self::handle_data(receiver, self.db.clone());
        let blocks = Self::blocks(self.rpc.clone(), sender.clone());
        self.runtime.block_on(future::join(data_in, blocks));
        log::info!("All Done");
        Ok(())
    }

    async fn blocks(rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>) {
        match rpc.subscribe_blocks(sender).await {
            Ok(_) => (),
            Err(e) => error!("{:?}", e),
        };
    }

    async fn handle_data(mut receiver: UnboundedReceiver<Data<T>>, db: Arc<Database<P>>) {
        while let Some(data) = receiver.next().await {
            match data {
                Data::SyncProgress(missing_blocks) => {
                    println!("{} blocks missing", missing_blocks);
                }
                c => {
                    let db = db.clone();
                    let fut = async move || db.insert(c).map_err(|e| log::error!("{:?}", e)).await;
                    tokio::spawn(fut());
                }
            }
        }
    }
}
