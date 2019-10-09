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

use log::*;
use futures::{Future, Stream, sync::mpsc::{self, UnboundedReceiver, UnboundedSender}, future};
use tokio::runtime::Runtime;
use runtime_primitives::traits::Header;
use substrate_primitives::{
    storage::StorageKey,
    twox_128
};

use std::marker::PhantomData;

use crate::{
    database::Database,
    rpc::Rpc,
    error::Error as ArchiveError,
    types::{System, Data, storage::{StorageKeyType, TimestampOp}}
};

// TODO: the " 'static" constraint will be possible to remove Nov 7, with the hopeful and long-anticipated release of async-await
pub struct Archive<T: System + std::fmt::Debug + 'static> {
    marker: PhantomData<T>
}

impl<T> Archive<T> where T: System + std::fmt::Debug + 'static {

    pub fn run() -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        let mut rt = Runtime::new()?;
        let rpc = Rpc::<T>::new(&mut rt, &url::Url::parse("ws://127.0.0.1:9944")?)?;
        let db = Database::new()?;
        rt.spawn(rpc.subscribe_new_heads(sender.clone()).map_err(|e| println!("{:?}", e)));
        // rt.spawn(rpc.subscribe_finalized_blocks(sender.clone()).map_err(|e| println!("{:?}", e)));
        // rt.spawn(rpc.storage_keys(sender).map_err(|e| println!("{:?}", e)));
        // rt.spawn(rpc.subscribe_events(sender.clone()).map_err(|e| println!("{:?}", e)));

        tokio::run(Self::handle_data(receiver, db, rpc, sender));
        Ok(())
    }

    // use tokio_threadpool to asyncronize diesel queries
    // https://github.com/gotham-rs/gotham/issues/309
    // https://docs.rs/tokio-threadpool/0.1.8/tokio_threadpool/fn.blocking.html
    // put PgConnection in a mutex
    // this will allow us to send off multiple requests to insert into the database
    // in an asyncronous fashion
    // this becomes especially important when inserting batch requests for historical blocks that
    // are not yet in the database
    // without blocking our RPC from accepting new_heads therefore keeping up with the blocktime of
    // substrate/polkadot
    fn verify(db: Database, rpc: Rpc<T>) -> () /* impl Future<Item = (), Error = ()> */ {
        unimplemented!();
    }

    fn handle_data(receiver: UnboundedReceiver<Data<T>>,
                    db: Database,
                    rpc: Rpc<T>,
                    sender: UnboundedSender<Data<T>>,
    ) -> impl Future<Item = (), Error = ()> + 'static
    {
        // task for getting blocks
        // if we need data that depends on other data that needs to be received first (EX block needs hash from the header)
        receiver.for_each(move |data| {
            let res = db.insert(&data);
            match &data {
                Data::Header(header) => {
                    tokio::spawn(
                        rpc.block(header.hash(), sender.clone())
                        .map_err(|e| println!("{:?}", e))
                    );
                },
                Data::Block(block) => {
                    let header = &block.block.header;
                    let timestamp_key = b"Timestamp Now";
                    let storage_key = twox_128(timestamp_key);
                    tokio::spawn(
                        rpc.storage(
                            sender.clone(),
                            StorageKey(storage_key.to_vec()),
                            header.hash(),
                            StorageKeyType::Timestamp(TimestampOp::Now)
                        )
                        .map_err(|e| println!("{:?}", e))
                    );
                },
                _ => {}
            };
            match res {
                Err(e) => {
                    error!("Failed inserting all of block {:?} ", e);
                },
                Ok(_) => {
                    info!("Succesfully inserted block into db");
                }
            };
            future::ok(())
        })
    }
}
