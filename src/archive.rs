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
    marker::PhantomData,
};
use subxt::system::System;

use crate::{
    // blocks_archive::BlocksArchive,
    error::Error as ArchiveError,
    rpc::Rpc,
    types::{BatchData, ChainInfo, Data, Substrate},
};

// with the hopeful and long-anticipated release of async-await
pub struct Archive<T: Substrate + Send + Sync, P: TypeDetective> {
    rpc: Arc<Rpc<T>>,
    /// queue of hashes to be fetched from other threads
    queue: Arc<RwLock<HashSet<T::Hash>>>,
    _marker: PhantomData<P>
}

impl<T, P> Archive<T, P>
where
    T: Substrate + Send + Sync,
    P: TypeDetective + Send + Sync,
{
    pub fn new(decoder: Decoder<P>, rpc: subxt::Client<T>) -> Result<Self, ArchiveError> {
        let rpc = Rpc::<T>::new(rpc);
        let rpc = Arc::new(rpc);
        let queue = Arc::new(RwLock::new(HashSet::new()));
        Ok(Self { rpc, queue, _marker: PhantomData })
    }
}
