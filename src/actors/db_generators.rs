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

//! Work Generated and gathered from the PostgreSQL Database
//! IE: Missing Blocks/Storage/Inherents/Transactions
//! Gathers Missing blocks -> passes to metadata -> passes to extractors -> passes to decode -> passes to insert

use crate::{
    backend::ChainAccess,
    types::{NotSignedBlock, Substrate},
};
use async_std::prelude::*;
use async_std::stream;
use bastion::prelude::*;
use sc_client_api::client::BlockBackend as _;
use sp_runtime::generic::BlockId;
use std::{sync::Arc, time::Duration};

pub fn actor<T>(client: Arc<impl ChainAccess<NotSignedBlock> + 'static>) -> Result<ChildrenRef, ()>
where
    T: Substrate,
{
    // generate work from missing blocks
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let mut interval = stream::interval(Duration::from_secs(20));
            let client = client.clone();
            /// query for missing blocks
            async move {
                while let Some(_) = interval.next().await {
                    let block = client.block(&BlockId::Number(5));
                    println!("Got the fifth block: {:?}", block);
                }
                Ok(())
            }
        })
    })
}
