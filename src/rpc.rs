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


use futures::{Future, Stream};
use failure::{Error, ResultExt};
use jsonrpc_core_client::{/*transports::http*/RpcError, TypedSubscriptionStream};
use jsonrpc_pubsub::{Subscriber};
use sr_primitives::generic::{SignedBlock};
use node_primitives::{Hash, Header, Block}; // Block == Block<Header, UncheckedExtrinsic>
use substrate_primitives::U256;
use substrate_rpc::author::{hash::ExtrinsicOrHash, AuthorClient};
use substrate_rpc::chain::{ChainClient, number::NumberOrHex}; // Blocks/headers/bodies/
use substrate_rpc::state::StateClient; // smart contract storage
use substrate_rpc::Metadata;


use crate::error::ErrorKind;

// env_logger::init();
/*
    rt::run(rt::lazy(|| {
        let uri = "http://localhost:9933";

        http::connect(uri)
            .and_then(|client: AuthorClient<Hash, Hash>| remove_all_extrinsics(client))
            .map_err(|e| {
                println!("Error: {:?}", e);
            })
    }))
*/

pub fn query_block(client: ChainClient<Hash, Hash, Header, SignedBlock<Block>>, block_num: u64)
    -> impl Future<Item = SignedBlock<Block>, Error = RpcError>
{
    client.
        block_hash(Some(NumberOrHex::Hex(U256::from(block_num))))
        .and_then(move |hash| {
            client.block(hash)
        })
        .map(|b| { b.ok_or(RpcError::Other(ErrorKind::ValueNotPresent.into())) })
        .and_then(|b| { b })
}
// useless function
pub fn subscribe_heads(client: ChainClient<Hash, Hash, Header, SignedBlock<Block>>) -> impl Future<Item = TypedSubscriptionStream<Header>, Error = RpcError>
{
    client.subscribe_new_heads()
}

pub fn subscribe_finalized_heads(client: ChainClient<Hash, Hash, Header, SignedBlock<Block>>) -> impl Future<Item = TypedSubscriptionStream<Header>, Error = RpcError> {
    client.subscribe_finalized_heads()
}

fn unsubscribe_finalized_heads() {
    unimplemented!();
}
#[cfg(test)]
mod tests {

    #[test]
    fn can_query_blocks() {

    }

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
