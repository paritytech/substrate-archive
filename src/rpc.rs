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

use futures::{Future, Stream, sync::mpsc};
use failure::{Error, ResultExt};
use jsonrpc_core_client::{/*transports::http*/RpcError, TypedSubscriptionStream};
use sr_primitives::generic::{SignedBlock};
use node_primitives::{Hash, Header, Block}; // Block == Block<Header, UncheckedExtrinsic>
use sr_primitives::generic::{Era, /* Header */};
use sr_primitives::traits::{StaticLookup, /* BlakeTwo256 */ };
use substrate_subxt::srml::{balances::Balances, contracts::Contracts, system::System};
use substrate_subxt::{Client, ClientBuilder};
use tokio::runtime::Runtime as TokioRuntime;

use crate::error::ErrorKind;
use crate::types::{Data, DataEntryType};

pub struct Runtime;

impl System for Runtime {
    type Index = <node_runtime::Runtime as srml_system::Trait>::Index;
    type BlockNumber = <node_runtime::Runtime as srml_system::Trait>::BlockNumber;
    type Hash = <node_runtime::Runtime as srml_system::Trait>::Hash;
    type Hashing = <node_runtime::Runtime as srml_system::Trait>::Hashing;
    type AccountId = <node_runtime::Runtime as srml_system::Trait>::AccountId;
    type Lookup = <node_runtime::Runtime as srml_system::Trait>::Lookup;
    type Header = <node_runtime::Runtime as srml_system::Trait>::Header;
    type Event = <node_runtime::Runtime as srml_system::Trait>::Event;

    type SignedExtra = (
        srml_system::CheckVersion<node_runtime::Runtime>,
        srml_system::CheckGenesis<node_runtime::Runtime>,
        srml_system::CheckEra<node_runtime::Runtime>,
        srml_system::CheckNonce<node_runtime::Runtime>,
        srml_system::CheckWeight<node_runtime::Runtime>,
        srml_balances::TakeFees<node_runtime::Runtime>,
    );
    fn extra(nonce: Self::Index) -> Self::SignedExtra {
        (
            srml_system::CheckVersion::<node_runtime::Runtime>::new(),
            srml_system::CheckGenesis::<node_runtime::Runtime>::new(),
            srml_system::CheckEra::<node_runtime::Runtime>::from(Era::Immortal),
            srml_system::CheckNonce::<node_runtime::Runtime>::from(nonce),
            srml_system::CheckWeight::<node_runtime::Runtime>::new(),
            srml_balances::TakeFees::<node_runtime::Runtime>::from(0),
        )
    }
}

impl Balances for Runtime {
    type Balance = <node_runtime::Runtime as srml_balances::Trait>::Balance;
}

impl Contracts for Runtime {}

type Index = <Runtime as System>::Index;
type AccountId = <Runtime as System>::AccountId;
type Address = <<Runtime as System>::Lookup as StaticLookup>::Source;
type Balance = <Runtime as Balances>::Balance;


// temporary util function to get a Substrate Client and TokioRuntime
pub fn client() -> (TokioRuntime, Client<Runtime>) {
    let mut rt = TokioRuntime::new().unwrap();
    let client_future = ClientBuilder::<Runtime>::new().build();
    let client = rt.block_on(client_future).unwrap();
    (rt, client)
}

/*
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
 */

pub fn subscribe_new_heads(client: Client<Runtime>, sender: mpsc::UnboundedSender<Data>) -> impl Future<Item = (), Error = substrate_subxt::Error>
{
    client.subscribe_blocks()
          .and_then(|stream| {
              stream.for_each(move |_head| {
                  sender.unbounded_send(Data {
                      info: DataEntryType::NewHead
                  }).map_err(|e| println!("{:?}", e)).unwrap();
                  futures::future::ok(())
              }).map_err(|e| substrate_subxt::Error::Rpc(e))
          })
}

pub fn subscribe_finalized_blocks(client: Client<Runtime>, sender: mpsc::UnboundedSender<Data>) -> impl Future<Item = (), Error = substrate_subxt::Error>
{
    client.subscribe_finalized_blocks()
        .and_then(|stream| {
            stream.for_each(move |_head| {
                sender.unbounded_send(Data {
                    info: DataEntryType::FinalizedBlock
                }).map_err(|e| println!("{:?}", e)).unwrap();
                futures::future::ok(())
            }).map_err(|e| substrate_subxt::Error::Rpc(e))
        })
}

fn unsubscribe_finalized_heads() {
    unimplemented!();
}

fn unsubscribe_new_heads() {
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
