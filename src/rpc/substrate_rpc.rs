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

//! A simple shim over the Substrate Rpc
use codec::Decode;
use futures::{
    compat::{Future01CompatExt, Stream01CompatExt},
    future::{self, TryFutureExt},
    stream::{Stream, TryStreamExt},
};
use jsonrpc_core_client::{transports::ws, RpcChannel};
use runtime_metadata::RuntimeMetadataPrefixed;
use substrate_primitives::storage::{StorageData, StorageKey};
use substrate_rpc_api::{
    author::AuthorClient,
    chain::ChainClient,
    state::StateClient,
    system::{Properties, SystemClient},
};
use substrate_rpc_primitives::{list::ListOrValue, number::NumberOrHex};

use std::convert::TryInto;

use crate::{
    error::Error as ArchiveError,
    metadata::Metadata,
    types::{SubstrateBlock, System},
};

impl<T: System> From<RpcChannel> for SubstrateRpc<T> {
    fn from(channel: RpcChannel) -> Self {
        Self {
            state: channel.clone().into(),
            chain: channel.clone().into(),
            author: channel.clone().into(),
            system: channel.clone().into(),
        }
    }
}

/// Communicate with Substrate node via RPC
pub struct SubstrateRpc<T: System> {
    state: StateClient<T::Hash>,
    chain: ChainClient<T::BlockNumber, T::Hash, <T as System>::Header, SubstrateBlock<T>>,
    #[allow(dead_code)] // TODO remove
    author: AuthorClient<T::Hash, T::Hash>, // TODO get types right
    system: SystemClient<T::Hash, T::BlockNumber>,
}

impl<T> SubstrateRpc<T>
where
    T: System,
{
    /// instantiate new client
    pub(crate) async fn connect(url: &url::Url) -> Result<Self, ArchiveError> {
        let (client, fut) = ws::raw_connect(websocket::ClientBuilder::from_url(url))
            .compat()
            .map_err(|e| ArchiveError::from(e))
            .await?;
        tokio::spawn(fut.compat());
        Ok(client)
    }

    /// send all new headers back to main thread
    pub(crate) async fn subscribe_new_heads(
        &self,
    ) -> Result<impl Stream<Item = Result<T::Header, ArchiveError>>, ArchiveError> {
        let stream = self
            .chain
            .subscribe_new_heads()
            .compat()
            .map_err(|e| ArchiveError::from(e))
            .await?;

        Ok(stream.compat().map_err(|e| ArchiveError::from(e)))
    }

    /// send all finalized headers back to main thread
    pub(crate) async fn subscribe_finalized_heads(
        &self,
    ) -> Result<impl Stream<Item = Result<T::Header, ArchiveError>>, ArchiveError> {
        let stream = self.chain.subscribe_finalized_heads().compat().await?;
        Ok(stream.compat().map_err(|e| ArchiveError::from(e)))
    }

    pub(crate) async fn metadata(&self, hash: Option<T::Hash>) -> Result<Metadata, ArchiveError> {
        let metadata_bytes = self.state.metadata(hash).compat().await?;
        let metadata: RuntimeMetadataPrefixed =
            Decode::decode(&mut &metadata_bytes[..]).expect("Decode failed");
        metadata.try_into().map_err(Into::into)
    }

    // TODO: make "Key" and "from" vectors
    /// Get a storage item
    /// must provide the key, hash of the block to get storage from, as well as the key type
    pub(crate) async fn storage(
        &self,
        key: StorageKey,
        hash: T::Hash,
        // from: StorageKeyType
    ) -> Result<Option<StorageData>, ArchiveError> {
        // let hash: Vec<u8> = hash.encode();
        // let hash: T::Hash = Decode::decode(&mut hash.as_slice()).unwrap();
        self.state
            .storage(key, Some(hash))
            .compat()
            .map_err(Into::into)
            .await
    }

    pub(crate) async fn properties(&self) -> Result<Properties, ArchiveError> {
        self.system
            .system_properties()
            .compat()
            .map_err(Into::into)
            .await
    }

    pub(crate) async fn storage_keys(
        &self,
        prefix: StorageKey,
        hash: Option<T::Hash>,
    ) -> Result<Vec<StorageKey>, ArchiveError> {
        self.state
            .storage_keys(prefix, hash)
            .compat()
            .map_err(Into::into)
            .await
    }

    pub(crate) async fn block_from_number(
        &self,
        number: Option<ListOrValue<NumberOrHex<T::BlockNumber>>>,
    ) -> Result<ListOrValue<Option<SubstrateBlock<T>>>, ArchiveError> {
        let hash = self.hash(number).await?;
        self.block(hash).await
    }

    /// Fetch a block by hash from Substrate RPC
    pub(crate) async fn block(
        &self,
        hash: ListOrValue<Option<T::Hash>>,
    ) -> Result<ListOrValue<Option<SubstrateBlock<T>>>, ArchiveError> {
        match hash {
            ListOrValue::Value(v) => {
                let block = self
                    .chain
                    .block(v)
                    .compat()
                    .map_err(|e| ArchiveError::from(e))
                    .await?;
                Ok(ListOrValue::Value(block))
            }
            ListOrValue::List(v) => {
                let mut futures = Vec::new();
                for hash in v.into_iter() {
                    futures.push(self.chain.block(hash).compat().map_err(ArchiveError::from))
                }
                Ok(ListOrValue::List(future::try_join_all(futures).await?))
            }
        }
    }

    pub(crate) async fn hash(
        &self,
        number: Option<ListOrValue<NumberOrHex<T::BlockNumber>>>,
    ) -> Result<ListOrValue<Option<T::Hash>>, ArchiveError> {
        self.chain
            .block_hash(number)
            .compat()
            .map_err(Into::into)
            .await
    }

    /// unsubscribe from finalized heads
    #[allow(dead_code)]
    fn unsubscribe_finalized_heads() {
        unimplemented!();
    }

    /// unsubscribe from new heads
    #[allow(dead_code)]
    fn unsubscribe_new_heads() {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{tests::Runtime, types::*};
    use std::str::FromStr;
    use substrate_primitives::{storage::StorageKey, twox_128};
    use substrate_primitives::{H256, U256};
    use tokio::runtime::Runtime as TokioRuntime;

    fn connect() -> (TokioRuntime, SubstrateRpc<Runtime>) {
        let mut runtime = TokioRuntime::new().unwrap();
        let rpc = runtime
            .block_on(SubstrateRpc::<Runtime>::connect(
                &url::Url::parse("ws://127.0.0.1:9944").unwrap(),
            ))
            .unwrap();
        (runtime, rpc)
    }

    // [WARNING] Needs an Rpc running on port 9944
    #[test]
    fn should_get_block() {
        let (mut rt, rpc) = connect();
        let block = rt
            .block_on(
                rpc.block(
                    "373c569f3520c7ba67a7ac1d6b8e4ead5bd27b1ec28f3e39f5f863c503956e31"
                        .parse()
                        .unwrap(),
                ),
            )
            .unwrap();
        println!("{:?}", block);
    }

    // [WARNING] Requires an Rpc running on port 9944
    #[test]
    fn should_get_hash() {
        let (mut rt, rpc) = connect();
        let hash = rt.block_on(rpc.hash(NumberOrHex::Number(6))).unwrap();
        println!("{:?}", hash);
    }

    #[test]
    fn should_get_storage() {
        let (mut rt, rpc) = connect();
        let timestamp_key = b"Timestamp Now";
        let storage_key = StorageKey(twox_128(timestamp_key).to_vec());
        let hash: <Runtime as System>::Hash =
            "373c569f3520c7ba67a7ac1d6b8e4ead5bd27b1ec28f3e39f5f863c503956e31"
                .parse()
                .unwrap();
        let storage = rt.block_on(rpc.storage(storage_key, hash));
    }
}
