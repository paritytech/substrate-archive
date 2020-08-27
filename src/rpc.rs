// Copyright 2017-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either vdebugersion 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

//! Substrate RPC

use jsonrpsee::{
    client::Subscription,
    common::{to_value as to_json_value, Params},
    Client,
};
use sp_core::Bytes;
use sp_runtime::traits::{Block as BlockT, HashFor};
use sp_version::RuntimeVersion;
use std::marker::PhantomData;

use crate::error::Error as Error;

/// Communicate with Substrate node via RPC
#[derive(Clone)]
pub struct Rpc<Block: BlockT> {
    client: Client,
    _marker: PhantomData<Block>,
}
// version/metadata subscribe blocks
/// Methods that return fetched value directly
impl<Block: BlockT> Rpc<Block> {
    pub(crate) async fn connect(url: &str) -> Result<Self, Error> {
        let client = jsonrpsee::ws_client(&url).await?;
        Ok(Rpc {
            client,
            _marker: PhantomData,
        })
    }

    pub(crate) async fn version(
        &self,
        hash: Option<&HashFor<Block>>,
    ) -> Result<RuntimeVersion, Error> {
        let params = Params::Array(vec![to_json_value(hash)?]);
        let version = self
            .client
            .request("state_getRuntimeVersion", params)
            .await?;
        Ok(version)
    }

    pub(crate) async fn metadata(
        &self,
        hash: Option<Block::Hash>,
    ) -> Result<Vec<u8>, Error> {
        let params = Params::Array(vec![to_json_value(hash)?]);
        let bytes: Bytes = self.client.request("state_getMetadata", params).await?;
        Ok(bytes.0)
    }

    pub(crate) async fn subscribe_finalized_heads(
        &self,
    ) -> Result<Subscription<Block::Header>, Error> {
        let subscription = self
            .client
            .subscribe(
                "chain_subscribeFinalizedHeads",
                Params::None,
                "chain_subscribeFinalizedHeads",
            )
            .await?;
        Ok(subscription)
    }
}
