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

//! Wrapper RPC convenience functions

use futures::{future::join, TryFutureExt};
use runtime_version::RuntimeVersion;
use sp_storage::{StorageChangeSet, StorageKey};
use substrate_rpc_primitives::number::NumberOrHex;
use subxt::{system::System, Client};

use crate::{
    error::Error as ArchiveError,
    types::{Substrate, SubstrateBlock},
};

/// Communicate with Substrate node via RPC
#[derive(Clone)]
pub struct Rpc<T: Substrate + Send + Sync> {
    client: Client<T>,
}

/// Methods that return fetched value directly
impl<T> Rpc<T>
where
    T: Substrate + Send + Sync,
{
    pub fn new(client: subxt::Client<T>) -> Self {
        Self { client }
    }

    pub(crate) async fn meta_and_version(
        &self,
        hash: Option<T::Hash>,
    ) -> Result<(RuntimeVersion, Vec<u8>), ArchiveError> {
        let meta = self
            .client
            .raw_metadata(hash.as_ref())
            .map_err(ArchiveError::from);
        let version = self
            .client
            .runtime_version(hash.as_ref())
            .map_err(ArchiveError::from);
        let (meta, version) = join(meta, version).await;
        let meta = meta?;
        let version = version?;
        Ok((version, meta))
        // Ok((version, Metadata::new(meta.as_slice())))
    }

    pub(crate) async fn version(
        &self,
        hash: Option<T::Hash>,
    ) -> Result<RuntimeVersion, ArchiveError> {
        self.client
            .runtime_version(hash.as_ref())
            .map_err(Into::into)
            .await
    }

    pub(crate) async fn metadata(&self, hash: Option<T::Hash>) -> Result<Vec<u8>, ArchiveError> {
        self.client
            .raw_metadata(hash.as_ref())
            .map_err(ArchiveError::from)
            .await
    }

    pub async fn block_from_number(
        &self,
        number: NumberOrHex<T::BlockNumber>,
    ) -> Result<Option<SubstrateBlock<T>>, ArchiveError> {
        let hash = self.client.block_hash(Some(number.into())).await?;
        self.client.block(hash).await.map_err(Into::into)
    }

    pub async fn query_storage(
        &self,
        from: T::Hash,
        to: Option<T::Hash>,
        keys: Vec<StorageKey>,
    ) -> Result<Vec<StorageChangeSet<<T as System>::Hash>>, ArchiveError> {
        self.client
            .query_storage(keys, from, to)
            .map_err(ArchiveError::from)
            .await
    }

    // pub async fn storage()

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

}
