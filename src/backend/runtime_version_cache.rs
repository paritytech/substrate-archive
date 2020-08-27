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

//! A cache of runtime versions
//! Will only call the `runtime_version` function once per wasm blob

use super::ReadOnlyBackend;
use crate::{
    error::{Error, Result},
    types::Block,
};
use arc_swap::ArcSwap;
use codec::Decode;
use hashbrown::HashMap;
use sc_executor::sp_wasm_interface::HostFunctions;
use sc_executor::{WasmExecutionMethod, WasmExecutor};
use sp_core::traits::CallInWasmExt;
use sp_runtime::{
    generic::SignedBlock,
    traits::{Block as BlockT, Header as _, NumberFor},
};
use sp_state_machine::BasicExternalities;
use sp_storage::well_known_keys;
use sp_version::RuntimeVersion;
use std::sync::Arc;

#[derive(Clone)]
pub struct RuntimeVersionCache<B: BlockT> {
    /// Hash of the WASM Blob -> RuntimeVersion
    versions: ArcSwap<HashMap<u64, RuntimeVersion>>,
    backend: Arc<ReadOnlyBackend<B>>,
    exec: WasmExecutor,
}

impl<B: BlockT> RuntimeVersionCache<B> {
    pub fn new(backend: Arc<ReadOnlyBackend<B>>) -> Self {
        // all _available_ functions
        // sp_io::storage::HostFunctions
        // sp_io::default_child_storage
        // sp_io::misc::HostFunctions
        // sp_io::offchain::HostFunctions
        // sp_io::crypto::HostFunctions
        // sp_io::hashing::HostFunctions
        // sp_io::logging::HostFunctions
        // sp_io::sandbox::HostFunctions
        // sp_io::trie::HostFunctions
        // sp_io::offchain_index::HostFunctions

        // remove some unnecessary host functions
        let funs = sp_io::SubstrateHostFunctions::host_functions()
            .into_iter()
            .filter(|f| !(f.name().matches("wasm_tracing").count() > 0))
            .filter(|f| !(f.name().matches("ext_offchain").count() > 0))
            .filter(|f| !(f.name().matches("ext_storage").count() > 0))
            .filter(|f| !(f.name().matches("ext_default_child_storage").count() > 0))
            .filter(|f| !(f.name().matches("ext_logging").count() > 0))
            .collect::<Vec<_>>();

        let exec = WasmExecutor::new(WasmExecutionMethod::Interpreted, Some(128), funs, 1);
        Self {
            versions: ArcSwap::from_pointee(HashMap::new()),
            backend,
            exec,
        }
    }

    /// Get a version of the runtime for some Block Hash
    /// Prefer `find_versions` when trying to get the runtime versions for
    /// many consecutive blocks
    pub fn get(&self, hash: B::Hash) -> Result<Option<RuntimeVersion>> {
        // Getting code from the backend is the slowest part of this. Takes an average of
        // 6ms
        let code = self
            .backend
            .storage(hash, well_known_keys::CODE)
            .ok_or(Error::from("storage does not exist"))?;

        let code_hash = crate::util::make_hash(&code);
        if self.versions.load().contains_key(&code_hash) {
            Ok(self.versions.load().get(&code_hash).map(|v| v.clone()))
        } else {
            log::debug!("new code hash: {:#X?}", code_hash);
            let mut ext: BasicExternalities = BasicExternalities::default();
            ext.register_extension(CallInWasmExt::new(self.exec.clone()));
            let v: RuntimeVersion = ext.execute_with(|| {
                let ver = sp_io::misc::runtime_version(&code).ok_or(Error::WasmExecutionError)?;
                decode_version(ver.as_slice())
            })?;
            log::debug!("Registered New Runtime Version: {:?}", v);
            self.versions.rcu(|cache| {
                let mut cache = HashMap::clone(&cache);
                cache.insert(code_hash, v.clone().into());
                cache
            });
            Ok(Some(v.into()))
        }
    }

    /// Recursively finds the versions of all the blocks while minimizing reads/calls to the backend.
    pub fn find_versions(&self, blocks: &[SignedBlock<B>]) -> Result<Vec<VersionRange<B>>> {
        let mut versions = Vec::new();
        self.find_pivot(blocks, &mut versions)?;
        Ok(versions)
    }

    /// Finds the versions of all the blocks.
    /// Returns a new set of type `Block`.
    ///
    /// # Panics
    /// panics if our search fails to get the version for a block
    pub fn find_versions_as_blocks(&self, blocks: Vec<SignedBlock<B>>) -> Result<Vec<Block<B>>>
    where
        NumberFor<B>: Into<u32>,
    {
        let versions = self.find_versions(blocks.as_slice())?;
        Ok(blocks
            .into_iter()
            .map(|b| {
                let v = versions
                    .iter()
                    .find(|v| v.contains_block(*b.block.header().number()))
                    .unwrap_or_else(|| {
                        panic!("No range for {}", b.block.header().number());
                    });
                Block::new(b, v.version.spec_version)
            })
            .collect())
    }

    /// This can be thought of as similiar to a recursive Binary Search
    fn find_pivot(
        &self,
        blocks: &[SignedBlock<B>],
        versions: &mut Vec<VersionRange<B>>,
    ) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        } else if blocks.len() == 1 {
            let version = self
                .get(blocks[0].block.header().hash())?
                .ok_or(Error::from("Version not found"))?;
            versions.push(VersionRange::new(&blocks[0], &blocks[0], version));
            return Ok(());
        }

        let first = self
            .get(blocks.first().unwrap().block.header().hash())?
            .ok_or(Error::from("Version not found"))?;
        let last = self
            .get(blocks.last().unwrap().block.header().hash())?
            .ok_or(Error::from("Version not found"))?;

        if first.spec_version != last.spec_version && blocks.len() > 2 {
            let half = blocks.len() / 2;
            let (first_half, last_half) = (&blocks[0..half], &blocks[half..blocks.len()]);
            self.find_pivot(first_half, versions)?;
            self.find_pivot(last_half, versions)?;
        } else if (first.spec_version != last.spec_version) && (blocks.len() == 2) {
            versions.push(VersionRange::new(&blocks[0], &blocks[0], first));
            versions.push(VersionRange::new(&blocks[1], &blocks[1], last));
        } else {
            versions.push(VersionRange::new(
                blocks.first().unwrap(),
                blocks.last().unwrap(),
                first,
            ));
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct VersionRange<B: BlockT> {
    pub start: NumberFor<B>,
    pub end: NumberFor<B>,
    pub version: RuntimeVersion,
}

impl<B: BlockT> VersionRange<B> {
    fn new(first: &SignedBlock<B>, last: &SignedBlock<B>, version: RuntimeVersion) -> Self {
        Self {
            start: *first.block.header().number(),
            end: *last.block.header().number(),
            version,
        }
    }

    fn contains_block(&self, b: NumberFor<B>) -> bool {
        (b > self.start && b < self.end) || b == self.start || b == self.end
    }
}

fn decode_version(version: &[u8]) -> Result<sp_version::RuntimeVersion> {
    let v: RuntimeVersion = sp_api::OldRuntimeVersion::decode(&mut &version[..])?.into();
    let core_api_id = sp_core::hashing::blake2_64(b"Core");
    if v.has_api_with(&core_api_id, |v| v >= 3) {
        sp_api::RuntimeVersion::decode(&mut &version[..]).map_err(Into::into)
    } else {
        Ok(v)
    }
}
