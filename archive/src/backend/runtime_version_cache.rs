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
use crate::error::{ArchiveResult, Error};
use arc_swap::ArcSwap;
use codec::Decode;
use hashbrown::HashMap;
use sc_executor::sp_wasm_interface::HostFunctions;
use sc_executor::{WasmExecutionMethod, WasmExecutor};
use sp_api::OldRuntimeVersion;
use sp_core::traits::CallInWasmExt;
use sp_runtime::traits::Block as BlockT;
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

    pub fn get(&self, hash: B::Hash) -> ArchiveResult<Option<RuntimeVersion>> {
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
            log::info!("Registered New Runtime Version: {:#?}", v);
            self.versions.rcu(|cache| {
                let mut cache = HashMap::clone(&cache);
                cache.insert(code_hash, v.clone().into());
                cache
            });
            Ok(Some(v.into()))
        }
    }
}

fn decode_version(version: &[u8]) -> ArchiveResult<sp_version::RuntimeVersion> {
    let v: RuntimeVersion = sp_api::OldRuntimeVersion::decode(&mut &version[..])?.into();
    let core_api_id = sp_core::hashing::blake2_64(b"Core");
    if v.has_api_with(&core_api_id, |v| v >= 3) {
        sp_api::RuntimeVersion::decode(&mut &version[..]).map_err(Into::into)
    } else {
        Ok(v)
    }
}
