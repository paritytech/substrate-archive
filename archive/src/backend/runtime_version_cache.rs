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
use arc_swap::ArcSwap;
use codec::Decode;
use hashbrown::HashMap;
use sp_runtime::traits::Block as BlockT;
use sp_storage::well_known_keys;
use sp_version::RuntimeVersion;
use std::sync::Arc;

#[derive(Clone)]
pub struct RuntimeVersionCache<B: BlockT> {
    /// Hash of the WASM Blob -> RuntimeVersion
    versions: ArcSwap<HashMap<u64, RuntimeVersion>>,
    backend: Arc<ReadOnlyBackend<B>>,
}

impl<B: BlockT> RuntimeVersionCache<B> {
    pub fn new(backend: Arc<ReadOnlyBackend<B>>) -> Self {
        Self {
            versions: ArcSwap::from_pointee(HashMap::new()),
            backend: backend,
        }
    }

    pub fn get(&self, hash: B::Hash) -> Option<RuntimeVersion> {
        let code = self.backend.storage(hash, well_known_keys::CODE)?;
        let hash = crate::util::make_hash(&code);

        if self.versions.load().contains_key(&hash) {
            self.versions.load().get(&hash).map(|v| v.clone())
        } else {
            let version: RuntimeVersion =
                Decode::decode(&mut sp_io::misc::runtime_version(&code)?.as_slice()).ok()?;
            self.versions.rcu(|cache| {
                let mut cache = HashMap::clone(&cache);
                cache.insert(hash, version.clone());
                cache
            });
            Some(version)
        }
    }
}
