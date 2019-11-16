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

//! Wrapper over code copied from substate-subxt
// substrate-archive will eventually use substrate-subxt once refactoring
// the way extrinsics are crawled from the substrate RPC

mod subxt_metadata;

use log::*;
use runtime_metadata::RuntimeMetadataPrefixed;
use substrate_primitives::{
    twox_128,
    storage::StorageKey
};

use std::{
    convert::TryFrom,
    fmt
};

pub use self::subxt_metadata::{ Metadata as SubxtMetadata, Error};
use crate::error::Error as ArchiveError;

pub struct Metadata {
    inner: SubxtMetadata
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner.pretty())
    }
}

impl Metadata {
    pub fn new(meta: RuntimeMetadataPrefixed) -> Result<Self, ArchiveError> {
        Self::try_from(meta)
    }

    pub fn from_subxt(meta: SubxtMetadata) -> Metadata {
        Metadata {
            inner: meta
        }
    }

    /// get storage keys for all possible values of storage for one block
    pub fn keys(&self, keys: Vec<StorageKey>) -> Vec<StorageKey> {
        let mut other_keys = Vec::new();
        for module in self.inner.modules() {
            trace!("MODULE: {:?}", module.name());
            for (call, _) in module.calls() {
                trace!("CALL: {:?}", call);
                trace!("Combined: {}", format!("{} {}", module.name(), call.as_str()));
                other_keys.push(
                    twox_128(format!("{} {}", module.name(), call.as_str()).as_bytes()).to_vec()
                )
            }
        }

        keys.into_iter().filter_map(|k| {
            other_keys.iter().find(|other| other == &&k.0)
                .map(|k| StorageKey(k.to_vec()))
        }).collect::<Vec<StorageKey>>()
    }
}

impl TryFrom<RuntimeMetadataPrefixed> for Metadata {
    type Error = ArchiveError;

    fn try_from(metadata: RuntimeMetadataPrefixed) -> Result<Self, Self::Error> {
        let metadata = SubxtMetadata::try_from(metadata).map_err(|e| ArchiveError::from(e))?;
        Ok(Self {
            inner: metadata
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use subxt_metadata::ModuleMetadata;
    use rand::Rng;
    /*
    fn create_test_modules() -> Vec<ModuleMetadata> {
        let mut calls = Vec::new();
        for call in 0..8 {
            calls.push(rand::thread_rng().gen::<[u8; 32]>());
        }
        let mut test_modules = Vec::new();

        // each module has 4 calls
        for call in calls.chunks(2) {
            let calls = HashMap::new();
            calls.insert("test_call_0", call[0]);
            calls.insert("test_call_1", call[1]);

            test_modules.push(
                ModuleMetadata {
                    index: 0,
                    name: "TestModule",
                    storage: HashMap::new(),
                    calls,
                    events: HashMap::new()
                }
            )
        }
        test_modules
    }

    fn create_test_metadata() -> SubxtMetadata {
        let modules = HashMap::new();

        for m in 0..8 {
            let test_modules = create_test_modules();
            modules.push("Test", test_modules);
        }
        SubxtMetadata {
            modules,
            modules_by_event_index: HashMap::new() // this is not tested
        }
    }

    #[test]
    fn should_find_matching_keys() {
        let test_metadata = create_test_metadata();
        let mut some_key;
        for m in test_metadata.modules() {
            for c in  m.calls() {
                some_key = StorageKey(twox_128(format!("{} {}", m, c).as_bytes()).to_vec());
                break;
            }
            break;
        }
        let meta = Metadata::from_subxt(test_metadata);
        meta.keys(vec![some_key]).unwrap()
    }
    */
}
