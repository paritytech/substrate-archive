// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher as _},
	sync::Arc,
};

use arc_swap::ArcSwap;
use codec::Decode;
use hashbrown::HashMap;

use sc_executor::{WasmExecutionMethod, WasmExecutor};
use sp_core::traits::ReadRuntimeVersion;
use sp_runtime::{
	generic::SignedBlock,
	traits::{Block as BlockT, Header as _, NumberFor},
};
use sp_state_machine::BasicExternalities;
use sp_storage::well_known_keys;
use sp_version::RuntimeVersion;
use sp_wasm_interface::HostFunctions;

use crate::{
	database::ReadOnlyDb,
	error::{BackendError, Result},
	read_only_backend::ReadOnlyBackend,
};

pub struct RuntimeVersionCache<B: BlockT, D: ReadOnlyDb> {
	/// Hash of the WASM Blob -> RuntimeVersion
	versions: ArcSwap<HashMap<u64, RuntimeVersion>>,
	backend: Arc<ReadOnlyBackend<B, D>>,
	exec: WasmExecutor,
}

impl<B: BlockT, D: ReadOnlyDb + 'static> RuntimeVersionCache<B, D> {
	pub fn new(backend: Arc<ReadOnlyBackend<B, D>>) -> Self {
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
			.filter(|f| f.name().matches("wasm_tracing").count() == 0)
			.filter(|f| f.name().matches("ext_offchain").count() == 0)
			.filter(|f| f.name().matches("ext_storage").count() == 0)
			.filter(|f| f.name().matches("ext_default_child_storage").count() == 0)
			.filter(|f| f.name().matches("ext_logging").count() == 0)
			.collect::<Vec<_>>();

		// TODO: https://github.com/paritytech/substrate-archive/issues/247
		let exec = WasmExecutor::new(WasmExecutionMethod::Interpreted, Some(128), funs, 1, None);
		Self { versions: ArcSwap::from_pointee(HashMap::new()), backend, exec }
	}

	/// Get a version of the runtime for some Block Hash
	/// Prefer `find_versions` when trying to get the runtime versions for
	/// many consecutive blocks
	pub fn get(&self, hash: B::Hash) -> Result<Option<RuntimeVersion>> {
		// Getting code from the backend is the slowest part of this. Takes an average of 6ms
		let code = self.backend.storage(hash, well_known_keys::CODE).ok_or(BackendError::StorageNotExist)?;

		let code_hash = make_hash(&code);
		if self.versions.load().contains_key(&code_hash) {
			Ok(self.versions.load().get(&code_hash).cloned())
		} else {
			log::debug!("Adding new runtime code hash to cache: {:#X?}", code_hash);
			let mut ext = BasicExternalities::default();
			ext.register_extension(sp_core::traits::ReadRuntimeVersionExt::new(self.exec.clone()));
			let version = decode_version(self.exec.read_runtime_version(&code, &mut ext)?.as_slice())?;
			log::debug!("Registered a new runtime version: {:?}", version);
			self.versions.rcu(|cache| {
				let mut cache = HashMap::clone(&cache);
				cache.insert(code_hash, version.clone());
				cache
			});
			Ok(Some(version))
		}
	}

	/// Recursively finds the versions of all the blocks while minimizing reads/calls to the backend.
	pub fn find_versions(&self, blocks: &[SignedBlock<B>]) -> Result<Vec<VersionRange<B>>> {
		let mut versions = Vec::with_capacity(256);
		self.find_pivot(blocks, &mut versions)?;
		Ok(versions)
	}

	/// This can be thought of as similar to a recursive Binary Search
	fn find_pivot(&self, blocks: &[SignedBlock<B>], versions: &mut Vec<VersionRange<B>>) -> Result<()> {
		if blocks.is_empty() {
			return Ok(());
		} else if blocks.len() == 1 {
			let version = self.get(blocks[0].block.hash())?.ok_or(BackendError::VersionNotFound)?;
			versions.push(VersionRange::new(&blocks[0], &blocks[0], version));
			return Ok(());
		}

		let first = self.get(blocks.first().unwrap().block.hash())?.ok_or(BackendError::VersionNotFound)?;
		let last = self.get(blocks.last().unwrap().block.hash())?.ok_or(BackendError::VersionNotFound)?;

		if first.spec_version != last.spec_version && blocks.len() > 2 {
			let half = blocks.len() / 2;
			let (first_half, last_half) = (&blocks[0..half], &blocks[half..blocks.len()]);
			self.find_pivot(first_half, versions)?;
			self.find_pivot(last_half, versions)?;
		} else if (first.spec_version != last.spec_version) && (blocks.len() == 2) {
			versions.push(VersionRange::new(&blocks[0], &blocks[0], first));
			versions.push(VersionRange::new(&blocks[1], &blocks[1], last));
		} else {
			versions.push(VersionRange::new(blocks.first().unwrap(), blocks.last().unwrap(), first));
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
		Self { start: *first.block.header().number(), end: *last.block.header().number(), version }
	}

	pub fn contains_block(&self, b: &NumberFor<B>) -> bool {
		(self.start..=self.end).contains(b)
	}
}

fn decode_version(version: &[u8]) -> Result<sp_version::RuntimeVersion> {
	let v: RuntimeVersion = sp_api::OldRuntimeVersion::decode(&mut &*version)?.into();
	let core_api_id = sp_core::hashing::blake2_64(b"Core");
	if v.has_api_with(&core_api_id, |v| v >= 3) {
		sp_api::RuntimeVersion::decode(&mut &*version).map_err(Into::into)
	} else {
		Ok(v)
	}
}

// Make a hash out of a byte string using the default hasher.
fn make_hash<K: Hash + ?Sized>(val: &K) -> u64 {
	let mut state = DefaultHasher::new();
	val.hash(&mut state);
	state.finish()
}
