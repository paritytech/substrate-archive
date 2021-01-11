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

//! various utilities that make interfacing with substrate easier

use std::convert::TryInto;

use codec::Decode;
use kvdb::DBValue;

use sp_runtime::{
	generic::BlockId,
	traits::{Block as BlockT, Header as HeaderT, UniqueSaturatedFrom, UniqueSaturatedInto, Zero},
};

use substrate_archive_common::{ArchiveError, ReadOnlyDB, Result};

pub type NumberIndexKey = [u8; 4];

#[allow(unused)]
pub(crate) mod columns {
	/// Metadata about chain
	pub const META: u32 = 0;
	pub const STATE: u32 = 1;
	pub const STATE_META: u32 = 2;
	/// maps hashes -> lookup keys and numbers to canon hashes
	pub const KEY_LOOKUP: u32 = 3;
	/// Part of Block
	pub const HEADER: u32 = 4;
	/// Part of Block
	pub const BODY: u32 = 5;
	/// Part of Block
	pub const JUSTIFICATION: u32 = 6;
	/// Stores the changes tries for querying changed storage of a block
	pub const CHANGES_TRIE: u32 = 7;
	pub const AUX: u32 = 8;
	/// Off Chain workers local storage
	pub const OFFCHAIN: u32 = 9;
	pub const CACHE: u32 = 10;
}

/// Keys of entries in COLUMN_META.
#[allow(unused)]
pub mod meta_keys {
	/// Type of storage (full or light).
	pub const TYPE: &[u8; 4] = b"type";
	/// Best block key.
	pub const BEST_BLOCK: &[u8; 4] = b"best";
	/// Last finalized block key.
	pub const FINALIZED_BLOCK: &[u8; 5] = b"final";
	/// Meta information prefix for list-based caches.
	pub const CACHE_META_PREFIX: &[u8; 5] = b"cache";
	/// Meta information for changes tries key.
	pub const CHANGES_TRIES_META: &[u8; 5] = b"ctrie";
	/// Genesis block hash.
	pub const GENESIS_HASH: &[u8; 3] = b"gen";
	/// Leaves prefix list key.
	pub const LEAF_PREFIX: &[u8; 4] = b"leaf";
	/// Children prefix list key.
	pub const CHILDREN_PREFIX: &[u8; 8] = b"children";
}

pub fn read_header<Block: BlockT, D: ReadOnlyDB>(
	db: &D,
	col_index: u32,
	col: u32,
	id: BlockId<Block>,
) -> Result<Option<Block::Header>> {
	match read_db(db, col_index, col, id)? {
		Some(header) => match Block::Header::decode(&mut &header[..]) {
			Ok(header) => Ok(Some(header)),
			Err(_) => Err(ArchiveError::from("Error decoding header")),
		},
		None => Ok(None),
	}
}

pub fn read_db<Block: BlockT, D: ReadOnlyDB>(
	db: &D,
	col_index: u32,
	col: u32,
	id: BlockId<Block>,
) -> Result<Option<DBValue>> {
	block_id_to_lookup_key(&*db, col_index, id).map(|key| key.and_then(|key| db.get(col, key.as_ref())))
}

pub fn block_id_to_lookup_key<Block, D>(db: &D, key_lookup_col: u32, id: BlockId<Block>) -> Result<Option<Vec<u8>>>
where
	Block: BlockT,
	sp_runtime::traits::NumberFor<Block>: UniqueSaturatedFrom<u64> + UniqueSaturatedInto<u64>,
	D: ReadOnlyDB,
{
	Ok(match id {
		BlockId::Number(n) => db.get(key_lookup_col, number_index_key(n)?.as_ref()),
		BlockId::Hash(h) => db.get(key_lookup_col, h.as_ref()),
	})
}

/// Convert block number into short lookup key (LE representation) for
// blocks that are in the canonical chain

/// In the current database schema, this kind of key is only used for
/// lookups into an index, NOT for storing header data or others
pub fn number_index_key<N: TryInto<u32>>(n: N) -> Result<NumberIndexKey> {
	let n = n.try_into().map_err(|_| ArchiveError::from("Block num cannot be converted to u32"))?;

	Ok([(n >> 24) as u8, ((n >> 16) & 0xff) as u8, ((n >> 8) & 0xff) as u8, (n & 0xff) as u8])
}

/// Database metadata.
#[derive(Debug)]
pub struct Meta<N, H> {
	/// Hash of the best known block.
	pub best_hash: H,
	/// Number of the best known block.
	pub best_number: N,
	/// Hash of the best finalized block.
	pub finalized_hash: H,
	/// Number of the best finalized block.
	pub finalized_number: N,
	/// Hash of the genesis block.
	pub genesis_hash: H,
}

/// Read meta from the database.
pub fn read_meta<Block: BlockT, D: ReadOnlyDB>(
	db: &D,
	col_header: u32,
) -> sp_blockchain::Result<Meta<<<Block as BlockT>::Header as HeaderT>::Number, Block::Hash>>
where
	Block: BlockT,
{
	let genesis_hash: Block::Hash = match read_genesis_hash(&*db)? {
		Some(genesis_hash) => genesis_hash,
		None => {
			return Ok(Meta {
				best_hash: Default::default(),
				best_number: Zero::zero(),
				finalized_hash: Default::default(),
				finalized_number: Zero::zero(),
				genesis_hash: Default::default(),
			})
		}
	};

	let load_meta_block = |desc, key| -> sp_blockchain::Result<_> {
		if let Some(Some(header)) = match db.get(columns::META, key) {
			Some(id) => db.get(col_header, &id).map(|b| Block::Header::decode(&mut &b[..]).ok()),
			None => None,
		} {
			let hash = header.hash();
			log::debug!("DB Opened blockchain db, fetched {} = {:?} ({})", desc, hash, header.number());
			Ok((hash, *header.number()))
		} else {
			Ok((genesis_hash, Zero::zero()))
		}
	};

	let (best_hash, best_number) = load_meta_block("best", meta_keys::BEST_BLOCK)?;
	let (finalized_hash, finalized_number) = load_meta_block("final", meta_keys::FINALIZED_BLOCK)?;

	Ok(Meta { best_hash, best_number, finalized_hash, finalized_number, genesis_hash })
}

/// Read genesis hash from database.
pub fn read_genesis_hash<Hash: Decode, D: ReadOnlyDB>(db: &D) -> sp_blockchain::Result<Option<Hash>> {
	match db.get(columns::META, meta_keys::GENESIS_HASH) {
		Some(h) => match Decode::decode(&mut &h[..]) {
			Ok(h) => Ok(Some(h)),
			Err(err) => Err(sp_blockchain::Error::Backend(format!("Error decoding genesis hash: {}", err))),
		},
		None => Ok(None),
	}
}
