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

//! Implements Blockchain Backend (and required associated traits) for ReadOnlyBackend type

use std::sync::Arc;

use codec::Decode;

use sp_blockchain::{
	Backend as BlockchainBackend, BlockStatus, Cache, CachedHeaderMetadata, Error as BlockchainError, HeaderBackend,
	HeaderMetadata, Info,
};
use sp_runtime::{
	generic::BlockId,
	traits::{Block as BlockT, Header as HeaderT, NumberFor},
	Justifications,
};

use crate::{
	database::ReadOnlyDb,
	read_only_backend::ReadOnlyBackend,
	util::{self, columns},
};

type ChainResult<T> = Result<T, BlockchainError>;

impl<Block: BlockT, D: ReadOnlyDb> BlockchainBackend<Block> for ReadOnlyBackend<Block, D> {
	fn body(&self, id: BlockId<Block>) -> ChainResult<Option<Vec<<Block as BlockT>::Extrinsic>>> {
		let res = util::read_db::<Block, D>(&*self.db, columns::KEY_LOOKUP, columns::BODY, id)
			.map_err(|e| BlockchainError::Backend(e.to_string()))?;

		match res {
			Some(body) => match Decode::decode(&mut &body[..]) {
				Ok(body) => Ok(Some(body)),
				Err(_) => Err(BlockchainError::Backend("Could not decode extrinsics".into())),
			},
			None => Ok(None),
		}
	}

	fn justifications(&self, id: BlockId<Block>) -> ChainResult<Option<Justifications>> {
		let res = util::read_db::<Block, D>(&*self.db, columns::KEY_LOOKUP, columns::JUSTIFICATION, id)
			.map_err(|e| BlockchainError::Backend(e.to_string()))?;

		match res {
			Some(justification) => match Decode::decode(&mut &justification[..]) {
				Ok(justification) => Ok(Some(justification)),
				Err(_) => Err(BlockchainError::JustificationDecode),
			},
			None => Ok(None),
		}
	}

	fn last_finalized(&self) -> ChainResult<Block::Hash> {
		Ok(util::read_meta::<Block, D>(&*self.db, columns::HEADER)?.finalized_hash)
	}

	// no cache for Read Only Backend (yet)
	fn cache(&self) -> Option<Arc<dyn Cache<Block>>> {
		None
	}

	/// Returns hashes of all blocks that are leaves of the block tree.
	/// in other words, that have no children, are chain heads.
	/// Results must be ordered best (longest, highest) chain first.
	fn leaves(&self) -> ChainResult<Vec<Block::Hash>> {
		unimplemented!()
	}

	/// Return hashes of all blocks that are children of the block with `parent_hash`.
	fn children(&self, _parent_hash: Block::Hash) -> ChainResult<Vec<Block::Hash>> {
		unimplemented!()
	}

	/// Get single indexed transaction by content hash. Note that this will only fetch transactions
	/// that are indexed by the runtime with `storage_index_transaction`.
	fn indexed_transaction(&self, hash: &Block::Hash) -> ChainResult<Option<Vec<u8>>> {
		Ok(self.db.get(columns::TRANSACTION, hash.as_ref()))
	}

	fn block_indexed_body(&self, _id: BlockId<Block>) -> ChainResult<Option<Vec<Vec<u8>>>> {
		unimplemented!()
	}
}

impl<Block: BlockT, D: ReadOnlyDb> HeaderBackend<Block> for ReadOnlyBackend<Block, D> {
	fn header(&self, id: BlockId<Block>) -> ChainResult<Option<Block::Header>> {
		util::read_header::<Block, D>(&*self.db, columns::KEY_LOOKUP, columns::HEADER, id)
			.map_err(|e| BlockchainError::Backend(e.to_string()))
	}

	fn info(&self) -> Info<Block> {
		// TODO: Remove expect
		let meta = util::read_meta::<Block, D>(&*self.db, columns::HEADER).expect("Metadata could not be read");
		log::warn!("Leaves are not counted on the Read Only Backend!");
		Info {
			best_hash: meta.best_hash,
			best_number: meta.best_number,
			genesis_hash: meta.genesis_hash,
			finalized_hash: meta.finalized_hash,
			finalized_number: meta.finalized_number,
			number_leaves: 0,
		}
	}

	fn status(&self, _id: BlockId<Block>) -> ChainResult<BlockStatus> {
		log::warn!("Read Only Backend does not track Block Status!");
		Ok(BlockStatus::Unknown)
	}

	fn number(&self, hash: Block::Hash) -> ChainResult<Option<<<Block as BlockT>::Header as HeaderT>::Number>> {
		Ok(self.header(BlockId::Hash(hash))?.map(|header| *header.number()))
	}

	fn hash(&self, number: NumberFor<Block>) -> ChainResult<Option<Block::Hash>> {
		Ok(self.header(BlockId::Number(number))?.map(|h| h.hash()))
	}
}

impl<Block: BlockT, D: ReadOnlyDb> HeaderMetadata<Block> for ReadOnlyBackend<Block, D> {
	type Error = BlockchainError;
	// TODO: Header Metadata isn't actually cached. We could cache it
	fn header_metadata(&self, hash: Block::Hash) -> ChainResult<CachedHeaderMetadata<Block>> {
		self.header(BlockId::hash(hash))?
			.map(|header| CachedHeaderMetadata::from(&header))
			.ok_or_else(|| BlockchainError::UnknownBlock(format!("header not found in db: {}", hash)))
	}

	fn insert_header_metadata(&self, _hash: Block::Hash, _header_metadata: CachedHeaderMetadata<Block>) {
		log::warn!("Cannot insert into a Read-Only Database");
	}

	fn remove_header_metadata(&self, _hash: Block::Hash) {
		log::warn!("Cannot remove or modify a Read-Only Database");
	}
}

#[cfg(test)]
mod tests {}
