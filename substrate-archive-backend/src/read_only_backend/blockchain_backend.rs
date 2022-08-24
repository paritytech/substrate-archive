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

//! Implements Blockchain Backend (and required associated traits) for ReadOnlyBackend type.

use codec::{Decode, Encode};

use sp_blockchain::{
	Backend as BlockchainBackend, BlockStatus, CachedHeaderMetadata, Error as BlockchainError, HeaderBackend,
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
	util::{self, columns, read_db},
};

type ChainResult<T> = Result<T, BlockchainError>;

#[derive(Debug, Encode, Decode)]
struct ExtrinsicHeader {
	hash: sp_core::H256,
	data: Vec<u8>,
}

/// Hash type that this backend uses for the database.
pub type DbHash = sp_core::H256;

/// An extrinsic entry in the database.
#[derive(Debug, Encode, Decode)]
enum DbExtrinsic<B: BlockT> {
	/// Extrinsic that contains indexed data.
	Indexed {
		/// Hash of the indexed part.
		hash: DbHash,
		/// Extrinsic header.
		header: Vec<u8>,
	},
	/// Complete extrinsic data.
	Full(B::Extrinsic),
}

pub(crate) struct JoinInput<'a, 'b>(&'a [u8], &'b [u8]);

pub(crate) fn join_input<'a, 'b>(i1: &'a [u8], i2: &'b [u8]) -> JoinInput<'a, 'b> {
	JoinInput(i1, i2)
}

impl<'a, 'b> codec::Input for JoinInput<'a, 'b> {
	fn remaining_len(&mut self) -> Result<Option<usize>, codec::Error> {
		Ok(Some(self.0.len() + self.1.len()))
	}

	fn read(&mut self, into: &mut [u8]) -> Result<(), codec::Error> {
		let mut read = 0;
		if self.0.len() > 0 {
			read = std::cmp::min(self.0.len(), into.len());
			self.0.read(&mut into[..read])?;
		}
		if read < into.len() {
			self.1.read(&mut into[read..])?;
		}
		Ok(())
	}
}

impl<Block: BlockT, D: ReadOnlyDb> BlockchainBackend<Block> for ReadOnlyBackend<Block, D> {
	fn body(&self, id: BlockId<Block>) -> ChainResult<Option<Vec<Block::Extrinsic>>> {
		if let Some(body) = read_db(&*self.db, columns::KEY_LOOKUP, columns::BODY, id)
			.map_err(|e| BlockchainError::Backend(e.to_string()))?
		{
			// Plain body
			match Decode::decode(&mut &body[..]) {
				Ok(body) => return Ok(Some(body)),
				Err(err) => return Err(BlockchainError::Backend(format!("Error decoding body: {}", err))),
			}
		}

		if let Some(index) = read_db(&*self.db, columns::KEY_LOOKUP, columns::BODY_INDEX, id)
			.map_err(|e| BlockchainError::Backend(e.to_string()))?
		{
			match Vec::<DbExtrinsic<Block>>::decode(&mut &index[..]) {
				Ok(index) => {
					let mut body = Vec::new();
					for ex in index {
						match ex {
							DbExtrinsic::Indexed { hash, header } => {
								match self.db.get(columns::TRANSACTION, hash.as_ref()) {
									Some(t) => {
										let mut input = join_input(header.as_ref(), t.as_ref());
										let ex = Block::Extrinsic::decode(&mut input).map_err(|err| {
											sp_blockchain::Error::Backend(format!(
												"Error decoding indexed extrinsic: {}",
												err
											))
										})?;
										body.push(ex);
									}
									None => {
										return Err(BlockchainError::Backend(format!(
											"Missing indexed transaction {:?}",
											hash
										)))
									}
								};
							}
							DbExtrinsic::Full(ex) => {
								body.push(ex);
							}
						}
					}
					return Ok(Some(body));
				}
				Err(err) => return Err(BlockchainError::Backend(format!("Error decoding body list: {}", err))),
			}
		}
		Ok(None)
	}

	fn justifications(&self, id: BlockId<Block>) -> ChainResult<Option<Justifications>> {
		match read_db(&*self.db, columns::KEY_LOOKUP, columns::JUSTIFICATIONS, id)
			.map_err(|e| BlockchainError::Backend(e.to_string()))?
		{
			Some(justifications) => match Decode::decode(&mut &justifications[..]) {
				Ok(justifications) => Ok(Some(justifications)),
				Err(err) => {
					return Err(sp_blockchain::Error::Backend(format!("Error decoding justifications: {}", err)))
				}
			},
			None => Ok(None),
		}
	}

	fn last_finalized(&self) -> ChainResult<Block::Hash> {
		Ok(util::read_meta::<Block, D>(&*self.db, columns::HEADER)?.finalized_hash)
	}

	/// Returns hashes of all blocks that are leaves of the block tree.
	/// in other words, that have no children, are chain heads.
	/// Results must be ordered best (longest, highest) chain first.
	fn leaves(&self) -> ChainResult<Vec<Block::Hash>> {
		unimplemented!()
	}

	fn displaced_leaves_after_finalizing(&self, _block_number: NumberFor<Block>) -> ChainResult<Vec<Block::Hash>> {
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

	fn block_indexed_body(&self, id: BlockId<Block>) -> ChainResult<Option<Vec<Vec<u8>>>> {
		let body = match read_db(&*self.db, columns::KEY_LOOKUP, columns::BODY_INDEX, id)
			.map_err(|e| BlockchainError::Backend(e.to_string()))?
		{
			Some(body) => body,
			None => return Ok(None),
		};
		match Vec::<DbExtrinsic<Block>>::decode(&mut &body[..]) {
			Ok(index) => {
				let mut transactions = Vec::new();
				for ex in index.into_iter() {
					if let DbExtrinsic::Indexed { hash, .. } = ex {
						match self.db.get(columns::TRANSACTION, hash.as_ref()) {
							Some(t) => transactions.push(t),
							None => {
								return Err(sp_blockchain::Error::Backend(format!(
									"Missing indexed transaction {:?}",
									hash
								)))
							}
						}
					}
				}
				Ok(Some(transactions))
			}
			Err(err) => Err(sp_blockchain::Error::Backend(format!("Error decoding body list: {}", err))),
		}
	}
}

impl<Block: BlockT, D: ReadOnlyDb> HeaderBackend<Block> for ReadOnlyBackend<Block, D> {
	fn header(&self, id: BlockId<Block>) -> ChainResult<Option<Block::Header>> {
		util::read_header::<Block, D>(&*self.db, columns::KEY_LOOKUP, columns::HEADER, id)
			.map_err(|e| BlockchainError::Backend(e.to_string()))
	}

	fn info(&self) -> Info<Block> {
		let meta = util::read_meta::<Block, D>(&*self.db, columns::HEADER).expect("Metadata could not be read");
		Info {
			best_hash: meta.best_hash,
			best_number: meta.best_number,
			genesis_hash: meta.genesis_hash,
			finalized_hash: meta.finalized_hash,
			finalized_number: meta.finalized_number,
			finalized_state: meta.finalized_state,
			number_leaves: 0,
			block_gap: meta.block_gap,
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
mod tests {
	use codec::Input;

	// Join Input is a struct/fn in [substrate](https://github.com/paritytech/substrate/blob/bab9deca26db20bfc914263e0542a7a1b0d8f174/client/db/src/utils.rs#L462)
	#[test]
	fn concat_imitates_join_input() {
		let buf1 = [1, 2, 3, 4];
		let buf2 = [5, 6, 7, 8];
		let mut test = [0, 0, 0];
		let joined = [buf1.as_ref(), buf2.as_ref()].concat();
		let mut joined = joined.as_slice();
		assert_eq!(joined.remaining_len().unwrap(), Some(8));

		joined.read(&mut test).unwrap();
		assert_eq!(test, [1, 2, 3]);
		assert_eq!(joined.remaining_len().unwrap(), Some(5));

		joined.read(&mut test).unwrap();
		assert_eq!(test, [4, 5, 6]);
		assert_eq!(joined.remaining_len().unwrap(), Some(2));

		joined.read(&mut test[0..2]).unwrap();
		assert_eq!(test, [7, 8, 6]);
		assert_eq!(joined.remaining_len().unwrap(), Some(0));
	}
}
