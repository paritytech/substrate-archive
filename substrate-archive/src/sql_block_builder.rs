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

//! Re-Build Substrate Blocks from a Postgres Database
//! Rather than fetching many blocks from RocksDB by number,
//! this is a (much) faster alternative

use std::marker::PhantomData;

use codec::{Decode, Encode, Error as DecodeError};
use sp_runtime::{
	generic::SignedBlock,
	traits::{Block as BlockT, Header as HeaderT},
};

use substrate_archive_common::{models::BlockModel, types};

pub struct SqlBlockBuilder<B: BlockT> {
	_marker: PhantomData<B>,
}

impl<'a, B: BlockT> SqlBlockBuilder<B> {
	/// With a vector of BlockModel
	pub fn with_vec(blocks: Vec<BlockModel>) -> Result<Vec<types::Block<B>>, DecodeError> {
		blocks
			.into_iter()
			.map(|b| {
				let (block, spec) = Self::with_single(b)?;
				let block = SignedBlock { block, justification: None };
				Ok(types::Block::new(block, spec))
			})
			.collect()
	}

	/// With a single BlockModel
	pub fn with_single(block: BlockModel) -> Result<(B, u32), DecodeError> {
		let BlockDecoder { header, ext, spec } = BlockDecoder::<B>::decode(block)?;
		let block = B::new(header, ext);
		Ok((block, spec))
	}
}

struct BlockDecoder<B: BlockT> {
	header: B::Header,
	ext: Vec<B::Extrinsic>,
	spec: u32,
}

impl<B: BlockT> BlockDecoder<B> {
	fn decode(block: BlockModel) -> Result<Self, DecodeError> {
		let block_num = Decode::decode(&mut (block.block_num as u32).encode().as_slice())?;
		let extrinsics_root = Decode::decode(&mut block.extrinsics_root.as_slice())?;
		let state_root = Decode::decode(&mut block.state_root.as_slice())?;
		let parent_hash = Decode::decode(&mut block.parent_hash.as_slice())?;
		let digest = Decode::decode(&mut block.digest.as_slice())?;
		let header = <B::Header as HeaderT>::new(block_num, extrinsics_root, state_root, parent_hash, digest);

		let ext = Decode::decode(&mut block.ext.as_slice())?;

		let spec = block.spec as u32;

		Ok(Self { header, ext, spec })
	}
}
