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

//! Re-Build Substrate Blocks from a Postgres Database
//! Rather than fetching many blocks from RocksDB by number,
//! this is a (much) faster alternative

use crate::error::Error as ArchiveError;
use codec::{Decode, Encode};
use sp_runtime::traits::{Block as BlockT, DigestFor, Header as HeaderT};
use std::marker::PhantomData;

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct SqlBlock {
    parent_hash: Vec<u8>,
    hash: Vec<u8>,
    block_num: i32,
    state_root: Vec<u8>,
    extrinsics_root: Vec<u8>,
    digest: Vec<u8>,
    ext: Vec<u8>,
    spec: i32,
}

pub struct BlockBuilder<Block: BlockT> {
    pool: sqlx::PgPool,
    _marker: PhantomData<Block>,
}

impl<'a, Block: BlockT> BlockBuilder<Block> {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            _marker: PhantomData,
        }
    }

    /// With a vector of SqlBlocks
    pub fn with_vec(&self, blocks: Vec<SqlBlock>) -> Result<Vec<Block>, ArchiveError> {
        blocks
            .into_iter()
            .map(|b| self.with_single(b))
            .collect::<Result<Vec<Block>, ArchiveError>>()
    }

    pub fn with_single(&self, block: SqlBlock) -> Result<Block, ArchiveError> {
        let digest: DigestFor<Block> = Decode::decode(&mut block.digest.as_slice())?;
        let (parent_hash, state_root, extrinsics_root) = Self::into_generic(
            block.parent_hash.as_slice(),
            block.state_root.as_slice(),
            block.extrinsics_root.as_slice(),
        )?;
        let num: <Block::Header as HeaderT>::Number =
            Decode::decode(&mut (block.block_num as u32).encode().as_slice())?;

        let header =
            <Block::Header as HeaderT>::new(num, extrinsics_root, state_root, parent_hash, digest);
        let ext: Vec<Block::Extrinsic> = Decode::decode(&mut block.ext.as_slice())?;
        Ok(Block::new(header, ext))
    }

    fn into_generic(
        parent_hash: &[u8],
        state_root: &[u8],
        extrinsics_root: &[u8],
    ) -> Result<
        (
            <Block::Header as HeaderT>::Hash,
            <Block::Header as HeaderT>::Hash,
            <Block::Header as HeaderT>::Hash,
        ),
        ArchiveError,
    > {
        let parent_hash = parent_hash.encode();
        let state_root = state_root.encode();
        let extrinsics_root = extrinsics_root.encode();

        Ok((
            Decode::decode(&mut parent_hash.as_slice())?,
            Decode::decode(&mut state_root.as_slice())?,
            Decode::decode(&mut extrinsics_root.as_slice())?,
        ))
    }
}
