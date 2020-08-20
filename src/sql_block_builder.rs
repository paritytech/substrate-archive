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

use crate::{error::Error, types};
use codec::{Decode, Encode};
use sp_runtime::{
    generic::SignedBlock,
    traits::{Block as BlockT, DigestFor, Header as HeaderT},
};
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

pub struct BlockBuilder<B: BlockT> {
    _marker: PhantomData<B>,
}

impl<'a, B: BlockT> BlockBuilder<B> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    /// With a vector of SqlBlocks
    pub fn with_vec(&self, blocks: Vec<SqlBlock>) -> Result<Vec<types::Block<B>>, Error> {
        blocks
            .into_iter()
            .map(|b| {
                let (b, s) = self.with_single(b)?;
                let b = SignedBlock {
                    block: b,
                    justification: None,
                };
                Ok(types::Block::new(b, s))
            })
            .collect()
    }

    pub fn with_single(&self, block: SqlBlock) -> Result<(B, u32), Error> {
        let digest: DigestFor<B> = Decode::decode(&mut block.digest.as_slice())?;
        let (parent_hash, state_root, extrinsics_root) = Self::into_generic(
            block.parent_hash.as_slice(),
            block.state_root.as_slice(),
            block.extrinsics_root.as_slice(),
        )?;
        let num: <B::Header as HeaderT>::Number =
            Decode::decode(&mut (block.block_num as u32).encode().as_slice())?;

        let header =
            <B::Header as HeaderT>::new(num, extrinsics_root, state_root, parent_hash, digest);
        let ext: Vec<B::Extrinsic> = Decode::decode(&mut block.ext.as_slice())?;
        let spec = block.spec;
        Ok((B::new(header, ext), spec as u32))
    }

    fn into_generic(
        mut parent_hash: &[u8],
        mut state_root: &[u8],
        mut extrinsics_root: &[u8],
    ) -> Result<
        (
            <B::Header as HeaderT>::Hash,
            <B::Header as HeaderT>::Hash,
            <B::Header as HeaderT>::Hash,
        ),
        Error,
    > {
        Ok((
            Decode::decode(&mut parent_hash)?,
            Decode::decode(&mut state_root)?,
            Decode::decode(&mut extrinsics_root)?,
        ))
    }
}
/* TODO: This test need to be rewritten. We shouldn't depend on test_util
 * or rocksdb for tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::test_util;
    use crate::queries;
    use polkadot_service::Block;
    use sp_runtime::generic::BlockId;
    use sqlx::PgPool;

    pub const DB_STR: &str = "/home/insipx/.local/share/polkadot/chains/ksmcc3/db";

    #[test]
    #[ignore]
    fn block_should_be_identical() {
        let url = std::env::var("DATABASE_URL").unwrap();
        let pool = futures::executor::block_on(PgPool::builder().max_size(1).build(&url)).unwrap();
        let backend = test_util::backend(DB_STR);
        let block = backend.block(&BlockId::Number(500)).unwrap();

        let sql_block = futures::executor::block_on(queries::get_full_block(&pool, 500)).unwrap();
        let full_sql_block = BlockBuilder::<Block>::new().with_single(sql_block).unwrap();
        assert_eq!(block.block, full_sql_block);
    }
}
*/
