// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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
use substrate_archive::{tasks::BlockExecutor, database::BlockModelDecoder};
use test_common::*;
use std::sync::Arc;
use sp_api::ProvideRuntimeApi;
use polkadot_service::Block;
use criterion::{black_box, criterion_group, criterion_main, Criterion};


// NOTE: Requires a Polkadot database synced up to block 3 Million plus 10,000
// Run these tests in release mode! Very very slow in debug mode.
fn bench_block_execution(c: &mut Criterion) {
    pretty_env_logger::try_init();
    let (client, db) = get_dot_runtime_api(8, 1024).unwrap();
    let client = Arc::new(client);
    let mut blocks = get_blocks().unwrap();
    let blocks = BlockModelDecoder::<Block>::with_vec(blocks.drain(0..100).collect()).unwrap();
    let block = blocks[0].clone();
    let db = db.clone();
    c.bench_function("execute 3_000_000", |b| {
        b.iter(||{
            let api = client.runtime_api();
            BlockExecutor::new(api, &db, block.inner.block.clone()).block_into_storage().unwrap();
        })
    });
}


criterion_group!(benches, bench_block_execution);
criterion_main!(benches);