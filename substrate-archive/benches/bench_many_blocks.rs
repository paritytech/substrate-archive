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
use codec::Encode;
use criterion::{BenchmarkId, Throughput, black_box, criterion_group, criterion_main, Criterion};
use substrate_archive::chain_traits::Block as _;


// NOTE: Requires a Polkadot database synced up to block 3 Million plus 10,000
// Run these tests in release mode! Very very slow in debug mode.
fn bench_block_execution(c: &mut Criterion) {
    pretty_env_logger::try_init();
    let (client, db) = get_dot_runtime_api(8, 1024).unwrap();
    let client = Arc::new(client);
    let mut blocks = get_blocks().unwrap();
    let blocks = BlockModelDecoder::<Block>::with_vec(blocks.drain(0..10).collect()).unwrap();
    let db = db.clone();
    let mut group = c.benchmark_group("many blocks bench");
    for block in blocks.into_iter() {
        let id = block.inner.block.header().number;
        group.throughput(Throughput::Elements(id as u64));
        group.bench_with_input(BenchmarkId::from_parameter(id), &block, |b, block| {
            let client = client.clone();
            b.iter(|| {
                let api = client.runtime_api();
                BlockExecutor::new(api, &db, block.inner.block.clone()).block_into_storage().unwrap();
            })
        });
    }
    group.finish();
}


criterion_group!(benches, bench_block_execution);
criterion_main!(benches);