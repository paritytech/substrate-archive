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

//! A simple example

use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use substrate_archive::{backend, twox_128, Archive, StorageKey};
use primitive_types::H256;
use sp_runtime::generic::BlockId;
use sc_client_api::backend::StorageProvider;
use codec::Decode;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Warn, log::LevelFilter::Info);
    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();

    // Open a RocksDB Database for the node we want to index
    let path = std::path::PathBuf::from("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
    let db = backend::open_database::<Block>(path.as_path(), 8192, spec.name(), spec.id()).unwrap();

    let client =
        backend::client::<Block, ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor, _>(
            db, spec,
        )
        .unwrap();

    // create the keys we want to query storage for
    let balances_key = twox_128(b"Balances").to_vec();
    let freebalance_key = twox_128(b"FreeBalance").to_vec();
    let mut balances_freebalance = balances_key.clone();
    balances_freebalance.extend(freebalance_key);

    println!("SystemAccount {}", hex::encode(balances_freebalance.as_slice()));
    let hash = hex::decode("ae327b35880aa9d028370f063e4c4d666f6bad89800dd979ca8b9dbf064393d0").unwrap();
    let hash = H256::from_slice(hash.as_slice());

    let keys = client.storage_keys(&BlockId::Hash(hash), &StorageKey(balances_freebalance)).unwrap();
    for key in keys.iter() {
        println!("Key: {}", hex::encode(key.0.as_slice()));
    }
    let time = std::time::Instant::now();
    let val = client.storage(&BlockId::Hash(hash), &keys[0]).unwrap().unwrap();
    let elapsed = time.elapsed();
    println!("Took {} seconds, {} milli-seconds, {} nano-seconds", elapsed.as_secs(), elapsed.as_millis(), elapsed.as_nanos());
    println!("{:?}", val);
    let free_balance: u128 = Decode::decode(&mut val.0.as_slice()).unwrap();
    println!("free: {}", free_balance);
}
