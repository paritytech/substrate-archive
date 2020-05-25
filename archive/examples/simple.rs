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
use substrate_archive::{backend, Archive, chain_traits::{HeaderBackend as _}, twox_128, StorageKey};

pub fn main() {
    let stdout = match option_env!("RUST_LOG").map(|o| o.to_lowercase()).as_deref() {
        Some("error") => log::LevelFilter::Error,
        Some("warn") => log::LevelFilter::Warn,
        Some("info") => log::LevelFilter::Info,
        Some("debug") => log::LevelFilter::Debug,
        Some("trace") => log::LevelFilter::Trace,
        _ => log::LevelFilter::Error,
    };
    substrate_archive::init_logger(stdout, log::LevelFilter::Info);

    let path = std::path::PathBuf::from("/home/insipx/.local/share/polkadot/chains/ksmcc4/db");
    let db =
        backend::open_database::<Block>(path.as_path(), 8192).unwrap();

    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let client =
        backend::client::<Block, ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor, _>(
            db, spec,
        )
        .unwrap();

    let info = client.info();
    println!("{:?}", info);

    // create the keys we want to query storage for
    let system_key = twox_128(b"System").to_vec();
    let events_key = twox_128(b"Events").to_vec();
    let accounts_key = twox_128(b"Account").to_vec();

    let democracy_key = twox_128(b"Democracy").to_vec();
    let public_proposal_count = twox_128(b"PublicPropCount").to_vec();
    let public_proposals = twox_128(b"PublicProps").to_vec();

    let mut keys = Vec::new();

    let mut system_accounts = system_key.clone();
    system_accounts.extend(accounts_key);
    let mut system_events = system_key.clone();
    system_events.extend(events_key);
    let mut democracy_public_proposal_count = democracy_key.clone();
    democracy_public_proposal_count.extend(public_proposal_count);
    let mut democracy_proposals = democracy_key.clone();
    democracy_proposals.extend(public_proposals);

    keys.push(StorageKey(system_accounts));
    keys.push(StorageKey(system_events));
    keys.push(StorageKey(democracy_public_proposal_count));
    keys.push(StorageKey(democracy_proposals));

    // run until we want to exit (Ctrl-C)
    let archive = Archive::init::<ksm_runtime::Runtime, _>(
        client,
        "ws://127.0.0.1:9944".to_string(),
        keys.as_slice(),
        None
    ).unwrap();

    Archive::block_until_stopped();
}
