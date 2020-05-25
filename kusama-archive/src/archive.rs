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

use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use substrate_archive::{Archive, backend::{self, ChainAccess}, chain_traits::HeaderBackend as _};
use std::sync::Arc;
use anyhow::Result;


pub fn run_archive(config: super::config::Config) -> Result<(Arc<impl ChainAccess<Block>>, Archive)> 
{
    let db = backend::open_database::<Block>(config.db_path(), 8192).unwrap();

    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let client =
        backend::client::<Block, ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor, _>(
            db, spec,
        )
        .unwrap();

    let info = client.info();
    println!("{:?}", info);

   
    // TODO: use a better error-handling (this-error) crate with substrate-archive
    // (failure is deprecated)
    // run until we want to exit (Ctrl-C)
    let archive = Archive::init::<ksm_runtime::Runtime, _>(
        client.clone(),
        "ws://127.0.0.1:9944".to_string(),
        config.keys(),
        config.psql_url()
    ).expect("Init Failed");     
    

    Ok((
        client, archive
    ))
}
