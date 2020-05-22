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

//! Collects storage left over from the last run in substrate_dir() (~/.local/share/substrate_archive/temp_storage)
//! and re-starts the deferred-storage actor

use crate::{
    error::Error as ArchiveError,
    simple_db::SimpleDb,
};
use bastion::prelude::*;
use sqlx::PgConnection;
use crate::types::*;
use std::fs;


pub fn actor<T>(pool: sqlx::Pool<PgConnection>, workers: ChildrenRef) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    Bastion::children(|children| {
        children.with_exec(move |_: BastionContext| {
            let pool = pool.clone();
            let workers = workers.clone();
            async move {
                match entry::<T>(pool, workers) {
                    Ok(_) => (),
                    Err(e) => log::error!("{:?}", e)
                }
                Ok(())
            }
        })
    })
}

fn entry<T>(pool: sqlx::Pool<PgConnection>, workers: ChildrenRef) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let mut substrate_archive_path = crate::util::substrate_dir();
    substrate_archive_path.push("temp_storage");

    let mut storage: Vec<Storage<T>> = Vec::new();

    if substrate_archive_path.is_dir() {
        for file in fs::read_dir(substrate_archive_path)? {
            let file = file?;
            let temp_db = SimpleDb::<Vec<Storage<T>>>::new(file.path())?;
            let s = temp_db.get()?;
            storage.extend(s.into_iter());
        }
    } else {
        log::info!("No storage needs collecting!");
    }

    fs::remove_dir_all(substrate_archive_path.as_path())?;

    log::info!("Restarting deferred storage workers with {} entries", storage.len());
    super::defer_storage::actor::<T>(pool.clone(), workers.clone(), storage)
        .expect("Couldn't restart deferred storage workers");

    Ok(())
}
