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

use crate::types::*;
use crate::{
    actors::scheduler::{Algorithm, Scheduler},
    error::Error as ArchiveError,
    simple_db::SimpleDb,
};
use bastion::prelude::*;
use std::fs;

pub fn actor<T>(defer_workers: ChildrenRef) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let workers = defer_workers.clone();
            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                sched.add_worker("defer", &workers);
                match entry::<T>(&mut sched) {
                    Ok(_) => (),
                    Err(e) => log::error!("{:?}", e),
                }
                Ok(())
            }
        })
    })
}

fn entry<T>(sched: &mut Scheduler<'_>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let mut sub_archive_path = crate::util::substrate_dir();
    sub_archive_path.push("temp_storage");

    let mut storage: Vec<Storage<T>> = Vec::new();
    let archive_path = sub_archive_path.as_path();
    if sub_archive_path.exists() && sub_archive_path.is_dir() {
        for file in fs::read_dir(archive_path)? {
            log::info!("{:?}", file);
            let file = file?;
            let temp_db = SimpleDb::<Vec<Storage<T>>>::new(file.path())?;
            let s = temp_db.get()?;
            storage.extend(s.into_iter());
        }
    } else {
        log::info!("No storage needs collecting!");
        return Ok(());
    }

    fs::remove_dir_all(sub_archive_path.as_path())?;

    log::info!(
        "sending {} collected entries to deferred storage workers",
        storage.len()
    );
    sched
        .tell_next("defer", storage)
        .expect("Couldn't send message to storage workers");

    Ok(())
}
