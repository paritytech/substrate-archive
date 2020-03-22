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

use diesel::{r2d2::ConnectionManager, Connection};
use r2d2::{Pool, PooledConnection};
use tokio::task;
// use tokio_threadpool::{blocking, BlockingError};
// TODO after merge, before push: https://docs.rs/tokio/0.2.1/tokio/task/fn.spawn_blocking.html
use crate::error::Error as ArchiveError;
/// Allows for creating asyncronous database requests
#[derive(Debug)]
pub struct AsyncDiesel<T: Connection + 'static> {
    pool: Pool<ConnectionManager<T>>,
}

impl<T> Clone for AsyncDiesel<T>
where
    T: Connection + 'static,
{
    fn clone(&self) -> AsyncDiesel<T> {
        AsyncDiesel {
            pool: self.pool.clone(), // clones the underlying Arc<>
        }
    }
}

// TODO: " 'static " should be removable once async/await is released nov 7
impl<T> AsyncDiesel<T>
where
    T: Connection + 'static,
{
    /// Create a new instance of an asyncronous diesel
    pub fn new(db_url: &str) -> Result<Self, ArchiveError> {
        Self::new_pool(db_url, r2d2::Builder::default())
    }

    /// create a new instance of asyncronous diesel, using custom ConnectionManager
    pub fn new_pool(
        db_url: &str,
        builder: r2d2::Builder<ConnectionManager<T>>,
    ) -> Result<Self, ArchiveError> {
        let manager = ConnectionManager::new(db_url);
        let pool = builder.build(manager)?;
        Ok(AsyncDiesel { pool })
    }

    /// Run a database operation asyncronously
    /// The closure is supplied with a 'ConnectionManager instance' for connecting to the DB
    pub async fn run<F, R, E>(&self, fun: F) -> Result<R, E>
    where
        F: FnOnce(PooledConnection<ConnectionManager<T>>) -> Result<R, E>
            + Send
            // + std::marker::Unpin
            + 'static,
        R: Send + 'static,
        T: Send + 'static,
        E: From<r2d2::Error> + From<task::JoinError> + Send + 'static,
    {
        // TODO Remove unwrap()
        let pool = self.pool.clone();
        let mut fun = Some(fun);
        task::spawn_blocking(move || (fun.take().expect("Made it some; qed"))(pool.get()?)).await?
    }
}

#[cfg(test)]
mod tests {
    //! Must be conected to a database
    use super::*;
}
