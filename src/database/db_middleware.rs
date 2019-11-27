/*
 * Copyright (c) 2017-2019 Parity Technologies (UK) Ltd.
 * This file is part of substrate-archive
 *
 *  This file is free software: you may copy, redistribute and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation, either version 3 of the License, or (at your
 *  option) any later version.
 *
 *  This file is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * This file incorporates work covered by the following copyright and
 * permission notice:
 *
 *     Copyright (c) 2017, MIT, The Gotham Project Developers. <https://github.com/gotham-rs/gotham>,
 *     <https://github.com/gotham-rs/gotham/blob/master/middleware/diesel/src/repo.rs>
 *
 *     Permission to use, copy, modify, and/or distribute this software
 *     for any purpose with or without fee is hereby granted, provided
 *     that the above copyright notice and this permission notice appear
 *     in all copies.
 *
 *     THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
 *     WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 *     WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
 *     AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
 *     CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
 *     OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 *     NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 *     CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use diesel::{r2d2::ConnectionManager, Connection};
use futures::{
    future::Future,
    task::{Context, Poll},
};
use futures01::Async;
use r2d2::{Pool, PooledConnection};
use tokio_threadpool::{blocking, BlockingError};

use core::pin::Pin;

use crate::error::Error as ArchiveError;

struct DatabaseFuture<T, F, R, E>
where
    T: Connection + 'static,
    F: FnOnce(PooledConnection<ConnectionManager<T>>) -> Result<R, E>,
{
    fun: Option<F>,
    pool: Pool<ConnectionManager<T>>,
}

impl<T, F, R, E> DatabaseFuture<T, F, R, E>
where
    T: Connection + 'static,
    F: FnOnce(PooledConnection<ConnectionManager<T>>) -> Result<R, E>,
{
    fn new(fun: F, pool: Pool<ConnectionManager<T>>) -> Self {
        Self {
            fun: Some(fun),
            pool,
        }
    }
}

// https://stackoverflow.com/questions/56058494/when-is-it-safe-to-move-a-member-value-out-of-a-pinned-future
impl<T, F, R, E> Future for DatabaseFuture<T, F, R, E>
where
    T: Connection + 'static,
    F: FnOnce(PooledConnection<ConnectionManager<T>>) -> Result<R, E>
        + Send
        + std::marker::Unpin
        + 'static,
    E: From<BlockingError> + From<r2d2::Error>,
{
    type Output = Result<R, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let pool = self.pool.get()?;

        // need to take() to avoid shared-reference borrowing constraints
        // this is safe because we do not meddle with self-referential references

        match blocking(|| {
            (self
                .fun
                .take()
                .expect("Only way to create database future is to instantiate with a concrete Fn"))(
                pool,
            )
        }) {
            Ok(Async::Ready(t)) => Poll::Ready(t),
            Ok(Async::NotReady) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }
}

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
    T: Connection,
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
            + 'static
            + std::marker::Unpin,
        T: Send,
        E: From<BlockingError> + From<r2d2::Error>,
    {
        DatabaseFuture::new(fun, self.pool.clone()).await
    }
}

#[cfg(test)]
mod tests {
    //! Must be conected to a database
    use super::*;
}
