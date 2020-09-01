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

//! IO for the PostgreSQL database connected to Substrate Archive Node
//! Handles inserting of data into the database

mod batch;
pub mod listener;
mod models;
pub mod queries;

use async_trait::async_trait;
use batch::Batch;
use codec::Encode;
use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};
use sqlx::prelude::*;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres};

pub use self::listener::*;
pub use self::models::*;

use crate::{error::Result, types::*};

pub type DbReturn = Result<u64>;
pub type DbConn = sqlx::pool::PoolConnection<Postgres>;

#[async_trait]
pub trait Insert: Send {
    async fn insert(mut self, conn: &mut DbConn) -> DbReturn
    where
        Self: Sized;
}

#[derive(Clone)]
pub struct Database {
    /// pool of database connections
    pool: PgPool,
    url: String,
}

impl Database {
    /// Connect to the database
    pub async fn new(url: String) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .min_connections(4)
            .max_connections(28)
            .idle_timeout(std::time::Duration::from_millis(3600)) // kill connections after 5 minutes of idle
            .connect(url.as_str())
            .await?;
        Ok(Self { pool, url })
    }

    /// Start the database with a pre-defined pool
    #[allow(unused)]
    pub fn with_pool(url: String, pool: PgPool) -> Self {
        Self { pool, url }
    }

    #[allow(unused)]
    pub async fn insert(&self, data: impl Insert) -> Result<u64> {
        let mut conn = self.pool.acquire().await?;
        let res = data.insert(&mut conn).await?;
        Ok(res)
    }

    pub async fn conn(&self) -> Result<DbConn> {
        self.pool.acquire().await.map_err(Into::into)
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

#[async_trait]
impl<B> Insert for Block<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
        log::info!("Inserting single block");
        log::trace!(
            "block_num = {:?}, hash = {:X?}",
            self.inner.block.header().number(),
            hex::encode(self.inner.block.header().hash().as_ref())
        );
        let query = sqlx::query(
            r#"
            INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec) VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING
        "#,
        );
        let parent_hash = self.inner.block.header().parent_hash().as_ref();
        let hash = self.inner.block.header().hash();
        let block_num: u32 = (*self.inner.block.header().number()).into();
        let state_root = self.inner.block.header().state_root().as_ref();
        let extrinsics_root = self.inner.block.header().extrinsics_root().as_ref();
        let digest = self.inner.block.header().digest().encode();
        let extrinsics = self.inner.block.extrinsics().encode();

        query
            .bind(parent_hash)
            .bind(hash.as_ref())
            .bind(block_num)
            .bind(state_root)
            .bind(extrinsics_root)
            .bind(digest.as_slice())
            .bind(extrinsics.as_slice())
            .bind(self.spec)
            .execute(conn)
            .await
            .map(|d| d.rows_affected())
            .map_err(Into::into)
    }
}

#[async_trait]
impl<B> Insert for BatchBlock<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
        let mut batch = Batch::new(
            "blocks",
            r#"
            INSERT INTO "blocks" (
                parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec
            ) VALUES
            "#,
            r#"
            ON CONFLICT DO NOTHING
            "#,
        );
        for b in self.inner.into_iter() {
            batch.reserve(8)?;
            if batch.current_num_arguments() > 0 {
                batch.append(",");
            }
            let parent_hash = b.inner.block.header().parent_hash().as_ref();
            let hash = b.inner.block.header().hash();
            let block_num: u32 = (*b.inner.block.header().number()).into();
            let state_root = b.inner.block.header().state_root().as_ref();
            let extrinsics_root = b.inner.block.header().extrinsics_root().as_ref();
            let digest = b.inner.block.header().digest().encode();
            let extrinsics = b.inner.block.extrinsics().encode();
            batch.append("(");
            batch.bind(parent_hash)?;
            batch.append(",");
            batch.bind(hash.as_ref())?;
            batch.append(",");
            batch.bind(block_num)?;
            batch.append(",");
            batch.bind(state_root)?;
            batch.append(",");
            batch.bind(extrinsics_root)?;
            batch.append(",");
            batch.bind(digest.as_slice())?;
            batch.append(",");
            batch.bind(extrinsics.as_slice())?;
            batch.append(",");
            batch.bind(b.spec)?;
            batch.append(")");
        }
        Ok(batch.execute(conn).await?)
    }
}

#[async_trait]
impl<B: BlockT> Insert for StorageModel<B> {
    async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
        log::info!("Inserting Single Storage");
        sqlx::query(
            r#"
                INSERT INTO storage (
                    block_num, hash, is_full, key, storage
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (hash, key, md5(storage)) DO UPDATE SET
                    hash = EXCLUDED.hash,
                    key = EXCLUDED.key,
                    storage = EXCLUDED.storage,
                    is_full = EXCLUDED.is_full
            "#,
        )
        .bind(self.block_num())
        .bind(self.hash().as_ref())
        .bind(self.is_full())
        .bind(self.key().0.as_slice())
        .bind(self.data().map(|d| d.0.as_slice()))
        .execute(conn)
        .await
        .map(|d| d.rows_affected())
        .map_err(Into::into)
    }
}

#[async_trait]
impl<B: BlockT> Insert for Vec<StorageModel<B>> {
    async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
        let mut batch = Batch::new(
            "storage",
            r#"
            INSERT INTO "storage" (
                block_num, hash, is_full, key, storage
            ) VALUES
            "#,
            r#"
            ON CONFLICT (hash, key, md5(storage)) DO UPDATE SET
                hash = EXCLUDED.hash,
                key = EXCLUDED.key,
                storage = EXCLUDED.storage,
                is_full = EXCLUDED.is_full
            "#,
        );

        for s in self.into_iter() {
            batch.reserve(5)?;
            if batch.current_num_arguments() > 0 {
                batch.append(",");
            }
            batch.append("(");
            batch.bind(s.block_num())?;
            batch.append(",");
            batch.bind(s.hash().as_ref())?;
            batch.append(",");
            batch.bind(s.is_full())?;
            batch.append(",");
            batch.bind(s.key().0.as_slice())?;
            batch.append(",");
            batch.bind(s.data().map(|d| d.0.as_slice()))?;
            batch.append(")");
        }
        Ok(batch.execute(conn).await?)
    }
}

#[async_trait]
impl Insert for Metadata {
    async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
        log::debug!("Inserting Metadata");
        sqlx::query(
            r#"
            INSERT INTO metadata (version, meta)
            VALUES($1, $2)
            ON CONFLICT DO NOTHING
        "#,
        )
        .bind(self.version())
        .bind(self.meta())
        .execute(conn)
        .await
        .map(|d| d.rows_affected())
        .map_err(Into::into)
    }
}

#[async_trait]
impl<K, V> Insert for FrameEntry<K, V> 
where
    K: Send + Sync + Serialize + DeserializeOwned,
    V: Send + Sync + Serialize + DeserializeOwned,
{
    async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
        let mut query = format!("INSERT INTO {} (block_num, hash, key, value)", self.table().to_string());
        query.push_str("VALUES ($1, $2, $3, $4)");
        query.push_str("ON CONFLICT DO NOTHING");
        sqlx::query(query.as_str())
         .bind(self.block_num())
         .bind(self.hash())
         .bind(sqlx::types::Json(self.key()))
         .bind(sqlx::types::Json(self.value()))
         .execute(conn)
         .await
         .map(|d| d.rows_affected())
         .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    //! Must be connected to a local database
    use super::*;
    use sqlx::types::Json;

    #[derive(Serialize, Deserialize, Clone, Default, Debug, Eq, PartialEq, sqlx::FromRow)]
    pub struct TestAccountData<Balance> {
        pub free: Balance,
        pub reserved: Balance,
        pub misc_frozen: Balance,
        pub fee_frozen: Balance
    }

    #[test]
    fn should_insert_frame_specific_entry() {
        crate::initialize();
        let _guard = crate::TestGuard::lock();
        smol::block_on(async move {
            let mut conn = crate::PG_POOL.acquire().await.unwrap();
            
            let acc = TestAccountData::<u32> {
                free: 32,
                reserved: 3200,
                misc_frozen: 320000,
                fee_frozen: 32000000
            };
            let test_data = FrameEntry::new(Frame::System, 0, crate::DUMMY_HASH.to_vec(), "SystemAccount".to_string(), Some(acc.clone()));
            test_data.insert(&mut conn).await.unwrap();

            let data = sqlx::query_as::<_, (Json<String>, Json<TestAccountData<u32>>)>("SELECT key, value FROM frame_system")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            assert_eq!(("SystemAccount".to_string(), acc), (data.0.0, data.1.0));
         });
    }
}
