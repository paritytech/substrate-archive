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

//! A method of dynamic queries with SQLx
//! Taken from this Gist by @mehcode (Github): https://gist.github.com/mehcode/c476922be0290a4f8502d18701cc8c74
//! This is sortof temporary until SQLx develops their dynamic query builder: https://github.com/launchbadge/sqlx/issues/291
//! and `Quaint` switches to SQLx as a backend: https://github.com/prisma/quaint/issues/138
use crate::error::Result;
use sqlx::prelude::*;
use sqlx::{
    encode::Encode,
    postgres::{PgArguments, PgConnection, Postgres},
    Arguments,
};

const CHUNK_MAX: usize = 30_000;

pub struct Chunk {
    query: String,
    pub arguments: PgArguments,

    // FIXME: Would be nice if PgArguments exposed the # of args as `.len()`
    pub args_len: usize,
}

pub struct Batch {
    #[allow(unused)]
    name: &'static str,
    leading: String,
    trailing: String,
    with: Option<Box<dyn Fn(&mut Chunk) -> Result<()> + Send>>,
    chunks: Vec<Chunk>,
    index: usize,
    len: usize,
}

impl Batch {
    pub fn new(name: &'static str, leading: &str, trailing: &str) -> Self {
        Self {
            name,
            leading: leading.to_owned(),
            trailing: trailing.to_owned(),
            chunks: vec![Chunk::new(leading)],
            with: None,
            index: 0,
            len: 0,
        }
    }

    #[allow(unused)]
    pub fn new_with(
        name: &'static str,
        leading: &str,
        trailing: &str,
        with: impl Fn(&mut Chunk) -> Result<()> + Send + 'static,
    ) -> Result<Self> {
        let mut chunk = Chunk::new(leading);
        with(&mut chunk)?;

        Ok(Self {
            name,
            leading: leading.to_owned(),
            trailing: trailing.to_owned(),
            with: Some(Box::new(with)),
            chunks: vec![chunk],
            index: 0,
            len: 0,
        })
    }

    // ensure there is enough room for N more arguments
    pub fn reserve(&mut self, arguments: usize) -> Result<()> {
        self.len += 1;

        if self.chunks[self.index].args_len + arguments > CHUNK_MAX {
            let mut chunk = Chunk::new(&self.leading);

            if let Some(with) = &self.with {
                with(&mut chunk)?;
            }

            self.chunks.push(chunk);
            self.index += 1;
        }

        Ok(())
    }

    pub fn append(&mut self, sql: &str) {
        self.chunks[self.index].append(sql);
    }

    pub fn bind<'a, T: 'a>(&mut self, value: T) -> Result<()>
    where
        T: Encode<'a, Postgres> + Type<Postgres> + Send,
    {
        self.chunks[self.index].bind(value)
    }

    pub async fn execute(self, conn: &mut PgConnection) -> Result<u64> {
        let mut rows_affected = 0;
        if self.len > 0 {
            for mut chunk in self.chunks {
                chunk.append(&self.trailing);
                let done = chunk.execute(conn).await?;
                rows_affected += done;
            }
        }

        Ok(rows_affected)
    }

    // TODO: Better name?
    pub fn current_num_arguments(&self) -> usize {
        self.chunks[self.index].args_len
    }
}

impl Chunk {
    fn new(sql: &str) -> Self {
        let mut query = String::with_capacity(1024 * 8);
        query.push_str(sql);

        Self {
            query,
            arguments: PgArguments::default(),
            args_len: 0,
        }
    }

    pub fn append(&mut self, sql: &str) {
        self.query.push_str(sql);
    }

    pub fn bind<'a, T: 'a>(&mut self, value: T) -> Result<()>
    where
        T: Encode<'a, Postgres> + Type<Postgres> + Send,
    {
        self.arguments.add(value);
        self.query.push('$');
        itoa::fmt(&mut self.query, self.args_len + 1)?;
        self.args_len += 1;

        Ok(())
    }

    async fn execute(self, conn: &mut PgConnection) -> Result<u64> {
        let done = sqlx::query_with(&*self.query, self.arguments.into_arguments())
            .execute(conn)
            .await?;
        Ok(done.rows_affected())
    }
}
