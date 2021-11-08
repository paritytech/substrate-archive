// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use xtra::prelude::*;

use crate::{
	actors::workers::database::{DatabaseActor, GetState},
	database::{queries, DbConn},
};

pub struct ExtrinsicsDecoder {
	conn: DbConn,
	addr: Address<DatabaseActor>,
	max_block_load: u32
}

impl ExtrinsicsDecoder {
	pub fn new(max_block_load: u32, addr: Address<DatabaseActor>) -> Result<Self> {
		let db = addr.send(GetState::Conn).await??.conn();
		Ok(Self { conn, addr, max_block_load })
	}

	async fn crawl_missing_extrinsics() -> Result<Vec<u32>> {
		todo!()
	}

	async fn insert_missing_extrinsics() -> Result<()> {
		todo!()
	}
}

#[async_trait::async_trait]
impl Actor for ExtrinsicsDecoder {}

pub struct Index;
impl Message for Index {
	type Result = ();
}

#[async_trait::async_trait]
impl Handler<Index> for ExtrinsicsDecoder {
	async fn handle(&mut self, _: Index, ctx: &mut Context<Self>) {
		todo!()
	}
}
