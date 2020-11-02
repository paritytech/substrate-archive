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

use sp_runtime::traits::Block as BlockT;
use substrate_archive_common::{database::ReadOnlyDB, Result};

#[async_trait::async_trait(?Send)]
pub trait Archive<B: BlockT + Unpin, D: ReadOnlyDB>
where
	B::Hash: Unpin,
{
	/// start driving the execution of the archive
	fn drive(&mut self) -> Result<()>;

	/// this method will block indefinitely
	async fn block_until_stopped(&self) -> ();

	/// shutdown the system
	fn shutdown(self) -> Result<()>;

	/// Shutdown the system when self is boxed (useful when erasing the types of the runtime)
	fn boxed_shutdown(self: Box<Self>) -> Result<()>;

	/// Get a reference to the context the actors are using
	fn context(&self) -> Result<super::actors::ActorContext<B, D>>;
}
