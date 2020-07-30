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

mod aggregator;
mod database;
mod metadata;

pub use self::aggregator::Aggregator;
pub use self::database::GetState;
pub use self::metadata::Metadata;

pub use super::generators::Generator;
use super::{actor_pool::ActorPool, ActorContext};
pub use database::DatabaseActor;

/// any messages defined in the workers
pub mod msg {
    pub use super::aggregator::IncomingData;
    pub use super::database::VecStorageWrap;
}
