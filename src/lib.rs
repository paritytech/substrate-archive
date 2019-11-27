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

#[macro_use]
extern crate diesel;
mod archive;
mod database;
mod error;
mod extrinsics;
mod metadata;
mod queries;
mod srml_ext;
#[cfg(test)]
mod tests;
mod types;
mod util;

pub use archive::Archive;

pub use error::Error;
pub use srml_ext::{NotHandled, SrmlExt};
pub use types::{ExtractCall, Module, System};

pub mod rpc;
pub mod srml {
    pub use srml_finality_tracker::Call as FinalityCall;
    pub use srml_im_online::Call as ImOnlineCall;
    pub use srml_system;
    pub use srml_timestamp::Call as TimestampCall;
}
