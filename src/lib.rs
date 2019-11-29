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

#![feature(async_closure)]

#[macro_use]
extern crate diesel;
mod archive;
mod database;
mod error;
mod extrinsics;
mod frame_ext;
mod metadata;
mod queries;
#[cfg(test)]
mod tests;
mod types;
mod util;

pub use archive::Archive;
pub use error::Error;
pub use extrinsics::{OldExtrinsic, RawExtrinsic};
pub use frame_ext::{FrameExt, NotHandled};
pub use types::{ExtractCall, Module, System, ToDatabaseExtrinsic};

pub mod rpc;
pub mod frame {
    pub use frame_system;
    pub use pallet_sudo;
}
pub use util::init_logger;
