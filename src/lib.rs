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

#[macro_use] extern crate diesel;
mod types;
mod error;
mod archive;
mod database;
mod queries;
mod util;
mod extrinsics;
#[cfg(test)]
mod tests;
mod srml_ext;
mod metadata;


pub use archive::Archive;
pub use extrinsics::{RawExtrinsic, OldExtrinsic};
pub use types::{System, Module, ExtractCall, ToDatabaseExtrinsic};
pub use srml_ext::{SrmlExt, NotHandled};
pub use error::Error;

pub mod rpc;
pub mod paint {
    pub use paint_system;
    pub use paint_sudo;
}
pub use util::init_logger;
