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

use std::io;
use thiserror::Error;

pub(crate) type Result<T, E = BackendError> = std::result::Result<T, E>;

/// Substrate Archive Error Enum
#[derive(Error, Debug)]
pub enum BackendError {
	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	Codec(#[from] codec::Error),
	#[error("Blockchain error: {0}")]
	Blockchain(String),
	#[error("Wasm exists but could not extract runtime version")]
	WasmExecutionError,
	#[error("Version not found")]
	VersionNotFound,
	#[error("Storage does not exist")]
	StorageNotExist,
	#[error("Unexpected Error: {0}")]
	Msg(String),
}

// this conversion is required for our Error type to be
// Send + Sync
impl From<sp_blockchain::Error> for BackendError {
	fn from(e: sp_blockchain::Error) -> Self {
		Self::Blockchain(e.to_string())
	}
}

impl From<&str> for BackendError {
	fn from(e: &str) -> Self {
		Self::Msg(e.to_string())
	}
}

impl From<String> for BackendError {
	fn from(e: String) -> Self {
		Self::Msg(e)
	}
}
