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

use std::{env, fmt, io, num};
use thiserror::Error;

pub type Result<T, E = ArchiveError> = std::result::Result<T, E>;

/// Substrate Archive Error Enum
#[derive(Debug, Error)]
pub enum ArchiveError {
	// Rust std io error
	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	Env(#[from] env::VarError),
	#[error(transparent)]
	Conversion(#[from] num::TryFromIntError),

	// encoding error
	#[error(transparent)]
	Codec(#[from] codec::Error),
	#[error(transparent)]
	Serialization(#[from] serde_json::Error),

	// database error
	#[error(transparent)]
	Fmt(#[from] fmt::Error),
	#[error("sqlx error: {0}")]
	Sql(#[from] sqlx::Error),
	#[error("migration error: {0}")]
	Migration(#[from] sqlx::migrate::MigrateError),

	/// background job error
	#[error("Background job err {0}")]
	BgJob(#[from] coil::EnqueueError),
	#[error("Background Job {0}")]
	BgJobGen(#[from] coil::Error),
	#[error("Failed getting background task {0}")]
	BgJobGet(#[from] coil::FetchError),
	#[error("Error while decoding job data {0}")]
	De(#[from] rmp_serde::decode::Error),

	// actor and channel error
	#[error("Trying to send to disconnected actor")]
	Disconnected,
	#[error("Sending on a disconnected channel")]
	Channel,

	#[error("{0}")]
	ConvertStorageChanges(String),

	// archive backend error
	#[error("Backend error: {0}")]
	Backend(#[from] substrate_archive_backend::BackendError),

	#[error(transparent)]
	Api(#[from] sp_api::ApiError),

	// WASM tracing error
	#[error("Tracing: {0}")]
	Trace(#[from] TracingError),

	#[error("{0}")]
	Shutdown(String),

	#[error("Rust Standard Library does not support negative durations")]
	TimestampOutOfRange,
}

#[derive(Error, Debug)]
pub enum TracingError {
	#[error("Traces for block {0} not found")]
	NoTraceForBlock(u32),
	#[error("Traces could not be accessed from within Arc")]
	NoTraceAccess,
	#[error("Parent ID for span does not exist in the tree")]
	ParentNotFound,
	#[error("Wrong Type")]
	TypeError,
}

impl From<sp_blockchain::Error> for ArchiveError {
	fn from(e: sp_blockchain::Error) -> Self {
		Self::Backend(substrate_archive_backend::BackendError::Blockchain(e.to_string()))
	}
}

impl From<xtra::Disconnected> for ArchiveError {
	fn from(_: xtra::Disconnected) -> Self {
		Self::Disconnected
	}
}

impl<T> From<flume::SendError<T>> for ArchiveError {
	fn from(_: flume::SendError<T>) -> Self {
		Self::Channel
	}
}
