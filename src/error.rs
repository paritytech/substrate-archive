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

use std::{env, io};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

/// Substrate Archive Error Enum
#[derive(Error, Debug)]
pub enum Error {
    #[error("Io Error")]
    Io(#[from] io::Error),
    #[error("environment variable for `DATABASE_URL` not found")]
    Env(#[from] env::VarError),
    #[error("decode {0}")]
    Codec(#[from] codec::Error),
    #[error("Formatting {0}")]
    Fmt(#[from] std::fmt::Error),
    #[error("serialization error")]
    Serialization(#[from] serde_json::Error),
    #[error("sqlx error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("blockchain error: {0}")]
    Blockchain(String),
    /// an error occurred while enqueuing a background job
    #[error("Background job err {0}")]
    BgJob(#[from] coil::EnqueueError),
    #[error("Background Job {0}")]
    BgJobGen(#[from] coil::Error),
    #[error("Failed getting background task {0}")]
    BgJobGet(#[from] coil::FetchError),
    #[error("could not build threadpool")]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
    /// Error occured while serializing/deserializing data
    #[error("Error while decoding job data {0}")]
    De(#[from] rmp_serde::decode::Error),
    #[error(
        "the chain given to substrate-archive is different then the running chain. Trying to run {0}, running {1}"
    )]
    MismatchedChains(String, String),
    #[error("wasm exists but could not extract runtime version")]
    WasmExecutionError,
    #[error("sending on disconnected channel")]
    Channel,
    #[error("Trying to send to disconnected actor")]
    Disconnected,
    #[error("Unexpected Error {0}")]
    General(String),

    #[cfg(test)]
    #[error("{0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),
}

impl From<&str> for Error {
    fn from(e: &str) -> Error {
        Error::General(e.to_string())
    }
}

impl From<String> for Error {
    fn from(e: String) -> Error {
        Error::General(e)
    }
}

// this conversion is required for our Error type to be
// Send + Sync
impl From<sp_blockchain::Error> for Error {
    fn from(e: sp_blockchain::Error) -> Error {
        Error::Blockchain(e.to_string())
    }
}

impl From<xtra::Disconnected> for Error {
    fn from(_: xtra::Disconnected) -> Error {
        Error::Disconnected
    }
}

impl<T> From<flume::SendError<T>> for Error {
    fn from(_: flume::SendError<T>) -> Error {
        Error::Channel
    }
}
