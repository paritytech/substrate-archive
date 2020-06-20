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

pub type ArchiveResult<T> = Result<T, Error>;

/// Substrate Archive Error Enum
#[derive(Error, Debug)]
pub enum Error {
    #[error("Io Error")]
    Io(#[from] io::Error),
    #[error("environment variable for `DATABASE_URL` not found")]
    Env(#[from] env::VarError),
    #[error("decode")]
    Codec(#[from] codec::Error),
    #[error("serialization error")]
    Serialization(#[from] serde_json::Error),
    #[error("sqlx error")]
    Sql(#[from] sqlx::Error),
    #[error("blockchain error")]
    Blockchain(#[from] sp_blockchain::Error),
    #[error("JSONRPC request failed")]
    RpcRequest(#[from] jsonrpsee::client::RequestError),
    #[error("DNS error")]
    Dns(#[from] jsonrpsee::transport::ws::WsNewDnsError),

    #[error("sending on disconnected channel")]
    Channel,
    #[error("Unexpected Error {0}")]
    General(String),

    #[cfg(test)]
    #[error("{0}")]
    Bincode(#[from] bincode::ErrorKind),
}

impl<T> From<crossbeam::SendError<T>> for Error {
    fn from(_: crossbeam::SendError<T>) -> Error {
        Error::Channel
    }
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
