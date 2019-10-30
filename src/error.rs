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

use failure::Fail;
use codec::Error as CodecError;
use futures::sync::mpsc::SendError;
use jsonrpc_core_client::RpcError as JsonRpcError;
use std::io::Error as IoError;
use url::ParseError;
use diesel::result::{Error as DieselError, ConnectionError};
use std::env::VarError as EnvironmentError;
use tokio_threadpool::BlockingError;
use r2d2::Error as R2d2Error;
use std::num::TryFromIntError;
use crate::metadata::Error as MetadataError;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Could not send to parent process {}", _0)]
    Send(String),
    #[fail(display = "RPC Error: {}", _0)]
    Rpc(#[fail(cause)] JsonRpcError),
    #[fail(display = "Io: {}", _0)]
    Io(#[fail(cause)] IoError),
    #[fail(display = "Parse: {}", _0)]
    Parse(#[fail(cause)] ParseError),
    #[fail(display  = "Db: {}", _0)]
    Db(#[fail(cause)] DieselError),
    #[fail(display = "Db Connection: {}", _0)]
    DbConnection(#[fail(cause)] ConnectionError),
    #[fail(display = "Environment: {}", _0)]
    Environment(#[fail(cause)] EnvironmentError),
    #[fail(display = "Codec: {}", _0)]
    Codec(#[fail(cause)] CodecError),
    #[fail(display = "Db Pool {}", _0)]
    DbPool(#[fail(cause)] R2d2Error),
    #[fail(display = "ThreadPool {}", _0)]
    ThreadPool(#[fail(cause)] BlockingError),
    #[fail(display = "Int Conversion Error: {}", _0)]
    IntConversion(#[fail(cause)] TryFromIntError),

    #[fail(display = "Call type unhandled, not committing to database")]
    UnhandledCallType,
    // if trying to insert unsupported type into database
    // (as of this writing, anything other than a block or storage type)
    #[fail(display = "Unhandled Data type, not committing to database")]
    UnhandledDataType(String),
    #[fail(display = "{} not found, or does not exist", _0)]
    DataNotFound(String),
    #[fail(display = "Metadata {}", _0)]
    Metadata(MetadataError),
}

impl From<MetadataError> for Error {
    fn from(err: MetadataError) -> Error {
        Error::Metadata(err)
    }
}

impl From<TryFromIntError> for Error {
    fn from(err: TryFromIntError) -> Error {
        Error::IntConversion(err)
    }
}

impl From<R2d2Error> for Error {
    fn from(err: R2d2Error) -> Error {
        Error::DbPool(err)
    }
}

impl From<BlockingError> for Error {
    fn from(err: BlockingError) -> Error {
        Error::ThreadPool(err)
    }
}

impl From<CodecError> for Error {
    fn from(err: CodecError) -> Error {
        Error::Codec(err)
    }
}

impl From<EnvironmentError> for Error {
    fn from(err: EnvironmentError) -> Error {
        Error::Environment(err)
    }
}

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Error {
        Error::DbConnection(err)
    }
}

impl From<DieselError> for Error {
    fn from(err: DieselError) -> Error {
        Error::Db(err)
    }
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Error {
        Error::Io(err)
    }
}

impl<T> From<SendError<T>> for Error {
    fn from(err: SendError<T>) -> Error {
        Error::Send(err.to_string())
    }
}

impl From<JsonRpcError> for Error {
    fn from(err: JsonRpcError) -> Error {
        Error::Rpc(err)
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Error {
        Error::Parse(err)
    }
}
