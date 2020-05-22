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

use bincode::ErrorKind as BincodeError;
use codec::Error as CodecError;
use failure::Fail;
use futures::channel::mpsc::TrySendError;
use jsonrpsee::client::RequestError as JsonrpseeRequest;
use jsonrpsee::transport::ws::WsNewDnsError;
use serde_json::Error as SerdeError;
use sp_blockchain::Error as BlockchainError;
use sqlx::Error as SqlError;
use std::env::VarError as EnvironmentError;
use std::io::Error as IoError;
use std::num::TryFromIntError;
use std::sync::PoisonError;

pub type ArchiveResult<T> = Result<T, Error>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Could not send to parent process {}", _0)]
    Send(String),
    #[fail(display = "Could not send message {}", _0)]
    TrySend(String),
    #[fail(display = "Task Join {}", _0)]
    Join(String),
    #[fail(display = "Io: {}", _0)]
    Io(#[fail(cause)] IoError),
    #[fail(display = "Environment: {}", _0)]
    Environment(#[fail(cause)] EnvironmentError),
    #[fail(display = "Codec: {:?}", _0)]
    Codec(#[fail(cause)] CodecError),
    #[fail(display = "Int Conversion Error: {}", _0)]
    IntConversion(#[fail(cause)] TryFromIntError),
    #[fail(display = "Serialization: {}", _0)]
    Serialize(#[fail(cause)] SerdeError),
    #[fail(display = "Sql {}", _0)]
    Sql(#[fail(cause)] SqlError),
    #[fail(display = "Blockchain {}", _0)]
    Blockchain(String),
    #[fail(display = "Invalid Block Range from {} to {}. {}", _0, _1, _2)]
    InvalidBlockRange {
        from: String,
        to: String,
        details: String,
    },
    #[fail(display = "Bincode encoding {}", _0)]
    Bincode(#[fail(cause)] Box<BincodeError>),
    #[fail(display = "Rpc Request {}", _0)]
    JsonrpseeRequest(#[fail(cause)] JsonrpseeRequest),
    #[fail(display = "Ws DNS Failure {}", _0)]
    WsDns(#[fail(cause)] WsNewDnsError),
    #[fail(display = "Concurrency Error, Mutex Poisoned!")]
    Concurrency,
    #[fail(display = "Call type unhandled, not committing to database")]
    UnhandledCallType,
    // if trying to insert unsupported type into database
    // (as of this writing, anything other than a block or storage type)
    #[fail(display = "Unhandled or Unknown Data type, not committing to database")]
    UnhandledDataType,
    #[fail(display = "{} not found, or does not exist", _0)]
    DataNotFound(String),
    #[fail(display = "{}", _0)]
    UnexpectedType(String),
    #[fail(display = "Unexpected Error Occurred: {}", _0)]
    General(String),
    // #[fail(display = "Metadata {}", _0)]
    // Metadata(MetadataError),
}

impl From<WsNewDnsError> for Error {
    fn from(err: WsNewDnsError) -> Error {
        Error::WsDns(err)
    }
}

impl From<JsonrpseeRequest> for Error {
    fn from(err: JsonrpseeRequest) -> Error {
        Error::JsonrpseeRequest(err)
    }
}

impl From<Box<BincodeError>> for Error {
    fn from(err: Box<BincodeError>) -> Error {
        Error::Bincode(err)
    }
}

impl From<BlockchainError> for Error {
    fn from(err: BlockchainError) -> Error {
        Error::Blockchain(format!("{:?}", err))
    }
}

impl From<SqlError> for Error {
    fn from(err: SqlError) -> Error {
        Error::Sql(err)
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Error {
        Error::General(err.to_string())
    }
}

impl From<SerdeError> for Error {
    fn from(err: SerdeError) -> Error {
        Error::Serialize(err)
    }
}

impl From<TryFromIntError> for Error {
    fn from(err: TryFromIntError) -> Error {
        Error::IntConversion(err)
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

impl From<IoError> for Error {
    fn from(err: IoError) -> Error {
        Error::Io(err)
    }
}

impl<T> From<TrySendError<T>> for Error {
    fn from(err: TrySendError<T>) -> Error {
        Error::Send(err.to_string())
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_err: PoisonError<T>) -> Error {
        Error::Concurrency
    }
}
