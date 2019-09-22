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
use substrate_subxt::Error as SubxtError;
use futures::sync::mpsc::SendError;
use jsonrpc_core_client::RpcError as JsonRpcError;
use std::io::Error as IoError;

#[derive(Debug, Fail)]
pub enum Error {
    /// An error originating from Subxt
    /// Data Provided is the error message of the underlying error
    #[fail(display = "Subxt: {}", _0)]
    Subxt(String),
    #[fail(display = "Could not send to parent process {}", _0)]
    Send(String),
    #[fail(display = "RPC Error: {}", _0)]
    Rpc(#[fail(cause)] JsonRpcError),
    #[fail(display = "Io: {}", _0)]
    Io(#[fail(cause)] IoError),

}


impl From<IoError> for Error {
    fn from(err: IoError) -> Error {
        Error::Io(err)
    }
}

impl From<SubxtError> for Error {
    fn from(err: SubxtError) -> Error {
        match err {
            SubxtError::Codec(e) => Error::Subxt(e.to_string()),
            SubxtError::Io(e) => Error::Subxt(e.to_string()),
            SubxtError::Rpc(e) => Error::Subxt(e.to_string()),
            SubxtError::SecretString(e) => Error::Subxt(format!("{:?}", e)),
            SubxtError::Metadata(e) => Error::Subxt(format!("{:?}", e)),
            SubxtError::Other(s) => Error::Subxt(s),
        }
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
/*
impl Error {

    // TODO: Possibly separate these out or try to preserve original error type, or implement std::Error on Subxt's error type
    pub(crate) fn subxt(&self, err: SubxtError) -> Error {
        match err {
            SubxtError::Codec(e) => Error::from(ErrorKind::Subxt(e.to_string())),
            SubxtError::Io(e) => Error::from(ErrorKind::Subxt(e.to_string())),
            SubxtError::Rpc(e) => Error::from(ErrorKind::Subxt(e.to_string())),
            SubxtError::SecretString(e) => Error::from(ErrorKind::Subxt(format!("{:?}", e))),
            SubxtError::Metadata(e) => Error::from(ErrorKind::Subxt(format!("{:?}", e))),
            SubxtError::Other(s) => Error::from(ErrorKind::Subxt(s)),
        }
    }
}
*/
