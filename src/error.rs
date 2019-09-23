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
use futures::sync::mpsc::SendError;
use jsonrpc_core_client::RpcError as JsonRpcError;
use std::io::Error as IoError;
use url::ParseError;

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
