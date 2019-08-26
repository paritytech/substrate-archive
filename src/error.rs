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

use failure::{Context, Fail, Backtrace};
use std::fmt::Display;

#[derive(Debug)]
struct ArchiveError {
    inner: Context<ErrorKind>
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum ErrorKind {
    // this is a shit error message
    #[fail(display = "Value not found during execution of the RPC")]
    ValueNotPresent
}

impl Fail for ArchiveError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }
    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for ArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl ArchiveError {
    pub fn kind(&self) -> ErrorKind {
        *self.inner.get_context()
    }
}

impl From<ErrorKind> for ArchiveError {
    fn from(kind: ErrorKind) -> ArchiveError {
        ArchiveError { inner: Context::new(kind) }
    }
}

impl From<Context<ErrorKind>> for ArchiveError {
    fn from(inner: Context<ErrorKind>) -> ArchiveError {
        ArchiveError { inner: inner }
    }
}
