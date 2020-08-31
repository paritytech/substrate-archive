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

//! Main messages and NewTypes that can be sent between actors

use crate::{error::Result, types::*};
use sp_runtime::traits::Block as BlockT;
use xtra::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Die;
impl Message for Die {
    type Result = Result<()>;
}

impl Message for Metadata {
    type Result = ();
}

impl<B: BlockT> Message for Block<B> {
    type Result = ();
}

impl<B: BlockT> Message for BatchBlock<B> {
    type Result = ();
}

impl<Block: BlockT> Message for Storage<Block> {
    type Result = ();
}

#[derive(Debug)]
pub struct VecStorageWrap<B: BlockT>(pub Vec<Storage<B>>);

impl<B: BlockT> Message for VecStorageWrap<B> {
    type Result = ();
}
