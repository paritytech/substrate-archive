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

use super::Result;

pub type KeyValuePair = (Box<[u8]>, Box<[u8]>);

// Archive specific K/V database reader implementation
pub trait ReadOnlyDB: Send + Sync {
    /// Read key/value pairs from the database
    fn get(&self, col: u32, key: &[u8]) -> Option<Vec<u8>>;
    /// Iterate over all blocks in the database
    fn iter<'a>(&'a self, col: u32) -> Box<dyn Iterator<Item = KeyValuePair> + 'a>;
    /// Catch up with the latest information added to the database
    fn catch_up_with_primary(&self) -> Result<()>;
}
