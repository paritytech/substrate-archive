// Copyright 2018-2021 Parity Technologies (UK) Ltd.
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

//! general utilities

use std::{
	fs, io,
	path::{Path, PathBuf},
};

/// Get the path to a local substrate directory where we can save data.
/// Platform | Value | Example
/// -- | -- | --
/// Linux | $XDG_DATA_HOME or $HOME/.local/share/substrate_archive | /home/alice/.local/share/substrate_archive/
/// macOS | $HOME/Library/Application Support/substrate_archive | /Users/Alice/Library/Application Support/substrate_archive/
/// Windows | {FOLDERID_LocalAppData}\substrate_archive | C:\Users\Alice\AppData\Local\substrate_archive
pub fn substrate_dir() -> io::Result<PathBuf> {
	let base_dirs = dirs::BaseDirs::new().ok_or_else(|| {
		io::Error::new(
			io::ErrorKind::Other,
			"No valid home directory path could be retrieved from the operating system",
		)
	})?;
	let mut path = base_dirs.data_local_dir().to_path_buf();
	path.push("substrate_archive");
	Ok(path)
}

/// Create an arbitrary directory on disk.
pub fn create_dir(path: &Path) -> io::Result<()> {
	if let Err(err) = fs::create_dir_all(path) {
		match err.kind() {
			io::ErrorKind::AlreadyExists => (),
			_ => return Err(err),
		}
	}
	Ok(())
}
