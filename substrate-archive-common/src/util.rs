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

//! logging and general utilities

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher as _;
use std::path::{Path, PathBuf};

#[cfg(feature = "logging")]
use fern::colors::{Color, ColoredLevelConfig};

use crate::error::ArchiveError;

#[cfg(feature = "logging")]
pub fn init_logger(std: log::LevelFilter, file: log::LevelFilter) -> Result<(), ArchiveError> {
	let colors = ColoredLevelConfig::new()
		.info(Color::Green)
		.warn(Color::Yellow)
		.error(Color::Red)
		.debug(Color::Blue)
		.trace(Color::Magenta);

	let mut log_dir = substrate_dir().unwrap();
	create_dir(log_dir.as_path())?;
	log_dir.push("archive.logs");

	let stdout_dispatcher = fern::Dispatch::new()
		.level_for("substrate_archive", std)
		.level_for("cranelift_wasm", log::LevelFilter::Error)
		.level_for("sqlx", log::LevelFilter::Error)
		.level_for("staking", log::LevelFilter::Warn)
		.level_for("cranelift_codegen", log::LevelFilter::Warn)
		.level_for("header", log::LevelFilter::Warn)
		// .level_for("", log::LevelFilter::Error)
		.format(move |out, message, record| {
			out.finish(format_args!(
				"{} {} {}",
				chrono::Local::now().format("[%H:%M]"),
				colors.color(record.level()),
				message,
			))
		})
		.chain(fern::Dispatch::new().level(std).chain(std::io::stdout()));

	let file_dispatcher = fern::Dispatch::new()
		.level(file)
		.level_for("substrate_archive", file)
		.level_for("cranelift_wasm", log::LevelFilter::Error)
		.level_for("sqlx", log::LevelFilter::Warn)
		.level_for("staking", log::LevelFilter::Warn)
		.level_for("cranelift_codegen", log::LevelFilter::Warn)
		// .level_for("desub_core", log::LevelFilter::Debug)
		// .level_for("kvdb_rocksdb", log::LevelFilter::Debug)
		// .level_for("kvdb_rocksdb", log::LevelFilter::Debug)
		.format(move |out, message, record| {
			out.finish(format_args!(
				"{} [{}][{}] {}::{};{}",
				chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
				record.target(),
				record.level(),
				message,
				format_opt(record.file().map(|s| s.to_string())),
				format_opt(record.line().map(|n| n.to_string()))
			))
		})
		.chain(fern::log_file(log_dir).expect("Failed to create substrate_archive.logs file"));

	fern::Dispatch::new().chain(stdout_dispatcher).chain(file_dispatcher).apply().expect("Could not init logging");
	Ok(())
}

fn format_opt(file: Option<String>) -> String {
	match file {
		None => "".to_string(),
		Some(f) => f,
	}
}

/// Get the path to a local substrate directory where we can save data.
/// Platform | Value | Example
/// -- | -- | --
/// Linux | $XDG_DATA_HOME or $HOME/.local/share/substrate_archive | /home/alice/.local/share/substrate_archive/
/// macOS | $HOME/Library/Application Support/substrate_archive | /Users/Alice/Library/Application Support/substrate_archive/
/// Windows | {FOLDERID_LocalAppData}\substrate_archive | C:\Users\Alice\AppData\Local\substrate_archive
pub fn substrate_dir() -> Result<PathBuf, ArchiveError> {
	if let Some(base_dirs) = dirs::BaseDirs::new() {
		let mut path = base_dirs.data_local_dir().to_path_buf();
		path.push("substrate_archive");
		Ok(path)
	} else {
		Err(ArchiveError::from("No valid home directory path could be retrieved from the operating system"))
	}
}

/// Create an arbitrary directory on disk.
pub fn create_dir(path: &Path) -> Result<(), ArchiveError> {
	if let Err(e) = std::fs::create_dir_all(path) {
		match e.kind() {
			std::io::ErrorKind::AlreadyExists => (),
			_ => return Err(ArchiveError::from(format!("A directory in the path '{:?}' could not be created", path))),
		}
	}
	Ok(())
}

/// Make a hash out of a byte string using the default hasher
pub fn make_hash<K: std::hash::Hash + ?Sized>(val: &K) -> u64 {
	let mut state = DefaultHasher::new();
	val.hash(&mut state);
	state.finish()
}
