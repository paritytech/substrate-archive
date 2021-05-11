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

//! logging

use std::{fs, io, path::PathBuf};

use fern::colors::{Color, ColoredLevelConfig};
use serde::Deserialize;

use crate::substrate_archive_default_dir;

#[derive(Clone, Debug, Deserialize)]
pub struct LoggerConfig {
	pub(crate) std: log::LevelFilter,
	pub(crate) file: Option<FileLoggerConfig>,
}

impl Default for LoggerConfig {
	fn default() -> Self {
		Self { std: log::LevelFilter::Debug, file: None }
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct FileLoggerConfig {
	#[serde(default = "default_file_log_level")]
	pub(crate) level: log::LevelFilter,
	pub(crate) dir: Option<PathBuf>,
	#[serde(default = "default_file_log_name")]
	pub(crate) name: String,
}

impl Default for FileLoggerConfig {
	fn default() -> Self {
		Self { level: default_file_log_level(), dir: None, name: default_file_log_name() }
	}
}

fn default_file_log_level() -> log::LevelFilter {
	log::LevelFilter::Debug
}

fn default_file_log_name() -> String {
	"substrate-archive.log".into()
}

pub fn init(config: LoggerConfig) -> io::Result<()> {
	let colors = ColoredLevelConfig::new()
		.info(Color::Green)
		.warn(Color::Yellow)
		.error(Color::Red)
		.debug(Color::Blue)
		.trace(Color::Magenta);

	let stdout_dispatcher = fern::Dispatch::new()
		.level(config.std)
		.level_for("substrate_archive", config.std)
		.level_for("cranelift_wasm", log::LevelFilter::Error)
		.level_for("sqlx", log::LevelFilter::Error)
		.level_for("staking", log::LevelFilter::Warn)
		.level_for("cranelift_codegen", log::LevelFilter::Warn)
		.level_for("header", log::LevelFilter::Warn)
		.level_for("frame_executive", log::LevelFilter::Error)
		.format(move |out, message, record| {
			out.finish(format_args!(
				"{} {} {}",
				chrono::Local::now().format("[%H:%M]"),
				colors.color(record.level()),
				message,
			))
		})
		.chain(io::stdout());

	if let Some(file) = config.file {
		let mut log_dir = file.dir.unwrap_or_else(substrate_archive_default_dir);
		fs::create_dir_all(log_dir.as_path())?;
		log_dir.push(file.name);

		let file_dispatcher = fern::Dispatch::new()
			.level(file.level)
			.level_for("substrate_archive", file.level)
			.level_for("cranelift_wasm", log::LevelFilter::Error)
			.level_for("sqlx", log::LevelFilter::Warn)
			.level_for("staking", log::LevelFilter::Warn)
			.level_for("cranelift_codegen", log::LevelFilter::Warn)
			.level_for("wasm-heap", log::LevelFilter::Error)
			.format(move |out, message, record| {
				out.finish(format_args!(
					"{} [{}][{}] {}::{};{}",
					chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
					record.target(),
					record.level(),
					message,
					record.file().unwrap_or_default(),
					record.line().map(|l| l.to_string()).unwrap_or_default(),
				))
			})
			.chain(fern::log_file(log_dir).expect("Failed to create log file"));
		fern::Dispatch::new().chain(stdout_dispatcher).chain(file_dispatcher).apply().expect("Could not init logging");
	} else {
		stdout_dispatcher.apply().expect("Could not init logging");
	}
	Ok(())
}
