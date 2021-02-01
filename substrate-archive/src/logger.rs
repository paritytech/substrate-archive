use std::io;

use fern::colors::{Color, ColoredLevelConfig};

use crate::util::{create_dir, substrate_dir};

pub fn init_logger(std: log::LevelFilter, file: log::LevelFilter) -> io::Result<()> {
	let colors = ColoredLevelConfig::new()
		.info(Color::Green)
		.warn(Color::Yellow)
		.error(Color::Red)
		.debug(Color::Blue)
		.trace(Color::Magenta);

	let mut log_dir = substrate_dir()?;
	create_dir(log_dir.as_path())?;
	log_dir.push("archive.logs");

	let stdout_dispatcher = fern::Dispatch::new()
		.level_for("substrate_archive", std)
		.level_for("cranelift_wasm", log::LevelFilter::Error)
		.level_for("sqlx", log::LevelFilter::Error)
		.level_for("staking", log::LevelFilter::Warn)
		.level_for("cranelift_codegen", log::LevelFilter::Warn)
		.level_for("header", log::LevelFilter::Warn)
		.level_for("", log::LevelFilter::Error)
		.level_for("frame_executive", log::LevelFilter::Error)
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
		.level_for("frame_executive", log::LevelFilter::Error)
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
