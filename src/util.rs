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
use log::*;
use fern::colors::{Color, ColoredLevelConfig};

// panics if it fails because of anything other than the directory already exists
pub fn create_dir(path: std::path::PathBuf) {
    match std::fs::create_dir(path) {
        Err(e) => {
            match e.kind() {
                std::io::ErrorKind::AlreadyExists => (),
                _ => {
                    error!("{}", e);
                    std::process::exit(0x0100);
                }
            }
        },
        Ok(_) => ()
    }
}

pub fn init_logger(level: log::LevelFilter) {
    let colors = ColoredLevelConfig::new()
        .info(Color::Green)
        .warn(Color::Yellow)
        .error(Color::Red)
        .debug(Color::Blue)
        .trace(Color::Magenta);

    let mut log_dir = dirs::data_local_dir()
        .expect("failed to find local data dir for logs");
    log_dir.push("substrate_archive");
    create_dir(log_dir.clone());
    log_dir.push("archive.logs");

    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                    "{} [{}][{}] {} ::{};{}",
                    chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                    record.target(),
                    colors.color(record.level()),
                    message,
                    format_opt(record.file().map(|s| s.to_string())),
                    format_opt(record.line().map(|n| n.to_string()))
                ))
        })
        .chain(
            fern::Dispatch::new()
            .level(log::LevelFilter::Info)
            .level_for("substrate_archive", log::LevelFilter::Trace)
            // .level_for("cratename", log::LevelFilter::Trace)
            // .level_for("crate_name", log::LevelFilter::Trace)
            // .level_for("crate_name", log::LevelFilter::Trace)
            .chain(fern::log_file(log_dir).expect("Failed to create edb.logs file"))
        )
        .chain(
            fern::Dispatch::new()
            .level(level)
            .chain(std::io::stdout())
        )
        .apply().expect("Could not init logging");
}

fn format_opt(file: Option<String>) -> String {
    match file {
        None => "".to_string(),
        Some(f) => f.to_string()
    }
}
