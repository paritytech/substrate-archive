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
use fern::colors::{Color, ColoredLevelConfig};
use log::*;
use chrono::{DateTime, TimeZone, Utc};
use std::convert::TryFrom;
use desub::{decoder::GenericExtrinsic, SubstrateType};

// panics if it fails because of anything other than the directory already exists
pub fn create_dir(path: std::path::PathBuf) {
    match std::fs::create_dir(path) {
        Err(e) => match e.kind() {
            std::io::ErrorKind::AlreadyExists => (),
            _ => {
                error!("{}", e);
                std::process::exit(0x0100);
            }
        },
        Ok(_) => (),
    }
}

/// tries to get timestamp inherent and convert it to UTC format
/// if it exists within the extrinsics
pub fn try_to_get_time(ext: &[GenericExtrinsic]) -> Option<DateTime<Utc>> {
    // todo: the assumption here is that the timestamp is a u64
    for e in ext.iter() {
        if e.ext_module() == "Timestamp" && e.ext_call() == "set" {
            let t = e.args().iter().find(|a| a.name == "now")?;
            let t: i64 = match t.arg {
                SubstrateType::U64(t) => {
                    let t = i64::try_from(t).ok();
                    if t.is_none() {
                        log::warn!("Not a valid UNIX timestamp");
                    }
                    t?
                },
                _ => return None
            };
            return Some(Utc.timestamp_millis(t));
        }
    }
    None
}

pub fn init_logger(std: log::LevelFilter, file_lvl: log::LevelFilter) {
    let colors = ColoredLevelConfig::new()
        .info(Color::Green)
        .warn(Color::Yellow)
        .error(Color::Red)
        .debug(Color::Blue)
        .trace(Color::Magenta);

    let mut log_dir = dirs::data_local_dir().expect("failed to find local data dir for logs");
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
                .level_for("substrate_archive", file_lvl)
                .level_for("desub_core", log::LevelFilter::Trace)
                // .level_for("crate_name", log::LevelFilter::Trace)
                // .level_for("crate_name", log::LevelFilter::Trace)
                .chain(
                    fern::log_file(log_dir).expect("Failed to create substrate_archive.logs file"),
                ),
        )
        .chain(fern::Dispatch::new().level(std).chain(std::io::stdout()))
        .apply()
        .expect("Could not init logging");
}

fn format_opt(file: Option<String>) -> String {
    match file {
        None => "".to_string(),
        Some(f) => f.to_string(),
    }
}
