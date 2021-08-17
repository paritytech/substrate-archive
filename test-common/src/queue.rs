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

//! Global Variables RabbitMq

use std::env;

use once_cell::sync::Lazy;
use lapin::ConnectionProperties;
use async_amqp::*;

pub static TASK_QUEUE: &str = "SA_TEST_QUEUE";
pub static AMQP_URL: &str = "amqp://localhost:5672";
pub static AMQP_CONN: Lazy<lapin::Connection> = Lazy::new(|| {
    let url = env::var("AMQP_URL").unwrap_or("amqp://localhost:5672".to_string());
    lapin::Connection::connect(&url, ConnectionProperties::default().with_async_std()).wait().expect("Cant connect to RabbitMQ")
});


