// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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
// along with substrate-archive. If not, see <http://www.gnu.org/licenses/>.

#![forbid(unsafe_code)]
#![deny(dead_code)]

mod codegen;
mod dummy_jobs;
mod runner;
mod sync;
mod test_guard;

use crate::test_guard::TestGuard;
use sa_work_queue::Job;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Once;

static INIT: Once = Once::new();

pub fn initialize() {
    INIT.call_once(|| {
        pretty_env_logger::init();
    });
}

#[derive(Serialize, Deserialize)]
struct Size {
    height: u32,
    width: u32,
}

#[sa_work_queue::background_job]
fn resize_image(_name: String) -> Result<(), sa_work_queue::PerformError> {
    std::thread::sleep(std::time::Duration::from_millis(150));
    Ok(())
}

#[sa_work_queue::background_job]
fn resize_image_gen<E: Serialize + DeserializeOwned + Send + std::fmt::Display>(
    _some: E,
) -> Result<(), sa_work_queue::PerformError> {
    Ok(())
}

#[test]
fn enqueue_8_jobs_limited_size() {
    initialize();
    let runner = TestGuard::runner(());
    log::info!("RUNNING `enqueue_8_jobs_limited_size`");

    let handle = runner.handle();
    smol::block_on(async {
        resize_image("lightsource".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image("gambit".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image("chess".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image("checkers".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image("L".to_string()).enqueue(&handle).await.unwrap();
        resize_image("sinks".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image("polkadot".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image("kusama".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
    });

    runner.run_pending_tasks().unwrap();
    runner.wait_for_all_tasks().unwrap();
}

#[test]
fn generic_jobs_can_be_enqueued() {
    initialize();
    let runner = TestGuard::builder(())
        .register_job::<resize_image_gen::Job<String>>()
        .build();
    log::info!("RUNNING `generic_jobs_can_be_enqueued`");
    let handle = runner.handle();

    smol::block_on(async {
        resize_image_gen("koala".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image_gen("cool_pic_no_2".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image_gen("electric".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image_gen("letter".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        resize_image_gen("L".to_string())
            .enqueue(&handle)
            .await
            .unwrap();
        runner.run_pending_tasks().unwrap();
        runner.wait_for_all_tasks().unwrap();
    });
}
