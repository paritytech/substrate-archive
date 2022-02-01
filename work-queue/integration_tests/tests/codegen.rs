// Copyright 2021 Parity Technologies (UK) Ltd.
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

use crate::dummy_jobs::*;
use crate::test_guard::TestGuard;
use anyhow::Result;
use sa_work_queue::PerformError;
use serde::{de::DeserializeOwned, Serialize};

#[test]
fn generated_jobs_serialize_all_arguments_except_first() {
	crate::initialize();
	#[sa_work_queue::background_job]
	fn check_arg_equal_to_env(env: &String, arg: String) -> Result<(), PerformError> {
		if env == &arg {
			Ok(())
		} else {
			Err("arg wasn't env!".into())
		}
	}

	let runner = TestGuard::runner("a".to_string());
	runner.handle().channel().confirm_select(Default::default()).wait().unwrap();
	smol::block_on(async {
		check_arg_equal_to_env("a".into()).enqueue(runner.handle()).await.unwrap();
		check_arg_equal_to_env("b".into()).enqueue(runner.handle()).await.unwrap();
		runner.run_pending_tasks().unwrap();
	});

	let _ = runner.wait_for_all_tasks();
}

#[test]
fn jobs_with_args_but_no_env() {
	crate::initialize();
	#[sa_work_queue::background_job]
	fn assert_foo(arg: String) -> Result<(), PerformError> {
		if arg == "foo" {
			Ok(())
		} else {
			Err("arg wasn't foo!".into())
		}
	}

	let runner = TestGuard::dummy_runner();
	smol::block_on(async {
		let conn = runner.handle();
		assert_foo("foo".into()).enqueue(conn).await.unwrap();
		assert_foo("not foo".into()).enqueue(conn).await.unwrap();
		runner.run_pending_tasks().unwrap();
	});
}

#[test]
fn env_can_have_any_name() {
	crate::initialize();
	#[sa_work_queue::background_job]
	fn env_with_different_name(environment: &String) -> Result<(), sa_work_queue::PerformError> {
		assert_eq!(environment, "my environment");
		Ok(())
	}

	let runner = TestGuard::runner(String::from("my environment"));
	smol::block_on(async {
		let conn = runner.handle();
		env_with_different_name().enqueue(conn).await.unwrap();

		runner.run_pending_tasks().unwrap();
		runner.wait_for_all_tasks().unwrap();
	})
}

#[test]
#[forbid(unused_imports)]
fn test_imports_only_used_in_job_body_are_not_warned_as_unused() {
	use std::io::prelude::*;
	crate::initialize();

	#[sa_work_queue::background_job]
	fn uses_trait_import() -> Result<(), sa_work_queue::PerformError> {
		let mut buf = Vec::new();
		buf.write_all(b"foo")?;
		let s = String::from_utf8(buf)?;
		assert_eq!(s, "foo");
		Ok(())
	}

	let runner = TestGuard::dummy_runner();
	smol::block_on(async {
		let mut conn = runner.handle();
		uses_trait_import().enqueue(&mut conn).await.unwrap();

		runner.run_pending_tasks().unwrap();
		runner.wait_for_all_tasks().unwrap();
	});
}

#[test]
fn proc_macro_accepts_arbitrary_where_clauses() {
	crate::initialize();
	#[sa_work_queue::background_job]
	fn can_specify_where_clause<S>(_eng: &(), arg: S) -> Result<(), sa_work_queue::PerformError>
	where
		S: Serialize + DeserializeOwned + std::fmt::Display,
	{
		arg.to_string();
		Ok(())
	}

	let runner = TestGuard::builder(()).register_job::<can_specify_where_clause::Job<String>>().build();

	smol::block_on(async {
		let mut conn = runner.handle();
		can_specify_where_clause("hello".to_string()).enqueue(&mut conn).await.unwrap();

		runner.run_pending_tasks().unwrap();
		runner.wait_for_all_tasks().unwrap();
	});
}
