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


use crate::backend::database::ReadOnlyDatabase;
use crate::backend::util::NUM_COLUMNS;
use kvdb_rocksdb::DatabaseConfig;


pub fn harness<F>(db: &str, fun: F)
where
    F: FnOnce(ReadOnlyDatabase)
{
    let secondary_db = tempdir::TempDir::new("archive-test")
        .expect("Couldn't create a temporary directory")
        .into_path();
    let conf = DatabaseConfig {
        secondary: Some(secondary_db.to_str().unwrap().to_string()),
        ..DatabaseConfig::with_columns(NUM_COLUMNS)
    };
    let db = ReadOnlyDatabase::open(&conf, db).expect("Couldn't open a secondary instance");
    fun(db);
}
