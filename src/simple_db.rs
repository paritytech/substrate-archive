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

//! a simple "database" that serializes to/from bincode
//! used for saving entries which aren't ready to insert into the database (because of E.G missing blocks)
//! and saves these entries to disk to be reloaded the next time a program starts
//! avoids having to search for missing entries in PostgreSQL and re-fetching from running node

use crate::error::Error;
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    default::Default,
    fs::{self, File, OpenOptions},
    io::prelude::*,
    io::SeekFrom,
    marker::PhantomData,
    path::PathBuf,
};

#[derive(Debug)]
pub(crate) struct SimpleDb<D: DeserializeOwned + Serialize + Default> {
    path: PathBuf,
    _marker: PhantomData<D>,
}
/// A simple DB that allows saving/retrieving rust data structures to/from a (compressed) file.
impl<D> SimpleDb<D>
where
    D: DeserializeOwned + Serialize + Default,
{
    pub(crate) fn new(path: PathBuf) -> Result<Self, Error> {
        if !path.as_path().exists() {
            File::create(path.as_path()).map_err(Error::from)?;
        }
        Ok(SimpleDb {
            path,
            _marker: PhantomData,
        })
    }

    /// Save structure to a file, serializing to Bincode and then compressing with DEFLATE
    pub(crate) fn save(&self, data: D) -> Result<(), Error> {
        self.mutate(|file| {
            let ser_data = bincode::serialize(&data)?;
            let mut e = DeflateEncoder::new(file, Compression::default());
            e.write_all(ser_data.as_slice())?;
            e.finish()?;
            Ok(())
        })?;
        Ok(())
    }

    /// Get structure from file, DEFLATING and then deserializing from Bincode
    pub(crate) fn get(&self) -> Result<D, Error> {
        let meta = fs::metadata(self.path.as_path())?;
        if meta.len() == 0 {
            log::info!("File length is 0");
            return Ok(D::default());
        }
        self.read(|file| {
            let mut deflater = DeflateDecoder::new(file);
            let mut buf = Vec::new();
            let bytes_read = deflater.read_to_end(&mut buf)?;
            log::info!("Read {} bytes from database file", bytes_read);
            bincode::deserialize(&buf[..]).map_err(Error::from)
        })
    }

    /// open backend
    fn open(&self) -> Result<File, Error> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.path.as_path())
            .map_err(Into::into)
    }

    /// mutate the file, always setting seek back to beginning
    fn mutate<F>(&self, mut fun: F) -> Result<(), Error>
    where
        F: FnMut(&mut File) -> Result<(), Error>,
    {
        let mut file = self.open()?;
        fun(&mut file)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    #[allow(unused)]
    /// read file, setting seek back to the start
    fn read<F>(&self, fun: F) -> Result<D, Error>
    where
        F: Fn(&File) -> Result<D, Error>,
    {
        let mut file = self.open()?;
        let ret = fun(&file)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    #[test]
    fn save() {
        pretty_env_logger::try_init();
        let db = SimpleDb::<HashMap<String, usize>>::new(PathBuf::from("/tmp/SOME")).unwrap();
        let mut data = HashMap::new();
        data.insert("Hello".to_string(), 45);
        data.insert("Byte".to_string(), 34);
        db.save(data.clone()).unwrap();
    }

    #[test]
    fn get() {
        pretty_env_logger::try_init();
        let db = SimpleDb::<HashMap<String, usize>>::new(PathBuf::from("/tmp/SOME")).unwrap();
        let mut data = HashMap::new();
        data.insert("Hello".to_string(), 45);
        data.insert("Byte".to_string(), 34);
        db.save(data.clone()).unwrap();
        // TODO make this an assert
        log::info!("DATA: {:?}", db.get().unwrap());
    }
}
