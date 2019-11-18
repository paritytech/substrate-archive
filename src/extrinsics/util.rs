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

use super::{DbExtrinsic, Extrinsic};
use crate::{error::Error, types::{System, DecodeExtrinsic}};
use codec::Encode;
use log::error;
use runtime_primitives::{traits::SignedExtension};

pub const LATEST_TRANSACTION_VERSION: u8 = 4;
pub const EARLIEST_TRANSACTION_VERSION: u8 = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum SupportedVersions {
    Three,
    Four
}

impl SupportedVersions {
    pub fn is_supported(ver: u8) -> bool {
        ver >= EARLIEST_TRANSACTION_VERSION && ver <= LATEST_TRANSACTION_VERSION
    }
}

impl From<SupportedVersions> for u8 {
    fn from(ver: SupportedVersions) -> u8 {
        match ver {
            SupportedVersions::Three => 3,
            SupportedVersions::Four => 4
        }
    }
}

impl From<&SupportedVersions> for i32 {
    fn from(ver: &SupportedVersions) -> i32 {
        match ver {
            SupportedVersions::Three => 3,
            SupportedVersions::Four => 4,
        }
    }
}

impl std::fmt::Display for SupportedVersions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", i32::from(self))
    }
}

pub fn get_extrinsics<T, Address, Call, Signature, Extra>(
    extrinsics: &[T::Extrinsic],
    header: &T::Header,
    // db: &AsyncDiesel<PgConnection>,
) -> Result<Vec<DbExtrinsic>, Error>
where
    T: System,
    Extra: SignedExtension
{

    extrinsics
        .iter()
        // enumerate is used here to preserve order/index of extrinsics
        .enumerate()
        .map(|(idx, x)| {
            Ok((idx, x.decode()?.into_extrinsic().unwrap() ))
        })
        .collect::<Vec<Result<(usize, Extrinsic<Address, Call, Signature, Extra>), Error>>>()
        .into_iter()
        // we don't want to skip over _all_ extrinsics if decoding one extrinsic does not work
        .filter_map(|x: Result<(usize, Extrinsic<Address, Call, Signature, Extra>), _>| {
            match x {
                Ok(v) => {
                    let number = (*header.number()).into();
                    Some(v.1.database_format(v.0.try_into().unwrap(), header, number))
                },
                Err(e) => {
                    error!("{:?}", e);
                    None
                }
            }
        })
        .collect::<Result<Vec<DbExtrinsic>, Error>>()
}

//TODO There needs to be a better way than verbatim copying code from substrate...
pub fn encode_with_vec_prefix<T: Encode, F: Fn(&mut Vec<u8>)>(encoder: F) -> Vec<u8> {
    let size = std::mem::size_of::<T>();
    let reserve = match size {
        0..=0b00111111 => 1,
        0..=0b00111111_11111111 => 2,
        _ => 4,
    };
    let mut v = Vec::with_capacity(reserve + size);
    v.resize(reserve, 0);
    encoder(&mut v);

    // need to prefix with the total length to ensure it's binary compatible with
    // Vec<u8>.
    let mut length: Vec<()> = Vec::new();
    length.resize(v.len() - reserve, ());
    length.using_encoded(|s| {
        v.splice(0..reserve, s.iter().cloned());
    });

    v
}
