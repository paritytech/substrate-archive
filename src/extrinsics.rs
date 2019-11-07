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

pub mod v3;

use log::{debug, warn, info};
use runtime_primitives::{
    generic,
    OpaqueExtrinsic,
    traits::{SignedExtension, Extrinsic as ExtrinsicTrait, Header, Hash as HashTrait}
};
use codec::{Encode, Decode, Input, Error as CodecError};

use std::fmt;

use self::v3::UncheckedExtrinsicV3;
use crate::{
    database::models::{InsertInherentOwned, InsertTransactionOwned},
    types::ExtractCall,
    srml_ext::SrmlExt,
    error::Error
};

const LATEST_TRANSACTION_VERSION: u8 = 4;
const EARLIEST_TRANSACTION_VERSION: u8 = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum SupportedVersions {
    Three,
    Four
}

impl SupportedVersions {
    pub fn is_supported(ver: &u8) -> bool {
        return *ver >= EARLIEST_TRANSACTION_VERSION && *ver <= LATEST_TRANSACTION_VERSION
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

#[derive(Debug)]
pub struct Extrinsic<Address, Call, Signature, Extra: SignedExtension> {
    extrinsic: UncheckedExtrinsic<Address, Call, Signature, Extra>,
    version: SupportedVersions
}

#[derive(Debug)]
pub enum DbExtrinsic {
    Signed(InsertTransactionOwned),
    NotSigned(InsertInherentOwned)
}

impl<Address, Call, Signature, Extra> Extrinsic<Address, Call, Signature, Extra>
where
    Address: fmt::Debug + Decode,
    Call: fmt::Debug + ExtractCall + Decode,
    Signature: fmt::Debug + Decode,
    Extra: SignedExtension
{
    pub fn new(opaque_ext: &OpaqueExtrinsic) -> Result<Self, Error> {
        let extrinsic = UncheckedExtrinsic::decode(&mut opaque_ext.encode().as_slice())
            .map_err(|e| Error::from(e))?;
        let version: SupportedVersions = (&extrinsic).into();
        Ok(Self { extrinsic, version })
    }

    pub fn function(&self) -> &Call {
        self.extrinsic.function()
    }

    pub fn version(&self) -> &SupportedVersions {
        &self.version
    }

    /// return inherent types that may be inserted into a postgres database
    pub fn database_format<H>(&self, index: i32, header: &H, number: i64) -> Result<DbExtrinsic, Error>
    where
        H: Header,
        H::Hashing: HashTrait,
        generic::UncheckedExtrinsic<Address, Call, Signature, Extra>: Encode,
        UncheckedExtrinsicV3<Address, Call, Signature, Extra>: Encode
    {
        if let Some(b) = self.is_signed() {
            if b {
                self.format_signed(index, header, number)
            } else {
                self.format_unsigned(index, header, number)
            }
        } else {
            // TODO: if is_signed is just None, does that 100% mean the extrinsic exists/is not signed?
            self.format_unsigned(index, header, number)
        }
    }

    fn format_signed<H>(&self, index: i32, header: &H, number: i64) -> Result<DbExtrinsic, Error>
    where
        H: Header,
        H::Hashing: HashTrait,
        generic::UncheckedExtrinsic<Address, Call, Signature, Extra>: Encode,
        UncheckedExtrinsicV3<Address, Call, Signature, Extra>: Encode
    {
        info!("SIGNED EXTRINSIC: {:?}", &self);
        let (module, call) = self.function().extract_call();
        let res = call.function();
        let (fn_name, params);
        if res.is_err() {
            warn!("Call not found, formatting as raw rust. Call: {:?}", &self);
            fn_name = format!("{:?}", &self);
            params = Vec::new();
        } else {
            let (name, p) = res?;
            fn_name = name;
            params = p;
        }

        let transaction_hash = self.extrinsic.hash::<H::Hashing>().as_ref().to_vec();
        info!("TRANSACTION HASH: {:?}", transaction_hash);

        Ok(
            DbExtrinsic::Signed (
                InsertTransactionOwned {
                    transaction_hash: transaction_hash.to_vec(), // TODO
                    block_num: number,
                    hash: header.hash().as_ref().to_vec(),
                    from_addr: Vec::new(), // TODO
                    to_addr: Some(Vec::new()), // TODO
                    call: fn_name,
                    nonce: 0,
                    tx_index: index,
                    signature: Vec::new(), // TODO
                    transaction_version: i32::from(self.version()),
                }
            )
        )
    }

    fn format_unsigned<H>(&self, index: i32, header: &H, number: i64) -> Result<DbExtrinsic, Error>
    where
        H: Header
    {
        let (module, call) = self.function().extract_call();
        let res = call.function();
        let (fn_name, params);
        if res.is_err() {
            debug!("Call not found, formatting as raw rust. Call: {:?}", &self);
            fn_name = format!("{:?}", &self);
            params = Vec::new();
        } else {
            let (name, p) = res?;
            fn_name = name;
            params = p;
        }

        Ok(
            DbExtrinsic::NotSigned (
                InsertInherentOwned {
                    hash: header.hash().as_ref().to_vec(),
                    block_num: number,
                    module: module.to_string(),
                    call: fn_name,
                    parameters: Some(params),
                    in_index: index,
                    transaction_version: i32::from(self.version())
                }
            )
        )
    }
}

impl<Address, Call, Signature, Extra> ExtrinsicTrait for Extrinsic<Address, Call, Signature, Extra>
where
    Extra: SignedExtension
{
    type Call = Call;
    type SignaturePayload = (
        Address,
        Signature,
        Extra
    );

    fn is_signed(&self) -> Option<bool> {
        self.extrinsic.is_signed()
    }
}

#[derive(PartialEq, Eq, Clone)]
enum UncheckedExtrinsic<Address, Call, Signature, Extra: SignedExtension> {
    V3(UncheckedExtrinsicV3<Address, Call, Signature, Extra>),
    V4(generic::UncheckedExtrinsic<Address, Call, Signature, Extra>),
}

impl<Address, Call, Signature, Extra> std::fmt::Debug
    for UncheckedExtrinsic<Address, Call, Signature, Extra>
where
    Address: std::fmt::Debug,
    Call: std::fmt::Debug,
    Extra: SignedExtension,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            UncheckedExtrinsic::V3(e) => {
                write!(
                    f,
                    "UncheckedExtrinsic({:?}, {:?})",
                    e.signature.as_ref().map(|x| (&x.0, &x.2)),
                    e.function,
                )
            },
            UncheckedExtrinsic::V4(e) => {
                write!(f, "{:?}", e)
            }
        }
    }
}

impl<Address, Call, Signature, Extra: SignedExtension> UncheckedExtrinsic<Address, Call, Signature, Extra> {

    fn function(&self) -> &Call {
        match &self {
            UncheckedExtrinsic::V3(ext) => &ext.function,
            UncheckedExtrinsic::V4(ext) => &ext.function
        }
    }

    fn version(&self) -> SupportedVersions {
        match &self {
            UncheckedExtrinsic::V3(_) => SupportedVersions::Three,
            UncheckedExtrinsic::V4(_) => SupportedVersions::Four
        }
    }

    /// Hash of the extrinsic
    fn hash<Hash: HashTrait>(&self) ->  Hash::Output
    where
        generic::UncheckedExtrinsic<Address, Call, Signature, Extra>: Encode,
        UncheckedExtrinsicV3<Address, Call, Signature, Extra>: Encode,
    {
        match &self {
            UncheckedExtrinsic::V3(e)
                => Hash::hash_of::<UncheckedExtrinsicV3<Address, Call, Signature, Extra>>(e),
            UncheckedExtrinsic::V4(e)
                => Hash::hash_of::<generic::UncheckedExtrinsic<Address, Call, Signature, Extra>>(e),
        }
    }
}

impl<A, C, S, E: SignedExtension> From<&UncheckedExtrinsic<A, C, S, E>> for SupportedVersions
{
    fn from(ext: &UncheckedExtrinsic<A, C, S, E>) -> SupportedVersions {
        match ext {
            UncheckedExtrinsic::V3(_) => SupportedVersions::Three,
            UncheckedExtrinsic::V4(_) => SupportedVersions::Four
        }
    }
}

impl<Address, Call, Signature, Extra: SignedExtension> ExtrinsicTrait
    for UncheckedExtrinsic<Address, Call, Signature, Extra>
{
    type Call = Call;
    type SignaturePayload = (
        Address,
        Signature,
        Extra
    );

    fn is_signed(&self) -> Option<bool> {
        match &self {
            UncheckedExtrinsic::V3(ext) => ext.is_signed(),
            UncheckedExtrinsic::V4(ext) => ext.is_signed()
        }
    }
}

impl<Address, Call, Signature, Extra> Decode
    for UncheckedExtrinsic<Address, Call, Signature, Extra>
where
    Address: Decode,
    Signature: Decode,
    Call: Decode,
    Extra: SignedExtension,
{
    fn decode<I: Input>(input: &mut I) -> Result<Self, CodecError> {

        // This is a little more complicated than usual since the binary format must be compatible
        // with substrate's generic `Vec<u8>` type. Basically this just means accepting that there
        // will be a prefix of vector length (we don't need
        // to use this).
        let _length_do_not_remove_me_see_above: Vec<()> = Decode::decode(input)?;

        let version = input.read_byte()?;

        let is_signed = version & 0b1000_0000 != 0;
        let version = version & 0b0111_1111;
        match version {
            3 => {
                Ok(UncheckedExtrinsic::V3(UncheckedExtrinsicV3::decode(is_signed, input)?))
            },
            4 => {
                Ok(UncheckedExtrinsic::V4(
                    generic::UncheckedExtrinsic { // current version
                        signature: if is_signed { Some(Decode::decode(input)?) } else { None },
                        function: Decode::decode(input)?,
                    }
                ))
            },
            _ => return Err("Invalid transaction version".into())
        }
    }
}

impl<Address, Call, Signature, Extra> Encode for UncheckedExtrinsic<Address, Call, Signature, Extra>
where
    Address: Encode,
    Signature: Encode,
    Call: Encode,
    Extra: SignedExtension
{
    fn encode(&self) -> Vec<u8> {
        match self {
            UncheckedExtrinsic::V3(e) => e.encode(),
            UncheckedExtrinsic::V4(e) => e.encode()
        }
    }
}

// NEED TO IMPL ENCODE HERE
