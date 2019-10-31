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

use runtime_primitives::{generic, traits::{SignedExtension, Extrinsic}};
use codec::{Decode, Input, Error as CodecError};

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

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UncheckedExtrinsic<Address, Call, Signature, Extra: SignedExtension> {
    V3(UncheckedExtrinsicV3<Address, Call, Signature, Extra>),
    V4(generic::UncheckedExtrinsic<Address, Call, Signature, Extra>),
}

impl<Address, Call, Signature, Extra: SignedExtension> UncheckedExtrinsic<Address, Call, Signature, Extra> {

    pub fn function(&self) -> &Call {
        match &self {
            UncheckedExtrinsic::V3(ext) => &ext.function,
            UncheckedExtrinsic::V4(ext) => &ext.function
        }
    }
/*
    pub fn signature<E: Extrinsic>(&self) -> Option<E::SignaturePayload> {
        match &self {
            UncheckedExtrinsic::V3(ext) => ext.signature.map(|s| s as E::SignaturePayload),
            UncheckedExtrinsic::V4(ext) => ext.signature
        }
    }
*/
}

impl<Address, Call, Signature, Extra: SignedExtension> Extrinsic
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UncheckedExtrinsicV3<Address, Call, Signature, Extra>
where
    Extra: SignedExtension
{
    pub signature: Option<(Address, Signature, Extra)>,
    pub function: Call
}

impl<Address, Call, Signature, Extra> UncheckedExtrinsicV3<Address, Call, Signature, Extra>
where
    Address: Decode,
    Signature: Decode,
    Call: Decode,
    Extra: SignedExtension
{
    fn decode<I: Input>(is_signed: bool, input: &mut I) -> Result<Self, CodecError> {

        Ok(UncheckedExtrinsicV3 {
            signature: if is_signed { Some(Decode::decode(input)?) } else { None },
            function: Decode::decode(input)?,
        })
    }
}

impl<Address, Call, Signature, Extra: SignedExtension> Extrinsic
    for UncheckedExtrinsicV3<Address, Call, Signature, Extra>
{
    type Call = Call;

    type SignaturePayload = (
        Address,
        Signature,
        Extra,
    );

    fn is_signed(&self) -> Option<bool> {
        Some(self.signature.is_some())
    }
}
