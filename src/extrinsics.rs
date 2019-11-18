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

mod util;
pub use self::util::get_extrinsics;

use log::{error, trace, debug, warn, info};
use runtime_primitives::{
    generic,
    OpaqueExtrinsic,
    traits::{SignedExtension, Extrinsic as ExtrinsicTrait, Header, Hash as HashTrait}
};
use codec::{Encode, Decode, Input, Error as CodecError};

use std::fmt;

use self::util::{SupportedVersions, encode_with_vec_prefix};

use crate::{
    database::models::{InsertInherentOwned, InsertTransactionOwned},
    types::ExtractCall,
    error::Error,
};

#[derive(Debug, Clone)]
pub struct Extrinsic<Address, Call, Signature, Extra: SignedExtension> {
    extrinsic: UncheckedExtrinsic<Address, Call, Signature, Extra>
}

/// An extrinsic ready to be inserted into a Database
#[derive(Debug)]
pub enum DbExtrinsic {
    Signed(InsertTransactionOwned),
    NotSigned(InsertInherentOwned)
}

pub trait ExtractExtrinsic {
    /// Gets the extrinsic in all of it's parts
    fn extrinsic(&self) -> SplitOpaqueExtrinsic;

    /*
    /// Consumes and converts into concrete type
    fn into_extrinsic(&self
    ) -> Option<&Extrinsic<Address, Call, Signature, Extra>>
    where
        Extra: SignedExtension
    {
        None
    }
    */
}

// Encoded Extrinsic but everything is split into parts
pub struct SplitOpaqueExtrinsic {
    /// signature of the extrinsic (if it is signed)
    sig: Option<Vec<u8>>, // encoded
    /// address of the extrinsic
    addr: Option<Vec<u8>>, // encoded
    /// Any Signed Extra
    extra: Option<Vec<u8>>, // encoded
    /// function of the extrinsic
    call: Vec<u8>  // encoded
}

impl SplitOpaqueExtrinsic {
    pub fn new(sig: Option<Vec<u8>>, addr: Option<Vec<u8>>, extra: Option<Vec<u8>>, call: Vec<u8>) -> Self {
        Self { sig, addr, extra, call }
    }
}

impl<Address, Call, Signature, Extra> ExtractExtrinsic<Address, Call, Signature, Extra>
    for Extrinsic<Address, Call, Signature, Extra>
where
    Address: Encode,
    Call: Encode,
    Signature: Encode,
    Extra: SignedExtension + Encode,
{
    fn extrinsic(&self) -> SplitOpaqueExtrinsic {
        let (signed, call) = self.extrinsic.split();
        if let Some(s) = signed {
            SplitOpaqueExtrinsic {
                sig: Some(s.0.encode()),
                addr: Some(s.1.encode()),
                extra: Some(s.2.encode()),
                call: call.encode()
            }
        } else {
            SplitOpaqueExtrinsic {
                sig: None,
                addr: None,
                extra: None,
                call: call.encode(),
            }
        }
    }

    fn into_extrinsic(&self
    ) -> Option<&Extrinsic<Address, Call, Signature, Extra>> {
        Some(self)
    }
}

impl<Address, Call, Signature, Extra> Extrinsic<Address, Call, Signature, Extra>
where
    Address: fmt::Debug + Decode,
    Call: fmt::Debug + ExtractCall + Decode,
    Signature: fmt::Debug + Decode,
    Extra: SignedExtension
{
    pub fn new(opaque_ext: &OpaqueExtrinsic) -> Result<Self, Error> {
        trace!("Opaque Extrinsic: {:?}", opaque_ext);
        let extrinsic = UncheckedExtrinsic::decode(&mut opaque_ext.encode().as_slice())?;
        // let version: SupportedVersions = (&extrinsic).into();
        // trace!("Version: {}", version);
        Ok(Self { extrinsic })
    }

    pub fn function(&self) -> &Call {
        self.extrinsic.function()
    }

    pub fn version(&self) -> &SupportedVersions {
        &self.extrinsic.version.into()
    }

    /// return inherent types that may be inserted into a postgres database
    pub fn database_format<H>(&self, index: i32, header: &H, number: i64) -> Result<DbExtrinsic, Error>
    where
        H: Header,
        H::Hashing: HashTrait,
        generic::UncheckedExtrinsic<Address, Call, Signature, Extra>: Encode,
        UncheckedExtrinsic<Address, Call, Signature, Extra>: Encode
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
        UncheckedExtrinsic<Address, Call, Signature, Extra>: Encode
    {
        info!("SIGNED EXTRINSIC: {:?}", &self);
        let (module, call) = self.function().extract_call();
        let res = call.function();
        let (fn_name, _params);
        if res.is_err() {
            warn!("Call not found, formatting as raw rust. Call: {:?}", &self);
            fn_name = format!("{:?}", &self);
        } else {
            let (name, p) = res?;
            fn_name = name;
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
                    module: module.to_string(),
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

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct UncheckedExtrinsic<Address, Call, Signature, Extra: SignedExtension> {
    pub signature: Option<(Address, Signature, Extra)>,
    pub function: Call,
    version: usize,
}

impl<Address, Call, Signature, Extra> UncheckedExtrinsic<Address, Call, Signature, Extra>
where
    Extra: SignedExtension,
    Self: Encode
{
     fn split(&self) -> (Option<&(Address, Signature, Extra)>, &Call) {
         (self.signature.as_ref(), &self.function)
     }

    fn function(&self) -> &Call {
        &self.function
    }

    fn hash<Hash: HashTrait>(&self) -> Hash::Output {
        Hash::hash_of::<UncheckedExtrinsic<Address, Call, Signature, Extra>>(self)
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
        Some(self.signature.is_some())
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

        trace!("VERSION: {}", version);
        Ok(Self {
            signature: if is_signed { Some(Decode::decode(input).map_err(|e| { error!("Error decoding signature"); e })?) } else { None },
            function: Decode::decode(input).map_err(|e| { error!("Error decoding call"); e })?,
            version: version,
        })
    }
}

impl<Address, Call, Signature, Extra: SignedExtension> Encode
    for UncheckedExtrinsic<Address, Call, Signature, Extra>
where
    Address: Encode,
    Signature: Encode,
    Call: Encode,
    Extra: SignedExtension
{
    fn encode(&self) -> Vec<u8> {
        encode_with_vec_prefix::<Self, _>(|v| {
            match self.signature.as_ref() {
                Some(s) => {
                    v.push(3 | 0b1000_0000);
                    s.encode_to(v);
                }
                None => {
                    v.push(3 & 0b0111_1111);
                }
            }
            self.function.encode_to(v);
        })
    }
}

