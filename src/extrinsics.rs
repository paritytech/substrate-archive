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

use log::{error, warn, debug};
use serde_json::json;
use runtime_primitives::{
    traits::{SignedExtension, Header},
    generic::UncheckedExtrinsic,
};
use codec::{Decode, Input, Error as CodecError};
use polkadot_runtime::{
    RegistrarCall,
    Runtime as RuntimeT
};
use crate::{
    error::Error,
    database::models::{InsertTransactionOwned, InsertInherentOwned},
    types::{ExtractCall, System, ToDatabaseExtrinsic},
};

const LATEST_TRANSACTION_VERSION: u8 = 4;

/// A Backwards-Compatible Extrinsic
pub struct OldExtrinsic<Address, Call, Signature, Extra>
where
    Extra: SignedExtension
{
    pub signature: Option<(Address, Signature, Extra)>,
    pub function: Call,
    pub version: u8,
}

impl<Address, Call, Signature, Extra> Decode
    for OldExtrinsic<Address, Call, Signature, Extra>
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
        let signature = if is_signed {
            Some(Decode::decode(input).map_err(|e|  { warn!("Error decoding signature"); e })?)
        } else {
            None
        };
        let mut bytes: Vec<u8> = Vec::new();
        while input.remaining_len()?.expect("Failed unwrapping length in Decode") > 0 {
            let byte = input.read_byte()?;
            bytes.push(byte);
        }

        log::trace!("bytes read from decoding extrinsic: {:X?}", bytes);

        if bytes[0] == 0x16 {
            let mut bytes = &bytes[1..];
            log::trace!("BYTES WITHOUT 0x16: {:?}", bytes);
            let reg: Result<RegistrarCall<RuntimeT>, _> = Decode::decode(&mut bytes).map_err(|_| warn!("Did not decode"));
            log::trace!("REGISTRAR CALL: {:?}", reg);
        }

        log::trace!("bytes read from decoding extrinsic: {:X?}", bytes);
        let function = Decode::decode(&mut bytes.as_slice()).map_err(|e| { warn!("Error decoding call"); e })?;
        Ok(Self { signature, function, version })
    }
}

// implement conversion between extrinsics and RawExtrinsic
impl<Address, Call, Signature, Extra> From<OldExtrinsic<Address, Call, Signature, Extra>> for RawExtrinsic
where
    Extra: SignedExtension,
    Call: ExtractCall + std::fmt::Debug + 'static
{
    fn from(ext: OldExtrinsic<Address, Call, Signature, Extra>) -> RawExtrinsic {
        if ext.signature.is_some() {
            RawExtrinsic::Signed(SignedExtrinsic {
                signature: Vec::new(),
                address: Vec::new(),
                call: Box::new(ext.function),
                version: ext.version
            })
        } else {
            RawExtrinsic::NotSigned(NotSignedExtrinsic {
                call: Box::new(ext.function),
                version: ext.version
            })
        }
    }
}


impl<Address, Call, Signature, Extra> From<UncheckedExtrinsic<Address, Call, Signature, Extra>> for RawExtrinsic
where
    Extra: SignedExtension,
    Call: ExtractCall + std::fmt::Debug + 'static
{
    fn from(ext: UncheckedExtrinsic<Address, Call, Signature, Extra>) -> RawExtrinsic {
        if ext.signature.is_some() {
            RawExtrinsic::Signed(SignedExtrinsic {
                signature: Vec::new(),
                address: Vec::new(),
                call: Box::new(ext.function),
                version: LATEST_TRANSACTION_VERSION
            })
        } else {
            RawExtrinsic::NotSigned(NotSignedExtrinsic {
                call: Box::new(ext.function),
                version: LATEST_TRANSACTION_VERSION
            })
        }
    }
}

#[derive(Debug)]
pub struct SignedExtrinsic {
    pub signature: Vec<u8>,
    pub address: Vec<u8>,
    // pub extra: Extra, // TODO: We don't even collect this yet
    pub call: Box<dyn ExtractCall>,
    version: u8,
}

#[derive(Debug)]
pub struct NotSignedExtrinsic {
    pub call: Box<dyn ExtractCall>,
    pub version: u8,
}

#[derive(Debug)]
pub enum RawExtrinsic {
    Signed(SignedExtrinsic),
    NotSigned(NotSignedExtrinsic),
}

impl RawExtrinsic {

    pub fn database_format<H>(&self, index: i32, header: &H, number: i64) -> Result<DbExtrinsic, Error>
        where H: Header
    {
        match self {
            RawExtrinsic::Signed(ext) => {
                let (module, call) = ext.call.extract_call();
                let res = call.function();

                let (fn_name, params) = if res.is_err() {
                    warn!("Call not found, formatting as raw rust. Call: {:?}", &self);
                    (format!("{:?}", &self), json!({}))
                } else {
                    res?
                };
                Ok(DbExtrinsic::Signed(InsertTransactionOwned {
                    // transaction_hash: Vec::new(),
                    block_num: number,
                    hash: header.hash().as_ref().to_vec(),
                    from_addr: Vec::new(), // TODO
                    to_addr: Some(Vec::new()), // TODO
                    module: module.to_string(),
                    call: fn_name,
                    parameters: Some(params),
                    nonce: 0,
                    tx_index: index,
                    signature: Vec::new(), // TODO
                    transaction_version: i32::from(self.version()),
                }))
            },
            RawExtrinsic::NotSigned(ext) => {
                let (module, call) = ext.call.extract_call();
                let res = call.function();
                let (fn_name, params) = if res.is_err() {
                    debug!("Call not found, formatting as raw rust. Call: {:?}", &self);
                    (format!("{:?}", &self), json!({}))
                } else {
                    res?
                };
                Ok(DbExtrinsic::NotSigned(InsertInherentOwned {
                    hash: header.hash().as_ref().to_vec(),
                    block_num: number,
                    module: module.to_string(),
                    call: fn_name,
                    parameters: Some(params),
                    in_index: index,
                    transaction_version: i32::from(self.version())
                }))
            }
        }
    }

    fn version(&self) -> u8 {
        match self {
            RawExtrinsic::Signed(v) => {
                v.version
            },
            RawExtrinsic::NotSigned(v) => {
                v.version
            }
        }
    }
}

#[derive(Debug)]
pub enum DbExtrinsic {
    Signed(InsertTransactionOwned),
    NotSigned(InsertInherentOwned)
}


pub fn get_extrinsics<T>(
    extrinsics: &[T::Extrinsic],
    header: &T::Header,
    // db: &AsyncDiesel<PgConnection>,
) -> Result<Vec<DbExtrinsic>, Error>
where
    T: System,
{
    extrinsics
        .iter()
        // enumerate is used here to preserve order/index of extrinsics
        .enumerate()
        .map(|(idx, x)| {
            debug!("Decoding Extrinsic in block: {:?}", header.number());
            let decoded: RawExtrinsic = x.to_database()?;
            match &decoded {
                RawExtrinsic::Signed(ext) => {
                    let (module, call) = ext.call.extract_call();
                    let res = call.function();

                    let (fn_name, _params) = if res.is_err() {
                        // warn!("Call not found, formatting as raw rust. Call: {:?}", &self);
                        (format!("{:?}", ext), json!({}))
                    } else {
                        res?
                    };
                    log::trace!("Decoded: {}:{}", module, fn_name);
                },
                RawExtrinsic::NotSigned(ext) => {
                    let (module, call) = ext.call.extract_call();
                    let res = call.function();

                    let (fn_name, _params) = if res.is_err() {
                        // warn!("Call not found, formatting as raw rust. Call: {:?}", &self);
                        (format!("{:?}", ext), json!({}))
                    } else {
                        res?
                    };
                    log::trace!("Decoded: {}:{}", module, fn_name);
                }
            }
            Ok((idx, decoded))
        })
        .collect::<Vec<Result<(usize, RawExtrinsic), Error>>>()
        .into_iter()
        // we don't want to skip over _all_ extrinsics if decoding one extrinsic does not work
        .filter_map(|x: Result<(usize, RawExtrinsic), _>| {
            match x {
                Ok(v) => {
                    let number = (*header.number()).into();
                    let index: i32 = v.0 as i32;
                    Some(v.1.database_format(index, header, number))
                },
                Err(e) => {
                    error!("{:?}", e);
                    None
                }
            }
        })
        .collect::<Result<Vec<DbExtrinsic>, Error>>()
}

impl<Address, Call, Signature, Extra> std::fmt::Debug
    for OldExtrinsic<Address, Call, Signature, Extra>
where
    Address: std::fmt::Debug,
    Call: std::fmt::Debug,
    Extra: SignedExtension,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "UncheckedExtrinsic({:?}, {:?})",
            self.signature.as_ref().map(|x| (&x.0, &x.2)),
            self.function,
        )
    }
}
