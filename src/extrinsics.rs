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

use log::{error, trace};
use runtime_primitives::{
    traits::SignedExtension
};
use codec::{Encode, Decode, Input, Error as CodecError};
use crate::types::{ExtrinsicExt, ExtractCall};

impl<Address, Call, Signature, Extra> ExtrinsicExt for OldExtrinsic<Address, Call, Signature, Extra>
where
    Signature: Into<Vec<u8>>,
    Call: ExtractCall + Clone
{
    type Address = Address;
    type Extra = Extra;

    fn signature(&self) -> Option<(Self::Address, Vec<u8>, Self::Extra)> {
        if self.signature.is_some() {
            let sig = self.signature.expect("Checked for some; qed");
            Some((sig.0, Vec::new(), sig.2)) // TODO: get actual signature
        } else {
            None
        }
    }

    fn call(&self) -> Box<dyn ExtractCall> {
        Box::new(self.call.clone())
    }
}

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

        trace!("VERSION: {}", version);
        Ok(Self {
            signature: if is_signed { Some(Decode::decode(input).map_err(|e| { error!("Error decoding signature"); e })?) } else { None },
            function: Decode::decode(input).map_err(|e| { error!("Error decoding call"); e })?,
            version,
        })
    }
}

/*
impl<Address, Call, Signature, Extra> Encode
	for OldExtrinsic<Address, Call, Signature, Extra>
where
	Address: Encode,
	Signature: Encode,
	Call: Encode,
	Extra: SignedExtension,
{
	fn encode(&self) -> Vec<u8> {
		super::encode_with_vec_prefix::<Self, _>(|v| {
			// 1 byte version id.
			match self.signature.as_ref() {
				Some(s) => {
					v.push(TRANSACTION_VERSION | 0b1000_0000);
					s.encode_to(v);
				}
				None => {
					v.push(TRANSACTION_VERSION & 0b0111_1111);
				}
			}
			self.function.encode_to(v);
		})
	}
}
*/
