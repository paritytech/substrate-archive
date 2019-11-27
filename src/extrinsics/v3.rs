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

use codec::{Decode, Encode, Error as CodecError, Input};
use runtime_primitives::traits::{Extrinsic, SignedExtension};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UncheckedExtrinsicV3<Address, Call, Signature, Extra>
where
    Extra: SignedExtension,
{
    pub signature: Option<(Address, Signature, Extra)>,
    pub function: Call,
}

impl<Address, Call, Signature, Extra> UncheckedExtrinsicV3<Address, Call, Signature, Extra>
where
    Address: Decode,
    Signature: Decode,
    Call: Decode,
    Extra: SignedExtension,
{
    pub fn decode<I: Input>(is_signed: bool, input: &mut I) -> Result<Self, CodecError> {
        Ok(UncheckedExtrinsicV3 {
            signature: if is_signed {
                Some(Decode::decode(input)?)
            } else {
                None
            },
            function: Decode::decode(input)?,
        })
    }
}

impl<Address, Call, Signature, Extra: SignedExtension> Extrinsic
    for UncheckedExtrinsicV3<Address, Call, Signature, Extra>
{
    type Call = Call;

    type SignaturePayload = (Address, Signature, Extra);

    fn is_signed(&self) -> Option<bool> {
        Some(self.signature.is_some())
    }
}

impl<Address, Call, Signature, Extra: SignedExtension> Encode
    for UncheckedExtrinsicV3<Address, Call, Signature, Extra>
where
    Address: Encode,
    Signature: Encode,
    Call: Encode,
    Extra: SignedExtension,
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

//TODO There needs to be a better way than verbatim copying code from substrate...
fn encode_with_vec_prefix<T: Encode, F: Fn(&mut Vec<u8>)>(encoder: F) -> Vec<u8> {
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
