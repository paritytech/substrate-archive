//? Copyright 2018-2019 Parity Technologies (UK) Ltd.
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

//! Decodes items before they are inserted into the DB.
//! Requires that metadata is already present in Postgres.

use super::{database::GetState, ActorPool, VecExtrinsic};
use crate::{database::queries, error::{Result, Error}, types::Extrinsic};
use codec::Encode;
use desub::decoder::Decoder as SubstrateDecoder;
use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};
use xtra::prelude::*;

pub struct Decoder<B: BlockT> {
    decoder: SubstrateDecoder,
    addr: Address<ActorPool<super::DatabaseActor<B>>>,
}

type DbAddr<B> = Address<ActorPool<super::DatabaseActor<B>>>;

impl<B: BlockT + Unpin> Decoder<B>
where
    NumberFor<B>: Into<u32>,
{
    pub fn new(decoder: SubstrateDecoder, addr: DbAddr<B>) -> Self {
        Self { decoder, addr }
    }

    async fn update_metadata(&mut self, spec: &u32) -> Result<()> {
        if self.decoder.has_version(spec) {
            Ok(())
        } else {
            let mut conn = self.addr.send(GetState::Conn.into()).await?.await?.conn();
            let meta = queries::get_metadata(&mut *conn, spec).await?;
            log::debug!("Registering metadata version {}", spec);
            self.decoder.register_version(*spec, meta);
            Ok(())
        }
    }

    async fn metadata_handler(&mut self, meta: crate::types::Metadata) -> Result<()> {
        log::info!("Registering metadata version: {}", meta.version());
        self.decoder.register_version(meta.version(), meta.meta());
        self.addr.send(meta.into()).await?.await;
        Ok(())
    }

    async fn block_handler(&self, blocks: crate::types::BatchBlock<B>) -> Result<()> {
        if blocks.inner().len() > 100_000 {
            log::info!("Decoding {} blocks, this could take a minute", blocks.inner().len());
        }
        
        let extrinsics = blocks
            .inner()
            .iter()
            .map(move |b| {
                let spec = b.spec;
                b.inner.block.extrinsics().iter().map(move |e| {
                    let num: u32 = (*b.inner.block.header().number()).into();
                    let hash = b.inner.block.header().hash().as_ref().to_vec();
                    let decoded = self.decoder.decode_extrinsic(spec, e.encode().as_slice())
                        .map_err(|e| Error::DetailedDecodeFail(e, num, hex::encode(hash.clone())))?;
                    
                    Ok(Extrinsic::new(num, hash, decoded))
                })
            })
            .flatten()
            .collect::<Result<Vec<crate::types::Extrinsic>>>()?;
        self.addr.send(blocks.into()).await?.await;
        self.addr.send(VecExtrinsic(extrinsics).into()).await?.await;
        // sends blocks + decoded extrinsics to database
        Ok(())
    }
}

impl<B: BlockT> Actor for Decoder<B> {}

#[async_trait::async_trait]
impl<B> Handler<crate::types::BatchBlock<B>> for Decoder<B>
where
    B: BlockT + Unpin,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blocks: crate::types::BatchBlock<B>, _: &mut Context<Self>) {
        for block in blocks.inner().iter() {
            if let Err(e) = self.update_metadata(&block.spec).await {
                log::error!("{:?}", e);
            }
        }
        if let Err(e) = self.block_handler(blocks).await {
            log::error!("{}", e.to_string());
        }
    }
}

#[async_trait::async_trait]
impl<B> Handler<crate::types::Metadata> for Decoder<B>
where
    NumberFor<B>: Into<u32>,
    B: BlockT + Unpin,
{
    async fn handle(&mut self, meta: crate::types::Metadata, _: &mut Context<Self>) {
        if let Err(e) = self.metadata_handler(meta).await {
            log::error!("{}", e.to_string());
        }
    }
}

#[async_trait::async_trait]
impl<B> Handler<super::Die> for Decoder<B>
where
    B: BlockT + Unpin,
{
    async fn handle(&mut self, _: super::Die, ctx: &mut Context<Self>) -> Result<()> {
        log::info!("Stopping Decoder");
        ctx.stop();
        Ok(())
    }
}
