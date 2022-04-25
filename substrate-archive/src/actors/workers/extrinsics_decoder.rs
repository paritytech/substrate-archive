// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use arc_swap::ArcSwap;
use async_std::task;
use itertools::Itertools;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use xtra::prelude::*;

use desub::Decoder;
use serde_json::Value;

use crate::{
	actors::{
		workers::database::{DatabaseActor, GetState},
		SystemConfig,
	},
	database::{models::CapsuleModel, models::ExtrinsicsModel, queries},
	error::{ArchiveError, Result},
	types::{BatchCapsules, BatchExtrinsics},
};

use serde::Deserialize;

/// Actor which crawls missing encoded extrinsics and
/// sends decoded JSON to the database.
/// Crawls missing extrinsics upon receiving an `Index` message.
pub struct ExtrinsicsDecoder {
	/// Pool of Postgres Connections.
	pool: PgPool,
	/// Address of the database actor.
	addr: Address<DatabaseActor>,
	/// Max amount of extrinsics to load at any one time.i.
	max_block_load: u32,
	/// Desub Legacy + current decoder.
	decoder: Arc<Decoder>,
	/// Cache of blocks where runtime upgrades occurred.
	/// number -> spec
	upgrades: ArcSwap<HashMap<u32, u32>>,
}

#[derive(Deserialize, Debug)]
struct AccountId(Vec<Vec<u8>>);

#[derive(Deserialize, Debug)]
struct CipherText(Vec<u8>);

#[derive(Deserialize, Debug)]
struct Ciphers(Vec<Cipher>);

#[derive(Deserialize, Debug)]
struct Cipher{
	cipher_text:Vec<u8>,
	difficulty:u32,
	release_block_num:u32
}

impl ExtrinsicsDecoder {
	pub async fn new<B: Send + Sync, Db: Send + Sync>(
		config: &SystemConfig<B, Db>,
		addr: Address<DatabaseActor>,
	) -> Result<Self> {
		let max_block_load = config.control.max_block_load;
		let pool = addr.send(GetState::Pool).await??.pool();
		// let chain = config.persistent_config.chain();
		// let decoder = Arc::new(Decoder::new(chain));
		let decoder = Arc::new(Decoder::new());
		let mut conn = pool.acquire().await?;
		let upgrades = ArcSwap::from_pointee(queries::upgrade_blocks_from_spec(&mut conn, 0).await?);
		log::info!("Started extrinsic decoder");
		Ok(Self { pool, addr, max_block_load, decoder, upgrades })
	}

	async fn crawl_missing_extrinsics(&mut self) -> Result<()> {
		let mut conn = self.pool.acquire().await?;
		let blocks = queries::blocks_missing_extrinsics(&mut conn, self.max_block_load).await?;

		let versions: Vec<u32> =
			blocks.iter().filter(|b| !self.decoder.has_version(&b.3)).map(|(_, _, _, v)| *v).unique().collect();
		// above and below line are separate to let immutable ref to `self.decoder` to go out of scope.
		for version in versions.iter() {
			let metadata = queries::metadata(&mut conn, *version as i32).await?;
			log::debug!("Registering version {}", version);
			Arc::get_mut(&mut self.decoder)
				.ok_or_else(|| ArchiveError::Msg("Reference to decoder is not safe to access".into()))?
				.register_version(*version, &metadata)?;
		}

		if let Some(first) = versions.first() {
			if let (Some(past), _, Some(past_metadata), _) =
				queries::past_and_present_version(&mut conn, *first as i32).await?
			{
				Arc::get_mut(&mut self.decoder)
					.ok_or_else(|| ArchiveError::Msg("Reference to decoder is not safe to access".into()))?
					.register_version(past, &past_metadata)?;
				log::debug!("Registered previous version {}", past);
			}
		}

		if self.upgrades.load().iter().max_by(|a, b| a.1.cmp(b.1)).map(|(_, v)| v)
			< blocks.iter().map(|&(_, _, _, v)| v).max().as_ref()
		{
			self.update_upgrade_blocks().await?;
		}
		let decoder = self.decoder.clone();
		let upgrades = self.upgrades.load().clone();

		let extrinsics_tuple =
			task::spawn_blocking(move || Ok::<_, ArchiveError>(Self::decode(&decoder, blocks, &upgrades))).await??;

		let extrinsics = extrinsics_tuple.0;
		self.addr.send(BatchExtrinsics::new(extrinsics)).await?;

		//send batch capsules to DatabaseActor
		let capsules = extrinsics_tuple.1;
		self.addr.send(BatchCapsules::new(capsules)).await?;

		Ok(())
	}

	fn decode(
		decoder: &Decoder,
		blocks: Vec<(u32, Vec<u8>, Vec<u8>, u32)>,
		upgrades: &HashMap<u32, u32>,
	) -> Result<(Vec<ExtrinsicsModel>, Vec<CapsuleModel>)> {
		let mut extrinsics = Vec::new();
		let mut capsules = Vec::new();
		if blocks.len() > 2 {
			let first = blocks.first().expect("Checked len; qed");
			let last = blocks.last().expect("Checked len; qed");
			log::info!(
				"Decoding {} extrinsics in blocks {}..{} versions {}..{}",
				blocks.len(),
				first.0,
				last.0,
				first.3,
				last.3
			);
		}
		for (number, hash, ext, spec) in blocks.into_iter() {
			if let Some(version) = upgrades.get(&number) {
				let previous = upgrades
					.values()
					.sorted()
					.tuple_windows()
					.find(|(_curr, next)| *next >= version)
					.map(|(c, _)| c)
					.ok_or(ArchiveError::PrevSpecNotFound(*version))?;
				let ext1 = decoder.decode_extrinsics(*previous, ext.as_slice())?;
				extrinsics.push(ExtrinsicsModel::new(hash.to_owned(), number, ext1.to_owned())?);

				//construct capsule list for batch
				Self::construct_capsules(&number, &hash, &ext1, &mut capsules);
			} else {
				let ext1 = decoder.decode_extrinsics(spec, ext.as_slice())?;
				extrinsics.push(ExtrinsicsModel::new(hash.to_owned(), number, ext1.to_owned())?);

				//construct capsule list for batch
				Self::construct_capsules(&number, &hash, &ext1, &mut capsules);
			}
		}
		Ok((extrinsics, capsules))
	}

	//construct capsule list for batch
	fn construct_capsules(number: &u32, hash: &Vec<u8>, ext: &Value, capsules: &mut Vec<CapsuleModel>) {
		if ext.is_array() {
			let extrinsics = ext.as_array().unwrap();
			for extrinsic in extrinsics {
				if extrinsic.is_object() {
					//This operation will always succeed,because the type has been determined.
					let extrinsic_map = extrinsic.as_object().unwrap();
					//Do not exclude empty string.
					let pallet_name = extrinsic_map["call_data"]["pallet_name"].as_str().unwrap_or("");
					let arguments = extrinsic_map["call_data"]["arguments"].to_owned();
					match pallet_name {
						"Timestamp" => {
							// TODO:Extrinsic of timestamp type to be determined,If is needed.
						}
						"CapsuleModule" => {
							// get account_id from arguments[0]
							let account_id_struct: Option<AccountId> =
								serde_json::from_value(arguments[0].to_owned()).unwrap_or(None);
							let account_id = match account_id_struct {
								Some(value) => Some(value.0),
								None => None,
							};

							// get cipher_text from arguments[1]
							let option_cipher_text_struct:Option<CipherText> = serde_json::from_value(arguments[1].to_owned()).unwrap_or(None);
							// get json string from matching chiper_text
							let ciphers_json_str = match option_cipher_text_struct {
								Some(cipher_text_struct) => {
									// fmt utf8 string to String
									let cipher_json_str_result = String::from_utf8(cipher_text_struct.0);
									let cipher_json_str = match cipher_json_str_result {
										Ok(ciphers_json) => ciphers_json,
										Err(_) => "".to_string()
									};
									cipher_json_str
								}
								None => "".to_string()
							};
							// Deserialize to Ciphers struct
							let option_ciphers:Option<Ciphers> = serde_json::from_str(&ciphers_json_str).unwrap_or(None);
							// get Vec<Cipher> from Ciphers struct tuple's first object
							let cipher_vector = match option_ciphers {
								Some(ciphers_struct) => {
									ciphers_struct.0
								}
								None => {
									vec![]
								}
							};

							//Traverse the vector and construct the CapsuleModel
							for cipher in cipher_vector{
								let cipher_text = cipher.cipher_text;
								let release_block_number = cipher.release_block_num;
								let difficulty = cipher.difficulty;

								let capsule_model_result = CapsuleModel::new(
									hash.to_vec(),
									number.to_owned(),
									Option::from(cipher_text),
									account_id.to_owned(),
									&pallet_name,
									Option::from(release_block_number),
									difficulty
								);
								match capsule_model_result {
									Ok(capsule_model) => {
										capsules.push(capsule_model);
									}
									Err(_) => {
										log::debug! {"Construct capsule model failed!"};
									}
								}
							}
						}
						_ => {
							log::debug! {"Other type of Extrinsic!"};
						}
					}
				}
			}
		}
	}

	async fn update_upgrade_blocks(&self) -> Result<()> {
		let max_spec = *self.upgrades.load().iter().max_by(|a, b| a.1.cmp(b.1)).map(|(k, _)| k).unwrap_or(&0);
		let mut conn = self.pool.acquire().await?;
		let upgrade_blocks = queries::upgrade_blocks_from_spec(&mut conn, max_spec).await?;
		self.upgrades.rcu(move |upgrades| {
			let mut upgrades = HashMap::clone(upgrades);
			upgrades.extend(upgrade_blocks.iter());
			upgrades
		});
		Ok(())
	}
}

#[async_trait::async_trait]
impl Actor for ExtrinsicsDecoder {}

pub struct Index;
impl Message for Index {
	type Result = ();
}

#[async_trait::async_trait]
impl Handler<Index> for ExtrinsicsDecoder {
	async fn handle(&mut self, _: Index, ctx: &mut Context<Self>) {
		match self.crawl_missing_extrinsics().await {
			Err(ArchiveError::Disconnected) => ctx.stop(),
			Ok(_) => {}
			Err(e) => log::error!("{:?}", e),
		}
	}
}
