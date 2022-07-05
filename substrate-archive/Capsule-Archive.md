# trex-archive

#### 1.First, solve the problem of creating the transaction table in the PostgreSQL database.
Table Name:trexes

Table design: 
```sql
CREATE TABLE IF NOT EXISTS extrinsics (
	id SERIAL NOT NULL,
	hash bytea NOT NULL PRIMARY KEY,
	number int check (number >= 0 and number < 2147483647) NOT NULL UNIQUE,
	extrinsics jsonb NOT NULL
);
```

> Refer to substrate-archive/src/database/models.rs as below:
```rust
#[derive(Debug, Serialize, FromRow)]
pub struct TrexModel{
	pub id: Option<i32>,
	pub hash: Vec<u8>,
	pub number: u32,
	pub cipher:Option<Vec<u8>>,
	pub account_id:Option<Vec<Vec<u8>>>,
	pub trex_type:String,
	pub release_number: Option<u32>
}


impl TrexModel {
	pub fn new(block_id: Vec<u8>, block_num: u32, cipher: Option<Vec<u8>>, account_id:Option<Vec<Vec<u8>>>, trex_type:Vec<u8>, release_number:Option<u32>) -> Result<Self>{
		let block_id = block_id.try_into().unwrap_or(vec![]);
		let block_num = block_num.try_into().unwrap_or(0u32);
		let trex_type = String::from_utf8(trex_type).unwrap_or(String::from(""));
		Ok(Self{id: None, hash:block_id, number:block_num,cipher,account_id,trex_type,release_number})
	}
}
```

#### 2.Resolve type addition in defined type.
> Refer to substrate-archive/src/types.rs as below:

```rust
#[derive(Debug)]
pub struct  BatchTrexes {
	pub inner: Vec<TrexModel>,
}

impl BatchTrexes {
	pub fn new(trexes: Vec<TrexModel>) -> Self { Self { inner: trexes}}

	pub fn inner(self) -> Vec<TrexModel> { self.inner}

	pub fn len(&self) -> usize { self.inner.len() }
}

impl Message for BatchTrexes {
	type Result = ();
}
```

#### 3.Because the fetching of the data source of externalics is homologous with the data of trexes, in order to avoid repeated fetching, the scheme is to organize Vec<TrexModel> in the actor of ExtrinsicsDecoder.
> Refer to substrate-archive/src/actors/workers/extrinsics_decoder.rs -> fn crawl_missing_extrinsics as below:
```rust
let extrinsics_tuple = task::spawn_blocking(move || Ok::<_, ArchiveError>(Self::decode(&decoder, blocks, &upgrades))).await??;

let extrinsics= extrinsics_tuple.0;
self.addr.send(BatchExtrinsics::new(extrinsics)).await?;

//send batch trexes to DatabaseActor
let trexes = extrinsics_tuple.1;
self.addr.send(BatchTrexes::new(trexes)).await?;
```
> This operation will send extrinsics and trexes to DatabaseActor.

#### 4.The organized Vec< TrexModel > needs to be sent to DatabaseActor for processing.
> Refer to substrate-archive/src/actors/workers/database.rs as below:
```rust
#[async_trait::async_trait]
impl Handler<BatchTrexes> for DatabaseActor {
	async fn handle(&mut self, trexes: BatchTrexes, _: &mut Context<Self>) {
		let len = trexes.len();
		let now = std::time::Instant::now();
		if let Err(e) = self.db.insert(trexes.inner()).await {
			log::error!("{}", e.to_string());
		}
		log::debug!("took {:?} to insert {} trexes", now.elapsed(), len);
	}
}
```

#### 5.Implement the insert trait for TrexModel struct.
> Refer to substrate-archive/src/database.rs as below:
```rust
#[async_trait::async_trait]
impl Insert for Vec<TrexModel> {
	async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
		let mut batch = Batch::new(
			"trexes",
			r#"
			INSERT INTO "trexes" (
				hash, number, cipher, account_id, trex_type, release_number
			) VALUES
			"#,
			r#"
			ON CONFLICT DO NOTHING
			"#,
		);
		for trex in self.into_iter() {
			batch.reserve(6)?;
			if batch.current_num_arguments() > 0 {
				batch.append(",");
			}
			batch.append("(");
			batch.bind(trex.hash)?;
			batch.append(",");
			batch.bind(trex.number)?;
			batch.append(",");
			batch.bind(trex.cipher)?;
			batch.append(",");
			batch.bind(trex.account_id)?;
			batch.append(",");
			batch.bind(trex.trex_type)?;
			batch.append(",");
			batch.bind(trex.release_number)?;
			batch.append(")");
		}
		Ok(batch.execute(conn).await?)
	}
}
```
