CREATE TABLE IF NOT EXISTS metadata (
  version integer NOT NULL PRIMARY KEY,
  meta bytea NOT NULL
);

-- TODO create an index on blocks
-- TODO: change block_num => integer ( We are not getting to > then 2Bil blocks anytime soon)
CREATE TABLE IF NOT EXISTS blocks (
  id SERIAL NOT NULL,
  parent_hash bytea NOT NULL,
  hash bytea NOT NULL PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  -- FIXME: make block_num an integer
  block_num int check (block_num >= 0 and block_num < 2147483647) NOT NULL UNIQUE,
  state_root bytea NOT NULL,
  extrinsics_root bytea NOT NULL,
  digest bytea NOT NULL,
  ext bytea NOT NULL,
  spec integer NOT NULL REFERENCES metadata(version) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS storage (
  id SERIAL PRIMARY KEY,
  block_num int check (block_num >= 0 and block_num < 2147483647) NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  is_full boolean NOT NULL,
  key bytea NOT NULL,
  storage bytea
  -- CONSTRAINT m_hash_key_storage UNIQUE (hash, key, md5(storage))
);

CREATE UNIQUE INDEX only_unique_hash_key_storage ON storage (hash, key, md5(storage));

