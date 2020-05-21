CREATE TABLE IF NOT EXISTS metadata (
  version integer NOT NULL PRIMARY KEY,
  meta bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
  id SERIAL NOT NULL,
  parent_hash bytea NOT NULL,
  hash bytea NOT NULL PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  -- FIXME: make block_num an integer
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL UNIQUE,
  state_root bytea NOT NULL,
  extrinsics_root bytea NOT NULL,
  spec integer NOT NULL REFERENCES metadata(version) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS extrinsics (
  id SERIAL PRIMARY KEY,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  spec integer NOT NULL REFERENCES metadata(version) ON DELETE CASCADE ON UPDATE CASCADE,
  index integer NOT NULL,
  ext bytea NOT NULL -- the raw extrinsic payload
);

CREATE TABLE IF NOT EXISTS storage (
  id SERIAL PRIMARY KEY,
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  is_full boolean NOT NULL,
  key bytea NOT NULL,
  storage bytea,
  UNIQUE (hash, key, storage)
);
