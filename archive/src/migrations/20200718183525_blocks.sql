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
  spec integer NOT NULL REFERENCES metadata(version)
);

