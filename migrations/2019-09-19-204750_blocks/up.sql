-- The Table for blocks
CREATE TABLE blocks (
  id SERIAL NOT NULL,
  parent_hash bytea NOT NULL,
  hash bytea NOT NULL PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL UNIQUE,
  state_root bytea NOT NULL,
  extrinsics_root bytea NOT NULL
  -- time timestamptz
);
