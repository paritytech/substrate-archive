-- The Table for blocks
CREATE TABLE blocks (
  parent_hash bytea NOT NULL,
  hash bytea NOT NULL PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u32 in rust
  block bigint check (block >= 0 and block < '4294967296'::bigint) NOT NULL,
  state_root bytea NOT NULL,
  extrinsics_root bytea NOT NULL,
  time timestamp
);
