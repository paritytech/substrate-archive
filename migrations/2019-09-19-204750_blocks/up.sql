-- The Table for blocks
CREATE TABLE blocks (
  parent_hash bytea NOT NULL,
  hash bytea NOT NULL PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  block bigint check (block >= 0 and block < '9223372036854775807'::bigint) NOT NULL,
  state_root bytea NOT NULL,
  extrinsics_root bytea NOT NULL,
  time timestamp
);
