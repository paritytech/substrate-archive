CREATE TABLE signed_extrinsics (
  transaction_hash bytea PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u32 in rust
  block bigint check (block >= 0 and block < '4294967296'::bigint) NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE RESTRICT,
  from_addr bytea NOT NULL,
  to_addr bytea,
  call varchar NOT NULL,
  success bool NOT NULL,
  nonce int check (nonce >= 0) NOT NULL,
  tx_index int check (block >= 0) NOT NULL,
  signature bytea NOT NULL
);
