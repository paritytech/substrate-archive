CREATE TABLE signed_extrinsics (
  transaction_hash bytea PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  from_addr bytea NOT NULL,
  to_addr bytea,
  call varchar NOT NULL,
  -- success bool NOT NULL,
  nonce int check (nonce >= 0) NOT NULL,
  tx_index int check (tx_index >= 0) NOT NULL,
  signature bytea NOT NULL,
  transaction_version int check (transaction_version >= 0) NOT NULL
);
