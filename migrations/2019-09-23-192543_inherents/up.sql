CREATE TABLE inherents (
  -- a PostgreSQL-specific id. Does not exist on-chain
  id SERIAL PRIMARY KEY,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL,
  module varchar NOT NULL,
  call varchar NOT NULL,
  parameters bytea,
  -- success bool NOT NULL,
  in_index int check (in_index >= 0) NOT NULL,
  transaction_version int check (transaction_version >= 0) NOT NULL
);
