CREATE TABLE inherants (
  -- a PostgreSQL-specific id. Does not exist on-chain
  id SERIAL PRIMARY KEY,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE RESTRICT,
  -- a constrained biginteger type whose max value corresponds with that of a u32 in rust
  block bigint check (block >= 0 and block < '4294967296'::bigint) NOT NULL,
  module varchar NOT NULL,
  CALL varchar NOT NULL,
  success bool NOT NULL,
  in_index int check (in_index >= 0) NOT NULL
);
