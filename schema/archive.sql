CREATE TABLE IF NOT EXISTS blocks (
  id SERIAL NOT NULL,
  parent_hash bytea NOT NULL,
  hash bytea NOT NULL PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  -- FIXME: make block_num an integer
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL UNIQUE,
  state_root bytea NOT NULL,
  extrinsics_root bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS inherents (
  -- a PostgreSQL-specific id. Does not exist on-chain
  id SERIAL PRIMARY KEY,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL,
  module varchar NOT NULL,
  call varchar NOT NULL,
  parameters jsonb,
  -- success bool NOT NULL,
  in_index int check (in_index >= 0) NOT NULL,
  transaction_version int check (transaction_version >= 0) NOT NULL
);

CREATE TABLE IF NOT EXISTS signed_extrinsics (
  id SERIAL PRIMARY KEY,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL,
  -- transaction_hash bytea PRIMARY KEY,
  from_addr jsonb NOT NULL,
  -- to_addr bytea,
  module varchar NOT NULL,
  call varchar NOT NULL,
  parameters jsonb NOT NULL,
  -- success bool NOT NULL,
  -- nonce int check (nonce >= 0) NOT NULL,
  tx_index int check (tx_index >= 0) NOT NULL,
  signature jsonb NOT NULL,
  extra jsonb,
  transaction_version int check (transaction_version >= 0) NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
  address bytea NOT NULL PRIMARY KEY,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  free_balance bigint check (free_balance >= 0 and free_balance < '9223372036854775807'::bigint) NOT NULL,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  reserved_balance bigint check (reserved_balance >= 0 and reserved_balance < '9223372036854775807'::bigint) NOT NULL,
  account_index bytea NOT NULL,
  nonce int check (nonce >= 0) NOT NULL,
  -- hash of block that the account was created in
  create_hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  created bigint check (created >= 0 and created < '9223372036854775807'::bigint) NOT NULL,
  -- a constrained biginteger type whose max value corresponds with that of a u64 in rust
  updated bigint check (updated >= 0 and created < '9223372036854775807'::bigint) NOT NULL,
  active bool NOT NULL
);

CREATE TABLE IF NOT EXISTS storage (
  id SERIAL PRIMARY KEY,
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  module varchar NOT NULL, 
  function varchar NOT NULL,
  parameters jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id SERIAL PRIMARY KEY,
  block_num bigint check (block_num >= 0 and block_num < '9223372036854775807'::bigint) NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  module varchar NOT NULL,
  event varchar NOT NULL,
  parameters jsonb NOT NULL 
);
