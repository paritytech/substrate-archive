CREATE TABLE accounts (
  address bytea NOT NULL PRIMARY KEY,
  free_balance int NOT NULL,
  reserved_balance int NOT NULL,
  account_index bytea NOT NULL,
  nonce int NOT NULL,
  -- hash of block that the account was created in
  create_hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE RESTRICT,
  created bytea NOT NULL,
  updated bytea NOT NULL,
  active bool NOT NULL
);

