CREATE TABLE signed_extrinsics (
  transaction_hash bytea PRIMARY KEY,
  block bytea NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE RESTRICT,
  from_addr bytea NOT NULL,
  to_addr bytea,
  call varchar NOT NULL,
  success bool NOT NULL,
  nonce int NOT NULL,
  tx_index int NOT NULL,
  signature bytea NOT NULL
);
