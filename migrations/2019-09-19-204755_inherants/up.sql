CREATE TABLE inherants (
  id SERIAL PRIMARY KEY,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE RESTRICT,
  block bytea NOT NULL,
  module varchar NOT NULL,
  CALL varchar NOT NULL,
  success bool NOT NULL,
  in_index int NOT NULL
);
