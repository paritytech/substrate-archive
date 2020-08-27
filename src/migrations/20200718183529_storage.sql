CREATE TABLE IF NOT EXISTS storage (
  id SERIAL PRIMARY KEY,
  block_num int check (block_num >= 0 and block_num < 2147483647) NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  is_full boolean NOT NULL,
  key bytea NOT NULL,
  storage bytea
);


