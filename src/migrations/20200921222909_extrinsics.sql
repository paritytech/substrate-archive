CREATE TABLE IF NOT EXISTS extrinsics (
  block_num int check (block_num >= 0 and block_num < 2147483647) NOT NULL REFERENCES blocks(block_num) ON DELETE CASCADE ON UPDATE CASCADE,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  call_name varchar NOT NULL,
  call_module varchar NOT NULL,
  signature jsonb,
  args jsonb
);

