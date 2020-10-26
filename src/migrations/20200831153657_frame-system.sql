CREATE TABLE IF NOT EXISTS frame_system (
  block_num int check (block_num >= 0 and block_num < 2147483647) NOT NULL,
  hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  key jsonb NOT NULL,
  value jsonb
);

CREATE INDEX frame_system_block_num_index ON frame_system (block_num);

