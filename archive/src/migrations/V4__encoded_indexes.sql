CREATE UNIQUE INDEX only_unique_hash_key_storage ON storage (hash, key, md5(storage));
CREATE INDEX storage_block_num_index ON storage (block_num);
CREATE INDEX blocks_block_num_index ON blocks (block_num);
