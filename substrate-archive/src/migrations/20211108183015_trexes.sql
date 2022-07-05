CREATE TABLE IF NOT EXISTS trex (
     id SERIAL NOT NULL,
     hash bytea NOT NULL,
     number int check (number >= 0 and number < 2147483647) NOT NULL,
     cipher bytea,
     account_id bytea[],
     trex_type TEXT NOT NULL,
     release_block_num int check (release_block_num >= 0 and release_block_num < 2147483647),
     difficulty int,
     release_block_difficulty_index TEXT
);