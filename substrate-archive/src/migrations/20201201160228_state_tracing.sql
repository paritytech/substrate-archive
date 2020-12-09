CREATE TABLE IF NOT EXISTS state_traces (
	id SERIAL PRIMARY KEY,
	block_num int check (block_num >= 0 and block_num < 2147483647) NOT NULL,
	hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
	target varchar,
	name varchar,
	traces jsonb
);
