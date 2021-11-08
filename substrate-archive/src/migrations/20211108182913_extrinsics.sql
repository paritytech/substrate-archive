CREATE TABLE IF NOT EXISTS extrinsics (
	id SERIAL NOT NULL,
	hash bytea NOT NULL PRIMARY KEY,
	number int check (block_num >= 0 and block_num < 2147483647) NOT NULL UNIQUE,
	extrinsics jsonb NOT NULL
)
