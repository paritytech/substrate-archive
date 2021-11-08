CREATE TABLE IF NOT EXISTS extrinsics (
	id SERIAL NOT NULL,
	hash bytea NOT NULL PRIMARY KEY,
	number int check (number >= 0 and number < 2147483647) NOT NULL UNIQUE,
	extrinsics jsonb NOT NULL
)
