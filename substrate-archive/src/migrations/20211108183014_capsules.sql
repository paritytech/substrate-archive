CREATE TABLE IF NOT EXISTS capsules (
     id SERIAL NOT NULL,
     hash bytea NOT NULL,
     number int check (number >= 0 and number < 2147483647) NOT NULL,
     cipher bytea,
     account_id bytea[],
     capsule_type TEXT NOT NULL,
     release_number int check (release_number >= 0 and release_number < 2147483647)
);