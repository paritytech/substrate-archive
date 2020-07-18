-- Add migration script here
CREATE TABLE metadata (
  version integer NOT NULL PRIMARY KEY,
  meta bytea NOT NULL
);



