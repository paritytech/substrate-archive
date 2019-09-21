-- The Table for blocks
CREATE TABLE blocks (
  parent_hash bytea NOT NULL,
  hash bytea NOT NULL PRIMARY KEY,
  block int NOT NULL,
  state_root bytea NOT NULL,
  extrinsics_root bytea NOT NULL,
  time timestamp
);
