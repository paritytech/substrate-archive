-- Add migration script here
ALTER TABLE blocks
	ADD justification bytea;
