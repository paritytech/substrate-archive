# **[Unreleased]**

## Substrate Archive
- [Changed] Use SQLX for migrations
  - this will create a new table in the database, `sqlx_migrations` or similiar.
    - the older table, `__refinery_migrations` can be safely dropped

### Api Changes
- [Added] New `ArchiveBuilder` struct for constructing the indexer.
  - [Changed] returns a trait rather than a struct.
    - [Changed] the returned trait,`Archive` manages the underlying actor runtime
  - [Changed] no longer need to instantiate a client and manually pass it to the Archive
  - [Changed] rename `run_with` to `drive`
  - [Changed] Archive now accepts just a postgres URL instead of a postgres URL split into its parts. This should
  make configuring the archive more straightforward. Takes from environment variable `DATABASE_URL` if not passed to the 
  archive directly
- [Added] Archive now reads the `CHAIN_DATA_DB` environment variable if the path to the backend chain database is not passed directly.
- [Changed] `drive` changed from sync to async function
- [Removed] Archive no longer needs an RPC url to function

### Internal Changes
- [QoL] upgrade to SQLx 0.4.0
- [perf] Overhaul of block indexing. Now uses a Iterator to only collect batches of blocks from the database, 
taking advantage of sequential read-speeds. Gathering blocks by RPC is no longer done.
  - [perf] a new module `runtime_version_cache` is introduced in order to cache and run a binary search on runtime version & blocks.
- [perf] better queries for the set difference between the storage and blocks table
   - makes querying for missing storage more efficient
- [err] Better handling of SQL errors
- [perf] switch to a leaner, 'lower-level' actor framework (xtra) 
- [perf] switch to a background-task-queue for executing blocks. This uses significantly less memory and
  persists blocks that need to be executed on-disk.
- [QoL] remove the last frame dependency, `frame-system`. Archive now relies only on generic traits defined in substrate-core.
- new method of batch inserts avoids starving the Postgres Pool of connections

## Polkadot Archive
- [Changed] Config file is now optional. Can configure polkadot archive entirely through environment variables.
  - the environment variables that need to be set are `CHAIN_DATA_DB` and `DATABASE_URL`. 
- [Changed] Polkadot archive will archive `polkadot` by default if the `--chain` CLI option is not passed.
- [Changed] remove `rpc_url` from the polkadot-archive TOML configuration file
- [Changed] All options in config file apart from `db_url`.



# [v0.4.0]
- Speed up Storage Indexing by re-executing blocks
- [internal] Remove some depdendencies for service and backend
- fixes for storage inserts
- refactored error types
- integrate database migrations into substrate-archive library

# [v0.3.1]
- add node-template-archive

# **[v0.3.0]**
- Use a rocksdb-backed substrate client instead of RPC for indexing
- Create a CLI for indexing kusama
- New PostgreSQL Schema 
- Actors to model dataflow
