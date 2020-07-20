# **[Unreleased]**
- Use SQLX for migrations
  - this will create a new table in the database, `sqlx_migrations` or similiar.
    - the older table, `__refinery_migrations` can be safely dropped

## Api Changes
- New `ArchiveBuilder` struct for constructing the indexer.
  - returns a trait rather than a struct.
  - the returned trait,`Archive` manages the underlying actor runtime
  - Support Smol, and Async-Std in addition to Tokio
  - Archive must now be spawned manually from the chosen runtime
  - no longer need to instantiate a client and manually pass it to the Archive
  - rename `run_with` to `run`

## Internal Changes
- [QoL] upgrade to SQLx 0.4.0
- [perf] better queries for the set difference between the storage and blocks table
   - makes querying for missing storage more efficient
- [perf] better algorithm for the background-task missing-blocks generator
- [err] Better handling of SQL errors
- [perf] switch to a leaner, 'lower-level' actor framework (xtra) 
- more unified handling of threadpools
  - [perf] faster indexing/execution of blocks by throttling the amount of work threadpools handle at any given time.
- [QoL] remove the last frame dependency, `frame-system`. Archive now relies only on generic traits defined in substrate-core.
- new method of batch inserts avoids starving the Postgres Pool of connections




#[v0.4.0]
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
