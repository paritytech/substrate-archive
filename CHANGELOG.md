# Changelog

All notable changes for substrate-archive will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [v0.5.2] - 2021-06-02
### Added
- Test for tracing enabled wasm-blobs `v0.9.0`, `v0.9.1`, `v0.9.2`, `v0.9.3` ([#284](https://github.com/paritytech/substrate-archive/pull/284)) ([cd6a446](https://github.com/paritytech/substrate-archive/commit/cd6a446bc66002d1945cbdf0c1b39957218f90fd))
- Unit testing CI workflow ([#288](https://github.com/paritytech/substrate-archive/pull/288)) ([482af68](https://github.com/paritytech/substrate-archive/commit/482af68fff515a7e3a34ee0c512d735790193cd6))

### Changed
- Clarify release checklist ([#279](https://github.com/paritytech/substrate-archive/pull/279)) ([9abef6e](https://github.com/paritytech/substrate-archive/commit/9abef6e2bdda4c1492b6e232ec38c8c0d59a3749)) && ([#288](https://github.com/paritytech/substrate-archive/pull/288)) ([482af68](https://github.com/paritytech/substrate-archive/commit/482af68fff515a7e3a34ee0c512d735790193cd6))
- Update dependencies to match runtime `0.9.3`. 
- Refactor tracing to work with the latest tracing changes in substrate ([#273](https://github.com/paritytech/substrate-archive/pull/273)) ([b322ded](https://github.com/paritytech/substrate-archive/commit/b322ded5cf683270da6d21478e80c9f4dba706dc))

### Fixed
- Re-Compile WASM Runtime v0.8.30 with rust compiler version nightly-02-27-2021 to fix 'Storage Root Mismatch' when syncing.
- Fixed wasm runtimes so their names are uniform.

## [v0.5.1] - 2021-05-06
### Added
- Release checklist

### Changed
- Pinned to `substrate`/`polkadot` release v0.8.30
- Keeping the `substrate-archive` and `substrate-archive-backend` crates's versions aligned.

## [v0.5.0] - 2021-03-29
### Added
- `ArchiveBuilder` struct for constructing the indexer.
	- returns a trait, `Archive` that manages the underlying actor runtime
- Archive now reads the `CHAIN_DATA_DB` environment variable if the path to the backend chain database is not passed directly.
- `max_block_load` to configure the maximum number of blocks loaded at once
- Two new options to the configuration relative to state tracing:
	- `targets` for specifying runtime targets to trace in WASM
	- `folder` where WASM blobs with tracing enabled are kept.
	- More on state-tracing [here](https://github.com/paritytech/substrate-archive/wiki/6.\)-State-Tracing-&-Balance-Reconciliation)
- Runtime versions between blocks are cached, speeding up block indexing.
- Notification stream from Postgres indexes storage changes in the background.

### Changed
- no longer need to instantiate a client and manually pass it to the Archive
- rename `run_with` to `drive`
- Archive now accepts a postgres URL instead of a postgres URL split into its parts.
- Archive will take Postgres URL from environment variable `DATABASE_URL` as a fallback.
- Use SQLX for migrations
	- SQLX will create a new table in the database, `sqlx_migrations` or similiar.
	- the older table, `__refinery_migrations` can be safely dropped
- Print out the Postgres URL at startup
- config is now separated into sections for readability. Migration is manual but looking at the new `archive.conf` in `polkadot-archive` or `node-template-archive` folders should help.
- Postgres SQL queries are now type checked
- Refactor file layout to `substrate-archive` and `substrate-archive-backend`.
- Decouple Database actors
- upgrade to SQLx 0.5.0
- Overhaul of block indexing. Now uses a Iterator to only collect batches of blocks from the database,
	taking advantage of the better reading performance of sequential data access. Gathering blocks by RPC is no longer done.
- speed up query for difference between the storage and blocks table -
- switch to a leaner actor framework, [xtra](https://github.com/Restioson/xtra)
- switch to a persistant background-task-queue for executing blocks, [coil](https://github.com/insipx/coil).
- batch inserts no longer starve postgres pool of connections
- Switch substrate-archive `LocalCallExecutor` with Substrate's `LocalCallExecutor`

### Removed
- the last frame dependency, `frame-system`. Archive now relies only on generic traits defined in substrate-core.
- RPC URL. An RPC URL is no longer required to function.

## [v0.4.0] - 2021-01-24
### Added
- integrate database migrations into substrate-archive library

### Changed
- Speed up Storage Indexing by re-executing blocks
- refactored error types

### Removed
- dependencies for service and backend

### Fixed
- storage inserts

## [v0.3.1]
### Added
- node-template-archive

## [v0.3.0]
### Added
- Create a CLI for indexing kusama
- New PostgreSQL Schema
- Actors to model dataflow

### Changed
- Use a rocksdb-backed substrate client instead of RPC for indexing

