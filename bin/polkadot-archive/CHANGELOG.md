# Changelog

All notable changes for polkadot-archive will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- RabbitMq as a required dependency.
- Config option `task_url` to modify RabbitMq Task URL. Also configurable from `AMQP_URL` environment variable.

## [3.0.0] - 2021-06-24
### Changed
- bump polkadot to `v0.9.7`

### Removed
- remove `db_actor_pool_size` as an option from configuration file. ([#254](https://github.com/paritytech/substrate-archive/commit/36d955d379b1fdfb0ff063dce394d8a4d6430323))
- `max_tasks` from toml configuration file. ([#301](https://github.com/paritytech/substrate-archive/commit/f2a4b408123c5e64be04e70a890b1354475d812d))
- `task_workers` from toml configuration file. ([#301](https://github.com/paritytech/substrate-archive/commit/f2a4b408123c5e64be04e70a890b1354475d812d))

## [0.2.4] - 2021-06-02
### Changed
- bump polkadot to `v0.9.3`

## [0.2.3] - 2021-05-06
### Changed
- Config file is now optional. Can configure polkadot archive entirely through environment variables.
  - the environment variables that need to be set are `CHAIN_DATA_DB` and `DATABASE_URL`.
- Polkadot archive will archive `polkadot` by default if the `--chain` CLI option is not passed.

### Removed
- `rpc_url` from the polkadot-archive TOML configuration file


