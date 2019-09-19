# Substrate Archive Node
## Currently Under Development! (Or, Use at your own Risk)

Run this alongside the substrate client to sync *all* historical TxData. Allows
you to run queries on a PostgreSQL (Kafka?) database.

The (so far experimental) schema for the PostgreSQL database is described in the Pdf File at the root of this directory

## Required External Dependencies
- PostgreSQ

### Developing
Init script should install all dependencies and setup a default database user for you. Just make sure to change the password of user `archive` via psql in a production or security-sensitive environment.

Required Dependencies:
Ubuntu: `postgresql`, `postgresql-contrib`, `libpq-dev`
Rust: `diesel_cli`

