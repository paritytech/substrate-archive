# Substrate Archive Node

Run this alongside the substrate client to sync *all* historical TxData. Allows
you to run queries on a PostgreSQL (Kafka?) database.

## Currently Under Development!

As of now,`Runtime` is used throughout as a concrete type. Eventually, this will be replaced with `T: System`. It is runtime now for ease of testing.


