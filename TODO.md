# TODO

* [x] Add --engine argument
* [x] Factor out the query function to query(prql, from, to)
* [x] Switch to camino
* [x] Rename --engine to --backend
* [x] Add support from .prql files
* [x] Add DataFusion support
* [x] Add DF writer support (csv, parquet, json)
* [x] Make DuckDB an optional feature
* [x] Add logging and verbosity for debugging
* [x] Add --no-exec option
* [x] Allow multiple --from options with alias naming
* [x] Cleanup multiple --from code and enable
* [x] Reenable DuckDB backend for multiple sources
* [x] Add support for environment variables eg PQ_FROM_EMPLOYEES="employees.csv" -> `from employees="employees.csv"
* [x] Add formatted table output to DuckDB backend
* [x] Use an Enum for the output format checks
* [x] Make --sql an option for SQL query support
* [x] Add Github Actions to build binary artefacts
* [x] Add chinook example data
* [x] Add leading examples
* [x] Add support for DuckDB database files
* [x] Add support for PostgreSQL through DuckDB
* [x] Add support for SQLite through DuckDB
* [x] Add support for ?currentSchema=schema option postgres URI
* [x] Publish to crates.io
* [x] Publish to DockerHub/ghcr.io
* [x] Publish to homebrew
* [ ] Add tests
* [ ] Add pq-builder volume to speed up Docker builds
* [ ] Use an Enum for the backend checks/enumeration
* [ ] Expose Substrait JSON
* [ ] Add connectorx support (Postgresql, MySQL)
* [ ] Enable output formats for connectorx
* [ ] Add connectorx support (MS SQL, SQLite, BigQuery, ClickHouse)
* [ ] Support --schema argument
* [ ] Support globs in --from arguments
* [ ] Move single partitioned files to single output file
* [ ] Add abbreviations for keywords
* [ ] Add s3 support
* [ ] Add Iceberg support
* [ ] Add Delta Lake support
* [ ] Add avro support
* [ ] Switch to eyre from anyhow
