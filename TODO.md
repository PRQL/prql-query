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
* [ ] Add support for DuckDB database files
* [ ] Add tests
* [ ] Publish to crates.io
* [ ] Publish to homebrew
* [ ] Add pq-builder volume to speed up Docker builds
* [ ] Use an Enum for the backend checks/enumeration
* [ ] Add connectorx support (Postgresql, MySQL)
* [ ] Enable output formats for connectorx
* [ ] Add connectorx support (MS SQL, SQLite, BigQuery, ClickHouse)
* [ ] Support --schema argument
* [ ] Support globs in --from arguments
* [ ] Move single partitioned files to single output file
* [ ] Add abbreviations for keywords
* [ ] Add s3 support
* [ ] Add Delta Lake support
* [ ] Add Iceberg support
* [ ] Add avro support
* [ ] Switch to eyre from anyhow
