# PRQL CLI Tool

## Installation

### Download a binary from Github Release

Coming soon ...

### Run as a container image (Docker)

Coming soon ...

    docker build -t prql-tool .
    alias prql="docker run --rm -it -v $(pwd):/src -u $(id -u):$(id -g) prql-tool"

### Via Rust toolchain (Cargo)

    cargo install prql-tool

or

    git clone https://github.com/snth/prql.git
    cd prql/prql-tool
    cargo install --path .

## TODO

* [x] Add --engine argument
* [x] Factor out the query function to query(prql, from, to)
* [x] Switch to camino
* [x] Rename --engine to --backend
* [x] Add support from .prql files
* [x] Add DataFusion support
* [x] Add DF writer support (csv, parquet, json)
* [ ] Move single partitioned files to single output file
* [ ] Add s3 support
* [ ] Add Delta Lake support
* [ ] Add logging and verbosity for debugging
* [ ] Use an Enum for the backend checks/enumeration
* [ ] Allow multiple --from options with alias naming
* [ ] Add support for environment variables eg PRQL_FROM_EMPLOYEES="employees.csv" -> `from employees="employees.csv"
* [ ] Make DuckDB an optional feature
* [ ] Make --sql an option for SQL query support
* [ ] Support --schema argument
* [ ] Add sqlite support
* [ ] Add postgresql support
* [ ] Add mysql support
* [ ] Switch to eyre from anyhow
