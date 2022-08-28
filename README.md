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
* [ ] Switch to camino
* [ ] Add logging and verbosity for debugging
* [ ] Rename --engine to --backend
* [ ] Add DataFusion support
* [ ] Add DF writer support (csv, parquet, json)
* [ ] Allow multiple --from options with alias naming
* [ ] Add support for environment variables eg PRQL_FROM_EMPLOYEES="employees.csv" -> `from employees="employees.csv"
* [ ] Make DuckDB an optional feature
* [ ] Make --sql an option for SQL query support
* [ ] Add sqlite support
* [ ] Add postgresql support
* [ ] Add mysql support
* [ ] Switch to eyre from anyhow
