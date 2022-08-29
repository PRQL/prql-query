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

### Usage

At its simplest `prql` takes PRQL queries and transpiles them to SQL queries:

    $ prql "from a | select b"
    SELECT
      b
    FROM
      a

Input can also come from stdin:

    $ cat examples/queries/invoice_totals.prql | prql

For convenience, queries ending in ".prql" are assumed to be paths to PRQL query files and will be read in so this produces the same as above:

    $ prql examples/queries/invoice_totals.prql

When a `--from` argument is supplied which specifies a data file, the PRQL query will be applied to that data file. An appropriate `from <table>` pipeline step will automatically be inserted and should be ommitted from the query:

    $ prql --from examples/data/chinook/csv/invoices.csv "take 5"

When a `--to` argument is supplied, the output will be written there in the appropriate file format instead of stdout (the "" query is equivalent to `select *` and is required because `select *` currently does not work):

    $ prql --from examples/data/chinook/csv/invoices.csv --to invoices.parquet ""

Currently csv, parquet and json file formats are supported for both readers and writers:

    $ prql -f invoices.parquet -t customer_totals.json examples/queries/customer_totals.prql
    $ prql -f customer_totals.json "sort [-customer_total] | take 10"
    +-------------+--------------------+
    | customer_id | customer_total     |
    +-------------+--------------------+
    | 6           | 49.620000000000005 |
    | 26          | 47.620000000000005 |
    | 57          | 46.62              |
    | 46          | 45.62              |
    | 45          | 45.62              |
    | 28          | 43.620000000000005 |
    | 37          | 43.62              |
    | 24          | 43.62              |
    | 7           | 42.62              |
    | 25          | 42.62              |
    +-------------+--------------------+



## TODO

* [x] Add --engine argument
* [x] Factor out the query function to query(prql, from, to)
* [x] Switch to camino
* [x] Rename --engine to --backend
* [x] Add support from .prql files
* [x] Add DataFusion support
* [x] Add DF writer support (csv, parquet, json)
* [ ] Move single partitioned files to single output file
* [ ] Add avro support
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
