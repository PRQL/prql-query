# prql: query and transform data with PRQL

## Installation

### Download a binary from Github Release

Coming soon ...

### Run as a container image (Docker)

Coming soon ...

    docker build -t prql-tool .
    alias prql="docker run --rm -it -v $(pwd):/tmp -w /tmp -u $(id -u):$(id -g) prql-tool"
    prql --help

### Via Rust toolchain (Cargo)

    git clone -b tool https://github.com/snth/prql.git
    cd prql/prql-tool
    cargo install --path .

## Usage

### Generating SQL

At its simplest `prql` takes PRQL queries and transpiles them to SQL queries:

    $ prql "from a | select b"
    SELECT
      b
    FROM
      a

Input can also come from stdin:

    $ cat examples/queries/invoice_totals.prql | prql
    SELECT
      STRFTIME('%Y-%m', i.invoice_date) AS month,
      STRFTIME('%Y-%m-%d', i.invoice_date) AS day,
      COUNT(DISTINCT i.invoice_id) AS num_orders,
      SUM(ii.quantity) AS num_tracks,
      SUM(ii.unit_price * ii.quantity) AS total_price,
      SUM(SUM(ii.quantity)) OVER (
        PARTITION BY STRFTIME('%Y-%m', i.invoice_date)
        ORDER BY
          STRFTIME('%Y-%m-%d', i.invoice_date) ROWS BETWEEN UNBOUNDED PRECEDING
          AND CURRENT ROW
      ) AS running_total_num_tracks,
      LAG(SUM(ii.quantity), 7) OVER (
        ORDER BY
          STRFTIME('%Y-%m-%d', i.invoice_date) ROWS BETWEEN UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      ) AS num_tracks_last_week
    FROM
      invoices AS i
      JOIN invoice_items AS ii USING(invoice_id)
    GROUP BY
      STRFTIME('%Y-%m', i.invoice_date),
      STRFTIME('%Y-%m-%d', i.invoice_date)
    ORDER BY
      day

For convenience, queries ending in ".prql" are assumed to be paths to PRQL query files and will be read in so this produces the same as above:

    $ prql examples/queries/invoice_totals.prql

### Querying data from a database

With the functionality described above, you should be able to query your favourite SQL RDBMS using PRQL. For example with the `psql` client for PostgreSQL:

    $ echo 'from my_table | take 5' | prql | psql postgresql://username:password@host:port/database

Or using the `mysql` client for MySQL with a PRQL query stored in a file:

    $ prql my_query.prql | mysql -h myhost -d mydb -u myuser -p mypassword

Similarly for MS SQL Server and other databases.

### Querying data in files

For querying and transforming data stored on the local filesystem, `prql` comes in with a number of built-in backend query processing engines. The default backend is [Apache Arrow DataFusion](https://arrow.apache.org/datafusion/). However [DuckDB](https://duckdb.org/) and [SQLite](https://www.sqlite.org/) (planned) are also supported.

When `--from` arguments are supplied which specify data files, the PRQL query will be applied to those files. The files can be referenced in the queries by the filenames without the extensions, e.g. customers.csv can be referenced as the table `customers`. For convenience, unless a query already begins with a `from ...` step, a `from <table>` pipeline step will automatically be inserted at the beginning of the query referring to the first from argument encountered, i.e. the following two are equivalent:

    $ prql --from examples/data/chinook/csv/invoices.csv "from invoices|take 5"
    $ prql --from examples/data/chinook/csv/invoices.csv "take 5"
    +------------+-------------+-------------------------------+-------------------------+--------------+---------------+-----------------+---------------------+-------+
    | invoice_id | customer_id | invoice_date                  | billing_address         | billing_city | billing_state | billing_country | billing_postal_code | total |
    +------------+-------------+-------------------------------+-------------------------+--------------+---------------+-----------------+---------------------+-------+
    | 1          | 2           | 2009-01-01T00:00:00.000000000 | Theodor-Heuss-Straße 34 | Stuttgart    |               | Germany         | 70174               | 1.98  |
    | 2          | 4           | 2009-01-02T00:00:00.000000000 | Ullevålsveien 14        | Oslo         |               | Norway          | 0171                | 3.96  |
    | 3          | 8           | 2009-01-03T00:00:00.000000000 | Grétrystraat 63         | Brussels     |               | Belgium         | 1000                | 5.94  |
    | 4          | 14          | 2009-01-06T00:00:00.000000000 | 8210 111 ST NW          | Edmonton     | AB            | Canada          | T6G 2C7             | 8.91  |
    | 5          | 23          | 2009-01-11T00:00:00.000000000 | 69 Salem Street         | Boston       | MA            | USA             | 2113                | 13.86 |
    +------------+-------------+-------------------------------+-------------------------+--------------+---------------+-----------------+---------------------+-------+

You can also assign an alias for source file with the following form `--from <alias>=<filepath>` and then refer to it by that alias in your queries. So the following is another equivalent form of the queries above:

    $ prql --from i=examples/data/chinook/csv/invoices.csv "from i|take 5"

This works with multiple files which means that the extended example above can be run as follows:

    $ prql -b duckdb -f examples/data/chinook/csv/invoices.csv -f examples/data/chinook/csv/invoice_items.csv examples/queries/invoice_totals.prql

### Transforming data with `prql` and writing the output to files

When a `--to` argument is supplied, the output will be written there in the appropriate file format instead of stdout (the "" query is equivalent to `select *` and is required because `select *` currently does not work):

    $ prql --from examples/data/chinook/csv/invoices.csv --to invoices.parquet ""

Currently csv, parquet and json file formats are supported for both readers and writers:

    $ cat examples/queries/customer_totals.prql
    group [customer_id] (
        aggregate [
            customer_total = sum total,
        ])
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
* [x] Make DuckDB an optional feature
* [x] Add logging and verbosity for debugging
* [x] Add --no-exec option
* [x] Allow multiple --from options with alias naming
* [x] Cleanup multiple --from code and enable
* [x] Reenable DuckDB backend for multiple sources
* [ ] Add support for environment variables eg PRQL_FROM_EMPLOYEES="employees.csv" -> `from employees="employees.csv"
* [ ] Move single partitioned files to single output file
* [ ] Add abbreviations for keywords
* [ ] Add s3 support
* [ ] Add Delta Lake support
* [ ] Add formatted table output to DuckDB backend
* [ ] Add avro support
* [ ] Use an Enum for the backend checks/enumeration
* [ ] Make --sql an option for SQL query support
* [ ] Support --schema argument
* [ ] Add sqlite support
* [ ] Add sqlx support (Postgresql, MySQL, MS SQL)
* [ ] Switch to eyre from anyhow
