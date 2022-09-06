use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use polars::frame::DataFrame;

use connectorx::{
    prelude::*,
    sources::{
        postgres::{PostgresSource, BinaryProtocol as PostgresBinaryProtocol, rewrite_tls_args}, 
        mysql::{MySQLSource, BinaryProtocol as MySQLBinaryProtocol},
    },
    sql::CXQuery,
    transports::{
        PostgresArrow2Transport, 
        MySQLArrow2Transport
    },
    destinations::{
        arrow2::Arrow2Destination
    }
};
use url::Url;
use postgres::NoTls;

use crate::{SourcesType, ToType};
use prql_compiler::compile;
use polars::{df, prelude::*};

pub fn query(query: &str, sources: &SourcesType, to: &ToType, database: &str) -> Result<String> {

    // prepend CTEs for the source aliases
    let mut query = query.to_string();
    for (alias, source) in sources.iter() {
        // Needs the _{}_ on the LHS for _{}_.*
        query = format!("table {alias} = (from __{alias}__={source})\n{query}");
    }
    debug!("query = {query:?}");

    // compile the PRQL to SQL
    let sql : String = compile(&query)?;
    debug!("sql = {:?}", sql.split_whitespace().collect::<Vec<&str>>().join(" "));
    let cx_sql = [CXQuery::naked(sql)];
    debug!("cx_sql = {cx_sql:?}");

    let mut destination = Arrow2Destination::new();
    if database.starts_with("postgres:") {
        let url = Url::parse(&database)?;
        debug!("url = {url:?}");
        let (config, _) = rewrite_tls_args(&url)?;
        let source = PostgresSource::<PostgresBinaryProtocol, NoTls>::new(config, NoTls, 1)?;
        let dispatcher = Dispatcher::<
            _, 
            _, 
            PostgresArrow2Transport<PostgresBinaryProtocol, NoTls>
        >::new(source, &mut destination, &cx_sql, None);
        dispatcher.run()?;
    }
    if database.starts_with("mysql:") {
        let source = MySQLSource::<MySQLBinaryProtocol>::new(&database, 1)?;
        let dispatcher = Dispatcher::<
            _, 
            _, 
            MySQLArrow2Transport<MySQLBinaryProtocol>
        >::new(source, &mut destination, &cx_sql, None);
        dispatcher.run()?;
    }

    let df = destination.polars()?;

    process_results(df, to)
}

pub fn process_results(df: DataFrame, to: &ToType) -> Result<String> {
    let output = format!("{df}");
    Ok(output)
}
