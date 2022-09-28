use std::io::prelude::*;

use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

//use polars::{df, prelude::*};
//use polars::prelude::{DataFrame, CsvWriter, ParquetWriter};
use polars::prelude::{CsvWriter, ParquetWriter};

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

pub fn query(query: &str, sources: &SourcesType, dest: &mut dyn Write, database: &str, format: &str) -> Result<()> {

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

    process_results(&mut df, dest, format)
}

fn process_results(df: &mut DataFrame, dest: &mut dyn Write, format: &str) -> Result<()> {

    if format == "csv" {
        write_dataframe_to_csv(df, dest)?;
    } else if format == "json" {
        write_dataframe_to_json(df, dest)?;
    } else if format == "parquet" {
        write_dataframe_to_parquet(df, dest)?;
    } else if format == "table" {
        write_dataframe_to_table(df, dest)?;
    } else {
        unimplemented!("to");
    }

    Ok(())
}

fn write_dataframe_to_csv(df: &mut DataFrame, dest: &mut dyn Write) -> Result<()> {
    let mut writer = CsvWriter::new(dest);
    writer.has_header(true)
        .with_delimiter(b',')
        .finish(df);
    Ok(())
}

fn write_dataframe_to_json(df: &mut DataFrame, dest: &mut dyn Write) -> Result<()> {
    unimplemented!("write_dataframe_to_json");
}

fn write_dataframe_to_parquet(df: &mut DataFrame, dest: &mut dyn Write) -> Result<()> {
    let writer = ParquetWriter::new(dest);
    writer.finish(df)?;
    Ok(())
}

fn write_dataframe_to_table(df: &mut DataFrame, dest: &mut dyn Write) -> Result<()> {
    dest.write(format!("{df}").as_bytes());
    dest.write(b"\n");
    Ok(())
}
