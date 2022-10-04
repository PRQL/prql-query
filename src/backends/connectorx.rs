use std::io::prelude::*;

use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use arrow2::chunk::Chunk;

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

use crate::{SourcesType, OutputFormat, OutputWriter, get_dest_from_to};
use prql_compiler::compile;

pub fn query(query: &str, sources: &SourcesType, to: &str, database: &str, format: &OutputFormat, writer: &OutputWriter) -> Result<()> {

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

    let chunks = destination.arrow2()?;

    match writer {
        OutputWriter::arrow => write_results_with_arrow(&chunks, to, format),
        OutputWriter::backend => write_results_with_connectorx(&chunks, to, format)
    }
}

fn write_results_with_arrow(chunks: &[Chunk], to: &str, format: &OutputFormat) -> Result<()> {

    let mut dest: Box<dyn Write> = get_dest_from_to(to)?;

    match format {
        OutputFormat::csv => write_chunks_to_csv(chunks, &mut dest)?,
        OutputFormat::json => write_chunks_to_json(chunks, &mut dest)?,
        OutputFormat::parquet => write_chunks_to_parquet(chunks, &mut dest)?,
        OutputFormat::table => write_chunks_to_table(chunks, &mut dest)?,
    }

    Ok(())
}

fn write_chunks_to_csv(chunks: &[Chunk], dest: &mut dyn Write) -> Result<()> {
    {
        let mut writer = csv::Writer::new(dest);
        for rb in rbs {
            writer.write(rb)?;
        }
    }
    Ok(())
}

fn write_chunks_to_json(chunks: &[Chunk], dest: &mut dyn Write) -> Result<()> {
    {
        // let mut writer = json::ArrayWriter::new(&mut buf);
        let mut writer = json::LineDelimitedWriter::new(dest);
        writer.write_batches(&rbs)?;
        writer.finish()?;
    }
    Ok(())
}

fn write_chunks_to_parquet(chunks: &[Chunk], dest: &mut dyn Write) -> Result<()> {

    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Snappy,
        version: Version::V1,
    };

    let row_groups = RowGroupIterator::try_new(
        vec![Ok(chunk)].into_iter(),
        &schema,
        options,
        vec![Encoding::Plain, Encoding::Plain],
    )?;

    let mut writer = FileWriter::try_new(dest, schema, options)?;

    writer.start()?;
    for group in row_groups {
        let (group, len) = group?;
        writer.write(group, len)?;
    }
    let _ = writer.end(None)?;

    Ok(())
}

fn write_chunks_to_table(chunks: &[Chunk], dest: &mut dyn Write) -> Result<()> {
    dest.write(pretty_format_batches(rbs)?.to_string().as_bytes());
    dest.write(b"\n");
    Ok(())
}
