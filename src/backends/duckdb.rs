use std::io::prelude::*;

use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use arrow::{csv, json};
use parquet::arrow::arrow_writer;

use chrono::{DateTime, Utc};
use duckdb::{
    types::{FromSql, ValueRef},
    Connection,
};

use crate::{get_dest_from_to, get_sql_from_query, OutputFormat, OutputWriter, SourcesType};

pub fn query(
    query: &str,
    sources: &SourcesType,
    to: &str,
    database: &str,
    format: &OutputFormat,
    writer: &OutputWriter,
) -> Result<()> {
    let mut query = query.to_string();
    if query.starts_with("prql ") {
        // prepend CTEs for the source aliases
        let mut lines: Vec<String> = query.split("\n").map(|s| s.to_string()).collect();
        for (alias, source) in sources.iter() {
            // Needs the _{}_ on the LHS for _{}_.*
            lines.insert(
                1,
                format!("table {alias} = (from __{alias}__=__file_{alias}__)"),
            );
        }
        query = lines.join("\n");
        debug!("query = {query:?}");
    }

    // compile the PRQL to SQL
    let mut sql = get_sql_from_query(&query)?;
    debug!(
        "sql = {:?}",
        sql.split_whitespace().collect::<Vec<&str>>().join(" ")
    );

    if query.starts_with("prql ") {
        // replace the table placeholders again
        for (alias, source) in sources.iter() {
            let placeholder = format!("__file_{alias}__");
            debug!("placeholder = {placeholder:?}");
            let quoted_source = if source.ends_with(".csv") {
                format!("read_csv_auto('{source}')")
            } else if source.ends_with(".parquet") {
                format!("read_parquet('{source}')")
            } else {
                format!(r#"'{source}'"#)
            };
            debug!("quoted_source = {quoted_source:?}");
            sql = sql.replace(&placeholder, &quoted_source);
        }
        debug!("sql = {sql:?}");
    }

    // prepare the connection and statement
    let conn = if database == "" {
        debug!("Opening in-memory DuckDB database");
        Connection::open_in_memory()?
    } else {
        let dbpath = database.strip_prefix("duckdb://").map_or(database, |p| p);
        debug!("Opening DuckDB database: dbpath={:?}", dbpath);
        Connection::open(dbpath)?
    };

    // Install and load the parquet extension
    // FIXME: Be smarter about this and only do it where required
    let load_parquet_extension = "INSTALL parquet; LOAD parquet;";
    conn.execute_batch(load_parquet_extension);

    // Execute the query
    let mut stmt = conn.prepare(&sql)?;
    let rbs = stmt.query_arrow([])?.collect::<Vec<RecordBatch>>();

    match writer {
        OutputWriter::arrow => write_results_with_arrow(&rbs, to, format),
        OutputWriter::backend => write_results_with_duckdb(&rbs, to, format),
    }
}

fn write_results_with_duckdb(rbs: &[RecordBatch], to: &str, format: &OutputFormat) -> Result<()> {
    unimplemented!("write_results_with_duckdb");
}

fn write_results_with_arrow(rbs: &[RecordBatch], to: &str, format: &OutputFormat) -> Result<()> {
    let mut dest: Box<dyn Write> = get_dest_from_to(to)?;

    match format {
        OutputFormat::csv => write_record_batches_to_csv(rbs, &mut dest)?,
        OutputFormat::json => write_record_batches_to_json(rbs, &mut dest)?,
        OutputFormat::parquet => write_record_batches_to_parquet(rbs, &mut dest)?,
        OutputFormat::table => write_record_batches_to_table(rbs, &mut dest)?,
    }

    Ok(())
}

fn write_record_batches_to_csv(rbs: &[RecordBatch], dest: &mut dyn Write) -> Result<()> {
    {
        let mut writer = csv::Writer::new(dest);
        for rb in rbs {
            writer.write(rb)?;
        }
    }
    Ok(())
}

fn write_record_batches_to_json(rbs: &[RecordBatch], dest: &mut dyn Write) -> Result<()> {
    {
        // let mut writer = json::ArrayWriter::new(&mut buf);
        let mut writer = json::LineDelimitedWriter::new(dest);
        writer.write_batches(&rbs)?;
        writer.finish()?;
    }
    Ok(())
}

fn write_record_batches_to_parquet(rbs: &[RecordBatch], dest: &mut dyn Write) -> Result<()> {
    if rbs.is_empty() {
        return Ok(());
    }

    let schema = rbs[0].schema();
    {
        let mut writer = arrow_writer::ArrowWriter::try_new(dest, schema, None)?;

        for rb in rbs {
            writer.write(rb)?;
        }
        writer.close()?;
    }
    Ok(())
}

fn write_record_batches_to_table(rbs: &[RecordBatch], dest: &mut dyn Write) -> Result<()> {
    dest.write(pretty_format_batches(rbs)?.to_string().as_bytes());
    dest.write(b"\n");
    Ok(())
}
