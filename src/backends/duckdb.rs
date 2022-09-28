use std::io::prelude::*;

use anyhow::Result;
use log::{debug, info, warn, error};

use arrow::{csv, json};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use parquet::arrow::arrow_writer;

use duckdb::{Connection, types::{ValueRef, FromSql}};
use chrono::{DateTime, Utc};

use crate::{SourcesType, ToType};
use prql_compiler::compile;

pub fn query(query: &str, sources: &SourcesType, dest: &mut dyn Write, database: &str, format: &str) -> Result<()> {

    // prepend CTEs for the source aliases
    let mut query = query.to_string();
    for (alias, source) in sources.iter() {
        // Needs the _{}_ on the LHS for _{}_.*
        query = format!("table {alias} = (from __{alias}__=__file_{alias}__)\n{query}");
    }
    debug!("query = {query:?}");

    // compile the PRQL to SQL
    let mut sql : String = compile(&query)?;
    debug!("sql = {:?}", sql.split_whitespace().collect::<Vec<&str>>().join(" "));

    // replace the table placeholders again
    for (alias, source) in sources.iter() {
        let placeholder = format!("__file_{}__", &alias);
        let quoted_source = format!(r#""{}""#, &source);
        sql = sql.replace(&placeholder, &quoted_source);
    }
    debug!("sql = {sql:?}");

    // prepaze te connection and statement
    let conn = Connection::open_in_memory()?;
    let mut stmt = conn.prepare(&sql)?;

    let rbs = stmt.query_arrow([])?.collect::<Vec<RecordBatch>>();

    process_results(&rbs, dest, format)
}

fn process_results(rbs: &[RecordBatch], dest: &mut dyn Write, format: &str) -> Result<()> {

    if format == "csv" {
        write_record_batches_to_csv(rbs, dest)?;
    } else if format == "json" {
        write_record_batches_to_json(rbs, dest)?;
    } else if format == "parquet" {
        write_record_batches_to_parquet(rbs, dest)?;
    } else if format == "table" {
        dest.write(pretty_format_batches(rbs)?.to_string().as_bytes());
        dest.write(b"\n");
    } else {
        unimplemented!("to");
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
