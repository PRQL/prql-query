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
use regex::Regex;

use crate::{get_dest_from_to, get_sql_from_query, OutputFormat, OutputWriter, SourcesType};

pub fn query(
    query: &str,
    sources: &SourcesType,
    to: &str,
    database: &str,
    format: &OutputFormat,
    writer: &OutputWriter,
) -> Result<()> {
    let sql_query = if query.starts_with("prql ") {
        let mut stmts = parse(query)?;

        // prepend CTEs for each of the sources
        for (name, source) in sources.iter() {
            let source_sql = if source.ends_with(".csv") {
                format!("read_csv_auto('{source}')")
            } else if source.ends_with(".parquet") {
                format!("read_parquet('{source}')")
            } else if database.starts_with("postgres") {
                let mut parts: Vec<&str> = source.split('.').collect();
                if parts.len() == 1 {
                    parts.insert(0, "public");
                }
                let table = parts
                    .pop()
                    .ok_or(anyhow!("Couldn't extract table name from {source}."))?;
                let schema = parts
                    .pop()
                    .ok_or(anyhow!("Couldn't extract schema name from {source}."))?;
                format!("postgres_scan('{database}', '{schema}', '{table}')")
            } else {
                format!("'{source}'")
            };

            let mut relation_decl = parse(&format!(
                r#"
                let {name} = s"SELECT * FROM {source_sql}"
                "#
            ))?;

            stmts.insert(1, relation_decl.remove(0));
        }

        Ok(stmts)
            .and_then(prql_compiler::pl_to_rq)
            .and_then(|rq| prql_compiler::rq_to_sql(rq, None))
            .map_err(|e| anyhow!(e))?
    } else {
        query.to_string()
    };
    debug!("sql_query = {sql_query}");

    // prepare the connection and statement
    let conn = if database == "" {
        debug!("Opening in-memory DuckDB database");
        Connection::open_in_memory()?
    } else if database.starts_with("sqlite://") {
        let con = Connection::open_in_memory()?;
        // Install and load the sqlite_scanner extension
        let load_extension = "INSTALL sqlite_scanner; LOAD sqlite_scanner;";
        con.execute_batch(load_extension)?;
        let dbpath = database.strip_prefix("sqlite://").map_or(database, |p| p);
        let attach_sql = format!("CALL sqlite_attach('{dbpath}')");
        con.execute_batch(&attach_sql)?;
        con
    } else if database.starts_with("postgres") {
        let con = Connection::open_in_memory()?;
        // Check if a schema was specified
        let re = Regex::new(r"^(?P<uri>[^?]+)(?P<schema>\?currentSchema=.+)?$")?;
        let caps = re
            .captures(database)
            .ok_or(anyhow!("Couldn't match regex!"))?;
        let uri = caps
            .name("uri")
            .ok_or(anyhow!("Couldn't extract URI!"))?
            .as_str();
        debug!("uri={:?}", uri);
        let schema_param = caps
            .name("schema")
            .map_or("?currentSchema=public", |p| p.as_str());
        let schema = schema_param.split("=").last().map_or("public", |p| p);
        debug!("schema={:?}", schema);
        // Install and load the postgres_scanner extension
        let load_extension = "INSTALL postgres_scanner; LOAD postgres_scanner;";
        con.execute_batch(load_extension)?;
        let attach_sql = format!("CALL postgres_attach('{uri}', source_schema='{schema}')");
        debug!("attach_sql={:?}", attach_sql);
        con.execute_batch(&attach_sql)?;
        con
    } else {
        let dbpath = database.strip_prefix("duckdb://").map_or(database, |p| p);
        debug!("Opening DuckDB database: dbpath={:?}", dbpath);
        Connection::open(dbpath)?
    };

    // Install and load the parquet extension
    // FIXME: Be smarter about this and only do it where required
    let load_parquet_extension = "INSTALL parquet; LOAD parquet;";
    conn.execute_batch(load_parquet_extension)?;

    // Execute the query
    let mut stmt = conn.prepare(&sql_query)?;
    let rbs = stmt.query_arrow([])?.collect::<Vec<RecordBatch>>();

    match writer {
        OutputWriter::arrow => write_results_with_arrow(&rbs, to, format),
        OutputWriter::backend => write_results_with_duckdb(&rbs, to, format),
    }
}

fn parse(query: &str) -> Result<Vec<prql_compiler::ast::pl::Stmt>> {
    prql_compiler::prql_to_pl(query).map_err(|e| anyhow!(e))
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
