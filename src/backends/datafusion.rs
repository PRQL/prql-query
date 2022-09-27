use std::io::prelude::*;

use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use datafusion::arrow::{csv, json};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;

use datafusion::prelude::*;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};

use crate::{SourcesType, ToType};
use prql_compiler::compile;

pub async fn query(query: &str, sources: &SourcesType, dest: &mut dyn Write, database: &str, format: &str) -> Result<()> {

    // compile the PRQL to SQL
    let sql = compile(&query)?;
    debug!("sql = {:?}", sql.split_whitespace().collect::<Vec<&str>>().join(" "));

    // Create the context
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    for (alias, filename) in sources.iter() {
        if filename.ends_with("csv") {
            ctx.register_csv(alias, filename, CsvReadOptions::new()).await?;
        } else if filename.ends_with("json") {
            ctx.register_json(alias, filename, NdJsonReadOptions::default()).await?;
        } else if filename.ends_with("parquet") {
            ctx.register_parquet(alias, filename, ParquetReadOptions::default()).await?;
        } else {
            unimplemented!("filename={filename:?}");
        }
    }

    // Run the query
    let df = ctx.sql(&sql).await?;
    let rbs = df.collect().await?;
    // process_dataframe(df, to);
    process_results(&rbs, dest, format)
}

async fn process_dataframe(df: DataFrame, to: &ToType) -> Result<String> {
    // Produce the output
    // This is the easiest method and works fine for DataFusion but is
    // not portable to the other formats.
    let to = &to.to_string();
    if to == "-" {
        df.show().await?;
    } else if to.ends_with(".csv") {
        df.write_csv(to).await?;
    } else if to.ends_with(".parquet") {
        df.write_parquet(to, None).await?;
    } else if to.ends_with(".json") {
        df.write_json(to).await?;
    } else {
        unimplemented!("{to:?}");
    }

    Ok("".into())
}

pub fn process_results(rbs: &[RecordBatch], dest: &mut dyn Write, format: &str) -> Result<()> {

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

pub fn write_record_batches_to_csv(rbs: &[RecordBatch], dest: &mut dyn Write) -> Result<()> {
    {
        let mut writer = csv::Writer::new(dest);
        for rb in rbs {
            writer.write(rb)?;
        }
    }
    Ok(())
}

pub fn write_record_batches_to_json(rbs: &[RecordBatch], dest: &mut dyn Write) -> Result<()> {
    {
        // let mut writer = json::ArrayWriter::new(&mut buf);
        let mut writer = json::LineDelimitedWriter::new(dest);
        writer.write_batches(&rbs)?;
        writer.finish()?;
    }
    Ok(())
}

pub fn write_record_batches_to_parquet(rbs: &[RecordBatch], dest: &mut dyn Write) -> Result<()> {
    if rbs.is_empty() {
        return Ok(());
    }

    let schema = rbs[0].schema();
    {
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(dest, schema, None)?;

        for rb in rbs {
            writer.write(rb)?;
        }
        writer.close()?;
    }
    Ok(())
}
