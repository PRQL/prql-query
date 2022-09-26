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

pub async fn query(query: &str, sources: &SourcesType, to: &ToType, database: &str, format: &str) -> Result<String> {

    // compile the PRQL to SQL
    let sql = compile(&query)?;
    debug!("sql = {:?}", sql.split_whitespace().collect::<Vec<&str>>().join(" "));

    // Create the context
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    for (alias, filename) in sources.iter() {
        if filename.ends_with("csv") {
            ctx.register_csv(alias, filename, CsvReadOptions::new()).await?;
        } else if filename.ends_with("parquet") {
            ctx.register_parquet(alias, filename, ParquetReadOptions::default()).await?;
        } else if filename.ends_with("json") {
            ctx.register_json(alias, filename, NdJsonReadOptions::default()).await?;
        } else {
            unimplemented!("filename={filename:?}");
        }
    }

    // Run the query
    let df = ctx.sql(&sql).await?;
    let rbs = df.collect().await?;
    // process_dataframe(df, to);
    process_results(rbs, to, format)
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

pub fn process_results(rbs: Vec<RecordBatch>, to: &ToType, format: &str) -> Result<String> {

    let mut output = String::from("");
    let to = &to.to_string();

    if to != "-" {
        return Err(anyhow!("Currently only stdout is implemented."))
    }

    if format == "csv" {
        output = String::from_utf8(convert_record_batches_to_csv(&rbs)?)?;
    } else if format == "json" {
        output = String::from_utf8(convert_record_batches_to_json(&rbs)?)?;
    } else if format == "parquet" {
        output = String::from_utf8(convert_record_batches_to_parquet(&rbs)?)?;
    } else if format == "table" {
        output = pretty_format_batches(&rbs)?.to_string();
    } else {
        unimplemented!("{to:?}");
    }

    Ok(output)
}

pub fn convert_record_batches_to_csv(rbs: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = csv::Writer::new(&mut buf);
        for rb in rbs {
            writer.write(rb)?;
        }
    }
    Ok(buf)
}

pub fn convert_record_batches_to_json(rbs: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        // let mut writer = json::ArrayWriter::new(&mut buf);
        let mut writer = json::LineDelimitedWriter::new(&mut buf);
        writer.write_batches(&rbs)?;
        writer.finish()?;
    }
    Ok(buf)
}

pub fn convert_record_batches_to_parquet(rbs: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    if rbs.is_empty() {
        return Ok(vec![]);
    }

    let schema = rbs[0].schema();
    {
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(&mut buf, schema, None)?;

        for rb in rbs {
            writer.write(rb)?;
        }
        writer.close()?;
    }
    Ok(buf)
}
