use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;

use datafusion::prelude::*;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};

use crate::{SourcesType, ToType};
use prql_compiler::compile;

pub async fn query(query: &str, sources: &SourcesType, to: &ToType, database: &str) -> Result<String> {

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
    process_results(rbs, to)
}

pub fn process_results(rbs: Vec<RecordBatch>, to: &ToType) -> Result<String> {

    let mut output = String::from("");
    let to = &to.to_string();

    if to == "-" {
        output = pretty_format_batches(&rbs)?.to_string();
    }
    //} else if to.ends_with(".csv") {
        //df.write_csv(to).await?;
    //} else if to.ends_with(".parquet") {
        //df.write_parquet(to, None).await?;
    //} else if to.ends_with(".json") {
        //df.write_json(to).await?;
    //} else {
        //unimplemented!("{to:?}");
    //}

    Ok(output)
}

async fn process_dataframe(df: DataFrame, to: &ToType) -> Result<String> {
    // Produce the output
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
