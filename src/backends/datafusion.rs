use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use datafusion::prelude::*;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};

use crate::{SourcesType, ToType, standardise_sources};
use prql_compiler::compile;

pub async fn query(query: &str, sources: &SourcesType, to: &ToType) -> Result<String> {

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
