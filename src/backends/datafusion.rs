use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use datafusion::prelude::*;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};

use crate::{FromType, ToType, standardise_sources};
use prql_compiler::compile;

pub async fn query(prql: &str, from: &FromType, to: &ToType) -> Result<String> {
    let sources = standardise_sources(from)?;

    // pre-process the PRQL
    let prql = if ! prql.to_lowercase().starts_with("from") {
        format!("from {}|{}", sources[0].0, &prql)
    } else { prql.to_string() };
    debug!("prql = {prql:?}");

    // compile the PRQL to SQL
    let sql = compile(&prql)?;
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
