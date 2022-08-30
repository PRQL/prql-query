use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use datafusion::prelude::*;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};

use crate::{FromType, ToType};
use prql_compiler::compile;

pub async fn query(prql: &str, from: &FromType, to: &ToType) -> Result<String> {
    // preprocess the PRQL
    let prql = if ! prql.to_lowercase().starts_with("from") {
        format!("from {}|{}", "f0", &prql)
    } else { prql.to_string() };
    info!("prql = {prql:?}");

    // compile the PRQL to SQL
    let sql = compile(&prql)?;
    info!("sql = {:?}", sql.split_whitespace().collect::<Vec<&str>>().join(" "));

    // Create the context
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    for (i, path) in from.iter().enumerate() {
        let filename = &path.to_string();
        let alias : &str = &format!("f{i}");
        let ext = path.extension()
            .ok_or_else(|| anyhow!("Couldn't determine extension of {filename}"))?;
        if ext=="csv" {
            ctx.register_csv(alias, filename, CsvReadOptions::new()).await?;
        } else if ext=="parquet" {
            ctx.register_parquet(alias, filename, ParquetReadOptions::default()).await?;
        } else if ext=="json" {
            ctx.register_json(alias, filename, NdJsonReadOptions::default()).await?;
        } else {
            unimplemented!("ext={ext:?}");
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
