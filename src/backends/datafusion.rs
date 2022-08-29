use anyhow::Result;

use datafusion::prelude::*;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};

use prql_compiler::compile;

pub async fn query(prql: &str, from: &str, to: &str) -> Result<String> {
    // process the PRQL and get the SQL
    const FROM_PLACEHOLDER : &str = "__PRQL_PLACEHOLDER__";

    let prql = format!("from t={}\n{}", &FROM_PLACEHOLDER, &prql);

    // compile the PRQL to SQL
    let sql = compile(&prql)?;

    // Create the context
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    if from.ends_with(".csv") {
        ctx.register_csv(FROM_PLACEHOLDER, from, CsvReadOptions::new()).await?;
    } else if from.ends_with(".parquet") {
        ctx.register_parquet(FROM_PLACEHOLDER, from, ParquetReadOptions::default()).await?;
    } else if from.ends_with(".json") {
        ctx.register_json(FROM_PLACEHOLDER, from, NdJsonReadOptions::default()).await?;
    } else {
        unimplemented!("{from:?}");
    }

    // Run the query
    let df = ctx.sql(&sql).await?;

    // Produce the output
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
