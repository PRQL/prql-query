use anyhow::Result;
use log::{debug, info, warn, error};

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;

use duckdb::{Connection, types::{ValueRef, FromSql}};
use chrono::{DateTime, Utc};

use crate::{SourcesType, ToType};
use prql_compiler::compile;

pub fn query(query: &str, sources: &SourcesType, to: &ToType, database: &str) -> Result<String> {

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

    let rbs = stmt.query_arrow([])?.collect();

    process_results(rbs, to)
}

pub fn process_results(rbs: Vec<RecordBatch>, to: &ToType) -> Result<String> {
    let output = pretty_format_batches(&rbs)?.to_string();
    Ok(output)
}
