use anyhow::Result;
use log::{debug, info, warn, error};

use duckdb::{Connection, types::{ValueRef, FromSql}};
use chrono::{DateTime, Utc};

use crate::{SourcesType, ToType, standardise_sources};
use prql_compiler::compile;

pub fn query(query: &str, sources: &SourcesType, to: &ToType) -> Result<String> {

    // prepend CTEs for the source aliases
    let mut query = query.to_string();
    for (alias, filename) in sources.iter() {
        // Needs the _{}_ on the LHS for _{}_.*
        query = format!(r#"table {} = (from __{}__=__file_{}__)
                          {}"#, &alias, &alias, &alias, &query);
    }
    debug!("query = {query:?}");

    // compile the PRQL to SQL
    let mut sql : String = compile(&query)?;
    debug!("sql = {:?}", sql.split_whitespace().collect::<Vec<&str>>().join(" "));

    // replace the table placeholders again
    for (alias, filename) in sources.iter() {
        let placeholder = format!("__file_{}__", &alias);
        let quoted_filename = format!(r#""{}""#, &filename);
        sql = sql.replace(&placeholder, &quoted_filename);
    }
    debug!("sql = {sql:?}");

    let file_format : &str;
    if to != "-" {
        if to.ends_with(".csv") {
            file_format = "(FORMAT 'CSV')";
        } else if to.ends_with(".parquet") {
            file_format = "(FORMAT 'PARQUET')";
        } else {
            file_format = "";
        }
        sql = format!("COPY ({}) TO '{}' {}", sql, to, file_format);
    }
    debug!("sql = {:?}", sql.split_whitespace().collect::<Vec<&str>>().join(" "));

    // prepaze te connection and statement
    let conn = Connection::open_in_memory()?;
    let mut statement = conn.prepare(&sql)?;

    // determine the number of columns
    statement.execute([])?;
    let column_names = statement.column_names();
    let csv_header = column_names.join(",");
    let column_count = statement.column_count();

    // query the data
    let csv_rows = statement
        .query_map([], |row| {
            Ok((0..column_count)
               .map(|i| {
                   let value = row.get_ref_unwrap(i);
                   match value {
                       ValueRef::Null => "".to_string(),
                       ValueRef::Int(i) => i.to_string(),
                       ValueRef::TinyInt(i) => i.to_string(),
                       ValueRef::HugeInt(i) => i.to_string(),
                       ValueRef::BigInt(i) => i.to_string(),
                       ValueRef::Float(r) => r.to_string(),
                       ValueRef::Double(r) => r.to_string(),
                       ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
                       ValueRef::Timestamp(_, _) => {
                           let dt = DateTime::<Utc>::column_result(value).unwrap();
                           dt.format("%Y-%m-%d %H:%M:%S").to_string()
                       }
                       t => unimplemented!("{t:?}"),
                   }
               })
               .collect::<Vec<_>>()
               .join(","))
        })?
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<String>>()
        .join("\n");

        Ok(csv_header + "\n" + &csv_rows)
}
