use anyhow::Result;
use log::{debug, info, warn, error};

use duckdb::{Connection, types::{ValueRef, FromSql}};
use chrono::{DateTime, Utc};

use crate::{FromType, ToType, FROM_PLACEHOLDER};
use prql_compiler::compile;

pub fn query(prql: &str, from: &FromType, to: &ToType) -> Result<String> {

    // pre-process the PRQL
    let prql = format!("from t={}\n{}", &FROM_PLACEHOLDER, &prql);
    info!("prql = {prql:?}");

    // compile the PRQL to SQL
    let mut sql = compile(&prql)?.replace(&FROM_PLACEHOLDER, &from);

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
    info!("sql = {sql:?}");

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
