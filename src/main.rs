#![allow(unused)]

use anyhow::{Result, anyhow};

use std::path::PathBuf;
use std::io::prelude::*;
use std::io;
use std::fs;

use clap::Parser;
use prql_compiler::compile;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser,Debug)]
struct Cli {
    /// The file to read data FROM if given
    #[clap(short, long, value_parser)]
    from: Option<PathBuf>,

    /// The file to write TO if given, otherwise stdout
    #[clap(short, long, value_parser, default_value = "-")]
    to: PathBuf,

    /// The engine to use to process the query
    #[clap(short, long, value_parser, default_value = "duckdb")]
    engine: String,

    /// The PRQL query to be processed if given, otherwise stdin
    #[clap(value_parser, default_value = "-")]
    prql: String,
}

fn main() -> Result<()> {
    let mut output = String::from("");

    let args = Cli::parse();

    // args.prql
    let mut prql : String; 
    if args.prql == "-" {
        prql = String::new();
        io::stdin().read_to_string(&mut prql);
    }
    else {
        prql = String::from(&args.prql);
    }

    let to = args.to.to_str().ok_or(anyhow!("Couldn't convert PathBuf to str."))?.to_string();

    if args.from.is_none() {
        output = compile(&prql)?;
    } else {
        let from = args.from.unwrap().to_str().ok_or(anyhow!("Couldn't convert PathBuf to str."))?.to_string();

        if args.engine == "duckdb" {
            output = query_duckdb(&prql, &from, &to)?;
        } else {
            dbg!(&args.engine);
            unimplemented!("{}", &args.engine);
        }
    }

    if to == "-" {
        println!("{}", output);
    }


    Ok(())
}

fn query_duckdb(prql: &str, from: &str, to: &str) -> Result<String> {
    use duckdb::{Connection, types::{ValueRef, FromSql}};
    use chrono::{DateTime, Utc};

    // process the PRQL and get the SQL
    const FROM_PLACEHOLDER : &str = "__PRQL_PLACEHOLDER__";

    let prql = format!("from t={}\n{}", &FROM_PLACEHOLDER, &prql);

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
        dbg!(&to);
        sql = format!("COPY ({}) TO '{}' {}", sql, to, file_format);
        dbg!(&sql);
    }

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
