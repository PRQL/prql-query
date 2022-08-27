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

    /// The PRQL query to be processed if given, otherwise stdin
    #[clap(value_parser, default_value = "-")]
    prql: String
}

fn query(sql: &str) -> String {
    use duckdb::{Connection, types::{ValueRef, FromSql}};
    use chrono::{DateTime, Utc};

    let conn = Connection::open_in_memory().unwrap();
    let mut statement = conn.prepare(sql).unwrap();

    // determine the number of columns
    statement.execute([]).unwrap();
    let column_names = statement.column_names();
    let csv_header = column_names.join(",");
    let column_count = statement.column_count();

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
        })
        .unwrap()
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<String>>()
        .join("\n");

        csv_header + "\n" + &csv_rows
}

fn main() -> Result<()> {
    let mut from : Option<String> = None;
    const FROM_PLACEHOLDER : &str = "__PRQL_PLACEHOLDER__";

    let args = Cli::parse();
    println!("{:?}", args);

    // args.prql
    let mut prql : String; 
    if args.prql == "-" {
        prql = String::new();
        io::stdin().read_to_string(&mut prql);
    }
    else {
        prql = String::from(&args.prql);
    }

    // args.from
    if args.from.is_some() {
        prql = format!("from t={}\n", FROM_PLACEHOLDER)+ &prql;
    }

    // print the PRQL query that we generated
    println!("\nprql:\n{}", prql);
    // compile the PRQL to SQL
    let mut sql = compile(&prql).unwrap();

    // if we have a from then insert it
    if let Some(from_path) = args.from.clone() {
        let placeholder = format!("\"{}\"", FROM_PLACEHOLDER);
        let from_str = format!("'{}'", from_path.to_str().ok_or(anyhow!("Couldn't convert PathBuf to str."))?);
        sql = sql.replace(&placeholder, &from_str);
    }

    let to_str = args.to.to_str().unwrap();
    if to_str == "-" {
        println!("\nsql:\n{}", sql);
    } else {
        let file_format : &str;
        if to_str.ends_with(".csv") {
            file_format = "(FORMAT 'CSV')";
        } else if to_str.ends_with(".parquet") {
            file_format = "(FORMAT 'PARQUET')";
        } else {
            file_format = "";
        }
        println!("{}", to_str);
        sql = format!("COPY ({}) TO '{}' {}", sql, to_str, file_format);
        println!("\nsql:\n{}", sql);
    }

    if args.from.is_some() {
        let data = query(&sql);
        println!("{}", data);
    }

    Ok(())
}
