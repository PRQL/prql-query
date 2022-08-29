#![allow(unused)]

mod backends;

use anyhow::{Result, anyhow};

use camino::Utf8PathBuf;
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
    from: Option<Utf8PathBuf>,

    /// The file to write TO if given, otherwise stdout
    #[clap(short, long, value_parser, default_value = "-")]
    to: Utf8PathBuf,

    /// The backend to use to process the query
    #[clap(short, long, value_parser, default_value = "datafusion")]
    backend: String,

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
    } else if args.prql.ends_with(".prql") {
        prql = fs::read_to_string(&args.prql)?;
    }
    else {
        prql = String::from(&args.prql);
    }

    let to = args.to.to_string();

    if args.from.is_none() {
        output = compile(&prql)?;
    } else {
        let from = args.from.unwrap().to_string();

        if args.backend == "duckdb" {
            output = backends::duckdb::query(&prql, &from, &to)?;
        } else if args.backend == "datafusion" {
            // Create a tokio runtime to run async datafusion code
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            output = match rt.block_on(backends::datafusion::query(&prql, &from, &to)) {
                Ok(s) => s,
                Err(e) => return Err(e.into()),
            }
        } else {
            dbg!(&args.backend);
            unimplemented!("{}", &args.backend);
        }
    }

    if to == "-" && output != "" {
        println!("{}", output);
    }


    Ok(())
}
