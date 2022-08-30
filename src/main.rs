#![allow(unused)]

mod backends;

use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use camino::Utf8PathBuf;
use std::io::prelude::*;
use std::io;
use std::fs;

use clap::Parser;
use prql_compiler::compile;

cfg_if::cfg_if! {
    if #[cfg(feature = "datafusion")] {
        const DEFAULT_BACKEND : &str = "datafusion";
    } else if #[cfg(feature = "duckdb")] {
        const DEFAULT_BACKEND : &str = "duckdb";
    } else {
        const DEFAULT_BACKEND : &str = "";
    }
}

/// prql: query and transform data with PRQL
#[derive(Parser,Debug)]
struct Cli {
    /// The file to read data FROM if given
    #[clap(short, long, value_parser)]
    from: Option<Utf8PathBuf>,

    /// The file to write TO if given, otherwise stdout
    #[clap(short, long, value_parser, default_value = "-")]
    to: Utf8PathBuf,

    /// The backend to use to process the query
    #[clap(short, long, value_parser, default_value = DEFAULT_BACKEND)]
    backend: String,

    /// Only generate SQL without executing it against files
    #[clap(long, value_parser)]
    no_exec: bool,

    /// The PRQL query to be processed if given, otherwise stdin
    #[clap(value_parser, default_value = "-")]
    prql: String,
}

fn main() -> Result<()> {
    env_logger::init();

    let mut output = String::from("");

    let args = Cli::parse();
    info!("args = {args:?}");

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
    info!("prql = {prql:?}");

    let to = args.to.to_string();

    if args.from.is_some() || args.no_exec {
        output = compile(&prql)?;
    } else {
        let from = args.from.unwrap().to_string();

        let mut found_backend = false;
        #[cfg(feature = "duckdb")]
        if args.backend == "duckdb" {
            output = backends::duckdb::query(&prql, &from, &to)?;
            found_backend = true;
        } 
        #[cfg(feature = "datafusion")]
        if args.backend == "datafusion" {
            // Create a tokio runtime to run async datafusion code
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            output = match rt.block_on(backends::datafusion::query(&prql, &from, &to)) {
                Ok(s) => s,
                Err(e) => return Err(e.into()),
            };
            found_backend = true;
        }
        if !found_backend {
            dbg!(&args.backend);
            unimplemented!("{}", &args.backend);
        }
    }

    if to == "-" && output != "" {
        println!("{}", output);
    }


    Ok(())
}
