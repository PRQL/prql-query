#![allow(unused)]

mod backends;

use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use camino::Utf8PathBuf;
use std::io::prelude::*;
use std::{io,fs};
use std::collections::HashMap;

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

// Some type aliases for consistency
type FromType = Vec<Utf8PathBuf>;
type ToType = Utf8PathBuf;
const FROM_PLACEHOLDER : &str = "__PRQL_PLACEHOLDER__";

/// prql: query and transform data with PRQL
#[derive(Parser,Debug)]
struct Cli {
    /// The file(s) to read data FROM if given
    #[clap(short, long, value_parser)]
    from: Vec<Utf8PathBuf>,

    /// The file to write TO if given, otherwise stdout
    #[clap(short, long, value_parser, default_value = "-")]
    to: Utf8PathBuf,

    /// The backend to use to process the query
    #[clap(short, long, value_parser, default_value = DEFAULT_BACKEND)]
    backend: String,

    /// Only generate SQL without executing it against files
    #[clap(long, value_parser)]
    no_exec: bool,

    /// The PRQL query to be processed if given, otherwise read from stdin
    #[clap(value_parser, default_value = "-")]
    prql: String,
}

fn standardise_sources(from: &FromType) -> Result<Vec<(String,String)>> {
    debug!("from={from:?}");
    let mut sources : Vec<(String, String)> = Vec::<(String, String)>::new();
    for (i, filepath) in from.iter().enumerate() {
        let filestr = filepath.to_string();
        let mut parts : Vec<String> = filestr.split("=").map(|s| s.to_string()).collect();
        if parts.len()==1 {
            let components : Vec<&str> = filepath.components().map(|c| c.as_str()).collect();
            let fileparts : Vec<&str> = components.last().ok_or(anyhow!("There was no last component of: {}", filepath))?.split(".").collect();
            let newparts = format!("{}={}", fileparts[0], parts[0].to_string()).to_string();
            parts = newparts.split("=").map(|s| s.to_string()).collect();
        }
        sources.push((parts[0].to_string(), parts[1].to_string()));
    }
    debug!("sources={sources:?}");
    Ok(sources)
}

fn main() -> Result<()> {
    env_logger::init();

    let mut output = String::from("");

    let args = Cli::parse();
    debug!("args = {args:?}");

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
    prql = prql.trim().to_string();
    debug!("prql = {prql:?}");

    let to = args.to.to_string().trim_end_matches('/').to_string();

    if args.from.len()==0 || args.no_exec {
        output = compile(&prql)?;
    } else {

        let mut found_backend = false;
        #[cfg(feature = "duckdb")]
        if args.backend == "duckdb" {
            output = backends::duckdb::query(&prql, &args.from, &args.to)?;
            found_backend = true;
        } 
        #[cfg(feature = "datafusion")]
        if args.backend == "datafusion" {
            // Create a tokio runtime to run async datafusion code
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            output = match rt.block_on(backends::datafusion::query(&prql, &args.from, &args.to)) {
                Ok(s) => s,
                Err(e) => return Err(e.into()),
            };
            found_backend = true;
        }
        if !found_backend {
            // println!("No backends found! Consider running with the -no-exec flag set.");
            return Err(anyhow!("No backends found! Consider running with the -no-exec flag set."));
        }
    }

    if to == "-" && output != "" {
        println!("{}", output);
    }


    Ok(())
}
