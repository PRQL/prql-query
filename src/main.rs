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
type SourcesType = Vec<(String,String)>;

/// prql: query and transform data with PRQL
#[derive(Parser,Debug)]
struct Cli {
    /// The file(s) to read data FROM if given
    #[clap(short, long, value_parser, env = "PRQL_FROM")]
    from: Vec<Utf8PathBuf>,

    /// The file to write TO if given, otherwise stdout
    #[clap(short, long, value_parser, default_value = "-", env = "PRQL_TO")]
    to: Utf8PathBuf,

    /// The database to connect to
    #[clap(short, long, value_parser, env = "PRQL_DATABASE")]
    database: Option<String>,
    
    /// The backend to use to process the query
    #[clap(short, long, value_parser, env = "PRQL_BACKEND")]
    backend: Option<String>,

    /// Only generate SQL without executing it against files
    #[clap(long, value_parser)]
    no_exec: bool,

    /// The PRQL query to be processed if given, otherwise read from stdin
    #[clap(value_parser, default_value = "-", env = "PRQL_QUERY")]
    query: String,
}

fn main() -> Result<()> {
    env_logger::init();
    dotenvy::dotenv().ok();

    let mut output = String::from("");

    let args = Cli::parse();
    debug!("args = {args:?}");

    // args.query
    let mut query : String; 
    if args.query == "-" {
        query = String::new();
        io::stdin().read_to_string(&mut query);
    } else if args.query.ends_with(".prql") {
        query = fs::read_to_string(&args.query)?;
    }
    else {
        query = String::from(&args.query);
    }
    query = query.trim().to_string();
    debug!("query = {query:?}");

    // determine the sources
    let sources = standardise_sources(&args.from)?;

    // insert `from` clause in main pipeline if not given
    if ! query.to_lowercase().starts_with("from") && sources.len() > 0 {
        query = format!("from {}|{}", sources.last().unwrap().0, &query);
    }
    debug!("query = {query:?}");

    let to = args.to.to_string().trim_end_matches('/').to_string();

    let mut backend : String = String::from("");
    let mut database : String = String::from("");

    if let Some(args_database) = args.database {
        backend = if args_database.starts_with("duckdb") {
            String::from("duckdb")
        } else {
            String::from("connectorx")
        };
        database = args_database.to_string();
    } else {
        backend = String::from(DEFAULT_BACKEND);
    }
    debug!("database = {database:?}");
    //if args.backend.is_some() {
    if let Some(args_backend) = args.backend {
        // an explicitly provided backend overrides the one we inferred
        //backend = &args.backend.ok_or(anyhow!("No database given"))?.to_string();
        //backend = args_backend.clone();
        backend = args_backend;
    }
    debug!("backend = {backend:?}");

    if args.no_exec || (database=="" && args.from.len()==0)  {
        output = compile(&query)?;
    } else {
        let mut found_backend = false;

        #[cfg(feature = "connectorx")]
        if backend == "connectorx" {
            output = backends::connectorx::query(&query, &sources, &database)?;
            found_backend = true;
        } 
        #[cfg(feature = "datafusion")]
        if backend == "datafusion" {
            // Create a tokio runtime to run async datafusion code
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            output = match rt.block_on(backends::datafusion::query(&query, &sources, &args.to)) {
                Ok(s) => s,
                Err(e) => return Err(e.into()),
            };
            found_backend = true;
        }
        #[cfg(feature = "duckdb")]
        if backend == "duckdb" {
            output = backends::duckdb::query(&query, &sources, &args.to)?;
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

fn standardise_sources(from: &FromType) -> Result<SourcesType> {
    debug!("from={from:?}");
    // let mut sources : Vec<(String, String)> = Vec::<(String, String)>::new();
    let mut sources : SourcesType = SourcesType::new();
    for filepath in from.iter() {
        let filestr = filepath.as_str();
        let mut parts : Vec<&str> = filestr.split("=").collect();
        // FIXME: Should only to the following for files, currently this is getting
        //        it wrong for tablenames of the form schema_name.table_name.
        if parts.len()==1 {
            let last_component = filepath.components().last()
                .ok_or(anyhow!("There was no last component of: {}", filepath))?;
            let filename = last_component.as_str().split(".").next()
                .ok_or(anyhow!("No filename found in: {}", last_component))?;
            parts = vec![filename, parts[0]];
        }
        sources.push((parts[0].to_string(), parts[1].to_string()));
    }
    debug!("sources={sources:?}");
    Ok(sources)
}
