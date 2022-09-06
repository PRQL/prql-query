#![allow(unused)]

mod backends;

use anyhow::{Result, anyhow};
use log::{debug, info, warn, error};

use std::collections::{HashMap, HashSet};
use std::io::prelude::*;
use std::{io,fs};
use camino::Utf8Path;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;

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

const SUPPORTED_FILE_TYPES : [&str; 4] = ["csv", "parquet", "json", "avro"];

// Some type aliases for consistency
type FromType = Vec<String>;
type ToType = String;
type SourcesType = Vec<(String,String)>;
type QueryResult = Vec<RecordBatch>;

/// prql: query and transform data with PRQL
#[derive(Parser,Debug)]
struct Cli {
    /// The file(s) to read data FROM if given
    #[clap(short, long, value_parser, env = "PRQL_FROM")]
    from: Vec<String>,

    /// The file to write TO if given, otherwise stdout
    #[clap(short, long, value_parser, default_value = "-", env = "PRQL_TO")]
    to: String,

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
        let rbs : Vec<RecordBatch>;

        #[cfg(feature = "datafusion")]
        if backend == "datafusion" {
            // Create a tokio runtime to run async datafusion code
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            output = match rt.block_on(backends::datafusion::query(&query, &sources, &args.to, &database)) {
                Ok(s) => s,
                Err(e) => return Err(e.into()),
            };
            found_backend = true;
        }
        #[cfg(feature = "connectorx")]
        if backend == "connectorx" {
            output = backends::connectorx::query(&query, &sources, &args.to, &database)?;
            found_backend = true;
        } 
        #[cfg(feature = "duckdb")]
        if backend == "duckdb" {
            output = backends::duckdb::query(&query, &sources, &args.to, &database)?;
            found_backend = true;
        } 
        if !found_backend {
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
    let supported_file_types : HashSet<&str> = HashSet::from(SUPPORTED_FILE_TYPES);
    // let mut sources : Vec<(String, String)> = Vec::<(String, String)>::new();
    let mut sources : SourcesType = SourcesType::new();
    for fromstr in from.iter() {
        let mut fromparts : Vec<&str> = fromstr.split("=").collect();
        // FIXME: Should only to the following for files, currently this is getting
        //        it wrong for tablenames of the form schema_name.table_name.
        if fromparts.len()==1 {
            let filepath = Utf8Path::new(fromparts[0]);
            let fileext = filepath.extension()
                .ok_or(anyhow!("No extension in: {filepath}"))?;
            if supported_file_types.contains(&fileext) {
                // Dealing with a file
                let last_component = filepath.components().last()
                    .ok_or(anyhow!("There was no last component of: {filepath}"))?;
                let filename = last_component.as_str().split(".").next()
                    .ok_or(anyhow!("No filename found in: {last_component}"))?;
                fromparts = vec![filename, fromparts[0]];
            } else {
                // Dealing with a possible tablename with schema prefix
                let tableparts : Vec<&str> = fromparts[0].split(" ").collect();
                let tablename = tableparts.last()
                    .ok_or(anyhow!("No last tablepart"))?;
                fromparts = vec![tablename, fromparts[0]];
            }
        }
        sources.push((fromparts[0].to_string(), fromparts[1].to_string()));
    }
    debug!("sources={sources:?}");
    Ok(sources)
}
