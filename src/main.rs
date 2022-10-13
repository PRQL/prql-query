#![allow(unused)]

mod backends;

use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};

use camino::Utf8Path;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display};
use std::io::prelude::*;
use std::{fs, io};

use clap::{Parser, ValueEnum};
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

const SUPPORTED_FILE_TYPES: [&str; 4] = ["csv", "json", "parquet", "avro"];

// Some type aliases for consistency
type FromType = Vec<String>;
type SourcesType = Vec<(String, String)>;

/// pq: query and transform data with PRQL
#[derive(Parser, Debug)]
struct Cli {
    /// The file(s) to read data FROM if given
    #[clap(short, long, value_parser, env = "PQ_FROM")]
    from: Vec<String>,

    /// The file to write TO if given, otherwise stdout
    #[clap(short, long, value_parser, default_value = "-", env = "PQ_TO")]
    to: String,

    /// The database to connect to
    #[clap(short, long, value_parser, env = "PQ_DATABASE")]
    database: Option<String>,

    /// The backend to use to process the query
    #[clap(short, long, value_parser, env = "PQ_BACKEND")]
    backend: Option<String>,

    /// Only generate SQL without executing it against files
    #[clap(long, value_parser)]
    no_exec: bool,

    /// The format to use for the output
    #[clap(long, arg_enum, value_parser, env = "PQ_FORMAT")]
    format: Option<OutputFormat>,

    /// The Writer to use for writing the output
    #[clap(
        short,
        long,
        arg_enum,
        value_parser,
        default_value = "arrow",
        env = "PQ_WRITER"
    )]
    writer: OutputWriter,

    /// set this to pass a SQL query rather than a PRQL one
    #[clap(long, value_parser, default_value = "false", env = "PQ_SQL")]
    sql: bool,

    /// The PRQL query to be processed if given, otherwise read from stdin
    #[clap(value_parser, default_value = "-", env = "PQ_QUERY")]
    query: String,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[allow(non_camel_case_types)]
pub enum OutputFormat {
    csv,
    json,
    parquet,
    table,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
        // or, alternatively:
        // fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Copy, Clone, ValueEnum)]
#[allow(non_camel_case_types)]
pub enum OutputWriter {
    arrow,
    backend,
}

fn main() -> Result<()> {
    env_logger::init();
    dotenvy::dotenv().ok();

    let args = Cli::parse();
    debug!("args = {args:?}");

    // args.query
    let mut query: String;
    if args.query == "-" {
        if atty::is(atty::Stream::Stdin) {
            println!("Enter QUERY, then press Ctrl-d:");
            println!();
        }
        query = String::new();
        io::stdin().read_to_string(&mut query);
        println!("---");
    } else if args.query.ends_with(".prql") {
        query = fs::read_to_string(&args.query)?;
    } else {
        query = String::from(&args.query);
    }
    query = query.trim().to_string();
    debug!("query = {query:?}");

    // args.from
    // determine the sources
    let sources = standardise_sources(&args.from)?;

    if !args.sql {
        // insert `from` clause in main pipeline if not given
        if !query.to_lowercase().contains("from") && sources.len() > 0 {
            query = format!("from {}|{query}", sources.last().unwrap().0);
        }
        debug!("query = {query:?}");
    }

    // args.sql
    if !args.sql && !query.starts_with("prql ") {
        // prepend a PRQL header to signal this is a PRQL query rather than a SQL one
        query = format!("prql version:1 dialect:ansi\n{query}")
    }
    debug!("query = {query:?}");

    // args.to
    let to = args.to.to_string().trim_end_matches('/').to_string();
    debug!("to = {to:?}");

    debug!("args.format = {0:?}", &args.format);
    let format: OutputFormat;
    if let Some(args_format) = args.format {
        if to == "-"
            && atty::is(atty::Stream::Stdout)
            && vec![OutputFormat::parquet].contains(&args_format)
        {
            return Err(anyhow!("Cannot print format={args_format:?} to stdout."));
        } else if to != "-" && !to.ends_with(&args_format.to_string()) {
            return Err(anyhow!(
                "to={to:?} is incompatible with format={args_format:?}!"
            ));
        }
        format = args_format;
    } else {
        // i.e. args.format.is_none()
        if to == "-" {
            format = OutputFormat::table;
        } else {
            format = match to
                .split(".")
                .last()
                .ok_or(anyhow!("No extension format found in {to:?}"))?
            {
                "csv" => OutputFormat::csv,
                "json" => OutputFormat::json,
                "parquet" => OutputFormat::parquet,
                "table" | "tbl" => OutputFormat::table,
                fileext => return Err(anyhow!(".{fileext} files are currently not supported.")),
            };
        }
        info!("inferred format = {format:?}");
    }
    debug!("format = {0:?}", &args.format);

    let mut backend: String = String::from("");
    let mut database: String = String::from("");

    debug!("args.database = {0:?}", &args.database);
    if let Some(args_database) = args.database {
        backend = if args_database.starts_with("duckdb://") {
            String::from("duckdb")
        } else {
            // FIXME: Replace this with connectorx when implemented
            String::from("duckdb")
        };
        database = args_database.to_string();
    } else {
        backend = String::from(DEFAULT_BACKEND);
    }
    debug!("database = {database:?}");

    debug!("args.backend = {0:?}", &args.backend);
    if let Some(args_backend) = args.backend {
        // an explicitly provided backend overrides the one we inferred
        backend = args_backend;
    }
    debug!("backend = {backend:?}");

    debug!("args.writer = {0:?}", &args.writer);

    if args.no_exec || (database == "" && args.from.len() == 0 && !args.sql) {
        let sql = get_sql_from_query(&query)?;
        println!("{}", &sql);
    } else {
        let mut found_backend = false;

        #[cfg(feature = "datafusion")]
        if backend == "datafusion" {
            // Create a tokio runtime to run async datafusion code
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            rt.block_on(backends::datafusion::query(
                &query,
                &sources,
                &to,
                &database,
                &format,
                &args.writer,
            ))?;
            found_backend = true;
        }
        #[cfg(feature = "duckdb")]
        if backend == "duckdb" {
            backends::duckdb::query(&query, &sources, &to, &database, &format, &args.writer)?;
            found_backend = true;
        }
        if !found_backend {
            return Err(anyhow!(
                "No backends found! Consider running with the -no-exec flag set."
            ));
        }
    }

    Ok(())
}

fn get_dest_from_to(to: &str) -> Result<Box<dyn Write>> {
    // determine the destination
    let mut dest: Box<dyn Write>;
    if to == "-" {
        dest = Box::new(std::io::stdout());
    } else {
        dest = Box::new(std::fs::File::create(&to)?);
    }
    Ok(dest)
}

fn get_sql_from_query(query: &str) -> Result<String> {
    let sql = if query.starts_with("prql ") {
        compile(query)?
    } else {
        query.to_string()
    };
    Ok(sql)
}

fn standardise_sources(from: &FromType) -> Result<SourcesType> {
    debug!("from={from:?}");
    let supported_file_types: HashSet<&str> = HashSet::from(SUPPORTED_FILE_TYPES);
    // let mut sources : Vec<(String, String)> = Vec::<(String, String)>::new();
    let mut sources: SourcesType = SourcesType::new();
    for fromstr in from.iter() {
        let mut fromparts: Vec<&str> = fromstr.split("=").collect();
        // FIXME: Should only to the following for files, currently this is getting
        //        it wrong for tablenames of the form schema_name.table_name.
        if fromparts.len() == 1 {
            let filepath = Utf8Path::new(fromparts[0]);
            let fileext = filepath
                .extension()
                .ok_or(anyhow!("No extension in: {filepath}"))?;
            if supported_file_types.contains(&fileext) {
                // Dealing with a file
                let last_component = filepath
                    .components()
                    .last()
                    .ok_or(anyhow!("There was no last component of: {filepath}"))?;
                let filename = last_component
                    .as_str()
                    .split(".")
                    .next()
                    .ok_or(anyhow!("No filename found in: {last_component}"))?;
                fromparts = vec![filename, fromparts[0]];
            } else {
                // Dealing with a possible tablename with schema prefix
                let tableparts: Vec<&str> = fromparts[0].split(" ").collect();
                let tablename = tableparts.last().ok_or(anyhow!("No last tablepart"))?;
                fromparts = vec![tablename, fromparts[0]];
            }
        }
        sources.push((fromparts[0].to_string(), fromparts[1].to_string()));
    }
    debug!("sources={sources:?}");
    Ok(sources)
}
