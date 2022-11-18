// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::env;
use std::path::Path;

use ballista::prelude::{BallistaConfig, BallistaContext, Result};
use ballista_cli::{
    exec, print_format::PrintFormat, print_options::PrintOptions, BALLISTA_CLI_VERSION,
};
use clap::Parser;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'p',
        long,
        help = "Path to your data, default to current directory",
        validator(is_valid_data_dir)
    )]
    data_path: Option<String>,

    #[clap(
        short = 'c',
        long,
        help = "The batch size of each query, or use Ballista default",
        validator(is_valid_batch_size)
    )]
    batch_size: Option<usize>,

    #[clap(
        long,
        help = "The max concurrent tasks, only for Ballista local mode. Default: all available cores",
        validator(is_valid_concurrent_tasks_size)
    )]
    concurrent_tasks: Option<usize>,

    #[clap(
        short,
        long,
        multiple_values = true,
        help = "Execute commands from file(s), then exit",
        validator(is_valid_file)
    )]
    file: Vec<String>,

    #[clap(
        short = 'r',
        long,
        multiple_values = true,
        help = "Run the provided files on startup instead of ~/.ballistarc",
        validator(is_valid_file),
        conflicts_with = "file"
    )]
    rc: Option<Vec<String>>,

    #[clap(long, arg_enum, default_value_t = PrintFormat::Table)]
    format: PrintFormat,

    #[clap(long, help = "Ballista scheduler host")]
    host: Option<String>,

    #[clap(long, help = "Ballista scheduler port")]
    port: Option<u16>,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if !args.quiet {
        println!("Ballista CLI v{}", BALLISTA_CLI_VERSION);
    }

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(p).unwrap();
    };

    let mut ballista_config_builder =
        BallistaConfig::builder().set("ballista.with_information_schema", "true");

    if let Some(batch_size) = args.batch_size {
        ballista_config_builder =
            ballista_config_builder.set("ballista.batch.size", &batch_size.to_string());
    };

    let ballista_config = ballista_config_builder.build()?;

    let ctx = match (args.host, args.port) {
        (Some(ref host), Some(port)) => {
            // Distributed execution with Ballista Remote
            BallistaContext::remote(host, port, &ballista_config).await?
        }
        _ => {
            let concurrent_tasks = if let Some(concurrent_tasks) = args.concurrent_tasks {
                concurrent_tasks
            } else {
                num_cpus::get()
            };
            // In-process execution with Ballista Standalone
            BallistaContext::standalone(&ballista_config, concurrent_tasks, None).await?
        }
    };

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
    };

    let files = args.file;
    let rc = match args.rc {
        Some(file) => file,
        None => {
            let mut files = Vec::new();
            let home = dirs::home_dir();
            if let Some(p) = home {
                let home_rc = p.join(".ballistarc");
                if home_rc.exists() {
                    files.push(home_rc.into_os_string().into_string().unwrap());
                }
            }
            files
        }
    };
    if !files.is_empty() {
        exec::exec_from_files(files, &ctx, &print_options).await
    } else {
        if !rc.is_empty() {
            exec::exec_from_files(rc, &ctx, &print_options).await
        }
        exec::exec_from_repl(&ctx, &mut print_options).await;
    }

    Ok(())
}

fn is_valid_file(dir: &str) -> std::result::Result<(), String> {
    if Path::new(dir).is_file() {
        Ok(())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn is_valid_data_dir(dir: &str) -> std::result::Result<(), String> {
    if Path::new(dir).is_dir() {
        Ok(())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn is_valid_batch_size(size: &str) -> std::result::Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid batch size '{}'", size)),
    }
}

fn is_valid_concurrent_tasks_size(size: &str) -> std::result::Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid concurrent_tasks size '{}'", size)),
    }
}
