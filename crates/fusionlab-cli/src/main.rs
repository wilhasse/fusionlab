//! FusionLab CLI - Query runner and comparison tool
//!
//! A CLI tool for running queries against different execution strategies
//! and comparing their performance.

use clap::{Parser, Subcommand};
use fusionlab_core::{MySQLConfig, MySQLRunner};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "fusionlab")]
#[command(about = "FusionLab - Query execution strategies comparison tool")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a query directly against MySQL (baseline)
    Mysql {
        /// SQL query to execute
        #[arg(group = "input")]
        sql: Option<String>,

        /// Read SQL from a file
        #[arg(short, long, group = "input")]
        file: Option<PathBuf>,

        /// Show EXPLAIN output
        #[arg(short, long)]
        explain: bool,

        /// Show EXPLAIN ANALYZE output (MySQL 8.0.18+)
        #[arg(short, long)]
        analyze: bool,

        /// MySQL host
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// MySQL port
        #[arg(long, default_value = "3306")]
        port: u16,

        /// MySQL user
        #[arg(long, default_value = "root")]
        user: String,

        /// MySQL password
        #[arg(long, default_value = "root")]
        password: String,

        /// MySQL database
        #[arg(long, default_value = "ssb")]
        database: String,

        /// Show first N rows of results (0 = don't show rows)
        #[arg(long, default_value = "10")]
        show_rows: usize,
    },
    // Future commands (Step 1+):
    // Df { ... }      - Run via DataFusion
    // Explain { ... } - DataFusion EXPLAIN
    // Analyze { ... } - DataFusion EXPLAIN ANALYZE
    // Semijoin { ... } - Semijoin reduction strategy
    // Replay { ... }  - Replay workload
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Mysql {
            sql,
            file,
            explain,
            analyze,
            host,
            port,
            user,
            password,
            database,
            show_rows,
        } => {
            // Get SQL from argument or file
            let sql = match (sql, file) {
                (Some(s), _) => s,
                (_, Some(f)) => std::fs::read_to_string(&f)
                    .map_err(|e| anyhow::anyhow!("Failed to read file {:?}: {}", f, e))?,
                (None, None) => {
                    anyhow::bail!("Either SQL query or --file must be provided");
                }
            };

            let config = MySQLConfig {
                host,
                port,
                user,
                password: Some(password),
                database,
            };

            let runner = MySQLRunner::new(&config)?;

            // Print query
            println!("Query: {}", sql.trim());
            println!();

            // Run EXPLAIN if requested
            if explain {
                println!("[EXPLAIN]");
                let explain_output = runner.run_explain(&sql).await?;
                println!("{}", explain_output);
            }

            // Run EXPLAIN ANALYZE if requested
            if analyze {
                println!("[EXPLAIN ANALYZE]");
                let analyze_output = runner.run_explain_analyze(&sql).await?;
                println!("{}", analyze_output);
                println!();
            }

            // Run the actual query
            let result = runner.run_query(&sql).await?;

            // Print results
            println!("Rows:  {}", result.row_count);
            println!("Time:  {:.2}ms", result.duration_ms);

            // Show sample rows if requested
            if show_rows > 0 && !result.rows.is_empty() {
                println!();
                println!("[Results (first {} rows)]", show_rows.min(result.row_count));

                // Print header
                if !result.columns.is_empty() {
                    println!("{}", result.columns.join(" | "));
                    println!("{}", "-".repeat(60));
                }

                // Print rows
                for row in result.rows.iter().take(show_rows) {
                    println!("{}", row.join(" | "));
                }
            }

            runner.close().await;
        }
    }

    Ok(())
}
