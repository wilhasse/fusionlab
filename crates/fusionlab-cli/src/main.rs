//! FusionLab CLI - Query runner and comparison tool
//!
//! A CLI tool for running queries against different execution strategies
//! and comparing their performance.

use clap::{Parser, Subcommand, ValueEnum};
use fusionlab_core::{DataFusionRunner, MySQLConfig, MySQLRunner};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "fusionlab")]
#[command(about = "FusionLab - Query execution strategies comparison tool")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, ValueEnum)]
enum DataSource {
    /// Use in-memory SSB sample data
    Mem,
    /// Load data from CSV files (specify --csv-dir)
    Csv,
}

#[derive(Clone, ValueEnum)]
enum ExecutionMode {
    /// Collect all results at once
    Collect,
    /// Stream results incrementally
    Stream,
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

    /// Run a query using DataFusion (local Arrow execution)
    Df {
        /// SQL query to execute
        #[arg(group = "input")]
        sql: Option<String>,

        /// Read SQL from a file
        #[arg(short, long, group = "input")]
        file: Option<PathBuf>,

        /// Data source to use
        #[arg(long, value_enum, default_value = "mem")]
        source: DataSource,

        /// Directory containing CSV files (for --source=csv)
        #[arg(long)]
        csv_dir: Option<PathBuf>,

        /// Execution mode
        #[arg(long, value_enum, default_value = "collect")]
        mode: ExecutionMode,

        /// Show logical plan
        #[arg(short, long)]
        explain: bool,

        /// Show physical plan
        #[arg(short, long)]
        physical: bool,

        /// Show first N rows of results (0 = don't show rows)
        #[arg(long, default_value = "10")]
        show_rows: usize,
    },
    // Future commands:
    // Explain { ... } - DataFusion EXPLAIN (detailed)
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

        Commands::Df {
            sql,
            file,
            source,
            csv_dir,
            mode,
            explain,
            physical,
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

            let runner = DataFusionRunner::new();

            // Register data source
            match source {
                DataSource::Mem => {
                    println!("[DataFusion] Using in-memory SSB sample data");
                    runner
                        .register_ssb_sample()
                        .map_err(|e| anyhow::anyhow!("Failed to register sample data: {}", e))?;
                }
                DataSource::Csv => {
                    let csv_dir = csv_dir.ok_or_else(|| {
                        anyhow::anyhow!("--csv-dir is required when using --source=csv")
                    })?;
                    println!("[DataFusion] Loading CSV files from {:?}", csv_dir);

                    // Register SSB tables from CSV files
                    for table in &["lineorder", "customer", "supplier", "part", "date"] {
                        let path = csv_dir.join(format!("{}.csv", table));
                        if path.exists() {
                            runner
                                .register_csv(table, path.to_str().unwrap())
                                .await
                                .map_err(|e| {
                                    anyhow::anyhow!("Failed to register {}: {}", table, e)
                                })?;
                            println!("  Registered table: {}", table);
                        } else {
                            println!("  Warning: {} not found at {:?}", table, path);
                        }
                    }
                }
            }
            println!();

            // Print query
            println!("Query: {}", sql.trim());
            println!();

            // Show logical plan if requested
            if explain {
                println!("[Logical Plan]");
                let plan = runner
                    .explain(&sql)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get explain: {}", e))?;
                println!("{}", plan);
                println!();
            }

            // Show physical plan if requested
            if physical {
                println!("[Physical Plan]");
                let plan = runner
                    .explain_physical(&sql)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get physical plan: {}", e))?;
                println!("{}", plan);
                println!();
            }

            // Run the query
            let result = match mode {
                ExecutionMode::Collect => {
                    println!("[Execution Mode: collect]");
                    runner
                        .run_query_collect(&sql)
                        .await
                        .map_err(|e| anyhow::anyhow!("Query failed: {}", e))?
                }
                ExecutionMode::Stream => {
                    println!("[Execution Mode: stream]");
                    runner
                        .run_query_stream(&sql)
                        .await
                        .map_err(|e| anyhow::anyhow!("Query failed: {}", e))?
                }
            };

            // Print results
            println!("Rows:  {}", result.row_count);
            println!("Time:  {:.2}ms", result.duration_ms);

            // Show sample rows if requested
            if show_rows > 0 && result.row_count > 0 {
                println!();
                println!("[Results]");
                println!("{}", result.to_table());
            }
        }
    }

    Ok(())
}
