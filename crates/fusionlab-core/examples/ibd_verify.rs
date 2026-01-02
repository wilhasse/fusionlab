//! Example: Verify IBD reading by querying a .ibd file directly
//!
//! Usage:
//!   cargo run --example ibd_verify -- <ibd_path> <sdi_path>
//!
//! Example:
//!   cargo run --example ibd_verify -- /path/to/table.ibd /path/to/sdi.json

use fusionlab_core::DataFusionRunner;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <ibd_path> <sdi_path> [query]", args[0]);
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} table.ibd sdi.json", args[0]);
        eprintln!("  {} table.ibd sdi.json \"SELECT COUNT(*) FROM table\"", args[0]);
        std::process::exit(1);
    }

    let ibd_path = &args[1];
    let sdi_path = &args[2];
    let query = args.get(3).map(|s| s.as_str()).unwrap_or("SELECT * FROM ibd_table ORDER BY 1");

    let runner = DataFusionRunner::new();

    // Register the IBD file as a table
    match runner.register_ibd(Some("ibd_table"), ibd_path, sdi_path) {
        Ok(_) => println!("✓ Registered table from: {}", ibd_path),
        Err(e) => {
            eprintln!("✗ Failed to register IBD table: {}", e);
            std::process::exit(1);
        }
    }

    // Run query
    println!("Query: {}", query);
    println!();

    match runner.run_query_collect(query).await {
        Ok(result) => {
            println!("{}", result.to_table());
            println!();
            println!("Rows: {} | Duration: {:.2}ms", result.row_count, result.duration_ms);
        }
        Err(e) => {
            eprintln!("✗ Query failed: {}", e);
            std::process::exit(1);
        }
    }
}
