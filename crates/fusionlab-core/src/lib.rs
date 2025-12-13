//! FusionLab Core - Query execution and metrics
//!
//! Provides MySQL query runner with timing and EXPLAIN support.

use mysql_async::{prelude::*, Pool, Row};
use std::time::Instant;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FusionLabError {
    #[error("MySQL error: {0}")]
    MySQL(#[from] mysql_async::Error),
    #[error("Connection error: {0}")]
    Connection(String),
}

pub type Result<T> = std::result::Result<T, FusionLabError>;

/// Result of running a query
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Number of rows returned
    pub row_count: usize,
    /// Query execution time in milliseconds
    pub duration_ms: f64,
    /// Raw rows (for display)
    pub rows: Vec<Vec<String>>,
    /// Column names
    pub columns: Vec<String>,
}

/// Configuration for MySQL connection
#[derive(Debug, Clone)]
pub struct MySQLConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

impl Default for MySQLConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: Some("root".to_string()),
            database: "ssb".to_string(),
        }
    }
}

impl MySQLConfig {
    pub fn connection_url(&self) -> String {
        match &self.password {
            Some(pwd) => format!(
                "mysql://{}:{}@{}:{}/{}",
                self.user, pwd, self.host, self.port, self.database
            ),
            None => format!(
                "mysql://{}@{}:{}/{}",
                self.user, self.host, self.port, self.database
            ),
        }
    }
}

/// MySQL query runner with timing support
pub struct MySQLRunner {
    pool: Pool,
}

impl MySQLRunner {
    /// Create a new MySQL runner with the given configuration
    pub fn new(config: &MySQLConfig) -> Result<Self> {
        let url = config.connection_url();
        let pool = Pool::new(url.as_str());
        Ok(Self { pool })
    }

    /// Run a query and return results with timing
    pub async fn run_query(&self, sql: &str) -> Result<QueryResult> {
        let mut conn = self.pool.get_conn().await?;

        let start = Instant::now();
        let rows: Vec<Row> = conn.query(sql).await?;
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        // Extract column names from the first row if available
        let columns: Vec<String> = if let Some(first_row) = rows.first() {
            first_row
                .columns_ref()
                .iter()
                .map(|c| c.name_str().to_string())
                .collect()
        } else {
            vec![]
        };

        // Convert rows to strings for display
        let row_count = rows.len();
        let string_rows: Vec<Vec<String>> = rows
            .into_iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| {
                        row.get::<mysql_async::Value, _>(i)
                            .map(|v| format_value(&v))
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect()
            })
            .collect();

        drop(conn);

        Ok(QueryResult {
            row_count,
            duration_ms,
            rows: string_rows,
            columns,
        })
    }

    /// Run EXPLAIN on a query and return the output
    pub async fn run_explain(&self, sql: &str) -> Result<String> {
        let explain_sql = format!("EXPLAIN {}", sql);
        let result = self.run_query(&explain_sql).await?;
        Ok(format_table(&result.columns, &result.rows))
    }

    /// Run EXPLAIN ANALYZE on a query (MySQL 8.0.18+)
    pub async fn run_explain_analyze(&self, sql: &str) -> Result<String> {
        let explain_sql = format!("EXPLAIN ANALYZE {}", sql);
        let result = self.run_query(&explain_sql).await?;

        // EXPLAIN ANALYZE returns a single column with the tree output
        let output: String = result
            .rows
            .iter()
            .map(|row| row.first().cloned().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");

        Ok(output)
    }

    /// Close the connection pool
    pub async fn close(self) {
        self.pool.disconnect().await.ok();
    }
}

/// Format a MySQL value as a string
fn format_value(value: &mysql_async::Value) -> String {
    match value {
        mysql_async::Value::NULL => "NULL".to_string(),
        mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        mysql_async::Value::Int(i) => i.to_string(),
        mysql_async::Value::UInt(u) => u.to_string(),
        mysql_async::Value::Float(f) => f.to_string(),
        mysql_async::Value::Double(d) => d.to_string(),
        mysql_async::Value::Date(y, m, d, h, min, s, _) => {
            format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, min, s)
        }
        mysql_async::Value::Time(neg, d, h, m, s, _) => {
            let sign = if *neg { "-" } else { "" };
            format!("{}{}:{:02}:{:02}", sign, d * 24 + (*h as u32), m, s)
        }
    }
}

/// Format query results as an ASCII table
fn format_table(columns: &[String], rows: &[Vec<String>]) -> String {
    if columns.is_empty() {
        return String::new();
    }

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let mut output = String::new();

    // Header separator
    let separator: String = widths
        .iter()
        .map(|w| "-".repeat(*w + 2))
        .collect::<Vec<_>>()
        .join("+");
    let separator = format!("+{}+\n", separator);

    output.push_str(&separator);

    // Header row
    let header: String = columns
        .iter()
        .zip(&widths)
        .map(|(col, w)| format!(" {:width$} ", col, width = w))
        .collect::<Vec<_>>()
        .join("|");
    output.push_str(&format!("|{}|\n", header));

    output.push_str(&separator);

    // Data rows
    for row in rows {
        let row_str: String = row
            .iter()
            .zip(&widths)
            .map(|(cell, w)| format!(" {:width$} ", cell, width = w))
            .collect::<Vec<_>>()
            .join("|");
        output.push_str(&format!("|{}|\n", row_str));
    }

    output.push_str(&separator);

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_config_url() {
        let config = MySQLConfig::default();
        assert_eq!(
            config.connection_url(),
            "mysql://root:root@127.0.0.1:3306/ssb"
        );
    }

    #[test]
    fn test_format_table() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ];
        let table = format_table(&columns, &rows);
        assert!(table.contains("id"));
        assert!(table.contains("Alice"));
    }
}
