//! DataFusion query runner
//!
//! Provides local SQL execution using Apache DataFusion and Arrow.

use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use futures::StreamExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::ibd_provider::IbdTableProvider;
use crate::FusionLabError;

/// Result of running a DataFusion query
#[derive(Debug)]
pub struct DfQueryResult {
    /// Number of rows returned
    pub row_count: usize,
    /// Query execution time in milliseconds
    pub duration_ms: f64,
    /// Record batches (Arrow format)
    pub batches: Vec<RecordBatch>,
}

impl DfQueryResult {
    /// Format results as a pretty table
    pub fn to_table(&self) -> String {
        if self.batches.is_empty() {
            return "Empty result".to_string();
        }
        pretty_format_batches(&self.batches)
            .map(|t| t.to_string())
            .unwrap_or_else(|e| format!("Error formatting: {}", e))
    }
}

/// DataFusion query runner with in-memory data support
pub struct DataFusionRunner {
    ctx: SessionContext,
}

impl DataFusionRunner {
    /// Create a new DataFusion runner with an empty context
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        Self { ctx }
    }

    /// Get a reference to the session context
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get a mutable reference to the session context
    pub fn context_mut(&mut self) -> &mut SessionContext {
        &mut self.ctx
    }

    /// Register a CSV file as a table
    pub async fn register_csv(
        &self,
        table_name: &str,
        path: &str,
    ) -> Result<(), FusionLabError> {
        self.ctx
            .register_csv(table_name, path, CsvReadOptions::default())
            .await
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;
        Ok(())
    }

    /// Register an in-memory RecordBatch as a table
    pub fn register_batch(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<(), FusionLabError> {
        self.ctx
            .register_batch(table_name, batch)
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;
        Ok(())
    }

    /// Register an InnoDB .ibd file as a table
    ///
    /// # Arguments
    /// * `table_name` - Name to register the table as (or None to use the table's actual name)
    /// * `ibd_path` - Path to the .ibd file
    /// * `sdi_path` - Path to the SDI JSON file (from ibd2sdi)
    ///
    /// # Example
    /// ```ignore
    /// let runner = DataFusionRunner::new();
    /// runner.register_ibd(None, "/var/lib/mysql/mydb/mytable.ibd", "/tmp/mytable.json")?;
    /// let result = runner.run_query_collect("SELECT * FROM mytable").await?;
    /// ```
    pub fn register_ibd<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        table_name: Option<&str>,
        ibd_path: P,
        sdi_path: Q,
    ) -> Result<(), FusionLabError> {
        let provider = IbdTableProvider::try_new(ibd_path, sdi_path)
            .map_err(|e| FusionLabError::IbdReader(e.to_string()))?;

        let name = table_name
            .map(|s| s.to_string())
            .unwrap_or_else(|| provider.table_name().to_string());

        self.ctx
            .register_table(&name, Arc::new(provider))
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

        Ok(())
    }

    /// Register the SSB sample data for testing
    /// Creates small in-memory versions of SSB tables
    pub fn register_ssb_sample(&self) -> Result<(), FusionLabError> {
        // Sample lineorder data
        let lineorder = create_sample_lineorder()?;
        self.register_batch("lineorder", lineorder)?;

        // Sample customer data
        let customer = create_sample_customer()?;
        self.register_batch("customer", customer)?;

        // Sample supplier data
        let supplier = create_sample_supplier()?;
        self.register_batch("supplier", supplier)?;

        // Sample part data
        let part = create_sample_part()?;
        self.register_batch("part", part)?;

        // Sample date data
        let date = create_sample_date()?;
        self.register_batch("date", date)?;

        Ok(())
    }

    /// Run a query using collect() - gets all results at once
    pub async fn run_query_collect(&self, sql: &str) -> Result<DfQueryResult, FusionLabError> {
        let start = Instant::now();

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok(DfQueryResult {
            row_count,
            duration_ms,
            batches,
        })
    }

    /// Run a query using execute_stream() - processes batches incrementally
    pub async fn run_query_stream(&self, sql: &str) -> Result<DfQueryResult, FusionLabError> {
        let start = Instant::now();

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

        let mut stream = df
            .execute_stream()
            .await
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| FusionLabError::DataFusion(e.to_string()))?;
            batches.push(batch);
        }

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok(DfQueryResult {
            row_count,
            duration_ms,
            batches,
        })
    }

    /// Get the logical plan for a query
    pub async fn explain(&self, sql: &str) -> Result<String, FusionLabError> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

        let plan = df.logical_plan();
        Ok(format!("{}", plan.display_indent()))
    }

    /// Get the physical plan for a query
    pub async fn explain_physical(&self, sql: &str) -> Result<String, FusionLabError> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

        Ok(format!("{}", datafusion::physical_plan::displayable(plan.as_ref()).indent(true)))
    }
}

impl Default for DataFusionRunner {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions to create sample SSB data

fn create_sample_lineorder() -> Result<RecordBatch, FusionLabError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lo_orderkey", DataType::Int64, false),
        Field::new("lo_linenumber", DataType::Int32, false),
        Field::new("lo_custkey", DataType::Int64, false),
        Field::new("lo_partkey", DataType::Int64, false),
        Field::new("lo_suppkey", DataType::Int64, false),
        Field::new("lo_orderdate", DataType::Int32, false),
        Field::new("lo_quantity", DataType::Int32, false),
        Field::new("lo_extendedprice", DataType::Float64, false),
        Field::new("lo_discount", DataType::Int32, false),
        Field::new("lo_revenue", DataType::Float64, false),
    ]));

    // Sample data (100 rows for testing)
    let orderkeys: Vec<i64> = (1..=100).collect();
    let linenumbers: Vec<i32> = (1..=100).map(|i| (i % 7) + 1).collect();
    let custkeys: Vec<i64> = (1..=100).map(|i| (i % 30) + 1).collect();
    let partkeys: Vec<i64> = (1..=100).map(|i| (i % 200) + 1).collect();
    let suppkeys: Vec<i64> = (1..=100).map(|i| (i % 20) + 1).collect();
    let orderdates: Vec<i32> = (1..=100).map(|i| 19920101 + (i % 365) * 100).collect();
    let quantities: Vec<i32> = (1..=100).map(|i| (i % 50) + 1).collect();
    let extendedprices: Vec<f64> = (1..=100).map(|i| (i as f64) * 100.0).collect();
    let discounts: Vec<i32> = (1..=100).map(|i| i % 11).collect();
    let revenues: Vec<f64> = (1..=100)
        .map(|i| (i as f64) * 100.0 * (1.0 - (i % 11) as f64 / 100.0))
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(orderkeys)) as ArrayRef,
            Arc::new(Int32Array::from(linenumbers)) as ArrayRef,
            Arc::new(Int64Array::from(custkeys)) as ArrayRef,
            Arc::new(Int64Array::from(partkeys)) as ArrayRef,
            Arc::new(Int64Array::from(suppkeys)) as ArrayRef,
            Arc::new(Int32Array::from(orderdates)) as ArrayRef,
            Arc::new(Int32Array::from(quantities)) as ArrayRef,
            Arc::new(Float64Array::from(extendedprices)) as ArrayRef,
            Arc::new(Int32Array::from(discounts)) as ArrayRef,
            Arc::new(Float64Array::from(revenues)) as ArrayRef,
        ],
    )
    .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

    Ok(batch)
}

fn create_sample_customer() -> Result<RecordBatch, FusionLabError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_city", DataType::Utf8, false),
        Field::new("c_nation", DataType::Utf8, false),
        Field::new("c_region", DataType::Utf8, false),
    ]));

    let custkeys: Vec<i64> = (1..=30).collect();
    let names: Vec<String> = (1..=30).map(|i| format!("Customer#{:06}", i)).collect();
    let cities: Vec<&str> = vec![
        "UNITED ST0", "UNITED ST1", "UNITED ST2", "CHINA    0", "CHINA    1",
        "BRAZIL   0", "BRAZIL   1", "INDIA    0", "INDIA    1", "JAPAN    0",
        "UNITED ST0", "UNITED ST1", "UNITED ST2", "CHINA    0", "CHINA    1",
        "BRAZIL   0", "BRAZIL   1", "INDIA    0", "INDIA    1", "JAPAN    0",
        "UNITED ST0", "UNITED ST1", "UNITED ST2", "CHINA    0", "CHINA    1",
        "BRAZIL   0", "BRAZIL   1", "INDIA    0", "INDIA    1", "JAPAN    0",
    ];
    let nations: Vec<&str> = vec![
        "UNITED STATES", "UNITED STATES", "UNITED STATES", "CHINA", "CHINA",
        "BRAZIL", "BRAZIL", "INDIA", "INDIA", "JAPAN",
        "UNITED STATES", "UNITED STATES", "UNITED STATES", "CHINA", "CHINA",
        "BRAZIL", "BRAZIL", "INDIA", "INDIA", "JAPAN",
        "UNITED STATES", "UNITED STATES", "UNITED STATES", "CHINA", "CHINA",
        "BRAZIL", "BRAZIL", "INDIA", "INDIA", "JAPAN",
    ];
    let regions: Vec<&str> = vec![
        "AMERICA", "AMERICA", "AMERICA", "ASIA", "ASIA",
        "AMERICA", "AMERICA", "ASIA", "ASIA", "ASIA",
        "AMERICA", "AMERICA", "AMERICA", "ASIA", "ASIA",
        "AMERICA", "AMERICA", "ASIA", "ASIA", "ASIA",
        "AMERICA", "AMERICA", "AMERICA", "ASIA", "ASIA",
        "AMERICA", "AMERICA", "ASIA", "ASIA", "ASIA",
    ];

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(custkeys)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(cities)) as ArrayRef,
            Arc::new(StringArray::from(nations)) as ArrayRef,
            Arc::new(StringArray::from(regions)) as ArrayRef,
        ],
    )
    .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

    Ok(batch)
}

fn create_sample_supplier() -> Result<RecordBatch, FusionLabError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_city", DataType::Utf8, false),
        Field::new("s_nation", DataType::Utf8, false),
        Field::new("s_region", DataType::Utf8, false),
    ]));

    let suppkeys: Vec<i64> = (1..=20).collect();
    let names: Vec<String> = (1..=20).map(|i| format!("Supplier#{:06}", i)).collect();
    let cities: Vec<&str> = vec![
        "UNITED ST0", "UNITED ST1", "CHINA    0", "CHINA    1", "BRAZIL   0",
        "INDIA    0", "JAPAN    0", "GERMANY  0", "FRANCE   0", "UNITED KI0",
        "UNITED ST2", "UNITED ST3", "CHINA    2", "CHINA    3", "BRAZIL   1",
        "INDIA    1", "JAPAN    1", "GERMANY  1", "FRANCE   1", "UNITED KI1",
    ];
    let nations: Vec<&str> = vec![
        "UNITED STATES", "UNITED STATES", "CHINA", "CHINA", "BRAZIL",
        "INDIA", "JAPAN", "GERMANY", "FRANCE", "UNITED KINGDOM",
        "UNITED STATES", "UNITED STATES", "CHINA", "CHINA", "BRAZIL",
        "INDIA", "JAPAN", "GERMANY", "FRANCE", "UNITED KINGDOM",
    ];
    let regions: Vec<&str> = vec![
        "AMERICA", "AMERICA", "ASIA", "ASIA", "AMERICA",
        "ASIA", "ASIA", "EUROPE", "EUROPE", "EUROPE",
        "AMERICA", "AMERICA", "ASIA", "ASIA", "AMERICA",
        "ASIA", "ASIA", "EUROPE", "EUROPE", "EUROPE",
    ];

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(suppkeys)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(cities)) as ArrayRef,
            Arc::new(StringArray::from(nations)) as ArrayRef,
            Arc::new(StringArray::from(regions)) as ArrayRef,
        ],
    )
    .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

    Ok(batch)
}

fn create_sample_part() -> Result<RecordBatch, FusionLabError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_category", DataType::Utf8, false),
        Field::new("p_brand1", DataType::Utf8, false),
    ]));

    let partkeys: Vec<i64> = (1..=200).collect();
    let names: Vec<String> = (1..=200).map(|i| format!("Part#{:06}", i)).collect();
    let mfgrs: Vec<String> = (1..=200).map(|i| format!("MFGR#{}", (i % 5) + 1)).collect();
    let categories: Vec<String> = (1..=200)
        .map(|i| format!("MFGR#{}{}",  (i % 5) + 1, (i % 5) + 1))
        .collect();
    let brands: Vec<String> = (1..=200)
        .map(|i| format!("MFGR#{}{}{}", (i % 5) + 1, (i % 5) + 1, (i % 40) + 1))
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(partkeys)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(mfgrs)) as ArrayRef,
            Arc::new(StringArray::from(categories)) as ArrayRef,
            Arc::new(StringArray::from(brands)) as ArrayRef,
        ],
    )
    .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

    Ok(batch)
}

fn create_sample_date() -> Result<RecordBatch, FusionLabError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d_datekey", DataType::Int32, false),
        Field::new("d_date", DataType::Utf8, false),
        Field::new("d_year", DataType::Int32, false),
        Field::new("d_yearmonth", DataType::Utf8, false),
        Field::new("d_yearmonthnum", DataType::Int32, false),
    ]));

    // Generate dates for 1992-1998
    let mut datekeys = Vec::new();
    let mut dates = Vec::new();
    let mut years = Vec::new();
    let mut yearmonths = Vec::new();
    let mut yearmonthnums = Vec::new();

    for year in 1992..=1998 {
        for month in 1..=12 {
            for day in 1..=28 {
                let datekey = year * 10000 + month * 100 + day;
                datekeys.push(datekey);
                dates.push(format!("{:04}-{:02}-{:02}", year, month, day));
                years.push(year);
                yearmonths.push(format!("{}:{}", year, month));
                yearmonthnums.push(year * 100 + month);
            }
        }
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(datekeys)) as ArrayRef,
            Arc::new(StringArray::from(dates)) as ArrayRef,
            Arc::new(Int32Array::from(years)) as ArrayRef,
            Arc::new(StringArray::from(yearmonths)) as ArrayRef,
            Arc::new(Int32Array::from(yearmonthnums)) as ArrayRef,
        ],
    )
    .map_err(|e| FusionLabError::DataFusion(e.to_string()))?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn ibd_available() -> bool {
        if let Ok(path) = std::env::var("IBD_READER_LIB_PATH") {
            let lib_path = Path::new(&path);
            let lib_found = lib_path.join("libibd_reader.so").exists()
                || lib_path.join("libibd_reader.dylib").exists()
                || lib_path.join("ibd_reader.dll").exists();
            if lib_found {
                return true;
            }
        }

        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let default_path = manifest_dir.join("../../..").join("percona-parser/build");
        let fallback_path = manifest_dir.join("../../percona-parser/build");
        let candidates = [default_path, fallback_path];
        candidates.into_iter().any(|path| {
            path.join("libibd_reader.so").exists()
                || path.join("libibd_reader.dylib").exists()
                || path.join("ibd_reader.dll").exists()
        })
    }

    #[tokio::test]
    async fn test_simple_query() {
        let runner = DataFusionRunner::new();
        runner.register_ssb_sample().unwrap();

        let result = runner
            .run_query_collect("SELECT COUNT(*) as cnt FROM lineorder")
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
        assert!(result.duration_ms > 0.0);
    }

    #[tokio::test]
    async fn test_group_by_query() {
        let runner = DataFusionRunner::new();
        runner.register_ssb_sample().unwrap();

        let result = runner
            .run_query_collect(
                "SELECT lo_custkey, SUM(lo_revenue) as total
                 FROM lineorder
                 GROUP BY lo_custkey
                 ORDER BY total DESC
                 LIMIT 5",
            )
            .await
            .unwrap();

        assert!(result.row_count <= 5);
        println!("{}", result.to_table());
    }

    #[tokio::test]
    async fn test_stream_mode() {
        let runner = DataFusionRunner::new();
        runner.register_ssb_sample().unwrap();

        let result = runner
            .run_query_stream("SELECT * FROM lineorder LIMIT 10")
            .await
            .unwrap();

        assert_eq!(result.row_count, 10);
    }

    #[tokio::test]
    async fn test_ibd_table_provider() {
        let runner = DataFusionRunner::new();

        let ibd_path = "/home/cslog/mysql/percona-parser/tests/types_test.ibd";
        let sdi_path = "/home/cslog/mysql/percona-parser/tests/types_test_sdi.json";

        if !ibd_available() || !Path::new(ibd_path).exists() || !Path::new(sdi_path).exists() {
            return;
        }

        // Register the IBD table (table name is 'types_fixture' in SDI)
        runner.register_ibd(None, ibd_path, sdi_path).unwrap();

        // Query the table using its actual name from the SDI
        let result = runner
            .run_query_collect("SELECT * FROM types_fixture LIMIT 5")
            .await
            .unwrap();

        println!("Rows: {}", result.row_count);
        println!("Duration: {:.2}ms", result.duration_ms);
        println!("{}", result.to_table());

        assert!(result.row_count > 0);
    }

    #[tokio::test]
    async fn test_ibd_multi_table_join() {
        let runner = DataFusionRunner::new();

        let base_dir = "/home/cslog/mysql/percona-parser/tests";
        let types_ibd = format!("{}/types_test.ibd", base_dir);
        let types_sdi = format!("{}/types_test_sdi.json", base_dir);
        let json_ibd = format!("{}/json_test.ibd", base_dir);
        let json_sdi = format!("{}/json_test_sdi.json", base_dir);

        if !ibd_available()
            || !Path::new(&types_ibd).exists()
            || !Path::new(&types_sdi).exists()
            || !Path::new(&json_ibd).exists()
            || !Path::new(&json_sdi).exists()
        {
            return;
        }

        runner.register_ibd(None, &types_ibd, &types_sdi).unwrap();
        runner.register_ibd(None, &json_ibd, &json_sdi).unwrap();

        let result = runner
            .run_query_collect(
                "SELECT t.id, j.id \
                 FROM types_fixture t \
                 CROSS JOIN json_fixture j \
                 LIMIT 1",
            )
            .await
            .unwrap();

        assert!(result.row_count > 0);
    }
}
