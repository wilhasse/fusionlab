//! InnoDB .ibd file TableProvider for DataFusion
//!
//! Allows reading MySQL InnoDB data files directly as DataFusion tables.

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream;
use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fusionlab_ibd::{ColumnType, IbdReader};

/// Configuration for an InnoDB table
#[derive(Debug, Clone)]
pub struct IbdTableConfig {
    pub ibd_path: PathBuf,
    pub sdi_path: PathBuf,
    pub table_name: String,
}

/// TableProvider for InnoDB .ibd files
pub struct IbdTableProvider {
    config: IbdTableConfig,
    schema: SchemaRef,
    column_mapping: Vec<(String, ColumnType, usize)>, // (name, type, ibd_index)
}

impl Debug for IbdTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IbdTableProvider")
            .field("table_name", &self.config.table_name)
            .field("schema", &self.schema)
            .finish()
    }
}

impl IbdTableProvider {
    /// Create a new IbdTableProvider
    pub fn try_new<P: AsRef<Path>, Q: AsRef<Path>>(
        ibd_path: P,
        sdi_path: Q,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let reader = IbdReader::new()?;
        let table = reader.open_table(ibd_path.as_ref(), sdi_path.as_ref())?;

        let table_name = table.name().to_string();
        let columns = table.columns();

        // Build Arrow schema from IBD column info
        // Note: The C API skips internal columns (DB_TRX_ID, DB_ROLL_PTR) in row data,
        // so we track the sequential row index, not the SDI column index.
        let mut fields = Vec::new();
        let mut column_mapping = Vec::new();
        let mut row_idx: usize = 0;

        for col in columns {
            // Skip internal columns (DB_TRX_ID, DB_ROLL_PTR)
            if col.col_type == ColumnType::Internal {
                continue;
            }

            let arrow_type = ibd_to_arrow_type(col.col_type);
            let nullable = true; // Conservative - assume all columns can be NULL

            fields.push(Field::new(&col.name, arrow_type, nullable));
            column_mapping.push((col.name.clone(), col.col_type, row_idx));
            row_idx += 1;
        }

        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            config: IbdTableConfig {
                ibd_path: ibd_path.as_ref().to_path_buf(),
                sdi_path: sdi_path.as_ref().to_path_buf(),
                table_name,
            },
            schema,
            column_mapping,
        })
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        &self.config.table_name
    }
}

fn ibd_to_arrow_type(ibd_type: ColumnType) -> DataType {
    match ibd_type {
        ColumnType::Int => DataType::Int64,
        ColumnType::UInt => DataType::Int64, // Arrow doesn't have unsigned, use Int64
        ColumnType::Float | ColumnType::Double => DataType::Float64,
        // All other types stored as formatted strings for simplicity
        // TODO: Parse temporal types to native Arrow Date32/Timestamp for better performance
        ColumnType::String
        | ColumnType::Binary
        | ColumnType::DateTime
        | ColumnType::Timestamp
        | ColumnType::Date
        | ColumnType::Time
        | ColumnType::Decimal
        | ColumnType::Null
        | ColumnType::Internal => DataType::Utf8,
    }
}

#[async_trait]
impl TableProvider for IbdTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // No filter pushdown support yet
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IbdExec::new(
            self.config.clone(),
            self.schema.clone(),
            self.column_mapping.clone(),
            projection.cloned(),
        )))
    }
}

/// Physical execution plan for InnoDB table scan
#[derive(Debug)]
struct IbdExec {
    config: IbdTableConfig,
    column_mapping: Vec<(String, ColumnType, usize)>,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    properties: PlanProperties,
}

impl IbdExec {
    fn new(
        config: IbdTableConfig,
        schema: SchemaRef,
        column_mapping: Vec<(String, ColumnType, usize)>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projected_schema = match &projection {
            Some(indices) => Arc::new(schema.project(indices).unwrap()),
            None => schema,
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            config,
            column_mapping,
            projection,
            projected_schema,
            properties,
        }
    }
}

impl DisplayAs for IbdExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IbdExec: table={}, projection={:?}",
            self.config.table_name, self.projection
        )
    }
}

impl ExecutionPlan for IbdExec {
    fn name(&self) -> &str {
        "IbdExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let config = self.config.clone();
        let column_mapping = self.column_mapping.clone();
        let projection = self.projection.clone();
        let schema = self.projected_schema.clone();

        // Read all rows and convert to RecordBatch
        let batches = read_ibd_to_batches(&config, &column_mapping, projection.as_ref(), &schema)
            .map_err(|e| datafusion::error::DataFusionError::External(e))?;

        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn read_ibd_to_batches(
    config: &IbdTableConfig,
    column_mapping: &[(String, ColumnType, usize)],
    projection: Option<&Vec<usize>>,
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
    let reader = IbdReader::new()?;
    let mut table = reader.open_table(&config.ibd_path, &config.sdi_path)?;

    // Collect all rows
    let mut all_rows: Vec<Vec<fusionlab_ibd::ColumnValue>> = Vec::new();

    while let Some(row) = table.next_row()? {
        let mut row_values = Vec::with_capacity(column_mapping.len());
        for (_, _, ibd_idx) in column_mapping {
            row_values.push(row.get(*ibd_idx as u32)?);
        }
        all_rows.push(row_values);
    }

    if all_rows.is_empty() {
        return Ok(vec![]);
    }

    // Build Arrow arrays based on projection
    let indices: Vec<usize> = match projection {
        Some(proj) => proj.clone(),
        None => (0..column_mapping.len()).collect(),
    };

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(indices.len());

    for &col_idx in &indices {
        let (_, col_type, _) = &column_mapping[col_idx];
        let arrow_type = schema.field(arrays.len()).data_type();

        let array = build_array_from_values(&all_rows, col_idx, *col_type, arrow_type);
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(vec![batch])
}

fn build_array_from_values(
    rows: &[Vec<fusionlab_ibd::ColumnValue>],
    col_idx: usize,
    col_type: ColumnType,
    _arrow_type: &DataType,
) -> ArrayRef {
    use fusionlab_ibd::ColumnValue;

    match col_type {
        ColumnType::Int | ColumnType::UInt => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| match &row[col_idx] {
                    ColumnValue::Null => None,
                    ColumnValue::Int(v) => Some(*v),
                    ColumnValue::UInt(v) => Some(*v as i64),
                    ColumnValue::Formatted(s) => s.parse().ok(),
                    _ => None,
                })
                .collect();
            Arc::new(Int64Array::from(values))
        }
        ColumnType::Float | ColumnType::Double => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| match &row[col_idx] {
                    ColumnValue::Null => None,
                    ColumnValue::Float(v) => Some(*v),
                    ColumnValue::Formatted(s) => s.parse().ok(),
                    _ => None,
                })
                .collect();
            Arc::new(Float64Array::from(values))
        }
        _ => {
            // String-based types (including temporal, decimal, etc.)
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| match &row[col_idx] {
                    ColumnValue::Null => None,
                    v => Some(v.as_string()),
                })
                .collect();
            Arc::new(StringArray::from(values))
        }
    }
}
