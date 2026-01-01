//! Safe Rust wrapper for reading InnoDB .ibd files via percona-parser
//!
//! # Example
//!
//! ```ignore
//! use fusionlab_ibd::{IbdReader, IbdTable};
//!
//! let reader = IbdReader::new().unwrap();
//! let mut table = reader.open_table("/path/to/table.ibd", "/path/to/sdi.json").unwrap();
//!
//! while let Some(row) = table.next_row().unwrap() {
//!     println!("{}", row.to_string());
//! }
//! ```

pub mod ffi;

use ffi::{IbdColumnType, IbdResult};
use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;
use std::sync::Once;
use thiserror::Error;

static INIT: Once = Once::new();
static mut INIT_RESULT: i32 = 0;

/// Errors from IBD reading operations
#[derive(Error, Debug)]
pub enum IbdError {
    #[error("Invalid parameter")]
    InvalidParam,
    #[error("File not found: {0}")]
    FileNotFound(String),
    #[error("File read error: {0}")]
    FileRead(String),
    #[error("Invalid file format: {0}")]
    InvalidFormat(String),
    #[error("Decompression error")]
    Decompression,
    #[error("Decryption error")]
    Decryption,
    #[error("Memory allocation error")]
    Memory,
    #[error("Not implemented")]
    NotImplemented,
    #[error("Library error: {0}")]
    Library(String),
    #[error("No more rows")]
    NoMoreRows,
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

impl From<IbdResult> for Result<(), IbdError> {
    fn from(result: IbdResult) -> Self {
        match result {
            IbdResult::Success => Ok(()),
            IbdResult::ErrorInvalidParam => Err(IbdError::InvalidParam),
            IbdResult::ErrorFileNotFound => Err(IbdError::FileNotFound(String::new())),
            IbdResult::ErrorFileRead => Err(IbdError::FileRead(String::new())),
            IbdResult::ErrorInvalidFormat => Err(IbdError::InvalidFormat(String::new())),
            IbdResult::ErrorDecompression => Err(IbdError::Decompression),
            IbdResult::ErrorDecryption => Err(IbdError::Decryption),
            IbdResult::ErrorMemory => Err(IbdError::Memory),
            IbdResult::ErrorNotImplemented => Err(IbdError::NotImplemented),
            _ => Err(IbdError::Library("Unknown error".to_string())),
        }
    }
}

/// Initialize the library (called automatically)
fn ensure_init() -> Result<(), IbdError> {
    INIT.call_once(|| {
        unsafe {
            INIT_RESULT = ffi::ibd_init();
        }
    });

    unsafe {
        if INIT_RESULT == 0 {
            Ok(())
        } else {
            Err(IbdError::Library("Failed to initialize library".to_string()))
        }
    }
}

/// Column schema information
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub col_type: ColumnType,
    pub index: u32,
}

/// Column type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Null,
    Int,
    UInt,
    Float,
    Double,
    String,
    Binary,
    DateTime,
    Date,
    Time,
    Timestamp,
    Decimal,
    Internal,
}

impl From<IbdColumnType> for ColumnType {
    fn from(t: IbdColumnType) -> Self {
        match t {
            IbdColumnType::Null => ColumnType::Null,
            IbdColumnType::Int => ColumnType::Int,
            IbdColumnType::UInt => ColumnType::UInt,
            IbdColumnType::Float => ColumnType::Float,
            IbdColumnType::Double => ColumnType::Double,
            IbdColumnType::String => ColumnType::String,
            IbdColumnType::Binary => ColumnType::Binary,
            IbdColumnType::DateTime => ColumnType::DateTime,
            IbdColumnType::Date => ColumnType::Date,
            IbdColumnType::Time => ColumnType::Time,
            IbdColumnType::Timestamp => ColumnType::Timestamp,
            IbdColumnType::Decimal => ColumnType::Decimal,
            IbdColumnType::Internal => ColumnType::Internal,
        }
    }
}

/// Column value from a row
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Null,
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    /// Formatted string for temporal/decimal types
    Formatted(String),
}

impl ColumnValue {
    /// Get as string representation
    pub fn as_string(&self) -> String {
        match self {
            ColumnValue::Null => "NULL".to_string(),
            ColumnValue::Int(v) => v.to_string(),
            ColumnValue::UInt(v) => v.to_string(),
            ColumnValue::Float(v) => v.to_string(),
            ColumnValue::String(s) => s.clone(),
            ColumnValue::Binary(b) => format!("0x{}", hex::encode(b)),
            ColumnValue::Formatted(s) => s.clone(),
        }
    }

    /// Check if NULL
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }
}

/// A row from an InnoDB table
pub struct IbdRow {
    handle: ffi::IbdRowHandle,
    column_count: u32,
}

impl IbdRow {
    /// Get number of columns
    pub fn column_count(&self) -> u32 {
        self.column_count
    }

    /// Get column value by index
    pub fn get(&self, index: u32) -> Result<ColumnValue, IbdError> {
        if index >= self.column_count {
            return Err(IbdError::InvalidParam);
        }

        unsafe {
            let mut value: ffi::IbdColumnValue = std::mem::zeroed();
            let result = ffi::ibd_row_get_column(self.handle, index, &mut value);

            if result != 0 {
                return Err(IbdError::Library("Failed to get column value".to_string()));
            }

            if value.is_null != 0 {
                return Ok(ColumnValue::Null);
            }

            let col_type = IbdColumnType::from(value.col_type);

            // Use formatted string for most types as it's pre-formatted correctly
            let formatted = CStr::from_ptr(value.formatted.as_ptr())
                .to_string_lossy()
                .to_string();

            match col_type {
                IbdColumnType::Int => Ok(ColumnValue::Int(value.value.int_val)),
                IbdColumnType::UInt => Ok(ColumnValue::UInt(value.value.uint_val)),
                IbdColumnType::Float | IbdColumnType::Double => {
                    Ok(ColumnValue::Float(value.value.float_val))
                }
                IbdColumnType::String => Ok(ColumnValue::String(formatted)),
                IbdColumnType::Binary => {
                    // For binary, use the raw data from the union
                    let str_val = value.value.str_val;
                    if !str_val.data.is_null() && str_val.length > 0 {
                        let slice =
                            std::slice::from_raw_parts(str_val.data as *const u8, str_val.length);
                        Ok(ColumnValue::Binary(slice.to_vec()))
                    } else {
                        Ok(ColumnValue::Binary(Vec::new()))
                    }
                }
                _ => {
                    // DateTime, Date, Time, Timestamp, Decimal - use formatted
                    Ok(ColumnValue::Formatted(formatted))
                }
            }
        }
    }

    /// Get all values as tab-separated string
    pub fn to_string(&self) -> String {
        unsafe {
            let mut buffer = vec![0u8; 4096];
            let len =
                ffi::ibd_row_to_string(self.handle, buffer.as_mut_ptr() as *mut i8, buffer.len());

            if len > 0 && len < buffer.len() {
                String::from_utf8_lossy(&buffer[..len]).to_string()
            } else {
                String::new()
            }
        }
    }
}

impl Drop for IbdRow {
    fn drop(&mut self) {
        unsafe {
            ffi::ibd_free_row(self.handle);
        }
    }
}

/// IBD table iterator for reading rows
pub struct IbdTable {
    handle: ffi::IbdTableHandle,
    table_name: String,
    columns: Vec<ColumnInfo>,
}

impl IbdTable {
    /// Get table name
    pub fn name(&self) -> &str {
        &self.table_name
    }

    /// Get column schema
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    /// Get column count (excluding internal columns)
    pub fn column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.col_type != ColumnType::Internal)
            .count()
    }

    /// Read next row
    pub fn next_row(&mut self) -> Result<Option<IbdRow>, IbdError> {
        unsafe {
            let mut row_handle: ffi::IbdRowHandle = ptr::null_mut();
            let result = ffi::ibd_read_row(self.handle, &mut row_handle);

            if result != 0 {
                // No more rows
                return Ok(None);
            }

            if row_handle.is_null() {
                return Ok(None);
            }

            let column_count = ffi::ibd_row_column_count(row_handle);

            Ok(Some(IbdRow {
                handle: row_handle,
                column_count,
            }))
        }
    }

    /// Get total rows read so far
    pub fn row_count(&self) -> u64 {
        unsafe { ffi::ibd_get_row_count(self.handle) }
    }
}

impl Drop for IbdTable {
    fn drop(&mut self) {
        unsafe {
            ffi::ibd_close_table(self.handle);
        }
    }
}

/// IBD reader for opening and reading tables
pub struct IbdReader {
    handle: ffi::IbdReaderHandle,
}

impl IbdReader {
    /// Create a new reader
    pub fn new() -> Result<Self, IbdError> {
        ensure_init()?;

        unsafe {
            let handle = ffi::ibd_reader_create();
            if handle.is_null() {
                return Err(IbdError::Memory);
            }
            Ok(IbdReader { handle })
        }
    }

    /// Enable debug output
    pub fn set_debug(&mut self, enable: bool) {
        unsafe {
            ffi::ibd_reader_set_debug(self.handle, if enable { 1 } else { 0 });
        }
    }

    /// Get last error message
    pub fn last_error(&self) -> Option<String> {
        unsafe {
            let err = ffi::ibd_reader_get_error(self.handle);
            if err.is_null() {
                None
            } else {
                Some(CStr::from_ptr(err).to_string_lossy().to_string())
            }
        }
    }

    /// Open a table for reading
    pub fn open_table<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        ibd_path: P,
        sdi_path: Q,
    ) -> Result<IbdTable, IbdError> {
        let ibd_cstr = path_to_cstring(ibd_path.as_ref())?;
        let sdi_cstr = path_to_cstring(sdi_path.as_ref())?;

        unsafe {
            let mut table_handle: ffi::IbdTableHandle = ptr::null_mut();
            let result = ffi::ibd_open_table(
                self.handle,
                ibd_cstr.as_ptr(),
                sdi_cstr.as_ptr(),
                &mut table_handle,
            );

            if result != 0 {
                let err = self.last_error().unwrap_or_else(|| "Unknown error".to_string());
                return Err(IbdError::FileRead(err));
            }

            if table_handle.is_null() {
                return Err(IbdError::Memory);
            }

            // Get table info
            let mut name_buf = vec![0u8; 256];
            let mut column_count: u32 = 0;

            ffi::ibd_get_table_info(
                table_handle,
                name_buf.as_mut_ptr() as *mut i8,
                name_buf.len(),
                &mut column_count,
            );

            let table_name = CStr::from_ptr(name_buf.as_ptr() as *const i8)
                .to_string_lossy()
                .to_string();

            // Get column info
            let mut columns = Vec::with_capacity(column_count as usize);
            for i in 0..column_count {
                let mut col_name_buf = vec![0u8; 128];
                let mut col_type: i32 = 0;

                ffi::ibd_get_column_info(
                    table_handle,
                    i,
                    col_name_buf.as_mut_ptr() as *mut i8,
                    col_name_buf.len(),
                    &mut col_type,
                );

                let col_name = CStr::from_ptr(col_name_buf.as_ptr() as *const i8)
                    .to_string_lossy()
                    .to_string();

                columns.push(ColumnInfo {
                    name: col_name,
                    col_type: ColumnType::from(IbdColumnType::from(col_type)),
                    index: i,
                });
            }

            Ok(IbdTable {
                handle: table_handle,
                table_name,
                columns,
            })
        }
    }
}

impl Drop for IbdReader {
    fn drop(&mut self) {
        unsafe {
            ffi::ibd_reader_destroy(self.handle);
        }
    }
}

/// Get library version
pub fn version() -> String {
    unsafe {
        let v = ffi::ibd_get_version();
        if v.is_null() {
            "unknown".to_string()
        } else {
            CStr::from_ptr(v).to_string_lossy().to_string()
        }
    }
}

fn path_to_cstring(path: &Path) -> Result<CString, IbdError> {
    let path_str = path.to_str().ok_or_else(|| {
        IbdError::InvalidPath(format!("Path contains invalid UTF-8: {:?}", path))
    })?;

    CString::new(path_str)
        .map_err(|_| IbdError::InvalidPath(format!("Path contains null bytes: {:?}", path)))
}

// Needed for Binary type formatting
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let v = version();
        assert!(!v.is_empty());
    }

    #[test]
    fn test_create_reader() {
        let reader = IbdReader::new();
        assert!(reader.is_ok());
    }
}
