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
use std::os::raw::c_char;
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
    #[error("File write error: {0}")]
    FileWrite(String),
    #[error("Invalid file format: {0}")]
    InvalidFormat(String),
    #[error("Compression error")]
    Compression,
    #[error("Decompression error")]
    Decompression,
    #[error("Encryption error")]
    Encryption,
    #[error("Decryption error")]
    Decryption,
    #[error("Memory allocation error")]
    Memory,
    #[error("Not implemented")]
    NotImplemented,
    #[error("Keyring error")]
    Keyring,
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
            _ => Err(ibd_error_from_result(result, None)),
        }
    }
}

fn ibd_error_from_result(result: IbdResult, message: Option<String>) -> IbdError {
    let msg = message.unwrap_or_else(|| "Unknown error".to_string());
    match result {
        IbdResult::Success => IbdError::Library("Unexpected success".to_string()),
        IbdResult::ErrorInvalidParam => IbdError::InvalidParam,
        IbdResult::ErrorFileNotFound => IbdError::FileNotFound(msg),
        IbdResult::ErrorFileRead => IbdError::FileRead(msg),
        IbdResult::ErrorFileWrite => IbdError::FileWrite(msg),
        IbdResult::ErrorInvalidFormat => IbdError::InvalidFormat(msg),
        IbdResult::ErrorCompression => IbdError::Compression,
        IbdResult::ErrorDecompression => IbdError::Decompression,
        IbdResult::ErrorEncryption => IbdError::Encryption,
        IbdResult::ErrorDecryption => IbdError::Decryption,
        IbdResult::ErrorMemory => IbdError::Memory,
        IbdResult::ErrorNotImplemented => IbdError::NotImplemented,
        IbdResult::ErrorKeyring => IbdError::Keyring,
        IbdResult::ErrorUnknown => IbdError::Library(msg),
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
            let ibd_result = IbdResult::from(result);
            if ibd_result != IbdResult::Success {
                return Err(ibd_error_from_result(
                    ibd_result,
                    Some("Failed to get column value".to_string()),
                ));
            }

            if value.is_null != 0 {
                return Ok(ColumnValue::Null);
            }

            let col_type = IbdColumnType::from(value.col_type);

            // Use formatted string for most types as it's pre-formatted correctly
            let formatted = formatted_to_string(&value.formatted);

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

            let ibd_result = IbdResult::from(result);
            if ibd_result != IbdResult::Success {
                if ibd_result == IbdResult::ErrorFileRead {
                    return Ok(None);
                }
                return Err(ibd_error_from_result(
                    ibd_result,
                    Some("Failed to read row".to_string()),
                ));
            }

            if row_handle.is_null() {
                return Err(IbdError::Library(
                    "Reader returned success with null row handle".to_string(),
                ));
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

            let ibd_result = IbdResult::from(result);
            if ibd_result != IbdResult::Success {
                let err = self.last_error().unwrap_or_else(|| "Unknown error".to_string());
                return Err(ibd_error_from_result(ibd_result, Some(err)));
            }

            if table_handle.is_null() {
                return Err(IbdError::Memory);
            }

            // Get table info
            let mut name_buf = vec![0u8; 256];
            let mut column_count: u32 = 0;

            let table_info_result = ffi::ibd_get_table_info(
                table_handle,
                name_buf.as_mut_ptr() as *mut i8,
                name_buf.len(),
                &mut column_count,
            );
            let ibd_table_info = IbdResult::from(table_info_result);
            if ibd_table_info != IbdResult::Success {
                ffi::ibd_close_table(table_handle);
                return Err(ibd_error_from_result(
                    ibd_table_info,
                    Some("Failed to read table info".to_string()),
                ));
            }

            let table_name = CStr::from_ptr(name_buf.as_ptr() as *const i8)
                .to_string_lossy()
                .to_string();

            // Get column info
            let mut columns = Vec::with_capacity(column_count as usize);
            for i in 0..column_count {
                let mut col_name_buf = vec![0u8; 128];
                let mut col_type: i32 = 0;

                let col_result = ffi::ibd_get_column_info(
                    table_handle,
                    i,
                    col_name_buf.as_mut_ptr() as *mut i8,
                    col_name_buf.len(),
                    &mut col_type,
                );
                let ibd_col_result = IbdResult::from(col_result);
                if ibd_col_result != IbdResult::Success {
                    ffi::ibd_close_table(table_handle);
                    return Err(ibd_error_from_result(
                        ibd_col_result,
                        Some(format!("Failed to read column info for index {}", i)),
                    ));
                }

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

fn formatted_to_string(formatted: &[c_char]) -> String {
    let len = formatted
        .iter()
        .position(|c| *c == 0)
        .unwrap_or(formatted.len());
    let bytes: Vec<u8> = formatted[..len].iter().map(|c| *c as u8).collect();
    String::from_utf8_lossy(&bytes).to_string()
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
    use std::path::Path;

    fn ibd_lib_available() -> bool {
        let mut candidates = Vec::new();

        if let Ok(path) = std::env::var("IBD_READER_LIB_PATH") {
            candidates.push(Path::new(&path).to_path_buf());
        } else {
            let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
            candidates.push(manifest_dir.join("../../percona-parser/build"));
        }

        candidates.into_iter().any(|path| {
            path.join("libibd_reader.so").exists()
                || path.join("libibd_reader.dylib").exists()
                || path.join("ibd_reader.dll").exists()
        })
    }

    #[test]
    fn test_version() {
        if !ibd_lib_available() {
            return;
        }
        let v = version();
        assert!(!v.is_empty());
    }

    #[test]
    fn test_create_reader() {
        if !ibd_lib_available() {
            return;
        }
        let reader = IbdReader::new();
        assert!(reader.is_ok());
    }
}
