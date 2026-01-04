//! Raw FFI bindings to libibd_reader.so
//!
//! These are unsafe C bindings - use the safe wrappers in lib.rs instead.

use libc::{c_char, c_int, size_t};
use std::os::raw::c_void;

/// Result codes from the C library
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IbdResult {
    Success = 0,
    EndOfStream = 1,
    ErrorInvalidParam = -1,
    ErrorFileNotFound = -2,
    ErrorFileRead = -3,
    ErrorFileWrite = -4,
    ErrorInvalidFormat = -5,
    ErrorCompression = -6,
    ErrorDecompression = -7,
    ErrorEncryption = -8,
    ErrorDecryption = -9,
    ErrorMemory = -10,
    ErrorNotImplemented = -11,
    ErrorKeyring = -12,
    ErrorUnknown = -99,
}

impl From<i32> for IbdResult {
    fn from(code: i32) -> Self {
        match code {
            0 => IbdResult::Success,
            1 => IbdResult::EndOfStream,
            -1 => IbdResult::ErrorInvalidParam,
            -2 => IbdResult::ErrorFileNotFound,
            -3 => IbdResult::ErrorFileRead,
            -4 => IbdResult::ErrorFileWrite,
            -5 => IbdResult::ErrorInvalidFormat,
            -6 => IbdResult::ErrorCompression,
            -7 => IbdResult::ErrorDecompression,
            -8 => IbdResult::ErrorEncryption,
            -9 => IbdResult::ErrorDecryption,
            -10 => IbdResult::ErrorMemory,
            -11 => IbdResult::ErrorNotImplemented,
            -12 => IbdResult::ErrorKeyring,
            _ => IbdResult::ErrorUnknown,
        }
    }
}

/// Column value types
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IbdColumnType {
    Null = 0,
    Int = 1,
    UInt = 2,
    Float = 3,
    Double = 4,
    String = 5,
    Binary = 6,
    DateTime = 7,
    Date = 8,
    Time = 9,
    Timestamp = 10,
    Decimal = 11,
    Internal = 99,
}

impl From<i32> for IbdColumnType {
    fn from(code: i32) -> Self {
        match code {
            0 => IbdColumnType::Null,
            1 => IbdColumnType::Int,
            2 => IbdColumnType::UInt,
            3 => IbdColumnType::Float,
            4 => IbdColumnType::Double,
            5 => IbdColumnType::String,
            6 => IbdColumnType::Binary,
            7 => IbdColumnType::DateTime,
            8 => IbdColumnType::Date,
            9 => IbdColumnType::Time,
            10 => IbdColumnType::Timestamp,
            11 => IbdColumnType::Decimal,
            99 => IbdColumnType::Internal,
            _ => IbdColumnType::Null,
        }
    }
}

/// Opaque reader handle
pub type IbdReaderHandle = *mut c_void;

/// Opaque table iterator handle
pub type IbdTableHandle = *mut c_void;

/// Opaque row handle
pub type IbdRowHandle = *mut c_void;

/// String value structure (for str_val in union)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct IbdStrVal {
    pub data: *const c_char,
    pub length: size_t,
}

/// Union for column value (matches C layout exactly)
/// The largest member is 16 bytes (str_val or 2x pointer)
#[repr(C)]
#[derive(Copy, Clone)]
pub union IbdValueUnion {
    pub int_val: i64,
    pub uint_val: u64,
    pub float_val: f64,
    pub str_val: IbdStrVal,
}

/// Column value structure (matches C layout)
#[repr(C)]
pub struct IbdColumnValue {
    pub name: *const c_char,
    pub col_type: c_int,
    pub is_null: c_int,
    pub value: IbdValueUnion,
    pub formatted: [c_char; 256],
}

#[cfg(ibd_reader_available)]
#[link(name = "ibd_reader")]
extern "C" {
    // Library initialization
    pub fn ibd_init() -> c_int;
    pub fn ibd_cleanup();
    pub fn ibd_get_version() -> *const c_char;

    // Reader context
    pub fn ibd_reader_create() -> IbdReaderHandle;
    pub fn ibd_reader_destroy(reader: IbdReaderHandle);
    pub fn ibd_reader_get_error(reader: IbdReaderHandle) -> *const c_char;
    pub fn ibd_reader_set_debug(reader: IbdReaderHandle, enable: c_int);

    // Table operations
    pub fn ibd_open_table(
        reader: IbdReaderHandle,
        ibd_path: *const c_char,
        sdi_json_path: *const c_char,
        table_out: *mut IbdTableHandle,
    ) -> c_int;

    pub fn ibd_get_table_info(
        table: IbdTableHandle,
        table_name: *mut c_char,
        table_name_size: size_t,
        column_count: *mut u32,
    ) -> c_int;

    pub fn ibd_get_column_info(
        table: IbdTableHandle,
        column_index: u32,
        name: *mut c_char,
        name_size: size_t,
        col_type: *mut c_int,
    ) -> c_int;

    pub fn ibd_read_row(table: IbdTableHandle, row_out: *mut IbdRowHandle) -> c_int;

    pub fn ibd_row_column_count(row: IbdRowHandle) -> u32;

    pub fn ibd_row_get_column(
        row: IbdRowHandle,
        column_index: u32,
        value: *mut IbdColumnValue,
    ) -> c_int;

    pub fn ibd_row_to_string(row: IbdRowHandle, buffer: *mut c_char, buffer_size: size_t)
        -> size_t;

    pub fn ibd_free_row(row: IbdRowHandle);

    pub fn ibd_close_table(table: IbdTableHandle);

    pub fn ibd_get_row_count(table: IbdTableHandle) -> u64;
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_init() -> c_int {
    IbdResult::ErrorNotImplemented as c_int
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_cleanup() {}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_get_version() -> *const c_char {
    std::ptr::null()
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_reader_create() -> IbdReaderHandle {
    std::ptr::null_mut()
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_reader_destroy(_reader: IbdReaderHandle) {}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_reader_get_error(_reader: IbdReaderHandle) -> *const c_char {
    std::ptr::null()
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_reader_set_debug(_reader: IbdReaderHandle, _enable: c_int) {}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_open_table(
    _reader: IbdReaderHandle,
    _ibd_path: *const c_char,
    _sdi_json_path: *const c_char,
    _table_out: *mut IbdTableHandle,
) -> c_int {
    IbdResult::ErrorNotImplemented as c_int
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_get_table_info(
    _table: IbdTableHandle,
    _table_name: *mut c_char,
    _table_name_size: size_t,
    _column_count: *mut u32,
) -> c_int {
    IbdResult::ErrorNotImplemented as c_int
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_get_column_info(
    _table: IbdTableHandle,
    _column_index: u32,
    _name: *mut c_char,
    _name_size: size_t,
    _col_type: *mut c_int,
) -> c_int {
    IbdResult::ErrorNotImplemented as c_int
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_read_row(_table: IbdTableHandle, _row_out: *mut IbdRowHandle) -> c_int {
    IbdResult::ErrorNotImplemented as c_int
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_row_column_count(_row: IbdRowHandle) -> u32 {
    0
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_row_get_column(
    _row: IbdRowHandle,
    _column_index: u32,
    _value: *mut IbdColumnValue,
) -> c_int {
    IbdResult::ErrorNotImplemented as c_int
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_row_to_string(
    _row: IbdRowHandle,
    _buffer: *mut c_char,
    _buffer_size: size_t,
) -> size_t {
    0
}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_free_row(_row: IbdRowHandle) {}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_close_table(_table: IbdTableHandle) {}

#[cfg(not(ibd_reader_available))]
pub unsafe fn ibd_get_row_count(_table: IbdTableHandle) -> u64 {
    0
}
