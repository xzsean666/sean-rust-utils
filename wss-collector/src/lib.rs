//! WebSocket collector library
//!
//! This library provides utilities for collecting data from WebSocket streams
//! with support for HTTP proxy connections.

pub mod wss_stream;
pub mod parquet_writer;
pub mod json_writer;
pub mod writer;
pub mod data_extract;

// Re-export public items for convenient access
pub use wss_stream::{connect_wss_stream, ProxyStream};
pub use parquet_writer::{ParquetWriter, ParquetWriterConfig, ColumnType, FilterCondition, FilterOperator};
pub use json_writer::{JsonWriter, JsonWriterConfig};
pub use writer::{Writer, WriterType, DataRow};
pub use data_extract::{extract_data_array, convert_to_rows};
