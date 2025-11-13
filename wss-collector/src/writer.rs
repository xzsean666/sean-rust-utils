//! Writer trait and types
//!
//! This module defines the common interface for different writer implementations

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use serde_json::Value;

/// Data row type - a map of column name to JSON value
pub type DataRow = HashMap<String, Value>;

/// Writer type selection
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum WriterType {
    /// Parquet format - batched writes
    Parquet,
    /// JSON format - line-by-line append
    Json,
}

impl Default for WriterType {
    fn default() -> Self {
        WriterType::Parquet
    }
}

/// Common interface for data writers
#[async_trait]
pub trait Writer: Send {
    /// Write data rows (pre-parsed from WebSocket messages)
    async fn write_rows(&mut self, rows: Vec<DataRow>) -> Result<()>;
    
    /// Flush any buffered data to disk
    async fn flush_buffer(&mut self) -> Result<()>;
}

