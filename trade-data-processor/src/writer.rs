//! Writer trait definition
//!
//! This module defines the Writer trait for writing data rows to various backends.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// Type alias for a data row (map of field names to JSON values)
pub type DataRow = HashMap<String, Value>;

/// Trait for writing data rows to a storage backend
#[async_trait]
pub trait Writer: Send {
    /// Write a batch of data rows
    async fn write_rows(&mut self, rows: Vec<DataRow>) -> Result<()>;
    
    /// Flush any buffered data to storage
    async fn flush_buffer(&mut self) -> Result<()>;
}

