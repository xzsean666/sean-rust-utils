//! Parquet file writing module
//!
//! This module handles all aspects of writing WebSocket data to Parquet files,
//! including schema inference, data conversion, and batch writing.

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use chrono::NaiveDate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, DirBuilderExt};

use crate::writer::{Writer, DataRow};

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    String,
    Int64,
    UInt64,
    Float64,
    Boolean,
}

/// Filter operator for comparing field values
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FilterOperator {
    Eq,   // Equal
    Ne,   // Not equal
    Gt,   // Greater than
    Lt,   // Less than
    Gte,  // Greater than or equal
    Lte,  // Less than or equal
    Contains, // String contains
}

/// Filter condition for data rows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterCondition {
    pub field: String,
    pub operator: FilterOperator,
    pub value: Value,
}

/// Configuration for ParquetWriter
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    /// Base path for storing parquet files
    pub path: String,
    /// Name prefix for parquet files
    pub name: String,
    /// Batch size - number of records to buffer before writing to file
    pub batch_size: usize,
    /// Whether batch_size was explicitly configured (Some) or auto-set (None)
    /// Used to determine filename format: no sequence for single file, sequence for multiple files
    pub has_batch_config: bool,
    /// Optional filter conditions - if empty, all data is written
    pub filter: Vec<FilterCondition>,
    /// Optional date to write data to - if None, uses current date
    pub date: Option<NaiveDate>,
    /// Whether to write to /tmp first and then copy to output directory
    /// This can improve performance by writing to faster storage first
    pub use_temp_dir: bool,
}

/// Main struct for writing data to Parquet files
pub struct ParquetWriter {
    config: ParquetWriterConfig,
    current_date: Option<NaiveDate>,
    schema: Option<Arc<Schema>>,
    column_types: HashMap<String, ColumnType>,
    buffer: Vec<DataRow>,
    file_sequence: u64, // Sequence number for unique file naming
}

impl ParquetWriter {
    pub fn new(config: ParquetWriterConfig) -> Self {
        Self {
            config,
            current_date: None,
            schema: None,
            column_types: HashMap::new(),
            buffer: Vec::new(),
            file_sequence: 0,
        }
    }
    /// Infer column type from JSON value
    pub fn infer_type(value: &Value) -> ColumnType {
        match value {
            Value::Bool(_) => ColumnType::Boolean,
            Value::Number(n) => {
                if n.is_u64() {
                    ColumnType::UInt64
                } else if n.is_i64() {
                    ColumnType::Int64
                } else {
                    ColumnType::Float64
                }
            }
            Value::String(s) => {
                // Try to parse as number
                if s.parse::<u64>().is_ok() {
                    ColumnType::UInt64
                } else if s.parse::<i64>().is_ok() {
                    ColumnType::Int64
                } else if s.parse::<f64>().is_ok() {
                    ColumnType::Float64
                } else {
                    ColumnType::String
                }
            }
            _ => ColumnType::String,
        }
    }

    /// Infer schema from data objects
    pub fn infer_schema_from_data(data: &[Value]) -> Result<(Arc<Schema>, HashMap<String, ColumnType>)> {
        if data.is_empty() {
            anyhow::bail!("Cannot infer schema from empty data");
        }

        let mut column_types: HashMap<String, ColumnType> = HashMap::new();
        let mut field_order: Vec<String> = Vec::new();

        // Analyze first object to get field names and types
        if let Value::Object(obj) = &data[0] {
            for (key, value) in obj.iter() {
                let col_type = Self::infer_type(value);
                column_types.insert(key.clone(), col_type);
                field_order.push(key.clone());
            }
        } else {
            anyhow::bail!("Expected object in data array");
        }

        // Sort field names for consistent schema
        field_order.sort();

        // Create Arrow fields
        let fields: Vec<Field> = field_order
            .iter()
            .map(|name| {
                let data_type = match column_types.get(name).unwrap() {
                    ColumnType::String => DataType::Utf8,
                    ColumnType::Int64 => DataType::Int64,
                    ColumnType::UInt64 => DataType::UInt64,
                    ColumnType::Float64 => DataType::Float64,
                    ColumnType::Boolean => DataType::Boolean,
                };
                Field::new(name, data_type, true)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));
        Ok((schema, column_types))
    }

    /// Check if a row matches a filter condition
    pub fn row_matches_condition(row: &DataRow, condition: &FilterCondition) -> bool {
        let row_value = match row.get(&condition.field) {
            Some(v) => v,
            None => return false,
        };

        match &condition.operator {
            FilterOperator::Eq => Self::values_equal(row_value, &condition.value),
            FilterOperator::Ne => !Self::values_equal(row_value, &condition.value),
            FilterOperator::Gt => Self::compare_values(row_value, &condition.value) == Some(std::cmp::Ordering::Greater),
            FilterOperator::Lt => Self::compare_values(row_value, &condition.value) == Some(std::cmp::Ordering::Less),
            FilterOperator::Gte => {
                matches!(
                    Self::compare_values(row_value, &condition.value),
                    Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
                )
            }
            FilterOperator::Lte => {
                matches!(
                    Self::compare_values(row_value, &condition.value),
                    Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
                )
            }
            FilterOperator::Contains => {
                if let (Value::String(s1), Value::String(s2)) = (row_value, &condition.value) {
                    s1.contains(s2.as_str())
                } else {
                    false
                }
            }
        }
    }

    /// Check if two JSON values are equal
    fn values_equal(v1: &Value, v2: &Value) -> bool {
        match (v1, v2) {
            (Value::String(s1), Value::String(s2)) => s1 == s2,
            (Value::Number(n1), Value::Number(n2)) => {
                n1.as_f64().unwrap_or(0.0) == n2.as_f64().unwrap_or(0.0)
            }
            (Value::Bool(b1), Value::Bool(b2)) => b1 == b2,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }

    /// Compare two JSON values (for Gt, Lt, Gte, Lte operators)
    fn compare_values(v1: &Value, v2: &Value) -> Option<std::cmp::Ordering> {
        match (v1, v2) {
            (Value::Number(n1), Value::Number(n2)) => {
                let f1 = n1.as_f64()?;
                let f2 = n2.as_f64()?;
                f1.partial_cmp(&f2)
            }
            (Value::String(s1), Value::String(s2)) => Some(s1.cmp(s2)),
            _ => None,
        }
    }

    /// Apply filters to data rows - returns true if row should be included
    /// If no filters are configured, all rows pass through
    pub fn apply_filters(&self, row: &DataRow) -> bool {
        // If no filters configured, include all data
        if self.config.filter.is_empty() {
            return true;
        }

        // Check if row matches ANY of the filter conditions (OR logic)
        self.config.filter.iter().any(|condition| {
            Self::row_matches_condition(row, condition)
        })
    }

    /// Convert data to rows
    pub fn convert_to_rows(data: Vec<Value>) -> Vec<DataRow> {
        data.into_iter()
            .filter_map(|value| {
                if let Value::Object(obj) = value {
                    Some(obj.into_iter().collect())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get directory path for a given date
    pub fn get_parquet_dir(&self, date: NaiveDate) -> PathBuf {
        let year = date.format("%Y").to_string();
        let month = date.format("%m").to_string();
        let day = date.format("%d").to_string();

        PathBuf::from(&self.config.path)
            .join(year)
            .join(month)
            .join(day)
    }

    /// Generate unique parquet file path with timestamp and sequence
    pub fn get_unique_parquet_path(&mut self, date: NaiveDate) -> PathBuf {
        let dir = self.get_parquet_dir(date);
        
        // Extract symbol name (last part after the last dash if it contains one)
        let clean_name = if self.config.name.contains('-') {
            if let Some(last_dash_idx) = self.config.name.rfind('-') {
                self.config.name[last_dash_idx + 1..].to_string()
            } else {
                self.config.name.clone()
            }
        } else {
            self.config.name.clone()
        };
        
        // Generate filename: only add sequence if batch_size was explicitly configured
        // If no batch config (None), use simple format: symbol_date.parquet
        // If batch_size is configured, add sequence: symbol_000001_date.parquet
        let filename = if self.config.has_batch_config {
            // Has batch_size configured, add sequence number
            self.file_sequence += 1;
            format!("{}_{:06}_{}.parquet", clean_name, self.file_sequence, date.format("%Y-%m-%d"))
        } else {
            // No batch_size configured (single file mode), no sequence needed
            format!("{}_{}.parquet", clean_name, date.format("%Y-%m-%d"))
        };
        dir.join(filename)
    }

    /// Validate schema of existing files in directory (optional, for startup check)
    pub fn validate_existing_files(&self, date: NaiveDate) -> Result<()> {
        let dir = self.get_parquet_dir(date);
        if !dir.exists() {
            return Ok(());
        }

        let Some(ref schema) = self.schema else {
            return Ok(());
        };

        info!("Validating existing parquet files in {:?}", dir);
        
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                    if let Ok(file) = File::open(&path) {
                        if let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) {
                            let file_schema = builder.schema();
                            if !schemas_compatible(file_schema.as_ref(), schema.as_ref()) {
                                warn!("Schema mismatch in file {:?}. Consider deleting old files.", path);
                            }
                        } else {
                            warn!("Corrupted parquet file: {:?}. Consider deleting.", path);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Convert buffer data to RecordBatch
    pub fn buffer_to_batch(&self, data: &[DataRow]) -> Result<RecordBatch> {
        if data.is_empty() {
            anyhow::bail!("Cannot create batch from empty data");
        }

        let Some(ref schema) = self.schema else {
            anyhow::bail!("Schema not initialized");
        };

        // Build arrays dynamically based on schema
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for field in schema.fields().iter() {
            let col_name = field.name();
            let col_type = self.column_types.get(col_name)
                .context(format!("Column type not found for {}", col_name))?;

            let array: ArrayRef = match col_type {
                ColumnType::String => {
                    let mut builder = StringBuilder::new();
                    for row in data {
                        if let Some(value) = row.get(col_name) {
                            match value {
                                Value::String(s) => builder.append_value(s),
                                Value::Number(n) => builder.append_value(&n.to_string()),
                                Value::Bool(b) => builder.append_value(&b.to_string()),
                                Value::Null => builder.append_null(),
                                _ => builder.append_value(&value.to_string()),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                ColumnType::UInt64 => {
                    let mut builder = UInt64Builder::new();
                    for row in data {
                        if let Some(value) = row.get(col_name) {
                            let num = match value {
                                Value::Number(n) => n.as_u64().unwrap_or(0),
                                Value::String(s) => s.parse::<u64>().unwrap_or(0),
                                _ => 0,
                            };
                            builder.append_value(num);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                ColumnType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for row in data {
                        if let Some(value) = row.get(col_name) {
                            let num = match value {
                                Value::Number(n) => n.as_i64().unwrap_or(0),
                                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                                _ => 0,
                            };
                            builder.append_value(num);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                ColumnType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for row in data {
                        if let Some(value) = row.get(col_name) {
                            let num = match value {
                                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                                Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                                _ => 0.0,
                            };
                            builder.append_value(num);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                ColumnType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for row in data {
                        if let Some(value) = row.get(col_name) {
                            let b = match value {
                                Value::Bool(b) => *b,
                                Value::String(s) => s.parse::<bool>().unwrap_or(false),
                                _ => false,
                            };
                            builder.append_value(b);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
            };

            arrays.push(array);
        }

        RecordBatch::try_new(schema.clone(), arrays)
            .context("Failed to create record batch")
    }

    /// Generate a unique temporary file path in /tmp
    fn generate_temp_path(&self, final_path: &PathBuf) -> Result<PathBuf> {
        let filename = final_path.file_name()
            .and_then(|n| n.to_str())
            .context("Failed to get filename from path")?;
        
        // Generate unique temp filename with timestamp and process ID
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("Failed to get timestamp")?
            .as_nanos();
        let pid = std::process::id();
        
        let temp_filename = format!("{}.{}.{}.tmp", filename, pid, timestamp);
        Ok(PathBuf::from("/tmp").join(temp_filename))
    }

    /// Write a single RecordBatch to a new parquet file
    /// If use_temp_dir is enabled, writes to /tmp first and then copies to final destination
    pub fn write_batch_to_file(&self, path: &PathBuf, batch: &RecordBatch) -> Result<()> {
        let Some(ref schema) = self.schema else {
            anyhow::bail!("Schema not initialized");
        };

        // Determine the actual write path
        let write_path = if self.config.use_temp_dir {
            // Generate temporary file path in /tmp
            self.generate_temp_path(path)
                .context("Failed to generate temp path")?
        } else {
            // Write directly to final destination
            path.clone()
        };

        // Create parent directories for the write path (temp or final)
        if let Some(parent) = write_path.parent() {
            if !parent.exists() {
                #[cfg(unix)]
                {
                    let mut builder = std::fs::DirBuilder::new();
                    builder.recursive(true).mode(0o775);
                    builder.create(parent)
                        .context(format!("Failed to create directory: {:?}", parent))?;
                }
                
                #[cfg(not(unix))]
                fs::create_dir_all(parent)
                    .context(format!("Failed to create directory: {:?}", parent))?;
            }
        }

        // Open file with explicit permissions (0664 - rw-rw-r--)
        #[cfg(unix)]
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o664)
            .open(&write_path)
            .context(format!("Failed to open parquet file: {:?}", write_path))?;
        
        #[cfg(not(unix))]
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&write_path)
            .context(format!("Failed to open parquet file: {:?}", write_path))?;

        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .context("Failed to create ArrowWriter")?;

        writer.write(batch).context("Failed to write batch to parquet")?;
        writer.close().context("Failed to close writer")?;

        // If using temp dir, copy file to final destination
        if self.config.use_temp_dir {
            // Create parent directories for final destination
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    #[cfg(unix)]
                    {
                        let mut builder = std::fs::DirBuilder::new();
                        builder.recursive(true).mode(0o775);
                        builder.create(parent)
                            .context(format!("Failed to create directory: {:?}", parent))?;
                    }
                    
                    #[cfg(not(unix))]
                    fs::create_dir_all(parent)
                        .context(format!("Failed to create directory: {:?}", parent))?;
                }
            }

            // Copy file from temp to final destination
            fs::copy(&write_path, path)
                .context(format!("Failed to copy file from {:?} to {:?}", write_path, path))?;

            // Set file permissions on the final file (0664 - rw-rw-r--)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o664);
                fs::set_permissions(path, perms)
                    .context(format!("Failed to set permissions on {:?}", path))?;
            }

            // Remove temporary file
            fs::remove_file(&write_path)
                .context(format!("Failed to remove temp file: {:?}", write_path))?;

            info!("Wrote {} records to {:?} (via temp file {:?})", batch.num_rows(), path, write_path);
        } else {
            info!("Wrote {} records to {:?}", batch.num_rows(), path);
        }

        Ok(())
    }

    /// Write data rows directly (called by Writer trait implementation)
    async fn write_rows_impl(&mut self, rows: Vec<DataRow>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // Use configured date if provided, otherwise use current date
        let today = self.config.date.unwrap_or_else(|| chrono::Utc::now().date_naive());

        // If schema not initialized, infer it from the first batch
        if self.schema.is_none() {
            // Convert rows to Value array for schema inference
            let data_array: Vec<Value> = rows.iter()
                .map(|row| Value::Object(row.clone().into_iter().collect()))
                .collect();
            
            let (schema, column_types) = Self::infer_schema_from_data(&data_array)
                .context("Failed to infer schema")?;
            
            info!("Inferred schema with {} columns:", schema.fields().len());
            for field in schema.fields() {
                info!("  - {}: {:?}", field.name(), field.data_type());
            }
            
            self.schema = Some(schema);
            self.column_types = column_types;
        }

        // Check if we need to flush data for a new day
        if self.current_date.is_some() && self.current_date != Some(today) {
            self.flush_buffer_impl().await?;
        }

        // Apply filters
        let filtered_rows: Vec<DataRow> = rows.into_iter()
            .filter(|row| self.apply_filters(row))
            .collect();

        if !filtered_rows.is_empty() {
            self.buffer.extend(filtered_rows);
            self.current_date = Some(today);
        } else if !self.config.filter.is_empty() {
            // Data was filtered out
            info!("All records filtered out by filter conditions");
        }

        // Flush buffer when it reaches configured batch_size
        if self.buffer.len() >= self.config.batch_size {
            self.flush_buffer_impl().await?;
        }

        Ok(())
    }

    async fn flush_buffer_impl(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        if self.schema.is_none() {
            warn!("Schema not initialized, cannot flush buffer");
            return Ok(());
        }

        let date = self.current_date.context("No current date set")?;
        
        info!("Flushing {} records to parquet", self.buffer.len());

        // Convert buffer to RecordBatch
        let batch = self.buffer_to_batch(&self.buffer)?;
        
        // Generate unique file path
        let path = self.get_unique_parquet_path(date);

        // Write to new file (no reading of old data!)
        self.write_batch_to_file(&path, &batch)?;

        // Clear buffer
        self.buffer.clear();

        Ok(())
    }
}

#[async_trait]
impl Writer for ParquetWriter {
    async fn write_rows(&mut self, rows: Vec<DataRow>) -> Result<()> {
        self.write_rows_impl(rows).await
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        self.flush_buffer_impl().await
    }
}

impl Drop for ParquetWriter {
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            warn!("Buffer not empty on drop, {} records will be lost", self.buffer.len());
        }
    }
}

/// Check if two schemas are compatible (same fields and types)
pub fn schemas_compatible(schema1: &Schema, schema2: &Schema) -> bool {
    if schema1.fields().len() != schema2.fields().len() {
        return false;
    }

    for (field1, field2) in schema1.fields().iter().zip(schema2.fields().iter()) {
        if field1.name() != field2.name() || field1.data_type() != field2.data_type() {
            return false;
        }
    }

    true
}
