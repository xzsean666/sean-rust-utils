//! JSON file writing module
//!
//! This module handles writing WebSocket data to JSON files with line-by-line append.
//! One JSON file per day containing newline-delimited JSON records.

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::NaiveDate;
use std::fs::{OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use tracing::info;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, DirBuilderExt};

use crate::parquet_writer::FilterCondition;
use crate::writer::{Writer, DataRow};

/// Configuration for JsonWriter
#[derive(Debug, Clone)]
pub struct JsonWriterConfig {
    /// Base path for storing JSON files
    pub path: String,
    /// Name prefix for JSON files
    pub name: String,
    /// Optional filter conditions - if empty, all data is written
    pub filter: Vec<FilterCondition>,
}

/// JSON writer that appends records line-by-line to daily files
pub struct JsonWriter {
    config: JsonWriterConfig,
    current_date: Option<NaiveDate>,
    current_file: Option<std::fs::File>,
}

impl JsonWriter {
    pub fn new(config: JsonWriterConfig) -> Self {
        Self {
            config,
            current_date: None,
            current_file: None,
        }
    }

    /// Get directory path for a given date
    fn get_json_dir(&self, date: NaiveDate) -> PathBuf {
        let year = date.format("%Y").to_string();
        let month = date.format("%m").to_string();
        let day = date.format("%d").to_string();

        PathBuf::from(&self.config.path)
            .join(year)
            .join(month)
            .join(day)
    }

    /// Generate JSON file path for a given date (one file per day)
    fn get_json_path(&self, date: NaiveDate) -> PathBuf {
        let dir = self.get_json_dir(date);
        let filename = format!(
            "{}_{}.jsonl",
            self.config.name,
            date.format("%Y-%m-%d")
        );
        dir.join(filename)
    }

    /// Open or create file for current date
    fn ensure_file(&mut self, date: NaiveDate) -> Result<&mut std::fs::File> {
        // If date changed, close old file
        if self.current_date.is_some() && self.current_date != Some(date) {
            self.current_file = None;
        }

        // If no file open, create/open one
        if self.current_file.is_none() {
            let path = self.get_json_path(date);
            
            // Create parent directories with explicit permissions (0775 - rwxrwxr-x)
            if let Some(parent) = path.parent() {
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

            // Open file in append mode with explicit permissions (0664 - rw-rw-r--)
            #[cfg(unix)]
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o664)
                .open(&path)
                .context(format!("Failed to open JSON file: {:?}", path))?;
            
            #[cfg(not(unix))]
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .context(format!("Failed to open JSON file: {:?}", path))?;

            info!("Opened JSON file for writing: {:?}", path);
            self.current_file = Some(file);
            self.current_date = Some(date);
        }

        Ok(self.current_file.as_mut().unwrap())
    }


    /// Apply filters to data rows - returns true if row should be included
    /// If no filters are configured, all rows pass through
    fn apply_filters(&self, row: &DataRow) -> bool {
        // If no filters configured, include all data
        if self.config.filter.is_empty() {
            return true;
        }

        // Check if row matches ANY of the filter conditions (OR logic)
        self.config.filter.iter().any(|condition| {
            crate::parquet_writer::ParquetWriter::row_matches_condition(row, condition)
        })
    }

    /// Write a single row to the JSON file
    fn write_row(&mut self, date: NaiveDate, row: &DataRow) -> Result<()> {
        let file = self.ensure_file(date)?;
        
        // Convert HashMap to JSON and write as a line
        let json_line = serde_json::to_string(&row)
            .context("Failed to serialize row to JSON")?;
        
        writeln!(file, "{}", json_line)
            .context("Failed to write JSON line to file")?;
        
        Ok(())
    }
}

#[async_trait]
impl Writer for JsonWriter {
    async fn write_rows(&mut self, rows: Vec<DataRow>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let now = chrono::Utc::now();
        let today = now.date_naive();

        // Apply filters
        let filtered_rows: Vec<DataRow> = rows.into_iter()
            .filter(|row| self.apply_filters(row))
            .collect();

        if filtered_rows.is_empty() {
            if !self.config.filter.is_empty() {
                info!("All records filtered out by filter conditions");
            }
            return Ok(());
        }

        // Write each row immediately (append mode)
        let mut written_count = 0;
        for row in filtered_rows {
            self.write_row(today, &row)?;
            written_count += 1;
        }

        info!("Wrote {} records to JSON file", written_count);

        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        // Flush file to disk if open
        if let Some(ref mut file) = self.current_file {
            file.flush().context("Failed to flush JSON file")?;
            info!("Flushed JSON file to disk");
        }
        Ok(())
    }
}

impl Drop for JsonWriter {
    fn drop(&mut self) {
        if let Some(ref mut file) = self.current_file {
            let _ = file.flush();
        }
    }
}

