//! Data merger module
//!
//! This module handles merging JSONL data from multiple sources with deduplication
//! and forward-fill logic to ensure every second has data.

use anyhow::{Context, Result};
use chrono::{NaiveDate, TimeZone, Utc};
use serde_json::Value;
use std::collections::BTreeMap;
use tracing::{info, debug, warn};

use crate::writer::DataRow;

/// Data merger with forward-fill capability
pub struct DataMerger {
    /// Map of timestamp (seconds) to data row
    data_by_second: BTreeMap<i64, DataRow>,
    /// The date being processed
    date: NaiveDate,
}

impl DataMerger {
    /// Create a new data merger for a specific date
    pub fn new(date: NaiveDate) -> Self {
        Self {
            data_by_second: BTreeMap::new(),
            date,
        }
    }

    /// Extract timestamp in seconds from a data row
    /// The "E" field contains timestamp in milliseconds
    pub fn extract_timestamp_seconds(row: &DataRow) -> Option<i64> {
        row.get("E")
            .and_then(|v| match v {
                Value::Number(n) => n.as_i64(),
                Value::String(s) => s.parse::<i64>().ok(),
                _ => None,
            })
            .map(|millis| millis / 1000) // Convert milliseconds to seconds
    }

    /// Add data from a JSONL source (one of the SSH servers)
    /// Deduplicates by timestamp - if a second already has data, it's skipped
    pub fn add_jsonl_data(&mut self, jsonl_content: &str, source_name: &str) -> Result<usize> {
        let mut added_count = 0;
        let mut skipped_count = 0;

        for (line_num, line) in jsonl_content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Parse JSON line
            let value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Line {} from {} failed to parse as JSON ({}), skipping", line_num + 1, source_name, e);
                    continue;
                }
            };

            // Convert to DataRow
            if let Value::Object(obj) = value {
                let mut row: DataRow = obj.into_iter().collect();

                // Extract timestamp
                if let Some(timestamp_sec) = Self::extract_timestamp_seconds(&row) {
                    // Check if this second already has data
                    if self.data_by_second.contains_key(&timestamp_sec) {
                        skipped_count += 1;
                        debug!("Skipping duplicate data for timestamp {} from {}", timestamp_sec, source_name);
                    } else {
                        // Normalize the E field to seconds precision (keep it in milliseconds but aligned to second)
                        let normalized_millis = timestamp_sec * 1000;
                        row.insert("E".to_string(), Value::Number(normalized_millis.into()));
                        
                        self.data_by_second.insert(timestamp_sec, row);
                        added_count += 1;
                    }
                } else {
                    warn!("Line {} from {} missing 'E' timestamp field, skipping", line_num + 1, source_name);
                }
            } else {
                warn!("Line {} from {} is not a JSON object, skipping", line_num + 1, source_name);
            }
        }

        info!("Added {} records from {} ({} skipped as duplicates)", added_count, source_name, skipped_count);
        Ok(added_count)
    }

    /// Apply forward-fill to ensure every second in the UTC day has data
    /// Fills the entire day (00:00:00 to 23:59:59 UTC) based on the date
    /// - If data starts after 00:00:00, backfill with the first data point
    /// - If data ends before 23:59:59, forward-fill with the last data point
    /// - For missing seconds in between, use the previous second's data
    pub fn apply_forward_fill(&mut self) -> Result<()> {
        if self.data_by_second.is_empty() {
            warn!("No data to forward-fill");
            return Ok(());
        }

        info!("Applying forward-fill for date {} (full UTC day)", self.date);

        // Calculate the start and end timestamps for the UTC day
        // Explicitly use UTC timezone to ensure correct day boundaries
        let day_start_naive = self.date.and_hms_opt(0, 0, 0)
            .context("Failed to create start of day")?;
        let day_end_naive = self.date.and_hms_opt(23, 59, 59)
            .context("Failed to create end of day")?;
        
        // Convert to UTC explicitly
        let day_start = Utc.from_utc_datetime(&day_start_naive).timestamp();
        let day_end = Utc.from_utc_datetime(&day_end_naive).timestamp();

        debug!("UTC day range: {} to {} ({} seconds)", day_start, day_end, day_end - day_start + 1);

        // Find the first and last actual data timestamp
        let first_data_timestamp = *self.data_by_second.keys().next()
            .context("No data available for forward-fill")?;
        let last_data_timestamp = *self.data_by_second.keys().last()
            .context("No data available for forward-fill")?;

        debug!("Actual data range: {} to {}", first_data_timestamp, last_data_timestamp);

        // Get the first and last available data points
        let first_data = self.data_by_second.get(&first_data_timestamp)
            .context("No data available for forward-fill")?
            .clone();
        let last_data = self.data_by_second.get(&last_data_timestamp)
            .context("No data available for forward-fill")?
            .clone();

        let mut current_data = first_data.clone();
        let mut filled_count = 0;

        // Iterate through every second of the UTC day
        for timestamp in day_start..=day_end {
            if let Some(data) = self.data_by_second.get(&timestamp) {
                // Data exists for this second, use it as the current data
                current_data = data.clone();
            } else {
                // No data for this second, need to fill
                let filled_data = if timestamp < first_data_timestamp {
                    // Before first data point: backfill with first data
                    first_data.clone()
                } else if timestamp > last_data_timestamp {
                    // After last data point: forward-fill with last data
                    last_data.clone()
                } else {
                    // Between data points: forward-fill with previous data
                    current_data.clone()
                };

                // Update the E field to reflect the current timestamp
                let mut filled_data = filled_data;
                let normalized_millis = timestamp * 1000;
                filled_data.insert("E".to_string(), Value::Number(normalized_millis.into()));
                
                // Also update T field if it exists
                if filled_data.contains_key("T") {
                    filled_data.insert("T".to_string(), Value::Number(normalized_millis.into()));
                }
                
                self.data_by_second.insert(timestamp, filled_data);
                filled_count += 1;
            }
        }

        info!("Forward-filled {} missing seconds (full UTC day: {} to {})", filled_count, day_start, day_end);
        Ok(())
    }

    /// Get all data rows sorted by timestamp
    pub fn get_sorted_rows(&self) -> Vec<DataRow> {
        self.data_by_second.values().cloned().collect()
    }

    /// Get the number of unique seconds with data
    pub fn len(&self) -> usize {
        self.data_by_second.len()
    }

    /// Check if the merger is empty
    pub fn is_empty(&self) -> bool {
        self.data_by_second.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_extract_timestamp_seconds() {
        let mut row = DataRow::new();
        row.insert("E".to_string(), Value::Number(1762411870001i64.into()));
        
        let timestamp = DataMerger::extract_timestamp_seconds(&row);
        assert_eq!(timestamp, Some(1762411870));
    }

    #[test]
    fn test_add_and_deduplicate() {
        let date = NaiveDate::from_ymd_opt(2025, 11, 6).unwrap();
        let mut merger = DataMerger::new(date);

        let jsonl1 = r#"{"E":1762411870001,"s":"BTCUSDT","p":103308.50797101}
{"E":1762411871001,"s":"BTCUSDT","p":103309.50797101}"#;

        let jsonl2 = r#"{"E":1762411870001,"s":"BTCUSDT","p":999999.99}
{"E":1762411872001,"s":"BTCUSDT","p":103310.50797101}"#;

        merger.add_jsonl_data(jsonl1, "source1").unwrap();
        assert_eq!(merger.len(), 2);

        merger.add_jsonl_data(jsonl2, "source2").unwrap();
        // Should have 3 unique seconds: 1762411870, 1762411871, 1762411872
        // The duplicate at 1762411870 from source2 should be skipped
        assert_eq!(merger.len(), 3);

        // Verify the first timestamp kept the original value
        let first_row = merger.data_by_second.get(&1762411870).unwrap();
        let price = first_row.get("p").unwrap().as_f64().unwrap();
        assert_eq!(price, 103308.50797101);
    }
}

