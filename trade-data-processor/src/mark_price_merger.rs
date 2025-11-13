//! Mark price data merger module
//!
//! This module handles merging mark-price data from Binance funding rate API.
//! Mark-price data includes:
//! - event_time (E): Event time in milliseconds (original value preserved)
//! - symbol (s): Symbol (trading pair, e.g., "BTCUSDT")
//! - mark_price (p): Mark price
//! - index_price (i): Index price
//! - estimated_settle_price (P): Estimated settle price
//! - funding_rate (r): Funding rate
//! - next_funding_time (T): Next funding time (optional)

use anyhow::{Context, Result};
use chrono::{NaiveDate, TimeZone, Utc};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use tracing::{info, debug, warn};

use crate::writer::DataRow;

/// Mark price data merger with forward-fill capability
/// Specifically handles Binance mark-price data with validation
/// Organizes data by symbol (trading pair)
pub struct MarkPriceMerger {
    /// Map of symbol to (dedup_key -> data row)
    /// First level: symbol (e.g., "BTCUSDT")
    /// Second level: dedup key (timestamp in seconds) -> data row
    /// Note: dedup key is derived from event_time but original event_time is preserved
    data_by_symbol: HashMap<String, BTreeMap<i64, DataRow>>,
    /// The date being processed
    date: NaiveDate,
}

impl MarkPriceMerger {
    /// Create a new mark price merger for a specific date
    pub fn new(date: NaiveDate) -> Self {
        Self {
            data_by_symbol: HashMap::new(),
            date,
        }
    }

    /// Extract symbol from a data row
    /// Tries new field name first ("symbol"), then falls back to short name ("s")
    pub fn extract_symbol(row: &DataRow) -> Option<String> {
        row.get("symbol")
            .or_else(|| row.get("s"))
            .and_then(|v| match v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            })
    }

    /// Extract deduplication key from a data row (timestamp in seconds)
    /// Tries new field name first ("event_time"), then falls back to short name ("E")
    /// Converts milliseconds to seconds for deduplication
    pub fn extract_dedup_key(row: &DataRow) -> Option<i64> {
        row.get("event_time")
            .or_else(|| row.get("E"))
            .and_then(|v| match v {
                Value::Number(n) => n.as_i64(),
                Value::String(s) => s.parse::<i64>().ok(),
                _ => None,
            })
            .map(|millis| millis / 1000) // Convert milliseconds to seconds
    }

    /// Normalize field names from short form to full names
    /// Maps: e->event_type, s->symbol, p->mark_price, i->index_price, P->estimated_settle_price, r->funding_rate, T->next_funding_time
    fn normalize_field_names(row: &mut DataRow) {
        let replacements = vec![
            ("e", "event_type"),
            ("s", "symbol"),
            ("p", "mark_price"),
            ("i", "index_price"),
            ("P", "estimated_settle_price"),
            ("r", "funding_rate"),
            ("T", "next_funding_time"),
            ("E", "event_time"),
        ];
        
        for (short, full) in replacements {
            if let Some(value) = row.remove(short) {
                row.insert(full.to_string(), value);
            }
        }
    }

    /// Add mark-price specific JSONL data with specialized validation
    /// Required fields: event_time/E (timestamp), symbol/s (symbol), mark_price/p, funding_rate/r
    /// Field names are normalized to full names during processing
    /// Original event_time value is preserved; deduplication uses a separate key
    pub fn add_jsonl_data(&mut self, jsonl_content: &str, source_name: &str) -> Result<usize> {
        let mut added_count = 0;
        let mut skipped_count = 0;
        let mut invalid_count = 0;
        let mut missing_symbol_count = 0;

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

                // Normalize field names to full names
                Self::normalize_field_names(&mut row);

                // Extract symbol - required for grouping by trading pair
                let symbol = match Self::extract_symbol(&row) {
                    Some(s) => s,
                    None => {
                        missing_symbol_count += 1;
                        warn!("Line {} from {} missing 'symbol' field, skipping", line_num + 1, source_name);
                        continue;
                    }
                };

                // Extract dedup key (timestamp in seconds)
                if let Some(dedup_key) = Self::extract_dedup_key(&row) {
                    // Validate mark-price specific fields
                    if Self::is_valid_mark_price_row(&row) {
                        // Get or create the BTreeMap for this symbol
                        let symbol_data = self.data_by_symbol.entry(symbol.clone()).or_insert_with(BTreeMap::new);
                        
                        // Check if this dedup key already has data for this symbol
                        if symbol_data.contains_key(&dedup_key) {
                            skipped_count += 1;
                            debug!("Skipping duplicate mark-price data for symbol {} dedup_key {} from {}", symbol, dedup_key, source_name);
                        } else {
                            // Add timestamp field for reference (in milliseconds, last 3 digits are 000)
                            row.insert("timestamp".to_string(), Value::Number((dedup_key * 1000).into()));
                            
                            symbol_data.insert(dedup_key, row);
                            added_count += 1;
                        }
                    } else {
                        invalid_count += 1;
                        warn!("Line {} from {} has invalid mark-price fields (missing mark_price or funding_rate), skipping", line_num + 1, source_name);
                    }
                } else {
                    warn!("Line {} from {} missing 'event_time' field, skipping", line_num + 1, source_name);
                }
            } else {
                warn!("Line {} from {} is not a JSON object, skipping", line_num + 1, source_name);
            }
        }

        info!("Added {} mark-price records from {} ({} skipped as duplicates, {} invalid, {} missing symbol)", 
              added_count, source_name, skipped_count, invalid_count, missing_symbol_count);
        Ok(added_count)
    }

    /// Validate that a row contains required mark-price fields
    /// Required: mark_price and funding_rate (with backward compatibility for short names)
    fn is_valid_mark_price_row(row: &DataRow) -> bool {
        // Check for required mark-price fields (try full names first, then short names)
        let has_price = row.get("mark_price").is_some() || row.get("p").is_some();
        let has_funding_rate = row.get("funding_rate").is_some() || row.get("r").is_some();
        
        // Price should be numeric (check full name first)
        let price_value = row.get("mark_price").or_else(|| row.get("p"));
        let price_valid = price_value
            .map(|v| matches!(v, Value::Number(_) | Value::String(_)))
            .unwrap_or(false);
        
        // Funding rate should be numeric (check full name first)
        let rate_value = row.get("funding_rate").or_else(|| row.get("r"));
        let rate_valid = rate_value
            .map(|v| matches!(v, Value::Number(_) | Value::String(_)))
            .unwrap_or(false);
        
        has_price && has_funding_rate && price_valid && rate_valid
    }

    /// Apply forward-fill to ensure every second in the UTC day has data for each symbol
    /// Fills the entire day (00:00:00 to 23:59:59 UTC) based on the date
    /// - If data starts after 00:00:00, backfill with the first data point
    /// - If data ends before 23:59:59, forward-fill with the last data point
    /// - For missing seconds in between, use the previous second's data
    /// 
    /// This method processes symbols in parallel for better performance
    pub fn apply_forward_fill(&mut self) -> Result<()> {
        use rayon::prelude::*;

        if self.data_by_symbol.is_empty() {
            warn!("No data to forward-fill");
            return Ok(());
        }

        info!("Applying forward-fill for date {} (mark-price, full UTC day) across {} symbols in parallel", self.date, self.data_by_symbol.len());

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

        // Collect symbols to process
        let symbols: Vec<String> = self.data_by_symbol.keys().cloned().collect();
        
        // Process symbols in parallel using rayon
        let results: Vec<(String, BTreeMap<i64, DataRow>, usize)> = symbols
            .par_iter()
            .filter_map(|symbol| {
                let symbol_data = match self.data_by_symbol.get(symbol) {
                    Some(data) if !data.is_empty() => data,
                    _ => return None,
                };

                debug!("Applying forward-fill for symbol: {}", symbol);

                // Find the first and last actual data timestamp for this symbol
                let first_data_timestamp = *symbol_data.keys().next()?;
                let last_data_timestamp = *symbol_data.keys().last()?;

                debug!("Symbol {} actual data range: {} to {}", symbol, first_data_timestamp, last_data_timestamp);

                // Get the first and last available data points for this symbol
                let first_data = symbol_data.get(&first_data_timestamp)?.clone();
                let last_data = symbol_data.get(&last_data_timestamp)?.clone();

                let mut filled_symbol_data = symbol_data.clone();
                let mut current_data = first_data.clone();
                let mut filled_count = 0;

                // Iterate through every second of the UTC day
                for timestamp in day_start..=day_end {
                    if let Some(data) = filled_symbol_data.get(&timestamp) {
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

                        // Update timestamp for forward-filled entries (preserve original event_time)
                        let mut filled_data = filled_data;
                        let filled_timestamp = timestamp * 1000; // in milliseconds (last 3 digits are 000)
                        // Keep original event_time, only update timestamp to the filled second
                        filled_data.insert("timestamp".to_string(), Value::Number(filled_timestamp.into()));
                        
                        filled_symbol_data.insert(timestamp, filled_data);
                        filled_count += 1;
                    }
                }

                info!("Forward-filled {} missing seconds for symbol {} (full UTC day: {} to {})", filled_count, symbol, day_start, day_end);
                
                Some((symbol.clone(), filled_symbol_data, filled_count))
            })
            .collect();

        // Update the data_by_symbol map with the filled data
        for (symbol, filled_data, _filled_count) in results {
            self.data_by_symbol.insert(symbol, filled_data);
        }

        Ok(())
    }

    /// Get all symbols that have data
    pub fn get_symbols(&self) -> Vec<String> {
        self.data_by_symbol.keys().cloned().collect()
    }

    /// Get all data rows for a specific symbol, sorted by timestamp
    pub fn get_sorted_rows_for_symbol(&self, symbol: &str) -> Option<Vec<DataRow>> {
        self.data_by_symbol.get(symbol).map(|data| {
            data.values().cloned().collect()
        })
    }

    /// Get the number of unique seconds with data for a specific symbol
    pub fn len_for_symbol(&self, symbol: &str) -> usize {
        self.data_by_symbol.get(symbol).map(|data| data.len()).unwrap_or(0)
    }

    /// Get the total number of symbols
    pub fn symbol_count(&self) -> usize {
        self.data_by_symbol.len()
    }

    /// Check if the merger is empty (has no symbols)
    pub fn is_empty(&self) -> bool {
        self.data_by_symbol.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_extract_dedup_key() {
        let mut row = DataRow::new();
        row.insert("event_time".to_string(), Value::Number(1762411870001i64.into()));
        
        let dedup_key = MarkPriceMerger::extract_dedup_key(&row);
        assert_eq!(dedup_key, Some(1762411870));
    }

    #[test]
    fn test_extract_dedup_key_with_short_name() {
        let mut row = DataRow::new();
        row.insert("E".to_string(), Value::Number(1762411870001i64.into()));
        
        let dedup_key = MarkPriceMerger::extract_dedup_key(&row);
        assert_eq!(dedup_key, Some(1762411870));
    }

    #[test]
    fn test_valid_mark_price_row() {
        let mut row = DataRow::new();
        row.insert("event_time".to_string(), Value::Number(1762411870001i64.into()));
        row.insert("mark_price".to_string(), Value::Number(103308.50797101.into()));
        row.insert("funding_rate".to_string(), Value::Number(0.0001.into()));
        
        assert!(MarkPriceMerger::is_valid_mark_price_row(&row));
    }

    #[test]
    fn test_valid_mark_price_row_with_short_names() {
        let mut row = DataRow::new();
        row.insert("E".to_string(), Value::Number(1762411870001i64.into()));
        row.insert("P".to_string(), Value::Number(103308.50797101.into()));
        row.insert("r".to_string(), Value::Number(0.0001.into()));
        
        assert!(MarkPriceMerger::is_valid_mark_price_row(&row));
    }

    #[test]
    fn test_invalid_mark_price_row_missing_price() {
        let mut row = DataRow::new();
        row.insert("event_time".to_string(), Value::Number(1762411870001i64.into()));
        row.insert("funding_rate".to_string(), Value::Number(0.0001.into()));
        
        assert!(!MarkPriceMerger::is_valid_mark_price_row(&row));
    }

    #[test]
    fn test_add_and_deduplicate() {
        let date = NaiveDate::from_ymd_opt(2025, 11, 6).unwrap();
        let mut merger = MarkPriceMerger::new(date);

        let jsonl1 = r#"{"E":1762411870001,"s":"BTCUSDT","p":"103308.50797101","r":"0.0001"}
{"E":1762411871001,"s":"BTCUSDT","p":"103309.50797101","r":"0.0001"}"#;

        let jsonl2 = r#"{"E":1762411870001,"s":"BTCUSDT","p":"999999.99","r":"0.0001"}
{"E":1762411872001,"s":"BTCUSDT","p":"103310.50797101","r":"0.0001"}"#;

        merger.add_jsonl_data(jsonl1, "source1").unwrap();
        assert_eq!(merger.len_for_symbol("BTCUSDT"), 2);

        merger.add_jsonl_data(jsonl2, "source2").unwrap();
        // Should have 3 unique seconds: 1762411870, 1762411871, 1762411872
        // The duplicate at 1762411870 from source2 should be skipped
        assert_eq!(merger.len_for_symbol("BTCUSDT"), 3);

        // Verify the first timestamp kept the original value
        let rows = merger.get_sorted_rows_for_symbol("BTCUSDT").unwrap();
        let first_row = &rows[0];
        let price = first_row.get("mark_price").unwrap().as_str().unwrap();
        assert_eq!(price, "103308.50797101");
        
        // Verify event_time is preserved in original form
        let event_time = first_row.get("event_time").unwrap().as_i64().unwrap();
        assert_eq!(event_time, 1762411870001);
    }

    #[test]
    fn test_multiple_symbols() {
        let date = NaiveDate::from_ymd_opt(2025, 11, 6).unwrap();
        let mut merger = MarkPriceMerger::new(date);

        let jsonl = r#"{"E":1762411870001,"s":"BTCUSDT","p":"103308.50797101","r":"0.0001"}
{"E":1762411870001,"s":"ETHUSDT","p":"3377.55407203","r":"0.00005065"}
{"E":1762411871001,"s":"BTCUSDT","p":"103309.50797101","r":"0.0001"}"#;

        merger.add_jsonl_data(jsonl, "source1").unwrap();
        
        // Should have 2 symbols
        assert_eq!(merger.symbol_count(), 2);
        assert_eq!(merger.len_for_symbol("BTCUSDT"), 2);
        assert_eq!(merger.len_for_symbol("ETHUSDT"), 1);

        let symbols = merger.get_symbols();
        assert!(symbols.contains(&"BTCUSDT".to_string()));
        assert!(symbols.contains(&"ETHUSDT".to_string()));
    }

    #[test]
    fn test_field_name_normalization() {
        let mut row = DataRow::new();
        row.insert("e".to_string(), Value::String("markPriceUpdate".to_string()));
        row.insert("E".to_string(), Value::Number(1762411870001i64.into()));
        row.insert("s".to_string(), Value::String("BTCUSDT".to_string()));
        row.insert("p".to_string(), Value::String("103308.50797101".to_string()));
        row.insert("i".to_string(), Value::String("11784.62659091".to_string()));
        row.insert("P".to_string(), Value::String("11784.25641265".to_string()));
        row.insert("r".to_string(), Value::String("0.00030000".to_string()));
        row.insert("T".to_string(), Value::Number(1562306400000i64.into()));

        MarkPriceMerger::normalize_field_names(&mut row);

        // Check that short names were replaced with full names
        assert!(row.contains_key("event_type"));
        assert!(row.contains_key("event_time"));
        assert!(row.contains_key("symbol"));
        assert!(row.contains_key("mark_price"));
        assert!(row.contains_key("index_price"));
        assert!(row.contains_key("estimated_settle_price"));
        assert!(row.contains_key("funding_rate"));
        assert!(row.contains_key("next_funding_time"));

        // Check that short names were removed
        assert!(!row.contains_key("e"));
        assert!(!row.contains_key("E"));
        assert!(!row.contains_key("s"));
        assert!(!row.contains_key("p"));
        assert!(!row.contains_key("i"));
        assert!(!row.contains_key("P"));
        assert!(!row.contains_key("r"));
        assert!(!row.contains_key("T"));
    }
}

