//! Data extraction utilities for WebSocket messages

use anyhow::{Context, Result};
use serde_json::Value;
use crate::writer::DataRow;

/// Extract data array from WebSocket message
/// Handles formats like: {"data": [...]} or direct array [...]
/// This is a utility function that should be called in main.rs before passing data to writers
pub fn extract_data_array(message: &str) -> Result<Vec<Value>> {
    let parsed: Value = serde_json::from_str(message)
        .context("Failed to parse JSON message")?;

    match parsed {
        // If it's already an array
        Value::Array(arr) => Ok(arr),
        // If it's an object with "data" field
        Value::Object(obj) => {
            if let Some(data_value) = obj.get("data") {
                match data_value {
                    // data.data is an array
                    Value::Array(arr) => Ok(arr.clone()),
                    // data.data is an object - check if it has nested "data" field
                    Value::Object(inner_obj) => {
                        if let Some(Value::Array(arr)) = inner_obj.get("data") {
                            // Handle {"data": {"data": [...]}}
                            Ok(arr.clone())
                        } else {
                            // data.data is an object without nested data - convert to single element array
                            Ok(vec![Value::Object(inner_obj.clone())])
                        }
                    }
                    // data.data is not an array or object - convert to single element array
                    other => Ok(vec![other.clone()]),
                }
            } else {
                // No "data" field - treat the whole object as single record
                Ok(vec![Value::Object(obj)])
            }
        }
        _ => anyhow::bail!("Unexpected JSON format"),
    }
}

/// Convert data array to rows (HashMap representation)
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

