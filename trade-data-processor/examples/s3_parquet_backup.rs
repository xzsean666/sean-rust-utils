//! Parquet files backup to S3 example
//! 
//! This example demonstrates how to backup parquet files to S3
//! after merging trading data.

use trade_data_processor::{S3Config, S3Helper};
use anyhow::{Context, Result};
use chrono::NaiveDate;
use std::path::{Path, PathBuf};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    println!("=== Parquet Backup to S3 Example ===\n");

    // Configuration
    let date = NaiveDate::from_ymd_opt(2025, 11, 10).unwrap();
    let local_output_dir = PathBuf::from("./data/merged/mark-price");
    
    // S3 Configuration (example with Backblaze B2)
    let s3_config = S3Config {
        provider: "b2".to_string(),
        bucket: "trading-data-backup".to_string(),
        access_key_id: std::env::var("B2_KEY_ID")
            .unwrap_or_else(|_| "your_b2_key_id".to_string()),
        secret_access_key: std::env::var("B2_APPLICATION_KEY")
            .unwrap_or_else(|_| "your_b2_application_key".to_string()),
        region: Some("us-west-002".to_string()),
        endpoint: Some("https://s3.us-west-002.backblazeb2.com".to_string()),
        force_path_style: Some(true),
        base_path: Some("mark-price/merged".to_string()),
    };

    // Create S3 helper
    println!("Creating S3 helper for {}...", s3_config.provider);
    let s3 = S3Helper::new(s3_config).await
        .context("Failed to create S3 helper")?;
    println!("✓ Connected to bucket: {}\n", s3.bucket());

    // Build the date path
    let year = date.format("%Y").to_string();
    let month = date.format("%m").to_string();
    let day = date.format("%d").to_string();
    
    let local_date_dir = local_output_dir.join(&year).join(&month).join(&day);
    
    println!("Local directory: {:?}", local_date_dir);
    
    // Check if local directory exists
    if !local_date_dir.exists() {
        println!("✗ Local directory does not exist!");
        println!("  This example expects parquet files to be in: {:?}", local_date_dir);
        println!("  You can modify this example to use a different directory.");
        return Ok(());
    }

    // Find all parquet files in the directory
    println!("\nScanning for parquet files...");
    let parquet_files = find_parquet_files(&local_date_dir)?;
    
    if parquet_files.is_empty() {
        println!("✗ No parquet files found in {:?}", local_date_dir);
        return Ok(());
    }

    println!("✓ Found {} parquet files\n", parquet_files.len());

    // Upload each file to S3
    println!("Starting backup to S3...");
    let mut uploaded = 0;
    let mut failed = 0;

    for local_file in &parquet_files {
        let file_name = local_file.file_name().unwrap().to_string_lossy();
        
        // Build S3 key: mark-price/merged/2025/11/10/BTCUSDT-mark-price-2025-11-10.parquet
        let s3_key = format!("{}/{}/{}/{}", year, month, day, file_name);
        
        print!("  Uploading {}... ", file_name);
        
        match s3.upload_file(local_file, &s3_key).await {
            Ok(etag) => {
                println!("✓ (ETag: {})", &etag[..8.min(etag.len())]);
                uploaded += 1;
                
                // Verify upload
                match s3.get_object_metadata(&s3_key).await {
                    Ok((size, _)) => {
                        let local_size = std::fs::metadata(local_file)?.len();
                        if size as u64 != local_size {
                            println!("    ⚠ Warning: Size mismatch! Local: {}, S3: {}", local_size, size);
                        }
                    }
                    Err(e) => {
                        println!("    ⚠ Warning: Could not verify upload: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("✗ Failed: {}", e);
                failed += 1;
            }
        }
    }

    println!();
    println!("=== Backup Summary ===");
    println!("Total files: {}", parquet_files.len());
    println!("Uploaded: {}", uploaded);
    println!("Failed: {}", failed);
    
    if failed == 0 {
        println!("\n✓ All files backed up successfully!");
    } else {
        println!("\n⚠ Some files failed to upload");
    }

    // List uploaded files
    println!("\n=== Verifying S3 contents ===");
    let s3_prefix = format!("{}/{}/{}", year, month, day);
    match s3.list_objects(&s3_prefix, None).await {
        Ok(objects) => {
            println!("Found {} objects in s3://{}/{}/", objects.len(), s3.bucket(), s3_prefix);
            for obj in objects.iter().take(5) {
                println!("  - {}", obj);
            }
            if objects.len() > 5 {
                println!("  ... and {} more", objects.len() - 5);
            }
        }
        Err(e) => {
            println!("✗ Failed to list S3 objects: {}", e);
        }
    }

    println!("\n=== Backup completed ===");
    Ok(())
}

/// Find all parquet files in a directory (non-recursive)
fn find_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "parquet" {
                    files.push(path);
                }
            }
        }
    }
    
    // Sort files by name for consistent ordering
    files.sort();
    
    Ok(files)
}

/// Alternative: Batch upload with parallel processing
#[allow(dead_code)]
async fn batch_upload_parquet_files(
    s3: &S3Helper,
    local_files: Vec<PathBuf>,
    date: NaiveDate,
) -> Result<usize> {
    let year = date.format("%Y").to_string();
    let month = date.format("%m").to_string();
    let day = date.format("%d").to_string();

    let files: Vec<_> = local_files
        .into_iter()
        .map(|local_file| {
            let file_name = local_file.file_name().unwrap().to_string_lossy().to_string();
            let s3_key = format!("{}/{}/{}/{}", year, month, day, file_name);
            (local_file, s3_key)
        })
        .collect();

    println!("Starting batch upload of {} files...", files.len());
    let uploaded = s3.upload_files_batch(files).await?;
    println!("Batch upload completed: {} files", uploaded);

    Ok(uploaded)
}

