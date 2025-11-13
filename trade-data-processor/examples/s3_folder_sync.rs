//! Example: Folder Sync with S3
//! 
//! This example demonstrates how to use the folder sync functionality
//! to synchronize local directories with S3 storage.
//!
//! Usage:
//! ```
//! cargo run --example s3_folder_sync
//! ```

use anyhow::Result;
use trade_data_processor::config::S3Config;
use trade_data_processor::s3_helper::{S3Helper, SyncOptions};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    // Configure S3 (adjust these values to match your setup)
    let s3_config = S3Config {
        provider: "aws".to_string(),
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID")
            .expect("AWS_ACCESS_KEY_ID not set"),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
            .expect("AWS_SECRET_ACCESS_KEY not set"),
        bucket: std::env::var("S3_BUCKET")
            .unwrap_or_else(|_| "my-test-bucket".to_string()),
        region: Some("us-east-1".to_string()),
        endpoint: None,
        force_path_style: None,
    };

    // Create S3 helper
    let s3_helper = S3Helper::new(s3_config).await?;

    // Local folder to sync
    let local_folder = "./data";
    
    // S3 prefix (folder path in bucket)
    let s3_prefix = "synced-data";
    
    // Database path for sync cache
    let db_path = "./.sync_cache";

    println!("\n=== Example 1: One-way sync (local -> S3) ===");
    
    // Configure sync options
    let mut options = SyncOptions::default();
    options.dry_run = true; // Set to false to actually sync
    
    println!("Syncing {} -> s3://{}/{}", local_folder, s3_helper.bucket(), s3_prefix);
    
    let stats = s3_helper
        .sync_folder(local_folder, s3_prefix, db_path, options.clone())
        .await?;
    
    println!("\nSync Statistics:");
    println!("  Files scanned: {}", stats.files_scanned);
    println!("  Files uploaded: {}", stats.files_uploaded);
    println!("  Files skipped: {}", stats.files_skipped);
    println!("  Files deleted: {}", stats.files_deleted);
    println!("  Bytes uploaded: {} KB", stats.bytes_uploaded / 1024);
    println!("  Errors: {}", stats.errors);

    println!("\n=== Example 2: Bidirectional sync (local <-> S3) ===");
    
    // Enable bidirectional sync
    options.dry_run = true; // Set to false to actually sync
    
    println!("Syncing {} <-> s3://{}/{}", local_folder, s3_helper.bucket(), s3_prefix);
    
    let stats = s3_helper
        .sync_folder_bidirectional(local_folder, s3_prefix, db_path, options.clone())
        .await?;
    
    println!("\nBidirectional Sync Statistics:");
    println!("  Files scanned: {}", stats.files_scanned);
    println!("  Files uploaded: {}", stats.files_uploaded);
    println!("  Files downloaded: {}", stats.files_downloaded);
    println!("  Files skipped: {}", stats.files_skipped);
    println!("  Bytes uploaded: {} KB", stats.bytes_uploaded / 1024);
    println!("  Bytes downloaded: {} KB", stats.bytes_downloaded / 1024);
    println!("  Errors: {}", stats.errors);

    println!("\n=== Example 3: Force sync (ignore cache) ===");
    
    // Force sync - re-check all files
    options.force = true;
    options.dry_run = true;
    
    let stats = s3_helper
        .sync_folder(local_folder, s3_prefix, db_path, options.clone())
        .await?;
    
    println!("\nForce Sync Statistics:");
    println!("  Files uploaded: {}", stats.files_uploaded);
    println!("  Errors: {}", stats.errors);

    println!("\n=== Example 4: Sync with deletion ===");
    
    // Enable deletion of files that don't exist in source
    options.force = false;
    options.delete = true;
    options.dry_run = true;
    
    let stats = s3_helper
        .sync_folder(local_folder, s3_prefix, db_path, options)
        .await?;
    
    println!("\nSync with Deletion Statistics:");
    println!("  Files uploaded: {}", stats.files_uploaded);
    println!("  Files deleted: {}", stats.files_deleted);
    println!("  Errors: {}", stats.errors);

    println!("\n✅ All examples completed!");
    println!("\nNote: Set dry_run = false in the code to perform actual syncing.");
    
    Ok(())
}

