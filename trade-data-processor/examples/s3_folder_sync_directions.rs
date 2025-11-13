//! Example: S3 Folder Sync with Direction Control
//!
//! Demonstrates how to sync folders between local and S3 with different directions:
//! - Local to S3 (one-way)
//! - S3 to Local (one-way)
//! - Bidirectional
//!
//! Run with: cargo run --example s3_folder_sync_directions

use anyhow::Result;
use trade_data_processor::{S3Config, S3Helper, SyncDirection, SyncOptions};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    // Configure S3 connection
    let s3_config = S3Config {
        provider: "aws".to_string(), // or "b2", "r2", etc.
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
        bucket: "my-backup-bucket".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: None, // Use default AWS endpoint
        force_path_style: None,
    };
    
    // Create S3 helper
    let s3_helper = S3Helper::new(s3_config).await?;
    
    // Local folder and S3 prefix
    let local_folder = PathBuf::from("./data/my-folder");
    let s3_prefix = "backups/my-folder";
    let db_path = PathBuf::from("./.sync-db");
    
    println!("\n=== Example 1: Sync Local -> S3 (one-way) ===\n");
    let mut options = SyncOptions {
        direction: SyncDirection::LocalToS3,
        force: false,
        delete: true, // Delete files in S3 that don't exist locally
        dry_run: false,
        exclude_patterns: vec![
            ".git".to_string(),
            "*.tmp".to_string(),
            "node_modules".to_string(),
        ],
        max_parallel: 4,
    };
    
    let stats = s3_helper.sync_folder(
        &local_folder,
        s3_prefix,
        &db_path,
        options.clone(),
    ).await?;
    
    println!("✓ Sync completed:");
    println!("  - Files scanned: {}", stats.files_scanned);
    println!("  - Files uploaded: {}", stats.files_uploaded);
    println!("  - Files skipped: {}", stats.files_skipped);
    println!("  - Files deleted: {}", stats.files_deleted);
    println!("  - Bytes uploaded: {}", stats.bytes_uploaded);
    println!("  - Errors: {}", stats.errors);
    
    println!("\n=== Example 2: Sync S3 -> Local (one-way) ===\n");
    options.direction = SyncDirection::S3ToLocal;
    options.delete = false; // Don't delete local files
    
    let stats = s3_helper.sync_folder(
        &local_folder,
        s3_prefix,
        &db_path,
        options.clone(),
    ).await?;
    
    println!("✓ Sync completed:");
    println!("  - Files scanned: {}", stats.files_scanned);
    println!("  - Files downloaded: {}", stats.files_downloaded);
    println!("  - Files skipped: {}", stats.files_skipped);
    println!("  - Files deleted: {}", stats.files_deleted);
    println!("  - Bytes downloaded: {}", stats.bytes_downloaded);
    println!("  - Errors: {}", stats.errors);
    
    println!("\n=== Example 3: Bidirectional Sync ===\n");
    options.direction = SyncDirection::Bidirectional;
    options.delete = true;
    
    let stats = s3_helper.sync_folder(
        &local_folder,
        s3_prefix,
        &db_path,
        options,
    ).await?;
    
    println!("✓ Sync completed:");
    println!("  - Files scanned: {}", stats.files_scanned);
    println!("  - Files uploaded: {}", stats.files_uploaded);
    println!("  - Files downloaded: {}", stats.files_downloaded);
    println!("  - Files skipped: {}", stats.files_skipped);
    println!("  - Files deleted: {}", stats.files_deleted);
    println!("  - Bytes uploaded: {}", stats.bytes_uploaded);
    println!("  - Bytes downloaded: {}", stats.bytes_downloaded);
    println!("  - Errors: {}", stats.errors);
    
    println!("\n=== Example 4: Dry Run (preview changes) ===\n");
    let dry_run_options = SyncOptions {
        direction: SyncDirection::LocalToS3,
        force: false,
        delete: true,
        dry_run: true, // Just show what would be done
        exclude_patterns: vec![".git".to_string()],
        max_parallel: 4,
    };
    
    let stats = s3_helper.sync_folder(
        &local_folder,
        s3_prefix,
        &db_path,
        dry_run_options,
    ).await?;
    
    println!("✓ Dry run completed (no actual changes made):");
    println!("  - Would upload: {}", stats.files_uploaded);
    println!("  - Would delete: {}", stats.files_deleted);
    
    Ok(())
}

