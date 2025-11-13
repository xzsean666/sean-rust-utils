//! Example: Sync mark-price data to Cloudflare R2
//! 
//! This example demonstrates how to synchronize local mark-price data
//! to Cloudflare R2 storage (S3-compatible).
//!
//! Usage:
//! ```bash
//! # Using config file
//! cargo run --example r2_sync_mark_price -- --config config/r2.config.yaml
//! 
//! # Dry run mode (preview without uploading)
//! cargo run --example r2_sync_mark_price -- --config config/r2.config.yaml --dry-run
//! 
//! # Force sync all files
//! cargo run --example r2_sync_mark_price -- --config config/r2.config.yaml --force
//! 
//! # Enable zstd compression (saves storage space on S3)
//! cargo run --example r2_sync_mark_price -- --config config/r2.config.yaml --compress
//! ```

use anyhow::{Result, Context};
use clap::Parser;
use std::path::PathBuf;
use trade_data_processor::config::S3Config;
use trade_data_processor::s3_helper::{S3Helper, SyncOptions, SyncDirection};
use tracing_subscriber;
use serde::Deserialize;

/// R2 sync configuration
#[derive(Debug, Deserialize)]
struct R2SyncConfig {
    s3: S3Config,
}

/// CLI arguments
#[derive(Parser, Debug)]
#[command(name = "r2-sync")]
#[command(about = "Sync mark-price data to Cloudflare R2")]
struct Args {
    /// Path to the configuration YAML file
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,

    /// Dry run mode (no actual uploads)
    #[arg(long)]
    dry_run: bool,

    /// Force sync all files (ignore cache)
    #[arg(long)]
    force: bool,

    /// Delete remote files that don't exist locally
    #[arg(long)]
    delete: bool,

    /// Enable zstd compression for S3 storage (local files remain uncompressed)
    #[arg(long)]
    compress: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging with info level
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    println!("=== Cloudflare R2 Sync Example ===\n");

    // Parse CLI arguments
    let args = Args::parse();

    // Load configuration
    let config_content = std::fs::read_to_string(&args.config)
        .context(format!("Failed to read config file: {:?}", args.config))?;
    let config: R2SyncConfig = serde_yaml::from_str(&config_content)
        .context("Failed to parse config YAML")?;

    let endpoint = config.s3.endpoint.clone().unwrap_or_else(|| "N/A".to_string());
    let bucket = &config.s3.bucket;
    
    println!("Connecting to Cloudflare R2...");
    println!("  Endpoint: {}", endpoint);
    println!("  Bucket: {}", bucket);
    println!();

    // Create S3 helper (works with R2 since it's S3-compatible)
    let s3_helper = S3Helper::new(config.s3.clone()).await?;
    println!("✓ Successfully connected to R2\n");

    // Get paths from config with defaults
    let local_folder = config.s3.local_path.as_deref()
        .unwrap_or("./data/merged/mark-price/2025/11/11");
    let r2_prefix = config.s3.remote_prefix.as_deref()
        .unwrap_or("mark-price/2025/11/11");
    let db_path = config.s3.cache_db_path.as_deref()
        .unwrap_or("./.r2_sync_cache");

    // Parse sync direction from config
    let sync_direction = match config.s3.sync_direction.as_deref() {
        Some("local_to_s3") => SyncDirection::LocalToS3,
        Some("s3_to_local") => SyncDirection::S3ToLocal,
        Some("bidirectional") => SyncDirection::Bidirectional,
        _ => SyncDirection::LocalToS3, // Default
    };

    // Check if local folder exists (for local_to_s3 or bidirectional)
    if matches!(sync_direction, SyncDirection::LocalToS3 | SyncDirection::Bidirectional) {
        if !std::path::Path::new(local_folder).exists() {
            eprintln!("❌ Error: Local folder does not exist: {}", local_folder);
            eprintln!("Please ensure the folder exists before syncing.");
            return Ok(());
        }
    }

    // Configure sync options
    let mut options = SyncOptions::default();
    options.direction = sync_direction;
    options.dry_run = args.dry_run;
    options.force = args.force;
    options.delete = args.delete;
    options.use_compression = args.compress;
    
    if args.dry_run {
        println!("🔍 DRY RUN MODE - No files will be uploaded/downloaded");
        println!("   Remove --dry-run flag to perform actual sync\n");
    } else {
        println!("🚀 SYNC MODE - Files will be synced\n");
    }

    if args.force {
        println!("⚡ FORCE MODE - Will resync all files\n");
    }

    if args.delete {
        println!("🗑️  DELETE MODE - Will remove files not in source\n");
    }

    if args.compress {
        println!("📦 COMPRESSION MODE - Files will be compressed with zstd for S3 storage");
        println!("   Local files remain uncompressed, S3 files will have .zst extension\n");
    }

    // Display sync information based on direction
    match options.direction {
        SyncDirection::LocalToS3 => {
            println!("=== Syncing Local Directory to R2 (One-way: Local → R2) ===");
            println!("  Source: {}", local_folder);
            println!("  Destination: r2://{}/{}", bucket, r2_prefix);
        }
        SyncDirection::S3ToLocal => {
            println!("=== Syncing R2 to Local Directory (One-way: R2 → Local) ===");
            println!("  Source: r2://{}/{}", bucket, r2_prefix);
            println!("  Destination: {}", local_folder);
        }
        SyncDirection::Bidirectional => {
            println!("=== Syncing Bidirectionally (Local ↔ R2) ===");
            println!("  Local: {}", local_folder);
            println!("  R2: r2://{}/{}", bucket, r2_prefix);
        }
    }
    println!();

    // Perform the sync
    println!("Starting sync...\n");
    
    let stats = s3_helper
        .sync_folder(local_folder, r2_prefix, db_path, options.clone())
        .await?;

    // Display results
    println!("\n=== Sync Statistics ===");
    println!("  Files scanned:    {}", stats.files_scanned);
    
    if stats.files_uploaded > 0 {
        println!("  Files uploaded:   {}", stats.files_uploaded);
        println!("  Bytes uploaded:   {} KB ({:.2} MB)", 
            stats.bytes_uploaded / 1024, 
            stats.bytes_uploaded as f64 / 1024.0 / 1024.0
        );
    }
    
    if stats.files_downloaded > 0 {
        println!("  Files downloaded: {}", stats.files_downloaded);
        println!("  Bytes downloaded: {} KB ({:.2} MB)", 
            stats.bytes_downloaded / 1024, 
            stats.bytes_downloaded as f64 / 1024.0 / 1024.0
        );
    }
    
    println!("  Files skipped:    {}", stats.files_skipped);
    
    if stats.files_deleted > 0 {
        println!("  Files deleted:    {}", stats.files_deleted);
    }
    
    if stats.errors > 0 {
        println!("  ⚠️  Errors:       {}", stats.errors);
    }

    println!();

    if args.dry_run {
        println!("✅ Dry run completed successfully!");
        println!("   Remove --dry-run flag to perform actual sync:");
        println!("   cargo run --example r2_sync_mark_price -- --config {:?}", args.config);
    } else {
        println!("✅ Sync completed successfully!");
        match options.direction {
            SyncDirection::LocalToS3 => {
                println!("   {} files uploaded to R2", stats.files_uploaded);
            }
            SyncDirection::S3ToLocal => {
                println!("   {} files downloaded from R2", stats.files_downloaded);
            }
            SyncDirection::Bidirectional => {
                println!("   {} files uploaded, {} files downloaded", 
                    stats.files_uploaded, stats.files_downloaded);
            }
        }
    }

    // Optional: List uploaded files
    if !args.dry_run && stats.files_uploaded > 0 {
        println!("\n=== Verifying Upload ===");
        println!("Listing files in r2://{}/{}...", bucket, r2_prefix);
        
        match s3_helper.list_objects(r2_prefix, Some(10)).await {
            Ok(objects) => {
                println!("✓ Found {} objects (showing first 10):", objects.len());
                for obj in objects.iter().take(10) {
                    println!("  - {}", obj);
                }
                if objects.len() > 10 {
                    println!("  ... and {} more", objects.len() - 10);
                }
            }
            Err(e) => {
                eprintln!("⚠️  Failed to list objects: {}", e);
            }
        }
    }

    Ok(())
}

