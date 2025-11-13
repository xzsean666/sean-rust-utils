//! Basic S3 operations example
//! 
//! This example demonstrates basic S3 operations:
//! - Upload a file
//! - Download a file
//! - List objects
//! - Check if object exists
//! - Delete an object

use trade_data_processor::{S3Config, S3Helper};
use anyhow::Result;
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    println!("=== S3 Helper Basic Operations Example ===\n");

    // Create S3 configuration (example with MinIO)
    let config = S3Config {
        provider: "generic".to_string(),
        bucket: "test-bucket".to_string(),
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:9000".to_string()),
        force_path_style: Some(true),
        base_path: Some("examples".to_string()),
    };

    // Create S3 helper
    println!("Creating S3 helper...");
    let s3 = S3Helper::new(config).await?;
    println!("✓ S3 helper created for bucket: {}\n", s3.bucket());

    // Example 1: Upload bytes
    println!("1. Uploading bytes to S3...");
    let data = Bytes::from("Hello from S3 Helper! This is a test file.");
    let key = "examples/hello.txt";
    match s3.upload_bytes(data, key).await {
        Ok(etag) => println!("✓ Uploaded successfully. ETag: {}\n", etag),
        Err(e) => println!("✗ Upload failed: {}\n", e),
    }

    // Example 2: Check if object exists
    println!("2. Checking if object exists...");
    match s3.object_exists(key).await {
        Ok(true) => println!("✓ Object exists: {}\n", key),
        Ok(false) => println!("✗ Object does not exist: {}\n", key),
        Err(e) => println!("✗ Check failed: {}\n", e),
    }

    // Example 3: Get object metadata
    println!("3. Getting object metadata...");
    match s3.get_object_metadata(key).await {
        Ok((size, last_modified)) => {
            println!("✓ Object metadata:");
            println!("  - Size: {} bytes", size);
            println!("  - Last modified: {}\n", last_modified);
        }
        Err(e) => println!("✗ Failed to get metadata: {}\n", e),
    }

    // Example 4: Download bytes
    println!("4. Downloading bytes from S3...");
    match s3.download_bytes(key).await {
        Ok(bytes) => {
            let content = String::from_utf8_lossy(&bytes);
            println!("✓ Downloaded {} bytes", bytes.len());
            println!("  Content: {}\n", content);
        }
        Err(e) => println!("✗ Download failed: {}\n", e),
    }

    // Example 5: Upload more files for listing
    println!("5. Uploading multiple files...");
    let files = vec![
        ("examples/file1.txt", "Content of file 1"),
        ("examples/file2.txt", "Content of file 2"),
        ("examples/data/file3.txt", "Content of file 3"),
    ];

    for (key, content) in &files {
        let data = Bytes::from(*content);
        match s3.upload_bytes(data, key).await {
            Ok(_) => println!("✓ Uploaded: {}", key),
            Err(e) => println!("✗ Failed to upload {}: {}", key, e),
        }
    }
    println!();

    // Example 6: List objects with prefix
    println!("6. Listing objects with prefix 'examples/'...");
    match s3.list_objects("examples/", Some(10)).await {
        Ok(objects) => {
            println!("✓ Found {} objects:", objects.len());
            for obj in objects {
                println!("  - {}", obj);
            }
            println!();
        }
        Err(e) => println!("✗ List failed: {}\n", e),
    }

    // Example 7: Copy object
    println!("7. Copying object...");
    let source_key = "examples/hello.txt";
    let dest_key = "examples/backup/hello.txt";
    match s3.copy_object(source_key, dest_key).await {
        Ok(_) => println!("✓ Copied {} to {}\n", source_key, dest_key),
        Err(e) => println!("✗ Copy failed: {}\n", e),
    }

    // Example 8: Delete object
    println!("8. Deleting object...");
    let key_to_delete = "examples/file1.txt";
    match s3.delete_object(key_to_delete).await {
        Ok(_) => println!("✓ Deleted: {}\n", key_to_delete),
        Err(e) => println!("✗ Delete failed: {}\n", e),
    }

    // Example 9: Batch delete
    println!("9. Batch deleting objects...");
    let keys_to_delete = vec![
        "examples/file2.txt".to_string(),
        "examples/data/file3.txt".to_string(),
        "examples/hello.txt".to_string(),
        "examples/backup/hello.txt".to_string(),
    ];

    match s3.delete_objects_batch(keys_to_delete.clone()).await {
        Ok(count) => println!("✓ Deleted {} objects\n", count),
        Err(e) => println!("✗ Batch delete failed: {}\n", e),
    }

    // Final listing
    println!("10. Final listing (should be empty or minimal)...");
    match s3.list_objects("examples/", None).await {
        Ok(objects) => {
            if objects.is_empty() {
                println!("✓ No objects found (cleaned up successfully)\n");
            } else {
                println!("✓ Remaining objects:");
                for obj in objects {
                    println!("  - {}", obj);
                }
                println!();
            }
        }
        Err(e) => println!("✗ List failed: {}\n", e),
    }

    println!("=== Example completed ===");
    Ok(())
}

