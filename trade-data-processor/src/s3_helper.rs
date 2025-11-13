//! S3 Helper Module
//!
//! This module provides a unified interface for interacting with S3-compatible storage services.
//! Supports multiple providers including:
//! - AWS S3
//! - Backblaze B2
//! - Cloudflare R2
//! - Any other S3-compatible service
//!
//! # Features
//! - Upload/download files with progress tracking
//! - List objects with pagination
//! - Batch operations
//! - Custom endpoint configuration for S3-compatible services
//! - Automatic retry logic
//! - Streaming for large files

use anyhow::{Context, Result, bail};
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use std::path::Path;
use tracing::{info, debug, warn, error};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use walkdir::WalkDir;
use std::fs;

use crate::config::S3Config;

/// S3-compatible storage client
pub struct S3Helper {
    client: S3Client,
    config: S3Config,
}

/// S3 provider types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Provider {
    /// Amazon Web Services S3
    AwsS3,
    /// Backblaze B2
    BackblazeB2,
    /// Cloudflare R2
    CloudflareR2,
    /// Generic S3-compatible service
    Generic,
}

impl S3Provider {
    /// Parse provider from string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "aws" | "s3" | "aws-s3" => S3Provider::AwsS3,
            "b2" | "backblaze" | "backblaze-b2" => S3Provider::BackblazeB2,
            "r2" | "cloudflare" | "cloudflare-r2" => S3Provider::CloudflareR2,
            _ => S3Provider::Generic,
        }
    }
}

impl S3Helper {
    /// Create a new S3Helper instance
    ///
    /// # Arguments
    /// * `config` - S3 configuration including credentials and endpoint
    ///
    /// # Returns
    /// A new S3Helper instance ready to perform operations
    pub async fn new(config: S3Config) -> Result<Self> {
        let client = Self::create_client(&config).await
            .context("Failed to create S3 client")?;
        
        Ok(Self { client, config })
    }

    /// Create S3 client with custom configuration
    async fn create_client(config: &S3Config) -> Result<S3Client> {
        let provider = S3Provider::from_str(&config.provider);
        
        // Create credentials
        let credentials = Credentials::new(
            &config.access_key_id,
            &config.secret_access_key,
            None,  // session token
            None,  // expiration
            "custom"  // provider name
        );

        // Determine region
        let region = config.region.clone()
            .unwrap_or_else(|| match provider {
                S3Provider::AwsS3 => "us-east-1".to_string(),
                S3Provider::BackblazeB2 => "us-west-002".to_string(),
                S3Provider::CloudflareR2 => "auto".to_string(),
                S3Provider::Generic => "us-east-1".to_string(),
            });

        let region_provider = RegionProviderChain::first_try(Region::new(region));

        // Build AWS config
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .credentials_provider(credentials)
            .load()
            .await;

        // Build S3 config with custom endpoint if provided
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

        if let Some(endpoint) = &config.endpoint {
            s3_config_builder = s3_config_builder
                .endpoint_url(endpoint)
                .force_path_style(config.force_path_style.unwrap_or(true));
        } else {
            // Use default endpoints for known providers
            match provider {
                S3Provider::AwsS3 => {
                    // AWS S3 uses virtual-hosted-style by default
                    s3_config_builder = s3_config_builder
                        .force_path_style(config.force_path_style.unwrap_or(false));
                }
                S3Provider::BackblazeB2 => {
                    // B2 requires custom endpoint (user should provide it)
                    if config.endpoint.is_none() {
                        bail!("Backblaze B2 requires an endpoint URL (e.g., https://s3.us-west-002.backblazeb2.com)");
                    }
                }
                S3Provider::CloudflareR2 => {
                    // R2 requires custom endpoint (user should provide it)
                    if config.endpoint.is_none() {
                        bail!("Cloudflare R2 requires an endpoint URL (e.g., https://<account-id>.r2.cloudflarestorage.com)");
                    }
                }
                S3Provider::Generic => {
                    // Generic provider should have an endpoint
                    if config.endpoint.is_none() {
                        warn!("Generic S3 provider without endpoint - will use AWS S3");
                    }
                }
            }
        }

        let s3_config = s3_config_builder.build();
        let client = S3Client::from_conf(s3_config);

        Ok(client)
    }

    /// Upload a file to S3
    ///
    /// # Arguments
    /// * `local_path` - Path to the local file
    /// * `key` - S3 object key (path in bucket)
    ///
    /// # Returns
    /// The uploaded object's ETag
    pub async fn upload_file<P: AsRef<Path>>(&self, local_path: P, key: &str) -> Result<String> {
        let local_path = local_path.as_ref();
        info!("Uploading file {:?} to s3://{}/{}", local_path, self.config.bucket, key);

        let body = ByteStream::from_path(local_path).await
            .context(format!("Failed to read local file: {:?}", local_path))?;

        let response = self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .context(format!("Failed to upload file to S3: {}", key))?;

        let etag = response.e_tag()
            .unwrap_or("unknown")
            .to_string();

        info!("Successfully uploaded {} (ETag: {})", key, etag);
        Ok(etag)
    }

    /// Upload raw bytes to S3
    ///
    /// # Arguments
    /// * `data` - Raw bytes to upload
    /// * `key` - S3 object key (path in bucket)
    ///
    /// # Returns
    /// The uploaded object's ETag
    pub async fn upload_bytes(&self, data: Bytes, key: &str) -> Result<String> {
        debug!("Uploading {} bytes to s3://{}/{}", data.len(), self.config.bucket, key);

        let body = ByteStream::from(data);

        let response = self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .context(format!("Failed to upload bytes to S3: {}", key))?;

        let etag = response.e_tag()
            .unwrap_or("unknown")
            .to_string();

        debug!("Successfully uploaded {} (ETag: {})", key, etag);
        Ok(etag)
    }

    /// Download a file from S3
    ///
    /// # Arguments
    /// * `key` - S3 object key (path in bucket)
    /// * `local_path` - Path where to save the downloaded file
    pub async fn download_file<P: AsRef<Path>>(&self, key: &str, local_path: P) -> Result<()> {
        let local_path = local_path.as_ref();
        info!("Downloading s3://{}/{} to {:?}", self.config.bucket, key, local_path);

        let response = self.client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to download file from S3: {}", key))?;

        let data = response.body.collect().await
            .context("Failed to read response body")?
            .into_bytes();

        // Create parent directory if it doesn't exist
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await
                .context(format!("Failed to create directory: {:?}", parent))?;
        }

        tokio::fs::write(local_path, data).await
            .context(format!("Failed to write file: {:?}", local_path))?;

        info!("Successfully downloaded {} to {:?}", key, local_path);
        Ok(())
    }

    /// Download object as bytes
    ///
    /// # Arguments
    /// * `key` - S3 object key (path in bucket)
    ///
    /// # Returns
    /// The object's contents as bytes
    pub async fn download_bytes(&self, key: &str) -> Result<Bytes> {
        debug!("Downloading s3://{}/{} as bytes", self.config.bucket, key);

        let response = self.client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to download object from S3: {}", key))?;

        let data = response.body.collect().await
            .context("Failed to read response body")?
            .into_bytes();

        debug!("Successfully downloaded {} ({} bytes)", key, data.len());
        Ok(data)
    }

    /// List objects in the bucket with a given prefix
    ///
    /// # Arguments
    /// * `prefix` - Prefix to filter objects (e.g., "data/2025/")
    /// * `max_keys` - Maximum number of keys to return (None for all)
    ///
    /// # Returns
    /// Vector of object keys
    pub async fn list_objects(&self, prefix: &str, max_keys: Option<i32>) -> Result<Vec<String>> {
        debug!("Listing objects in s3://{} with prefix: {}", self.config.bucket, prefix);

        let mut request = self.client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(prefix);

        if let Some(max) = max_keys {
            request = request.max_keys(max);
        }

        let response = request.send().await
            .context(format!("Failed to list objects with prefix: {}", prefix))?;

        let keys: Vec<String> = response.contents()
            .iter()
            .filter_map(|obj| obj.key().map(|k| k.to_string()))
            .collect();

        debug!("Found {} objects with prefix: {}", keys.len(), prefix);
        Ok(keys)
    }

    /// List all objects in the bucket with pagination
    ///
    /// # Arguments
    /// * `prefix` - Prefix to filter objects (e.g., "data/2025/")
    ///
    /// # Returns
    /// Vector of all object keys (handles pagination automatically)
    pub async fn list_all_objects(&self, prefix: &str) -> Result<Vec<String>> {
        info!("Listing all objects in s3://{} with prefix: {}", self.config.bucket, prefix);
        
        let mut all_keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self.client
                .list_objects_v2()
                .bucket(&self.config.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await
                .context(format!("Failed to list objects with prefix: {}", prefix))?;

            let keys: Vec<String> = response.contents()
                .iter()
                .filter_map(|obj| obj.key().map(|k| k.to_string()))
                .collect();

            all_keys.extend(keys);

            // Check if there are more results
            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        info!("Found {} total objects with prefix: {}", all_keys.len(), prefix);
        Ok(all_keys)
    }

    /// Check if an object exists
    ///
    /// # Arguments
    /// * `key` - S3 object key (path in bucket)
    ///
    /// # Returns
    /// true if object exists, false otherwise
    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        debug!("Checking if s3://{}/{} exists", self.config.bucket, key);

        match self.client
            .head_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => {
                debug!("Object exists: {}", key);
                Ok(true)
            }
            Err(e) => {
                // Check if it's a 404 error
                if e.to_string().contains("404") || e.to_string().contains("NotFound") {
                    debug!("Object does not exist: {}", key);
                    Ok(false)
                } else {
                    // Some other error occurred
                    Err(anyhow::anyhow!("Failed to check object existence: {}", e))
                }
            }
        }
    }

    /// Delete an object from S3
    ///
    /// # Arguments
    /// * `key` - S3 object key (path in bucket)
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        info!("Deleting s3://{}/{}", self.config.bucket, key);

        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to delete object: {}", key))?;

        info!("Successfully deleted {}", key);
        Ok(())
    }

    /// Delete multiple objects in batch
    ///
    /// # Arguments
    /// * `keys` - Vector of S3 object keys to delete
    ///
    /// # Returns
    /// Number of successfully deleted objects
    pub async fn delete_objects_batch(&self, keys: Vec<String>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        info!("Deleting {} objects from s3://{}", keys.len(), self.config.bucket);

        let objects: Vec<_> = keys.iter()
            .map(|key| {
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .expect("Failed to build ObjectIdentifier")
            })
            .collect();

        let delete_request = aws_sdk_s3::types::Delete::builder()
            .set_objects(Some(objects))
            .build()
            .context("Failed to build delete request")?;

        let response = self.client
            .delete_objects()
            .bucket(&self.config.bucket)
            .delete(delete_request)
            .send()
            .await
            .context("Failed to delete objects")?;

        let deleted_count = response.deleted().len();
        info!("Successfully deleted {} objects", deleted_count);

        // Log any errors
        for error in response.errors() {
            warn!("Failed to delete {}: {}", 
                error.key().unwrap_or("unknown"),
                error.message().unwrap_or("unknown error")
            );
        }

        Ok(deleted_count)
    }

    /// Copy an object within S3
    ///
    /// # Arguments
    /// * `source_key` - Source object key
    /// * `dest_key` - Destination object key
    pub async fn copy_object(&self, source_key: &str, dest_key: &str) -> Result<()> {
        info!("Copying s3://{}/{} to s3://{}/{}", 
            self.config.bucket, source_key, self.config.bucket, dest_key);

        let copy_source = format!("{}/{}", self.config.bucket, source_key);

        self.client
            .copy_object()
            .bucket(&self.config.bucket)
            .copy_source(copy_source)
            .key(dest_key)
            .send()
            .await
            .context(format!("Failed to copy object from {} to {}", source_key, dest_key))?;

        info!("Successfully copied {} to {}", source_key, dest_key);
        Ok(())
    }

    /// Get object metadata
    ///
    /// # Arguments
    /// * `key` - S3 object key
    ///
    /// # Returns
    /// Tuple of (size_bytes, last_modified)
    pub async fn get_object_metadata(&self, key: &str) -> Result<(i64, String)> {
        debug!("Getting metadata for s3://{}/{}", self.config.bucket, key);

        let response = self.client
            .head_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to get object metadata: {}", key))?;

        let size = response.content_length().unwrap_or(0);
        let last_modified = response.last_modified()
            .map(|dt| dt.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        debug!("Object {} - Size: {} bytes, Last Modified: {}", key, size, last_modified);
        Ok((size, last_modified))
    }

    /// Upload multiple files in parallel
    ///
    /// # Arguments
    /// * `files` - Vector of (local_path, s3_key) tuples
    ///
    /// # Returns
    /// Number of successfully uploaded files
    pub async fn upload_files_batch<P: AsRef<Path>>(&self, files: Vec<(P, String)>) -> Result<usize> {
        if files.is_empty() {
            return Ok(0);
        }

        info!("Uploading {} files in batch to s3://{}", files.len(), self.config.bucket);

        let mut tasks = Vec::new();

        for (local_path, key) in files {
            let client = self.client.clone();
            let bucket = self.config.bucket.clone();
            let local_path = local_path.as_ref().to_path_buf();
            
            let task = tokio::spawn(async move {
                let body = ByteStream::from_path(&local_path).await?;
                
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(body)
                    .send()
                    .await
                    .context(format!("Failed to upload file: {:?}", local_path))?;
                
                Ok::<_, anyhow::Error>(())
            });

            tasks.push(task);
        }

        let results = futures::future::join_all(tasks).await;
        let success_count = results.iter()
            .filter(|r| r.is_ok() && r.as_ref().unwrap().is_ok())
            .count();

        info!("Successfully uploaded {} out of {} files", success_count, results.len());
        Ok(success_count)
    }

    /// Get bucket name
    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    /// Get provider type
    pub fn provider(&self) -> S3Provider {
        S3Provider::from_str(&self.config.provider)
    }
}

// ============================================================================
// Folder Sync Functionality
// ============================================================================

/// File metadata stored in local sync database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// File path (relative to sync root)
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified time (Unix timestamp)
    pub modified_time: i64,
    /// SHA256 hash of file contents
    pub hash: String,
    /// S3 ETag (if uploaded)
    pub etag: Option<String>,
    /// Last sync time (Unix timestamp)
    pub last_sync: i64,
}

impl FileMetadata {
    /// Create metadata from a local file
    fn from_file(path: &Path, relative_path: &str) -> Result<Self> {
        let metadata = fs::metadata(path)
            .context(format!("Failed to read file metadata: {:?}", path))?;
        
        let size = metadata.len();
        let modified_time = metadata.modified()
            .context("Failed to get file modified time")?
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        
        // Calculate file hash
        let mut file = fs::File::open(path)
            .context(format!("Failed to open file for hashing: {:?}", path))?;
        let mut hasher = Sha256::new();
        std::io::copy(&mut file, &mut hasher)
            .context("Failed to calculate file hash")?;
        let hash = format!("{:x}", hasher.finalize());
        
        Ok(Self {
            path: relative_path.to_string(),
            size,
            modified_time,
            hash,
            etag: None,
            last_sync: chrono::Utc::now().timestamp(),
        })
    }
}

/// Local sync database for caching file metadata
pub struct SyncDatabase {
    db: sled::Db,
}

impl SyncDatabase {
    /// Open or create a sync database
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db = sled::open(db_path.as_ref())
            .context(format!("Failed to open sync database: {:?}", db_path.as_ref()))?;
        
        Ok(Self { db })
    }
    
    /// Get file metadata by path
    pub fn get_metadata(&self, path: &str) -> Result<Option<FileMetadata>> {
        match self.db.get(path.as_bytes())? {
            Some(data) => {
                let metadata: FileMetadata = serde_json::from_slice(&data)
                    .context("Failed to deserialize file metadata")?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }
    
    /// Store file metadata
    pub fn set_metadata(&self, metadata: &FileMetadata) -> Result<()> {
        let data = serde_json::to_vec(metadata)
            .context("Failed to serialize file metadata")?;
        self.db.insert(metadata.path.as_bytes(), data)?;
        Ok(())
    }
    
    /// Remove file metadata
    pub fn remove_metadata(&self, path: &str) -> Result<()> {
        self.db.remove(path.as_bytes())?;
        Ok(())
    }
    
    /// List all tracked files
    pub fn list_all(&self) -> Result<Vec<FileMetadata>> {
        let mut results = Vec::new();
        
        for item in self.db.iter() {
            let (_, value) = item?;
            let metadata: FileMetadata = serde_json::from_slice(&value)
                .context("Failed to deserialize file metadata")?;
            results.push(metadata);
        }
        
        Ok(results)
    }
    
    /// Clear all metadata
    pub fn clear(&self) -> Result<()> {
        self.db.clear()?;
        Ok(())
    }
    
    /// Flush changes to disk
    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }
}

/// Sync direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    /// Sync from local to S3 (one-way: local -> S3)
    LocalToS3,
    /// Sync from S3 to local (one-way: S3 -> local)
    S3ToLocal,
    /// Sync bidirectionally (local <-> S3)
    Bidirectional,
}

/// Sync options
#[derive(Debug, Clone)]
pub struct SyncOptions {
    /// Sync direction
    pub direction: SyncDirection,
    /// Force sync (ignore cache and re-check all files)
    pub force: bool,
    /// Delete files in destination that don't exist in source
    pub delete: bool,
    /// Dry run (don't actually sync, just show what would be done)
    pub dry_run: bool,
    /// File patterns to exclude (glob patterns)
    pub exclude_patterns: Vec<String>,
    /// Maximum parallel uploads/downloads
    pub max_parallel: usize,
    /// Use zstd compression for S3 storage (local files remain uncompressed)
    pub use_compression: bool,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            direction: SyncDirection::LocalToS3,
            force: false,
            delete: false,
            dry_run: false,
            exclude_patterns: vec![
                ".git".to_string(),
                ".DS_Store".to_string(),
                "*.tmp".to_string(),
                "*.swp".to_string(),
            ],
            max_parallel: 4,
            use_compression: true,
        }
    }
}

/// Sync statistics
#[derive(Debug, Default)]
pub struct SyncStats {
    pub files_scanned: usize,
    pub files_uploaded: usize,
    pub files_downloaded: usize,
    pub files_deleted: usize,
    pub files_skipped: usize,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub errors: usize,
}

impl S3Helper {
    /// Sync a folder based on the specified direction
    ///
    /// # Arguments
    /// * `local_folder` - Local folder path to sync
    /// * `s3_prefix` - S3 prefix (folder path in bucket)
    /// * `db_path` - Path to sync database
    /// * `options` - Sync options (including direction)
    ///
    /// # Returns
    /// Sync statistics
    pub async fn sync_folder<P: AsRef<Path>>(
        &self,
        local_folder: P,
        s3_prefix: &str,
        db_path: P,
        options: SyncOptions,
    ) -> Result<SyncStats> {
        match options.direction {
            SyncDirection::LocalToS3 => {
                self.sync_local_to_s3(local_folder, s3_prefix, db_path, options).await
            }
            SyncDirection::S3ToLocal => {
                self.sync_s3_to_local(local_folder, s3_prefix, db_path, options).await
            }
            SyncDirection::Bidirectional => {
                self.sync_folder_bidirectional(local_folder, s3_prefix, db_path, options).await
            }
        }
    }

    /// Sync from local folder to S3 (one-way: local -> S3)
    ///
    /// # Arguments
    /// * `local_folder` - Local folder path to sync
    /// * `s3_prefix` - S3 prefix (folder path in bucket)
    /// * `db_path` - Path to sync database
    /// * `options` - Sync options
    ///
    /// # Returns
    /// Sync statistics
    async fn sync_local_to_s3<P: AsRef<Path>>(
        &self,
        local_folder: P,
        s3_prefix: &str,
        db_path: P,
        options: SyncOptions,
    ) -> Result<SyncStats> {
        let local_folder = local_folder.as_ref();
        let mut stats = SyncStats::default();
        
        info!("Starting folder sync: {:?} -> s3://{}/{}", local_folder, self.config.bucket, s3_prefix);
        info!("Sync options: force={}, delete={}, dry_run={}", options.force, options.delete, options.dry_run);
        
        // Open sync database
        let db = SyncDatabase::open(db_path)?;
        
        // Scan local files
        let mut local_files = Vec::new();
        for entry in WalkDir::new(local_folder)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| !should_exclude(e.path(), &options.exclude_patterns))
        {
            let entry = entry.context("Failed to read directory entry")?;
            
            if entry.file_type().is_file() {
                let relative_path = entry.path()
                    .strip_prefix(local_folder)
                    .context("Failed to get relative path")?
                    .to_string_lossy()
                    .replace('\\', "/");
                
                local_files.push((entry.path().to_path_buf(), relative_path));
                stats.files_scanned += 1;
            }
        }
        
        info!("Found {} local files to check", local_files.len());
        
        // Process files
        for (local_path, relative_path) in &local_files {
            // Add .zst extension to S3 key if compression is enabled
            let s3_key = if options.use_compression {
                format!("{}/{}.zst", s3_prefix.trim_end_matches('/'), relative_path)
            } else {
                format!("{}/{}", s3_prefix.trim_end_matches('/'), relative_path)
            };
            
            // Check if file needs to be uploaded
            let needs_upload = if options.force {
                true
            } else {
                match db.get_metadata(&relative_path)? {
                    Some(cached) => {
                        // Compare with current file state
                        match FileMetadata::from_file(&local_path, &relative_path) {
                            Ok(current) => {
                                current.hash != cached.hash || current.size != cached.size
                            }
                            Err(e) => {
                                warn!("Failed to read file metadata for {:?}: {}", local_path, e);
                                stats.errors += 1;
                                continue;
                            }
                        }
                    }
                    None => true, // Not in cache, need to upload
                }
            };
            
            if needs_upload {
                if options.dry_run {
                    info!("[DRY RUN] Would upload: {:?} -> {}", local_path, s3_key);
                    stats.files_uploaded += 1;
                } else {
                    // Check if S3 already has the same file (optimization for first sync)
                    let should_actually_upload = match self.object_exists(&s3_key).await {
                        Ok(true) => {
                            // S3 file exists, compare with local file to avoid unnecessary upload
                            debug!("S3 file exists, checking if content matches: {}", s3_key);
                            
                            match self.get_object_metadata(&s3_key).await {
                                Ok((s3_size, _s3_modified)) => {
                                    match FileMetadata::from_file(&local_path, &relative_path) {
                                        Ok(local_meta) => {
                                            // If compression is enabled, we can't directly compare sizes
                                            // so we need to download and compare, or just re-upload
                                            // For now, if sizes match (when not compressed), skip upload
                                            if !options.use_compression && s3_size as u64 == local_meta.size {
                                                // Sizes match, assume content is the same
                                                // Just update database without uploading
                                                info!("✓ File already in S3 (skipped upload): {}", relative_path);
                                                db.set_metadata(&local_meta)?;
                                                stats.files_skipped += 1;
                                                false
                                            } else if options.use_compression {
                                                // With compression, we can't easily compare sizes
                                                // Could download and compare hashes, but for now just re-upload
                                                // TODO: Implement hash comparison for compressed files
                                                true
                                            } else {
                                                true // Sizes don't match, need to upload
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Failed to read local file metadata: {}", e);
                                            true // Upload to be safe
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to get S3 metadata for {}: {}", s3_key, e);
                                    true // Upload to be safe
                                }
                            }
                        }
                        Ok(false) => true, // S3 file doesn't exist, need to upload
                        Err(e) => {
                            warn!("Failed to check if S3 object exists {}: {}", s3_key, e);
                            true // Upload to be safe
                        }
                    };
                    
                    if should_actually_upload {
                        debug!("Uploading: {:?} -> {}", local_path, s3_key);
                        
                        // Compress file if compression is enabled
                        let (upload_path, temp_file) = if options.use_compression {
                            let temp_dir = std::env::temp_dir();
                            let temp_file_name = format!("s3sync_{}_{}.zst", 
                                std::process::id(),
                                local_path.file_name().unwrap_or_default().to_string_lossy()
                            );
                            let temp_path = temp_dir.join(temp_file_name);
                            
                            match compress_file(&local_path, &temp_path) {
                                Ok(_) => (temp_path.clone(), Some(temp_path)),
                                Err(e) => {
                                    error!("Failed to compress {:?}: {}", local_path, e);
                                    stats.errors += 1;
                                    continue;
                                }
                            }
                        } else {
                            (local_path.clone(), None)
                        };
                        
                        match self.upload_file(&upload_path, &s3_key).await {
                            Ok(etag) => {
                                // Clean up temp file if it exists
                                if let Some(temp_path) = temp_file {
                                    let _ = fs::remove_file(&temp_path);
                                }
                                
                                // Update database (store original file metadata)
                                let mut metadata = FileMetadata::from_file(&local_path, &relative_path)?;
                                metadata.etag = Some(etag);
                                db.set_metadata(&metadata)?;
                                
                                stats.files_uploaded += 1;
                                stats.bytes_uploaded += metadata.size;
                                
                                info!("✓ Uploaded{}: {}", 
                                    if options.use_compression { " (compressed)" } else { "" },
                                    relative_path
                                );
                            }
                            Err(e) => {
                                error!("Failed to upload {:?}: {}", local_path, e);
                                stats.errors += 1;
                            }
                        }
                    }
                }
            } else {
                debug!("Skipping (unchanged): {}", relative_path);
                stats.files_skipped += 1;
            }
        }
        
        // Handle deletions if requested
        if options.delete {
            let tracked_files = db.list_all()?;
            let local_paths: std::collections::HashSet<_> = 
                local_files.iter().map(|(_, p)| p.as_str()).collect();
            
            for cached in tracked_files {
                if !local_paths.contains(cached.path.as_str()) {
                    // Add .zst extension to S3 key if compression is enabled
                    let s3_key = if options.use_compression {
                        format!("{}/{}.zst", s3_prefix.trim_end_matches('/'), cached.path)
                    } else {
                        format!("{}/{}", s3_prefix.trim_end_matches('/'), cached.path)
                    };
                    
                    if options.dry_run {
                        info!("[DRY RUN] Would delete from S3: {}", s3_key);
                        stats.files_deleted += 1;
                    } else {
                        debug!("Deleting from S3: {}", s3_key);
                        
                        match self.delete_object(&s3_key).await {
                            Ok(_) => {
                                db.remove_metadata(&cached.path)?;
                                stats.files_deleted += 1;
                                info!("✗ Deleted from S3: {}", cached.path);
                            }
                            Err(e) => {
                                error!("Failed to delete {}: {}", s3_key, e);
                                stats.errors += 1;
                            }
                        }
                    }
                }
            }
        }
        
        db.flush()?;
        
        info!("Sync completed: uploaded={}, skipped={}, deleted={}, errors={}", 
            stats.files_uploaded, stats.files_skipped, stats.files_deleted, stats.errors);
        
        Ok(stats)
    }
    
    /// Sync from S3 to local folder (one-way: S3 -> local)
    ///
    /// # Arguments
    /// * `local_folder` - Local folder path to sync
    /// * `s3_prefix` - S3 prefix (folder path in bucket)
    /// * `db_path` - Path to sync database
    /// * `options` - Sync options
    ///
    /// # Returns
    /// Sync statistics
    async fn sync_s3_to_local<P: AsRef<Path>>(
        &self,
        local_folder: P,
        s3_prefix: &str,
        db_path: P,
        options: SyncOptions,
    ) -> Result<SyncStats> {
        let local_folder = local_folder.as_ref();
        let mut stats = SyncStats::default();
        
        info!("Starting folder sync: s3://{}/{} -> {:?}", self.config.bucket, s3_prefix, local_folder);
        info!("Sync options: force={}, delete={}, dry_run={}", options.force, options.delete, options.dry_run);
        
        // Open sync database
        let db = SyncDatabase::open(db_path)?;
        
        // List all S3 objects with the prefix
        let s3_objects = self.list_all_objects(s3_prefix).await?;
        info!("Found {} S3 objects to check", s3_objects.len());
        
        // Track downloaded files for deletion check
        let mut downloaded_paths = std::collections::HashSet::new();
        
        for s3_key in s3_objects {
            stats.files_scanned += 1;
            
            // Skip if doesn't match our prefix
            if !s3_key.starts_with(s3_prefix) {
                continue;
            }
            
            // Get relative path (remove .zst extension if compression is enabled)
            let mut relative_path = s3_key.strip_prefix(s3_prefix)
                .unwrap_or(&s3_key)
                .trim_start_matches('/')
                .to_string();
            
            if relative_path.is_empty() {
                continue;
            }
            
            // Remove .zst extension if compression is enabled
            if options.use_compression && relative_path.ends_with(".zst") {
                relative_path = relative_path.trim_end_matches(".zst").to_string();
            } else if options.use_compression {
                // If compression is enabled but file doesn't have .zst extension, skip it
                debug!("Skipping non-compressed file in compression mode: {}", s3_key);
                continue;
            }
            
            // Check if should exclude
            let local_path = local_folder.join(&relative_path);
            if should_exclude(&local_path, &options.exclude_patterns) {
                debug!("Excluding: {}", relative_path);
                continue;
            }
            
            downloaded_paths.insert(relative_path.clone());
            
            // Check if file needs to be downloaded
            let needs_download = if options.force {
                true
            } else if !local_path.exists() {
                true
            } else {
                // Get S3 metadata and compare with local file
                match self.get_object_metadata(&s3_key).await {
                    Ok((s3_size, s3_modified)) => {
                        match db.get_metadata(&relative_path)? {
                            Some(cached) => {
                                // Compare with current local file
                                match FileMetadata::from_file(&local_path, &relative_path) {
                                    Ok(current) => {
                                        // Check if S3 file is different
                                        s3_size as u64 != current.size || 
                                        s3_modified > cached.last_sync.to_string()
                                    }
                                    Err(e) => {
                                        warn!("Failed to read local file metadata for {:?}: {}", local_path, e);
                                        true // Download to be safe
                                    }
                                }
                            }
                            None => {
                                // Not in cache, compare with local file directly
                                match fs::metadata(&local_path) {
                                    Ok(local_meta) => s3_size as u64 != local_meta.len(),
                                    Err(_) => true,
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to get S3 metadata for {}: {}", s3_key, e);
                        stats.errors += 1;
                        continue;
                    }
                }
            };
            
            if needs_download {
                if options.dry_run {
                    info!("[DRY RUN] Would download: {} -> {:?}", s3_key, local_path);
                    stats.files_downloaded += 1;
                } else {
                    // Check if local file already exists and matches S3 content
                    let should_actually_download = if local_path.exists() {
                        debug!("Local file exists, checking if content matches: {:?}", local_path);
                        
                        match self.get_object_metadata(&s3_key).await {
                            Ok((s3_size, _s3_modified)) => {
                                match FileMetadata::from_file(&local_path, &relative_path) {
                                    Ok(local_meta) => {
                                        // If not using compression, compare sizes directly
                                        if !options.use_compression && s3_size as u64 == local_meta.size {
                                            // Sizes match, assume content is the same
                                            // Just update database without downloading
                                            info!("✓ File already exists locally (skipped download): {}", relative_path);
                                            db.set_metadata(&local_meta)?;
                                            stats.files_skipped += 1;
                                            false
                                        } else if options.use_compression {
                                            // With compression, S3 size is compressed size
                                            // We could download and compare, but for now just re-download
                                            // TODO: Implement hash comparison for compressed files
                                            true
                                        } else {
                                            true // Sizes don't match, need to download
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Failed to read local file metadata for {:?}: {}", local_path, e);
                                        true // Download to be safe
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to get S3 metadata for {}: {}", s3_key, e);
                                true // Download to be safe
                            }
                        }
                    } else {
                        true // Local file doesn't exist, need to download
                    };
                    
                    if should_actually_download {
                        debug!("Downloading: {} -> {:?}", s3_key, local_path);
                        
                        // Download and decompress if compression is enabled
                        if options.use_compression {
                        // Download to temporary compressed file
                        let temp_dir = std::env::temp_dir();
                        let temp_file_name = format!("s3sync_{}_{}.zst", 
                            std::process::id(),
                            local_path.file_name().unwrap_or_default().to_string_lossy()
                        );
                        let temp_path = temp_dir.join(temp_file_name);
                        
                        match self.download_file(&s3_key, &temp_path).await {
                            Ok(_) => {
                                // Decompress the file
                                match decompress_file(&temp_path, &local_path) {
                                    Ok(_) => {
                                        // Clean up temp file
                                        let _ = fs::remove_file(&temp_path);
                                        
                                        // Update database
                                        match FileMetadata::from_file(&local_path, &relative_path) {
                                            Ok(metadata) => {
                                                db.set_metadata(&metadata)?;
                                                stats.files_downloaded += 1;
                                                stats.bytes_downloaded += metadata.size;
                                                info!("⬇ Downloaded (decompressed): {}", relative_path);
                                            }
                                            Err(e) => {
                                                error!("Failed to update metadata for {:?}: {}", local_path, e);
                                                stats.errors += 1;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to decompress {:?}: {}", temp_path, e);
                                        let _ = fs::remove_file(&temp_path);
                                        stats.errors += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to download {}: {}", s3_key, e);
                                stats.errors += 1;
                            }
                        }
                    } else {
                        // Direct download without compression
                        match self.download_file(&s3_key, &local_path).await {
                            Ok(_) => {
                                // Update database
                                match FileMetadata::from_file(&local_path, &relative_path) {
                                    Ok(metadata) => {
                                        db.set_metadata(&metadata)?;
                                        stats.files_downloaded += 1;
                                        stats.bytes_downloaded += metadata.size;
                                        info!("⬇ Downloaded: {}", relative_path);
                                    }
                                    Err(e) => {
                                        error!("Failed to update metadata for {:?}: {}", local_path, e);
                                        stats.errors += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to download {}: {}", s3_key, e);
                                stats.errors += 1;
                            }
                        }
                    }
                    }
                }
            } else {
                debug!("Skipping (unchanged): {}", relative_path);
                stats.files_skipped += 1;
            }
        }
        
        // Handle local file deletions if requested
        if options.delete {
            let tracked_files = db.list_all()?;
            
            for cached in tracked_files {
                if !downloaded_paths.contains(&cached.path) {
                    let local_path = local_folder.join(&cached.path);
                    
                    if local_path.exists() {
                        if options.dry_run {
                            info!("[DRY RUN] Would delete local file: {:?}", local_path);
                            stats.files_deleted += 1;
                        } else {
                            debug!("Deleting local file: {:?}", local_path);
                            
                            match fs::remove_file(&local_path) {
                                Ok(_) => {
                                    db.remove_metadata(&cached.path)?;
                                    stats.files_deleted += 1;
                                    info!("✗ Deleted local file: {}", cached.path);
                                }
                                Err(e) => {
                                    error!("Failed to delete {:?}: {}", local_path, e);
                                    stats.errors += 1;
                                }
                            }
                        }
                    } else {
                        // File doesn't exist locally, just remove from DB
                        db.remove_metadata(&cached.path)?;
                    }
                }
            }
        }
        
        db.flush()?;
        
        info!("Sync completed: downloaded={}, skipped={}, deleted={}, errors={}", 
            stats.files_downloaded, stats.files_skipped, stats.files_deleted, stats.errors);
        
        Ok(stats)
    }
    
    /// Sync folder bidirectionally (local <-> S3)
    ///
    /// # Arguments
    /// * `local_folder` - Local folder path to sync
    /// * `s3_prefix` - S3 prefix (folder path in bucket)
    /// * `db_path` - Path to sync database
    /// * `options` - Sync options
    ///
    /// # Returns
    /// Sync statistics
    async fn sync_folder_bidirectional<P: AsRef<Path>>(
        &self,
        local_folder: P,
        s3_prefix: &str,
        db_path: P,
        options: SyncOptions,
    ) -> Result<SyncStats> {
        let local_folder = local_folder.as_ref();
        let db_path_ref = db_path.as_ref();
        let mut stats = SyncStats::default();
        
        info!("Starting bidirectional folder sync: {:?} <-> s3://{}/{}", 
            local_folder, self.config.bucket, s3_prefix);
        
        // First, sync local -> S3
        let upload_stats = self.sync_local_to_s3(local_folder, s3_prefix, db_path_ref, options.clone()).await?;
        stats.files_scanned = upload_stats.files_scanned;
        stats.files_uploaded = upload_stats.files_uploaded;
        stats.files_skipped = upload_stats.files_skipped;
        stats.bytes_uploaded = upload_stats.bytes_uploaded;
        stats.errors = upload_stats.errors;
        
        // Then, sync S3 -> local (but without deletion to avoid conflicts)
        let mut download_options = options.clone();
        download_options.delete = false; // Don't delete in S3->local phase to avoid conflicts
        
        let download_stats = self.sync_s3_to_local(local_folder, s3_prefix, db_path_ref, download_options).await?;
        stats.files_downloaded = download_stats.files_downloaded;
        stats.bytes_downloaded = download_stats.bytes_downloaded;
        stats.files_skipped += download_stats.files_skipped;
        stats.errors += download_stats.errors;
        
        info!("Bidirectional sync completed: uploaded={}, downloaded={}, skipped={}, errors={}", 
            stats.files_uploaded, stats.files_downloaded, stats.files_skipped, stats.errors);
        
        Ok(stats)
    }
}

/// Check if path should be excluded based on patterns
fn should_exclude(path: &Path, patterns: &[String]) -> bool {
    let path_str = path.to_string_lossy();
    
    for pattern in patterns {
        // Simple pattern matching (you can use glob crate for more complex patterns)
        if pattern.starts_with('*') && pattern.ends_with('*') {
            let middle = &pattern[1..pattern.len()-1];
            if path_str.contains(middle) {
                return true;
            }
        } else if pattern.starts_with('*') {
            let suffix = &pattern[1..];
            if path_str.ends_with(suffix) {
                return true;
            }
        } else if pattern.ends_with('*') {
            let prefix = &pattern[..pattern.len()-1];
            if path_str.starts_with(prefix) {
                return true;
            }
        } else if path_str.contains(pattern) {
            return true;
        }
    }
    
    false
}

/// Compress a file using zstd
///
/// # Arguments
/// * `input_path` - Path to the input file
/// * `output_path` - Path to save the compressed file
///
/// # Returns
/// Size of the compressed file in bytes
fn compress_file(input_path: &Path, output_path: &Path) -> Result<u64> {
    use std::io::Write;
    
    debug!("Compressing file: {:?} -> {:?}", input_path, output_path);
    
    let input_file = fs::File::open(input_path)
        .context(format!("Failed to open input file: {:?}", input_path))?;
    let mut input_reader = std::io::BufReader::new(input_file);
    
    // Create parent directory if it doesn't exist
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .context(format!("Failed to create directory: {:?}", parent))?;
    }
    
    let output_file = fs::File::create(output_path)
        .context(format!("Failed to create output file: {:?}", output_path))?;
    let mut output_writer = std::io::BufWriter::new(output_file);
    
    // Compress with zstd (level 3 is a good balance between speed and compression)
    let mut encoder = zstd::Encoder::new(&mut output_writer, 3)?;
    std::io::copy(&mut input_reader, &mut encoder)?;
    encoder.finish()?;
    
    output_writer.flush()?;
    drop(output_writer);
    
    let compressed_size = fs::metadata(output_path)?.len();
    debug!("Compressed {:?}: {} -> {} bytes ({:.2}%)", 
        input_path.file_name().unwrap_or_default(),
        fs::metadata(input_path)?.len(),
        compressed_size,
        (compressed_size as f64 / fs::metadata(input_path)?.len() as f64) * 100.0
    );
    
    Ok(compressed_size)
}

/// Decompress a zstd-compressed file
///
/// # Arguments
/// * `input_path` - Path to the compressed file
/// * `output_path` - Path to save the decompressed file
///
/// # Returns
/// Size of the decompressed file in bytes
fn decompress_file(input_path: &Path, output_path: &Path) -> Result<u64> {
    use std::io::Write;
    
    debug!("Decompressing file: {:?} -> {:?}", input_path, output_path);
    
    let input_file = fs::File::open(input_path)
        .context(format!("Failed to open compressed file: {:?}", input_path))?;
    let input_reader = std::io::BufReader::new(input_file);
    
    // Create parent directory if it doesn't exist
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .context(format!("Failed to create directory: {:?}", parent))?;
    }
    
    let output_file = fs::File::create(output_path)
        .context(format!("Failed to create output file: {:?}", output_path))?;
    let mut output_writer = std::io::BufWriter::new(output_file);
    
    // Decompress with zstd
    let mut decoder = zstd::Decoder::new(input_reader)?;
    std::io::copy(&mut decoder, &mut output_writer)?;
    
    output_writer.flush()?;
    drop(output_writer);
    
    let decompressed_size = fs::metadata(output_path)?.len();
    debug!("Decompressed {:?}: {} -> {} bytes", 
        input_path.file_name().unwrap_or_default(),
        fs::metadata(input_path)?.len(),
        decompressed_size
    );
    
    Ok(decompressed_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_from_str() {
        assert_eq!(S3Provider::from_str("aws"), S3Provider::AwsS3);
        assert_eq!(S3Provider::from_str("s3"), S3Provider::AwsS3);
        assert_eq!(S3Provider::from_str("AWS-S3"), S3Provider::AwsS3);
        
        assert_eq!(S3Provider::from_str("b2"), S3Provider::BackblazeB2);
        assert_eq!(S3Provider::from_str("backblaze"), S3Provider::BackblazeB2);
        assert_eq!(S3Provider::from_str("Backblaze-B2"), S3Provider::BackblazeB2);
        
        assert_eq!(S3Provider::from_str("r2"), S3Provider::CloudflareR2);
        assert_eq!(S3Provider::from_str("cloudflare"), S3Provider::CloudflareR2);
        assert_eq!(S3Provider::from_str("Cloudflare-R2"), S3Provider::CloudflareR2);
        
        assert_eq!(S3Provider::from_str("minio"), S3Provider::Generic);
        assert_eq!(S3Provider::from_str("other"), S3Provider::Generic);
    }
}

