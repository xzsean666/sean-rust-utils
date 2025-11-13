use aws_config::meta::region::RegionProviderChain;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use md5::{Md5, Digest};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{error, info};

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub provider: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub force_path_style: Option<bool>,
    pub use_compression: Option<bool>,
    pub compression_level: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum UploadStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "uploading")]
    Uploading,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed,
}

#[derive(Serialize, Debug)]
pub struct S3UrlResponse {
    pub url: Option<String>,
    pub status: UploadStatus,
    pub uploaded: bool,
    pub compressed: bool,
    pub md5: String,
    pub timeout_seconds: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UploadRecord {
    md5: String,
    s3_key: String,
    original_path: String,
    compressed: bool,
    uploaded_at: u64,
    status: UploadStatus,
    file_size: u64,
    timeout_seconds: u64,
}

#[derive(Debug)]
pub enum S3Error {
    FileReadError,
    CompressionError,
    UploadFailed(String),
    PresignFailed(String),
    DatabaseError(String),
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Error::FileReadError => write!(f, "Failed to read file for S3 upload"),
            S3Error::CompressionError => write!(f, "Failed to compress file for S3 upload"),
            S3Error::UploadFailed(e) => write!(f, "Failed to upload to S3: {}", e),
            S3Error::PresignFailed(e) => write!(f, "Failed to generate presigned URL: {}", e),
            S3Error::DatabaseError(e) => write!(f, "Database error: {}", e),
        }
    }
}

impl std::error::Error for S3Error {}

/// Calculate MD5 hash of file contents
pub async fn calculate_file_md5(file_path: &StdPath) -> Result<String, S3Error> {
    let mut file = File::open(file_path).await.map_err(|e| {
        error!("Failed to open file for MD5 calculation: {}", e);
        S3Error::FileReadError
    })?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await.map_err(|e| {
        error!("Failed to read file for MD5 calculation: {}", e);
        S3Error::FileReadError
    })?;

    let mut hasher = Md5::new();
    hasher.update(&buffer);
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

/// Calculate timeout based on file size (1MB = 2 seconds)
pub fn calculate_timeout(file_size_bytes: u64) -> u64 {
    // 1MB = 1_048_576 bytes, timeout = 2 seconds per MB
    // Add minimum timeout of 10 seconds and maximum of 3600 seconds (1 hour)
    let timeout = (file_size_bytes as f64 / 1_048_576.0 * 2.0).ceil() as u64;
    timeout.max(10).min(3600)
}

/// Check if file has been uploaded by MD5
fn get_upload_record(db: &Db, md5: &str) -> Result<Option<UploadRecord>, S3Error> {
    match db.get(md5) {
        Ok(Some(data)) => {
            match bincode::deserialize::<UploadRecord>(&data) {
                Ok(record) => Ok(Some(record)),
                Err(e) => {
                    error!("Failed to deserialize upload record: {}", e);
                    // Try to deserialize old format for backward compatibility
                    // If it fails, return None to trigger re-upload
                    Ok(None)
                }
            }
        }
        Ok(None) => Ok(None),
        Err(e) => {
            error!("Database read error: {}", e);
            Err(S3Error::DatabaseError(e.to_string()))
        }
    }
}

/// Save upload record to database
fn save_upload_record(db: &Db, record: &UploadRecord) -> Result<(), S3Error> {
    let data = bincode::serialize(record).map_err(|e| {
        error!("Failed to serialize upload record: {}", e);
        S3Error::DatabaseError(e.to_string())
    })?;

    db.insert(&record.md5, data).map_err(|e| {
        error!("Database write error: {}", e);
        S3Error::DatabaseError(e.to_string())
    })?;

    // Note: sled automatically flushes periodically, so we don't need to flush on every write
    // This reduces CPU usage and I/O overhead
    // Only flush when absolutely necessary (e.g., before shutdown)
    
    Ok(())
}

/// Create and configure an S3 client
pub async fn create_s3_client(config: &S3Config) -> Result<S3Client, S3Error> {
    let credentials = Credentials::new(
        &config.access_key_id,
        &config.secret_access_key,
        None,
        None,
        "static",
    );

    let region = RegionProviderChain::first_try(aws_config::Region::new(config.region.clone()));

    let mut config_builder = aws_config::from_env()
        .credentials_provider(credentials)
        .region(region);

    if let Some(endpoint) = &config.endpoint {
        config_builder = config_builder.endpoint_url(endpoint);
    }

    let aws_config = config_builder.load().await;

    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

    if config.force_path_style.unwrap_or(false) {
        s3_config_builder = s3_config_builder.force_path_style(true);
    }

    let s3_config = s3_config_builder.build();
    Ok(S3Client::from_conf(s3_config))
}

/// Check if a file exists in S3
pub async fn check_s3_file_exists(client: &S3Client, bucket: &str, key: &str) -> bool {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Upload a file to S3, optionally with compression
pub async fn upload_file_to_s3(
    client: &S3Client,
    bucket: &str,
    file_path: &StdPath,
    s3_key: &str,
    use_compression: bool,
    compression_level: i32,
) -> Result<(), S3Error> {
    // Read file
    let mut file = File::open(file_path).await.map_err(|e| {
        error!("Failed to open file for S3 upload: {}", e);
        S3Error::FileReadError
    })?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await.map_err(|e| {
        error!("Failed to read file for S3 upload: {}", e);
        S3Error::FileReadError
    })?;

    let data_to_upload = if use_compression {
        // Compress with ZSTD
        zstd::encode_all(buffer.as_slice(), compression_level).map_err(|e| {
            error!("Failed to compress file for S3 upload: {}", e);
            S3Error::CompressionError
        })?
    } else {
        buffer
    };

    // Upload to S3
    let body = ByteStream::from(data_to_upload);

    let mut request = client.put_object().bucket(bucket).key(s3_key).body(body);

    if use_compression {
        request = request.content_encoding("zstd");
    }

    request.send().await.map_err(|e| {
        error!("Failed to upload to S3: {}", e);
        S3Error::UploadFailed(e.to_string())
    })?;

    Ok(())
}

/// Generate a presigned URL for downloading a file from S3
pub async fn generate_presigned_url(
    client: &S3Client,
    bucket: &str,
    key: &str,
    expires_in_seconds: u64,
) -> Result<String, S3Error> {
    let presigned_request = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .presigned(
            aws_sdk_s3::presigning::PresigningConfig::expires_in(
                Duration::from_secs(expires_in_seconds),
            )
            .map_err(|e| {
                error!("Failed to create presigning config: {}", e);
                S3Error::PresignFailed(e.to_string())
            })?,
        )
        .await
        .map_err(|e| {
            error!("Failed to generate presigned URL: {}", e);
            S3Error::PresignFailed(e.to_string())
        })?;

    Ok(presigned_request.uri().to_string())
}

/// Background task to upload file to S3
pub async fn upload_file_background(
    client: Arc<S3Client>,
    config: Arc<S3Config>,
    db: Arc<Db>,
    file_path: PathBuf,
    original_path: String,
    md5: String,
    s3_key: String,
    use_compression: bool,
    compression_level: i32,
    file_size: u64,
    timeout_seconds: u64,
) {
    info!("Starting background upload for file: {} (MD5: {})", original_path, md5);

    // Update status to uploading
    let record = UploadRecord {
        md5: md5.clone(),
        s3_key: s3_key.clone(),
        original_path: original_path.clone(),
        compressed: use_compression,
        uploaded_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        status: UploadStatus::Uploading,
        file_size,
        timeout_seconds,
    };
    if let Err(e) = save_upload_record(&db, &record) {
        error!("Failed to update upload status to uploading: {}", e);
        return;
    }

    // Perform the actual upload
    let upload_result = upload_file_to_s3(
        &client,
        &config.bucket,
        &file_path,
        &s3_key,
        use_compression,
        compression_level,
    )
    .await;

    // Update status based on result
    let final_status = match upload_result {
        Ok(_) => {
            info!("Background upload completed successfully for: {}", original_path);
            UploadStatus::Completed
        }
        Err(e) => {
            error!("Background upload failed for {}: {}", original_path, e);
            UploadStatus::Failed
        }
    };

    let final_record = UploadRecord {
        md5: md5.clone(),
        s3_key: s3_key.clone(),
        original_path: original_path.clone(),
        compressed: use_compression,
        uploaded_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        status: final_status.clone(),
        file_size,
        timeout_seconds,
    };

    if let Err(e) = save_upload_record(&db, &final_record) {
        error!("Failed to update final upload status: {}", e);
    } else {
        info!("Updated upload status to {:?} for MD5: {}", final_status, md5);
    }
}

/// Handle S3 URL generation endpoint
pub async fn handle_get_s3_url(
    client: Arc<S3Client>,
    config: Arc<S3Config>,
    db: Arc<Db>,
    file_path: &StdPath,
    original_path: &str,
    force_update: bool,
) -> Result<S3UrlResponse, S3Error> {
    let use_compression = config.use_compression.unwrap_or(true);
    let compression_level = config.compression_level.unwrap_or(19);

    // Get file size for timeout calculation
    let file_size = tokio::fs::metadata(file_path)
        .await
        .map_err(|e| {
            error!("Failed to get file metadata: {}", e);
            S3Error::FileReadError
        })?
        .len();

    // Calculate timeout based on file size
    let timeout_seconds = calculate_timeout(file_size);
    info!("Calculated timeout for file size {} bytes: {} seconds", file_size, timeout_seconds);

    // Calculate MD5 of the file
    let md5 = calculate_file_md5(file_path).await?;
    info!("Calculated MD5 for file: {}", md5);

    // Use MD5 as S3 key with optional compression extension
    let final_s3_key = if use_compression {
        format!("{}.zstd", md5)
    } else {
        md5.clone()
    };

    // Check if we need to upload
    let should_upload = if force_update {
        info!("Force update requested, will re-upload file");
        true
    } else {
        // Check database for existing record
        match get_upload_record(&db, &md5)? {
            Some(record) => {
                info!("Found upload record in database: status={:?}", record.status);
                match record.status {
                    UploadStatus::Completed => {
                        // Verify the file still exists in S3
                        let exists = check_s3_file_exists(&client, &config.bucket, &final_s3_key).await;
                        if !exists {
                            info!("File not found in S3, will re-upload");
                            true
                        } else {
                            info!("File already exists in S3, returning presigned URL");
                            false
                        }
                    }
                    UploadStatus::Pending | UploadStatus::Uploading => {
                        // File is already being uploaded, return current status
                        info!("File is already being uploaded, returning current status");
                        false
                    }
                    UploadStatus::Failed => {
                        // Previous upload failed, retry
                        info!("Previous upload failed, will retry");
                        true
                    }
                }
            }
            None => {
                info!("No upload record found, will upload file");
                true
            }
        }
    };

    if should_upload {
        // Create pending record
        let record = UploadRecord {
            md5: md5.clone(),
            s3_key: final_s3_key.clone(),
            original_path: original_path.to_string(),
            compressed: use_compression,
            uploaded_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            status: UploadStatus::Pending,
            file_size,
            timeout_seconds,
        };
        save_upload_record(&db, &record)?;
        info!("Created pending upload record for MD5: {}", md5);

        // Start background upload task
        let file_path_clone = file_path.to_path_buf();
        let original_path_clone = original_path.to_string();
        tokio::spawn(upload_file_background(
            client.clone(),
            config.clone(),
            db.clone(),
            file_path_clone,
            original_path_clone,
            md5.clone(),
            final_s3_key.clone(),
            use_compression,
            compression_level,
            file_size,
            timeout_seconds,
        ));

        // Return pending status immediately
        return Ok(S3UrlResponse {
            url: None,
            status: UploadStatus::Pending,
            uploaded: false,
            compressed: use_compression,
            md5,
            timeout_seconds,
        });
    }

    // File is already uploaded or uploading, check current status
    let record = get_upload_record(&db, &md5)?
        .ok_or_else(|| S3Error::DatabaseError("Record not found after check".to_string()))?;

    match record.status {
        UploadStatus::Completed => {
            // Generate presigned URL (valid for 1 hour)
            let presigned_url =
                generate_presigned_url(&client, &config.bucket, &final_s3_key, 3600).await?;

            Ok(S3UrlResponse {
                url: Some(presigned_url),
                status: UploadStatus::Completed,
                uploaded: true,
                compressed: use_compression,
                md5,
                timeout_seconds: record.timeout_seconds,
            })
        }
        UploadStatus::Pending | UploadStatus::Uploading => {
            // Return current status without URL
            Ok(S3UrlResponse {
                url: None,
                status: record.status.clone(),
                uploaded: false,
                compressed: use_compression,
                md5,
                timeout_seconds: record.timeout_seconds,
            })
        }
        UploadStatus::Failed => {
            // Retry upload
            let record = UploadRecord {
                md5: md5.clone(),
                s3_key: final_s3_key.clone(),
                original_path: original_path.to_string(),
                compressed: use_compression,
                uploaded_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                status: UploadStatus::Pending,
                file_size,
                timeout_seconds,
            };
            save_upload_record(&db, &record)?;

            let file_path_clone = file_path.to_path_buf();
            let original_path_clone = original_path.to_string();
            tokio::spawn(upload_file_background(
                client.clone(),
                config.clone(),
                db.clone(),
                file_path_clone,
                original_path_clone,
                md5.clone(),
                final_s3_key.clone(),
                use_compression,
                compression_level,
                file_size,
                timeout_seconds,
            ));

            Ok(S3UrlResponse {
                url: None,
                status: UploadStatus::Pending,
                uploaded: false,
                compressed: use_compression,
                md5,
                timeout_seconds,
            })
        }
    }
}

