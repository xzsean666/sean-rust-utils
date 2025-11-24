use aws_config::meta::region::RegionProviderChain;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{error, info, warn};

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

const INDEX_TREE: &str = "index_records";
const UPLOAD_TREE: &str = "upload_records";

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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct MetadataBackup {
    version: u32,
    created_at: u64,
    records: BTreeMap<String, UploadRecord>,
}

#[derive(Debug)]
pub enum S3Error {
    BackupFailed(String),
    RestoreFailed(String),
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
            S3Error::BackupFailed(e) => write!(f, "Failed to backup metadata: {}", e),
            S3Error::RestoreFailed(e) => write!(f, "Failed to restore metadata: {}", e),
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

    let mut hasher = Md5::new();
    let mut buffer = [0u8; 8 * 1024];

    loop {
        let bytes_read = file.read(&mut buffer).await.map_err(|e| {
            error!("Failed to read file for MD5 calculation: {}", e);
            S3Error::FileReadError
        })?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Calculate timeout based on file size (1MB = 2 seconds)
pub fn calculate_timeout(file_size_bytes: u64) -> u64 {
    // 1MB = 1_048_576 bytes, timeout = 2 seconds per MB
    // Add minimum timeout of 10 seconds and maximum of 3600 seconds (1 hour)
    let timeout = (file_size_bytes as f64 / 1_048_576.0 * 2.0).ceil() as u64;
    timeout.max(10).min(3600)
}

fn get_db_tree(db: &Db, tree: &str) -> Result<sled::Tree, S3Error> {
    db.open_tree(tree).map_err(|e| {
        error!("Failed to open sled tree {}: {}", tree, e);
        S3Error::DatabaseError(e.to_string())
    })
}

fn read_index_md5(db: &Db, index_key: &str) -> Result<Option<String>, S3Error> {
    let index_tree = get_db_tree(db, INDEX_TREE)?;
    match index_tree.get(index_key) {
        Ok(Some(data)) => String::from_utf8(data.to_vec())
            .map(Some)
            .map_err(|e| {
                error!("Failed to decode index record {}: {}", index_key, e);
                S3Error::DatabaseError(e.to_string())
            }),
        Ok(None) => Ok(None),
        Err(e) => {
            error!("Database read error for index key {}: {}", index_key, e);
            Err(S3Error::DatabaseError(e.to_string()))
        }
    }
}

fn upsert_index_md5(db: &Db, index_key: &str, md5: &str) -> Result<(), S3Error> {
    let index_tree = get_db_tree(db, INDEX_TREE)?;
    index_tree
        .insert(index_key, md5.as_bytes())
        .map_err(|e| {
            error!(
                "Database write error for index key {} -> {}: {}",
                index_key, md5, e
            );
            S3Error::DatabaseError(e.to_string())
        })?;
    Ok(())
}

fn remove_index_md5(db: &Db, index_key: &str) -> Result<(), S3Error> {
    let index_tree = get_db_tree(db, INDEX_TREE)?;
    index_tree.remove(index_key).map_err(|e| {
        error!("Failed to remove index key {}: {}", index_key, e);
        S3Error::DatabaseError(e.to_string())
    })?;
    Ok(())
}

fn persist_upload_record(db: &Db, record: &UploadRecord) -> Result<(), S3Error> {
    let tree = get_db_tree(db, UPLOAD_TREE)?;
    let data = bincode::serialize(record).map_err(|e| {
        error!("Failed to serialize upload record {}: {}", record.md5, e);
        S3Error::DatabaseError(e.to_string())
    })?;

    tree.insert(record.md5.as_bytes(), data).map_err(|e| {
        error!("Failed to persist upload record {}: {}", record.md5, e);
        S3Error::DatabaseError(e.to_string())
    })?;

    Ok(())
}

fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Check locally recorded upload status for an MD5
pub fn check_s3_file_exists_local(db: &Db, md5: &str) -> Result<Option<UploadRecord>, S3Error> {
    let tree = get_db_tree(db, UPLOAD_TREE)?;

    let decode = |data: &[u8]| match bincode::deserialize::<UploadRecord>(data) {
        Ok(record) => Some(record),
        Err(e) => {
            error!("Failed to deserialize upload record: {}", e);
            None
        }
    };

    // Primary lookup
    match tree.get(md5) {
        Ok(Some(data)) => {
            if let Some(record) = decode(&data) {
                if record.status == UploadStatus::Completed {
                    return Ok(Some(record));
                }
            }
        }
        Ok(None) => {}
        Err(e) => {
            error!("Database read error: {}", e);
            return Err(S3Error::DatabaseError(e.to_string()));
        }
    }
    Ok(None)
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

/// Upload raw bytes to S3
pub async fn upload_bytes_to_s3(
    client: &S3Client,
    bucket: &str,
    bytes: Vec<u8>,
    s3_key: Option<&str>,
    use_compression: Option<bool>,
    compression_level: Option<i32>,
    metadata: Option<HashMap<String, String>>,
    db: Option<&Db>,
    index_key: Option<&str>,
) -> Result<String, S3Error> {
    let mut metadata = metadata.unwrap_or_default();
    let index_key_owned = index_key.map(|k| k.to_string());

    if index_key_owned.is_some() && db.is_none() {
        return Err(S3Error::DatabaseError(
            "Database handle is required when using index_key".to_string(),
        ));
    }

    if let (Some(db), Some(index_key)) = (db, index_key_owned.as_deref()) {
        if let Some(existing_md5) = read_index_md5(db, index_key)? {
            if let Some(existing) = check_s3_file_exists_local(db, &existing_md5)? {
                return Ok(existing.md5);
            } else {
                warn!(
                    "Removing stale index entry {} -> {} because metadata is missing",
                    index_key, existing_md5
                );
                remove_index_md5(db, index_key)?;
            }
        }
    }

    let use_compression = use_compression.unwrap_or(true);
    let compression_level = compression_level.unwrap_or(19);
    let mut final_s3_key = s3_key.map(|k| k.to_string());
    let original_file_size = bytes.len() as u64;
    let timeout_seconds = calculate_timeout(original_file_size);
    let original_path = metadata
        .get("file_path")
        .cloned()
        .unwrap_or_else(|| "".to_string());

    if final_s3_key.is_none() {
        let mut hasher = Md5::new();
        hasher.update(&bytes);
        final_s3_key = Some(format!("{:x}", hasher.finalize()));
    }

    let md5_key = final_s3_key
        .as_ref()
        .expect("S3 key should be set after MD5 calculation")
        .to_string();

    if let Some(db) = db {
        if let Some(existing) = check_s3_file_exists_local(db, &md5_key)? {
            if let Some(index_key) = index_key_owned.as_deref() {
                upsert_index_md5(db, index_key, &existing.md5)?;
            }
            return Ok(existing.md5);
        }
    }

    let (body_bytes, compressed) = if use_compression {
        let compressed = tokio::task::spawn_blocking(move || {
            zstd::encode_all(bytes.as_slice(), compression_level)
        })
        .await
        .map_err(|e| {
            error!("Compression task failed for S3 upload: {}", e);
            S3Error::CompressionError
        })?
        .map_err(|e| {
            error!("Failed to compress bytes for S3 upload: {}", e);
            S3Error::CompressionError
        })?;

        (compressed, true)
    } else {
        (bytes, false)
    };

    metadata.insert("compressed".to_string(), compressed.to_string());

    let mut request = client
        .put_object()
        .bucket(bucket)
        .key(
            final_s3_key
                .as_ref()
                .expect("S3 key should be set after MD5 calculation"),
        )
        .body(ByteStream::from(body_bytes));

    // Add custom metadata
    for (key, value) in metadata.iter() {
        request = request.metadata(key, value);
    }

    // Add compression info to metadata
    if compressed {
        request = request.content_encoding("zstd");
    }

    request.send().await.map_err(|e| {
        error!("Failed to upload to S3: {}", e);
        S3Error::UploadFailed(e.to_string())
    })?;

    let key = final_s3_key.expect("S3 key should be set after MD5 calculation");

    if let Some(db) = db {
        let record = UploadRecord {
            md5: key.clone(),
            s3_key: key.clone(),
            original_path,
            compressed,
            uploaded_at: current_timestamp_secs(),
            status: UploadStatus::Completed,
            file_size: original_file_size,
            timeout_seconds,
        };
        persist_upload_record(db, &record)?;

        if let Some(index_key) = index_key_owned.as_deref() {
            upsert_index_md5(db, index_key, &record.md5)?;
        }
    }

    Ok(key)
}

/// Upload a file to S3, optionally with compression (default true). If `s3_key`
/// is `None`, the MD5 hash of the uncompressed file will be used as the key.
/// Basic metadata such as `file_path` and `file_ext` is always attached.
pub async fn upload_file_to_s3(
    client: &S3Client,
    bucket: &str,
    file_path: &StdPath,
    s3_key: Option<&str>,
    use_compression: Option<bool>,
    compression_level: Option<i32>,
    metadata: Option<HashMap<String, String>>,
    db: Option<&Db>,
    index_key: Option<&str>,
) -> Result<String, S3Error> {
    let use_compression = use_compression.unwrap_or(true);
    let compression_level = compression_level.unwrap_or(19);
    let mut metadata = metadata.unwrap_or_default();
    let index_key_owned = index_key.map(|k| k.to_string());

    if index_key_owned.is_some() && db.is_none() {
        return Err(S3Error::DatabaseError(
            "Database handle is required when using index_key".to_string(),
        ));
    }

    if let (Some(db), Some(index_key)) = (db, index_key_owned.as_deref()) {
        if let Some(existing_md5) = read_index_md5(db, index_key)? {
            if let Some(existing) = check_s3_file_exists_local(db, &existing_md5)? {
                return Ok(existing.md5);
            } else {
                warn!(
                    "Removing stale index entry {} -> {} because metadata is missing",
                    index_key, existing_md5
                );
                remove_index_md5(db, index_key)?;
            }
        }
    }

    // Always attach basic file metadata for traceability
    let file_path_value = file_path.to_string_lossy().into_owned();
    metadata.insert("file_path".to_string(), file_path_value);
    let file_ext_value = file_path
        .extension()
        .map(|ext| ext.to_string_lossy().into_owned())
        .unwrap_or_else(|| "".to_string());
    metadata.insert("file_ext".to_string(), file_ext_value);

    if use_compression {
        // Compress with ZSTD off the async runtime to avoid blocking other tasks
        let file_bytes = tokio::fs::read(file_path).await.map_err(|e| {
            error!("Failed to read file for S3 upload: {}", e);
            S3Error::FileReadError
        })?;

        return upload_bytes_to_s3(
            client,
            bucket,
            file_bytes,
            s3_key,
            Some(true),
            Some(compression_level),
            Some(metadata),
            db,
            index_key_owned.as_deref(),
        )
        .await;
    }

    let mut final_s3_key = s3_key.map(|k| k.to_string());

    if final_s3_key.is_none() {
        let md5 = calculate_file_md5(file_path).await?;
        final_s3_key = Some(md5);
    }

    let key = final_s3_key
        .clone()
        .expect("S3 key should be set after MD5 calculation");

    metadata.insert("compressed".to_string(), "false".to_string());
    let original_path = metadata
        .get("file_path")
        .cloned()
        .unwrap_or_else(|| "".to_string());
    let file_metadata = tokio::fs::metadata(file_path).await.map_err(|e| {
        error!("Failed to read file metadata for S3 upload: {}", e);
        S3Error::FileReadError
    })?;
    let file_size = file_metadata.len();
    let timeout_seconds = calculate_timeout(file_size);

    if let Some(db) = db {
        if let Some(existing) = check_s3_file_exists_local(db, &key)? {
            if let Some(index_key) = index_key_owned.as_deref() {
                upsert_index_md5(db, index_key, &existing.md5)?;
            }
            return Ok(existing.md5);
        }
    }

    let body = ByteStream::from_path(file_path).await.map_err(|e| {
        error!("Failed to stream file for S3 upload: {}", e);
        S3Error::FileReadError
    })?;

    // Upload to S3
    let mut request = client.put_object().bucket(bucket).key(&key).body(body);

    // Add custom metadata
    for (key, value) in metadata.iter() {
        request = request.metadata(key, value);
    }

    request.send().await.map_err(|e| {
        error!("Failed to upload to S3: {}", e);
        S3Error::UploadFailed(e.to_string())
    })?;

    if let Some(db) = db {
        let record = UploadRecord {
            md5: key.clone(),
            s3_key: key.clone(),
            original_path,
            compressed: false,
            uploaded_at: current_timestamp_secs(),
            status: UploadStatus::Completed,
            file_size,
            timeout_seconds,
        };
        persist_upload_record(db, &record)?;

        if let Some(index_key) = index_key_owned.as_deref() {
            upsert_index_md5(db, index_key, &record.md5)?;
        }
    }

    Ok(key)
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
            aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(
                expires_in_seconds,
            ))
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

/// Generate a presigned URL using an object's MD5-based key
pub async fn generate_presigned_url_by_md5(
    client: &S3Client,
    bucket: &str,
    md5: &str,
    expires_in_seconds: u64,
) -> Result<String, S3Error> {
    // Optional existence check to provide clearer error when key is missing
    if !check_s3_file_exists(client, bucket, md5).await {
        let msg = format!("Object not found in S3 for md5: {}", md5);
        error!("{}", msg);
        return Err(S3Error::PresignFailed(msg));
    }

    generate_presigned_url(client, bucket, md5, expires_in_seconds).await
}

/// Generate a presigned URL using an index key mapped to an MD5
pub async fn generate_presigned_url_by_index_key(
    client: &S3Client,
    bucket: &str,
    db: &Db,
    index_key: &str,
    expires_in_seconds: u64,
) -> Result<String, S3Error> {
    let md5 = read_index_md5(db, index_key)?.ok_or_else(|| {
        let msg = format!("Index key not found: {}", index_key);
        S3Error::DatabaseError(msg)
    })?;

    generate_presigned_url_by_md5(client, bucket, &md5, expires_in_seconds).await
}

/// Backup all sled-stored upload metadata to S3
pub async fn backup_metadata_to_s3(
    client: &S3Client,
    bucket: &str,
    db: &Db,
    backup_key: &str,
) -> Result<usize, S3Error> {
    let mut records = BTreeMap::new();

    let tree = get_db_tree(db, UPLOAD_TREE)?;

    for entry in tree.iter() {
        let (key, value) = entry.map_err(|e| {
            error!("Failed to iterate sled database for metadata backup: {}", e);
            S3Error::DatabaseError(e.to_string())
        })?;

        let key_str = String::from_utf8(key.to_vec()).map_err(|e| {
            error!("Non-UTF8 key found during metadata backup: {}", e);
            S3Error::BackupFailed(e.to_string())
        })?;

        let record: UploadRecord = bincode::deserialize(&value).map_err(|e| {
            error!(
                "Failed to deserialize upload record for key {} during backup: {}",
                key_str, e
            );
            S3Error::BackupFailed(e.to_string())
        })?;

        records.insert(key_str, record);
    }

    let backup = MetadataBackup {
        version: 1,
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        records,
    };

    let serialized = bincode::serialize(&backup).map_err(|e| {
        error!("Failed to serialize metadata backup: {}", e);
        S3Error::BackupFailed(e.to_string())
    })?;

    client
        .put_object()
        .bucket(bucket)
        .key(backup_key)
        .metadata("metadata_backup", "sled_upload_records")
        .content_type("application/octet-stream")
        .body(ByteStream::from(serialized))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to upload metadata backup to S3: {}", e);
            S3Error::BackupFailed(e.to_string())
        })?;

    info!(
        "Backed up {} metadata records to s3://{}/{}",
        backup.records.len(),
        bucket,
        backup_key
    );

    Ok(backup.records.len())
}

/// Restore sled-stored upload metadata from an S3 backup
pub async fn restore_metadata_from_s3(
    client: &S3Client,
    bucket: &str,
    db: &Db,
    backup_key: &str,
    clear_existing: bool,
) -> Result<usize, S3Error> {
    let response = client
        .get_object()
        .bucket(bucket)
        .key(backup_key)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to download metadata backup from S3: {}", e);
            S3Error::RestoreFailed(e.to_string())
        })?;

    let backup_bytes = response
        .body
        .collect()
        .await
        .map_err(|e| {
            error!("Failed to read metadata backup body: {}", e);
            S3Error::RestoreFailed(e.to_string())
        })?
        .into_bytes()
        .to_vec();

    let backup: MetadataBackup = bincode::deserialize(&backup_bytes).map_err(|e| {
        error!("Failed to deserialize metadata backup: {}", e);
        S3Error::RestoreFailed(e.to_string())
    })?;

    let tree = get_db_tree(db, UPLOAD_TREE)?;

    if clear_existing {
        tree.clear().map_err(|e| {
            error!("Failed to clear upload tree before restore: {}", e);
            S3Error::DatabaseError(e.to_string())
        })?;
    }

    let mut restored = 0usize;
    for (key, record) in backup.records {
        let serialized = bincode::serialize(&record).map_err(|e| {
            error!("Failed to serialize upload record during restore: {}", e);
            S3Error::RestoreFailed(e.to_string())
        })?;

        tree.insert(key.as_bytes(), serialized).map_err(|e| {
            error!("Failed to insert restored record {}: {}", key, e);
            S3Error::DatabaseError(e.to_string())
        })?;

        restored += 1;
    }

    db.flush().map_err(|e| {
        error!("Failed to flush sled database after restore: {}", e);
        S3Error::DatabaseError(e.to_string())
    })?;

    info!(
        "Restored {} metadata records from s3://{}/{}",
        restored, bucket, backup_key
    );

    Ok(restored)
}
