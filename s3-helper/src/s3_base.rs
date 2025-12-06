use crate::kvdb::{KVDB, KVError};
use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    env, fs,
    path::Path as StdPath,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info, warn};

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
pub struct UploadRecord {
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
    BackupFailed(String),
    RestoreFailed(String),
    FileReadError,
    CompressionError,
    UploadFailed(String),
    PresignFailed(String),
    Database(KVError),
    DatabaseError(String),
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Error::FileReadError => write!(f, "Failed to read file for S3 upload"),
            S3Error::CompressionError => write!(f, "Failed to compress file for S3 upload"),
            S3Error::UploadFailed(e) => write!(f, "Failed to upload to S3: {}", e),
            S3Error::PresignFailed(e) => write!(f, "Failed to generate presigned URL: {}", e),
            S3Error::Database(e) => write!(f, "Database error: {}", e),
            S3Error::DatabaseError(e) => write!(f, "Database error: {}", e),
            S3Error::BackupFailed(e) => write!(f, "Failed to backup metadata: {}", e),
            S3Error::RestoreFailed(e) => write!(f, "Failed to restore metadata: {}", e),
        }
    }
}

impl std::error::Error for S3Error {}

impl From<KVError> for S3Error {
    fn from(err: KVError) -> Self {
        Self::Database(err)
    }
}

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
    let timeout = (file_size_bytes as f64 / 1_048_576.0 * 2.0).ceil() as u64;
    timeout.max(10).min(3600)
}

fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone)]
pub struct S3MetadataStore {
    uploads: KVDB,
    index: KVDB,
}

impl S3MetadataStore {
    /// Build metadata store for uploads and index trees from an existing KVDB handle.
    pub fn new(db: KVDB) -> Result<Self, S3Error> {
        Ok(Self {
            uploads: db.with_tree(UPLOAD_TREE)?,
            index: db.with_tree(INDEX_TREE)?,
        })
    }

    fn lookup_index_md5(&self, index_key: &str) -> Result<Option<String>, S3Error> {
        self.index.get::<String>(index_key).map_err(Into::into)
    }

    fn upsert_index_md5(&self, index_key: &str, md5: &str) -> Result<(), S3Error> {
        self.index
            .put(index_key, &md5.to_string())
            .map_err(Into::into)
    }

    fn remove_index_md5(&self, index_key: &str) -> Result<(), S3Error> {
        self.index.delete(index_key).map(|_| ()).map_err(Into::into)
    }

    fn persist_upload_record(&self, record: &UploadRecord) -> Result<(), S3Error> {
        self.uploads.put(&record.md5, record).map_err(Into::into)
    }

    /// Check locally recorded upload status for an MD5
    fn completed_upload(&self, md5: &str) -> Result<Option<UploadRecord>, S3Error> {
        let record = self.uploads.get::<UploadRecord>(md5)?;
        Ok(record.filter(|rec| rec.status == UploadStatus::Completed))
    }

    fn upload_records(&self) -> Result<BTreeMap<String, UploadRecord>, S3Error> {
        Ok(self
            .uploads
            .get_all::<UploadRecord>(None, None)?
            .into_iter()
            .collect())
    }

    fn backup_bytes(&self) -> Result<Vec<u8>, S3Error> {
        self.uploads.create_backup_bytes().map_err(Into::into)
    }

    fn restore_from_backup_bytes(
        &self,
        backup_bytes: &[u8],
        clear_existing: bool,
    ) -> Result<usize, S3Error> {
        let temp_dir = env::temp_dir().join(format!(
            "s3_metadata_restore_{}_{}",
            current_timestamp_secs(),
            std::process::id()
        ));

        if temp_dir.exists() {
            let _ = fs::remove_dir_all(&temp_dir);
        }

        KVDB::restore_from_bytes(backup_bytes, &temp_dir)?;
        let temp_db = KVDB::new(&temp_dir, UPLOAD_TREE)?;
        let temp_store = S3MetadataStore::new(temp_db)?;

        let upload_records = temp_store.upload_records()?;
        let index_records = temp_store.index.get_all::<String>(None, None)?;

        if clear_existing {
            self.uploads.clear()?;
            self.index.clear()?;
        }

        if !upload_records.is_empty() {
            self.uploads.put_many(upload_records.clone())?;
        }
        if !index_records.is_empty() {
            self.index.put_many(index_records.clone())?;
        }

        let _ = fs::remove_dir_all(&temp_dir);

        Ok(upload_records.len())
    }
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

    debug!(provider = %config.provider, "Building AWS config from defaults");

    let mut config_builder = aws_config::defaults(BehaviorVersion::latest())
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
    metadata_store: Option<&S3MetadataStore>,
    index_key: Option<&str>,
) -> Result<String, S3Error> {
    let mut metadata = metadata.unwrap_or_default();
    let index_key_owned = index_key.map(|k| k.to_string());

    if index_key_owned.is_some() && metadata_store.is_none() {
        return Err(S3Error::DatabaseError(
            "Database handle is required when using index_key".to_string(),
        ));
    }

    if let (Some(store), Some(index_key)) = (metadata_store, index_key_owned.as_deref()) {
        if let Some(existing_md5) = store.lookup_index_md5(index_key)? {
            if let Some(existing) = store.completed_upload(&existing_md5)? {
                return Ok(existing.md5);
            } else {
                warn!(
                    "Removing stale index entry {} -> {} because metadata is missing",
                    index_key, existing_md5
                );
                store.remove_index_md5(index_key)?;
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

    if let Some(store) = metadata_store {
        if let Some(existing) = store.completed_upload(&md5_key)? {
            info!(
                "Existing upload found for md5 {}; skipping upload (path: {}, compressed: {})",
                existing.md5, original_path, use_compression
            );
            if let Some(index_key) = index_key_owned.as_deref() {
                store.upsert_index_md5(index_key, &existing.md5)?;
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

    for (key, value) in metadata.iter() {
        request = request.metadata(key, value);
    }

    if compressed {
        request = request.content_encoding("zstd");
    }

    request.send().await.map_err(|e| {
        error!("Failed to upload to S3: {}", e);
        S3Error::UploadFailed(e.to_string())
    })?;

    let key = final_s3_key.expect("S3 key should be set after MD5 calculation");

    if let Some(store) = metadata_store {
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
        store.persist_upload_record(&record)?;

        if let Some(index_key) = index_key_owned.as_deref() {
            store.upsert_index_md5(index_key, &record.md5)?;
        }
    }

    Ok(key)
}

/// Upload a file to S3, optionally with compression (default true). If `s3_key`
/// is `None`, the MD5 hash of the uncompressed file will be used as the key.
pub async fn upload_file_to_s3(
    client: &S3Client,
    bucket: &str,
    file_path: &StdPath,
    s3_key: Option<&str>,
    use_compression: Option<bool>,
    compression_level: Option<i32>,
    metadata: Option<HashMap<String, String>>,
    metadata_store: Option<&S3MetadataStore>,
    index_key: Option<&str>,
) -> Result<String, S3Error> {
    let use_compression = use_compression.unwrap_or(true);
    let compression_level = compression_level.unwrap_or(19);
    let mut metadata = metadata.unwrap_or_default();
    let index_key_owned = index_key.map(|k| k.to_string());

    if index_key_owned.is_some() && metadata_store.is_none() {
        return Err(S3Error::DatabaseError(
            "Database handle is required when using index_key".to_string(),
        ));
    }

    if let (Some(store), Some(index_key)) = (metadata_store, index_key_owned.as_deref()) {
        if let Some(existing_md5) = store.lookup_index_md5(index_key)? {
            if let Some(existing) = store.completed_upload(&existing_md5)? {
                info!(
                    "Found existing upload for index key {} -> {}",
                    index_key, existing_md5
                );
                return Ok(existing.md5);
            } else {
                warn!(
                    "Removing stale index entry {} -> {} because metadata is missing",
                    index_key, existing_md5
                );
                store.remove_index_md5(index_key)?;
            }
        }
    }

    let file_path_value = file_path.to_string_lossy().into_owned();
    metadata.insert("file_path".to_string(), file_path_value);
    let file_ext_value = file_path
        .extension()
        .map(|ext| ext.to_string_lossy().into_owned())
        .unwrap_or_else(|| "".to_string());
    metadata.insert("file_ext".to_string(), file_ext_value);

    if use_compression {
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
            metadata_store,
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

    if let Some(store) = metadata_store {
        if let Some(existing) = store.completed_upload(&key)? {
            info!(
                "Existing upload found for md5 {}; skipping upload (path: {}, compressed: false)",
                existing.md5, original_path
            );
            if let Some(index_key) = index_key_owned.as_deref() {
                store.upsert_index_md5(index_key, &existing.md5)?;
            }
            return Ok(existing.md5);
        }
    }

    let body = ByteStream::from_path(file_path).await.map_err(|e| {
        error!("Failed to stream file for S3 upload: {}", e);
        S3Error::FileReadError
    })?;

    let mut request = client.put_object().bucket(bucket).key(&key).body(body);

    for (key, value) in metadata.iter() {
        request = request.metadata(key, value);
    }

    request.send().await.map_err(|e| {
        error!("Failed to upload to S3: {}", e);
        S3Error::UploadFailed(e.to_string())
    })?;

    if let Some(store) = metadata_store {
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
        store.persist_upload_record(&record)?;

        if let Some(index_key) = index_key_owned.as_deref() {
            store.upsert_index_md5(index_key, &record.md5)?;
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
    metadata_store: &S3MetadataStore,
    index_key: &str,
    expires_in_seconds: u64,
) -> Result<String, S3Error> {
    let md5 = metadata_store.lookup_index_md5(index_key)?.ok_or_else(|| {
        let msg = format!("Index key not found: {}", index_key);
        S3Error::DatabaseError(msg)
    })?;

    generate_presigned_url_by_md5(client, bucket, &md5, expires_in_seconds).await
}

/// Backup all KVDB-stored upload metadata to S3
pub async fn backup_metadata_to_s3(
    client: &S3Client,
    bucket: &str,
    metadata_store: &S3MetadataStore,
    backup_key: &str,
) -> Result<usize, S3Error> {
    let record_count = metadata_store.upload_records()?.len();
    let backup_bytes = metadata_store.backup_bytes()?;

    client
        .put_object()
        .bucket(bucket)
        .key(backup_key)
        .metadata("metadata_backup", "kvdb_backup")
        .content_type("application/octet-stream")
        .body(ByteStream::from(backup_bytes))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to upload metadata backup to S3: {}", e);
            S3Error::BackupFailed(e.to_string())
        })?;

    info!(
        "Backed up {} metadata records to s3://{}/{}",
        record_count, bucket, backup_key
    );

    Ok(record_count)
}

/// Restore KVDB-stored upload metadata from an S3 backup
pub async fn restore_metadata_from_s3(
    client: &S3Client,
    bucket: &str,
    metadata_store: &S3MetadataStore,
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

    let restored = metadata_store.restore_from_backup_bytes(&backup_bytes, clear_existing)?;

    info!(
        "Restored {} metadata records from s3://{}/{}",
        restored, bucket, backup_key
    );

    Ok(restored)
}

/// Struct-based helper that keeps the S3 client, bucket, and DB handy.
pub struct S3Helper {
    client: S3Client,
    bucket: String,
    metadata: Option<S3MetadataStore>,
    default_use_compression: bool,
    default_compression_level: i32,
}

impl S3Helper {
    pub async fn from_config(config: S3Config, db: Option<KVDB>) -> Result<Self, S3Error> {
        let client = create_s3_client(&config).await?;
        Self::new(
            client,
            config.bucket.clone(),
            db,
            config.use_compression,
            config.compression_level,
        )
    }

    pub fn new(
        client: S3Client,
        bucket: String,
        db: Option<KVDB>,
        default_use_compression: Option<bool>,
        default_compression_level: Option<i32>,
    ) -> Result<Self, S3Error> {
        let metadata = match db {
            Some(db) => Some(S3MetadataStore::new(db)?),
            None => None,
        };

        Ok(Self {
            client,
            bucket,
            metadata,
            default_use_compression: default_use_compression.unwrap_or(true),
            default_compression_level: default_compression_level.unwrap_or(19),
        })
    }

    pub fn client(&self) -> &S3Client {
        &self.client
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn metadata_store(&self) -> Option<&S3MetadataStore> {
        self.metadata.as_ref()
    }

    pub async fn upload_file(
        &self,
        file_path: &StdPath,
        index_key: Option<&str>,
    ) -> Result<String, S3Error> {
        self.upload_file_with_options(file_path, index_key, None, None, None, None)
            .await
    }

    pub async fn upload_file_with_options(
        &self,
        file_path: &StdPath,
        index_key: Option<&str>,
        s3_key: Option<&str>,
        use_compression: Option<bool>,
        compression_level: Option<i32>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<String, S3Error> {
        let use_compression = use_compression.unwrap_or(self.default_use_compression);
        let compression_level = compression_level.unwrap_or(self.default_compression_level);
        upload_file_to_s3(
            &self.client,
            &self.bucket,
            file_path,
            s3_key,
            Some(use_compression),
            Some(compression_level),
            metadata,
            self.metadata.as_ref(),
            index_key,
        )
        .await
    }

    pub async fn upload_bytes(
        &self,
        bytes: Vec<u8>,
        index_key: Option<&str>,
    ) -> Result<String, S3Error> {
        self.upload_bytes_with_option(bytes, index_key, None, None, None, None)
            .await
    }

    pub async fn upload_bytes_with_option(
        &self,
        bytes: Vec<u8>,
        index_key: Option<&str>,
        s3_key: Option<&str>,
        use_compression: Option<bool>,
        compression_level: Option<i32>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<String, S3Error> {
        let use_compression = use_compression.unwrap_or(self.default_use_compression);
        let compression_level = compression_level.unwrap_or(self.default_compression_level);
        upload_bytes_to_s3(
            &self.client,
            &self.bucket,
            bytes,
            s3_key,
            Some(use_compression),
            Some(compression_level),
            metadata,
            self.metadata.as_ref(),
            index_key,
        )
        .await
    }

    pub async fn presign(&self, key: &str, expires_in_seconds: u64) -> Result<String, S3Error> {
        generate_presigned_url(&self.client, &self.bucket, key, expires_in_seconds).await
    }

    pub async fn presign_by_md5(
        &self,
        md5: &str,
        expires_in_seconds: u64,
    ) -> Result<String, S3Error> {
        generate_presigned_url_by_md5(&self.client, &self.bucket, md5, expires_in_seconds).await
    }

    pub async fn presign_by_index_key(
        &self,
        index_key: &str,
        expires_in_seconds: u64,
    ) -> Result<String, S3Error> {
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| S3Error::DatabaseError("DB handle is required".to_string()))?;
        generate_presigned_url_by_index_key(
            &self.client,
            &self.bucket,
            metadata,
            index_key,
            expires_in_seconds,
        )
        .await
    }

    pub async fn backup_metadata(&self, backup_key: &str) -> Result<usize, S3Error> {
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| S3Error::DatabaseError("DB handle is required".to_string()))?;
        backup_metadata_to_s3(&self.client, &self.bucket, metadata, backup_key).await
    }

    pub async fn restore_metadata(
        &self,
        backup_key: &str,
        clear_existing: bool,
    ) -> Result<usize, S3Error> {
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| S3Error::DatabaseError("DB handle is required".to_string()))?;
        restore_metadata_from_s3(
            &self.client,
            &self.bucket,
            metadata,
            backup_key,
            clear_existing,
        )
        .await
    }

    pub fn get_file_metadata_by_md5(&self, md5: &str) -> Result<Option<UploadRecord>, S3Error> {
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| S3Error::DatabaseError("DB handle is required".to_string()))?;
        metadata.completed_upload(md5)
    }

    pub fn get_file_metadata_by_index_key(
        &self,
        index_key: &str,
    ) -> Result<Option<UploadRecord>, S3Error> {
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| S3Error::DatabaseError("DB handle is required".to_string()))?;

        let Some(md5) = metadata.lookup_index_md5(index_key)? else {
            return Ok(None);
        };

        match metadata.completed_upload(&md5)? {
            Some(record) => Ok(Some(record)),
            None => {
                warn!(
                    "Metadata missing for index key {} -> {}, removing index entry",
                    index_key, md5
                );
                metadata.remove_index_md5(index_key)?;
                Ok(None)
            }
        }
    }

    pub async fn object_exists(&self, key: &str) -> bool {
        check_s3_file_exists(&self.client, &self.bucket, key).await
    }
}
