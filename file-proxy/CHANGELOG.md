# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - 2025-11-10

### Added

#### New Endpoint: `/get_s3_url`
- **GET /get_s3_url**: Generate presigned S3 URLs for files
  - Automatically checks if file exists in S3
  - Uploads file to S3 if not present
  - Optional ZSTD compression (enabled by default)
  - Returns presigned URL valid for 1 hour
  - Response includes upload status and compression info

#### Configuration System
- **config.yaml Support**: S3/R2/B2 configuration via YAML file
  - Flexible S3-compatible storage configuration
  - Support for multiple providers (R2, B2, S3, MinIO, etc.)
  - Optional compression settings
  - Custom endpoint support
  - Path-style vs virtual-hosted style addressing

#### Documentation
- **API.md**: Complete API documentation with examples
- **QUICKSTART.md**: Quick start guide for the S3 URL feature
- **config.example.yaml**: Example configuration with multiple providers
- **config.yaml**: Default configuration template

### Changed

#### Dependencies
- Added `aws-sdk-s3` v1.13 for S3 operations
- Added `aws-config` v1.1 for AWS SDK configuration
- Added `aws-credential-types` v1.1 for credentials management
- Added `serde_yaml` v0.9 for YAML configuration parsing

#### Core Application
- **AppState**: Extended to include S3 client and configuration
  - `s3_client: Option<Arc<S3Client>>`: Shared S3 client instance
  - `s3_config: Option<Arc<S3Config>>`: S3 configuration settings

- **Error Handling**: Added new error types
  - `S3NotConfigured`: S3 features not available
  - `S3UploadError`: File upload to S3 failed
  - `S3PresignError`: Presigned URL generation failed

#### Configuration
- **Environment Variables**:
  - `CONFIG_PATH`: Path to config.yaml (default: `config/config.yaml`)
  - Existing: `FILE_PROXY_DIR`, `PORT`

### Technical Details

#### New Functions

1. **load_config(path: &str) -> Option<Config>**
   - Asynchronously loads YAML configuration
   - Graceful error handling with warnings

2. **create_s3_client(config: &S3Config) -> Result<S3Client>**
   - Creates AWS S3 client from configuration
   - Supports custom endpoints and regions
   - Handles credentials and path-style addressing

3. **get_s3_url(state, query) -> Result<Json<S3UrlResponse>>**
   - Main endpoint handler
   - Validates file path
   - Checks S3 for existing file
   - Uploads if needed
   - Generates presigned URL

4. **check_s3_file_exists(client, bucket, key) -> bool**
   - Checks if file exists in S3 bucket
   - Uses HEAD request for efficiency

5. **upload_file_to_s3(client, bucket, file_path, s3_key, use_compression) -> Result<()>**
   - Reads local file
   - Optional ZSTD compression
   - Uploads to S3 with proper metadata

6. **generate_presigned_url(client, bucket, key, expires_in) -> Result<String>**
   - Generates time-limited presigned URLs
   - Default expiration: 1 hour (3600 seconds)

#### Data Structures

```rust
struct S3Config {
    provider: String,
    access_key_id: String,
    secret_access_key: String,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    force_path_style: Option<bool>,
    use_compression: Option<bool>,
}

struct S3UrlResponse {
    url: String,
    uploaded: bool,
    compressed: bool,
}
```

### Security

- **Path Validation**: All file paths validated to prevent directory traversal
- **Presigned URLs**: Time-limited URLs (1 hour expiration)
- **Credentials**: Loaded from configuration file (not hardcoded)
- **S3 Permissions**: Only requires read/write to specified bucket

### Performance

- **Lazy Upload**: Files only uploaded if not already in S3
- **Compression**: ZSTD compression reduces storage and bandwidth
- **Async Operations**: All I/O operations are asynchronous
- **Shared Client**: S3 client shared via Arc for efficiency

### Compatibility

- **Backward Compatible**: All existing endpoints remain unchanged
- **Optional Feature**: S3 features disabled if not configured
- **Provider Agnostic**: Works with any S3-compatible storage
  - Cloudflare R2
  - Backblaze B2
  - AWS S3
  - MinIO
  - DigitalOcean Spaces
  - Wasabi
  - etc.

### Breaking Changes

None. This is a purely additive update.

### Migration Guide

No migration needed. To enable new S3 features:

1. Create `config/config.yaml` from example
2. Fill in your S3/R2/B2 credentials
3. Restart server with `CONFIG_PATH` environment variable
4. New `/get_s3_url` endpoint will be available

If no configuration is provided, the server runs as before with S3 features disabled.

### Future Enhancements (Ideas)

- [ ] Batch upload support
- [ ] Custom expiration time for presigned URLs
- [ ] File deletion from S3
- [ ] List S3 bucket contents
- [ ] Sync local directory to S3
- [ ] Metadata tagging for uploaded files
- [ ] Multi-part upload for large files
- [ ] Progress reporting for uploads
- [ ] Webhook notifications after upload
- [ ] Cache layer for presigned URLs

### Known Limitations

- Presigned URLs expire after 1 hour (hardcoded)
- Files are re-uploaded if S3 key changes
- No automatic cleanup of old S3 files
- No progress indication for large file uploads
- Single-threaded upload (no multi-part)

---

## [0.1.0] - Previous Version

### Initial Features
- File download with ZSTD compression
- Directory listing
- Health check endpoint
- Path traversal protection
- Docker support

