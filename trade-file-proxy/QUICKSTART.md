# Quick Start Guide - S3 URL Feature

## Overview

This guide will help you quickly set up and use the new `get_s3_url` endpoint.

## What's New?

The `/get_s3_url` endpoint allows you to:
- ✅ Get a shareable S3 presigned URL for any local file
- ✅ Automatically upload files to S3 if they don't exist
- ✅ Compress files with ZSTD before upload (configurable)
- ✅ Generate time-limited download URLs (1 hour validity)

## Setup

### 1. Configure S3/R2/B2

Copy the example config and edit with your credentials:

```bash
cp config/config.example.yaml config/config.yaml
nano config/config.yaml
```

**For Cloudflare R2:**
```yaml
s3:
  provider: "r2"
  access_key_id: "YOUR_R2_ACCESS_KEY"
  secret_access_key: "YOUR_R2_SECRET_KEY"
  bucket: "your-bucket-name"
  region: "auto"
  endpoint: "https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com"
  force_path_style: false
  use_compression: true
```

**For Backblaze B2:**
```yaml
s3:
  provider: "b2"
  access_key_id: "YOUR_KEY_ID"
  secret_access_key: "YOUR_APPLICATION_KEY"
  bucket: "your-bucket"
  region: "us-west-004"
  endpoint: "https://s3.us-west-004.backblazeb2.com"
  force_path_style: false
  use_compression: true
```

### 2. Build and Run

```bash
# Build release version
cargo build --release

# Run with config
CONFIG_PATH=config/config.yaml ./target/release/file-proxy

# Or set environment variable
export CONFIG_PATH=config/config.yaml
export FILE_PROXY_DIR=/path/to/your/data
./target/release/file-proxy
```

### 3. Test the Endpoint

```bash
# Assuming you have a file at /data/trades/btc_usdt.csv
curl "http://localhost:3000/get_s3_url?file=trades/btc_usdt.csv"
```

**Response:**
```json
{
  "url": "https://your-bucket.r2.cloudflarestorage.com/trades/btc_usdt.csv.zstd?X-Amz-...",
  "uploaded": true,
  "compressed": true
}
```

### 4. Use the URL

The returned URL can be shared with anyone and is valid for 1 hour:

```bash
# Download from the presigned URL
curl "https://your-bucket.r2.cloudflarestorage.com/trades/btc_usdt.csv.zstd?X-Amz-..." -o file.zstd

# Decompress if compressed=true
zstd -d file.zstd
```

## Python Example

```python
import requests
import zstandard as zstd

# Get S3 URL
response = requests.get(
    "http://localhost:3000/get_s3_url",
    params={"file": "trades/btc_usdt.csv"}
)

data = response.json()
print(f"URL: {data['url']}")
print(f"Uploaded: {data['uploaded']}")
print(f"Compressed: {data['compressed']}")

# Download from S3
s3_response = requests.get(data['url'])

# Decompress if needed
if data['compressed']:
    dctx = zstd.ZstdDecompressor()
    content = dctx.decompress(s3_response.content)
else:
    content = s3_response.content

# Save to file
with open('output.csv', 'wb') as f:
    f.write(content)
```

## Use Cases

1. **Large File Distribution**: Generate shareable links without serving files directly
2. **Bandwidth Optimization**: Offload download traffic to S3/R2
3. **Client Downloads**: Provide time-limited access to specific files
4. **Automated Archiving**: Upload files to cloud storage on-demand

## Troubleshooting

### S3 Not Configured
```
Error: S3 is not configured
```
**Solution**: Ensure `config.yaml` exists and is properly configured. Check `CONFIG_PATH` environment variable.

### File Not Found
```
Error: File not found
```
**Solution**: Verify the file exists in `FILE_PROXY_DIR`. Check file path is correct (relative to base directory).

### Upload Failed
```
Error: Failed to upload to S3
```
**Solution**: 
- Verify credentials are correct
- Check bucket name and endpoint URL
- Ensure bucket exists and you have write permissions
- Check network connectivity to S3 endpoint

### Presign Failed
```
Error: Failed to generate presigned URL
```
**Solution**: Verify your credentials have permission to read from the bucket.

## Configuration Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `provider` | Provider name (for logging) | Yes | - |
| `access_key_id` | Access key ID | Yes | - |
| `secret_access_key` | Secret access key | Yes | - |
| `bucket` | Bucket name | Yes | - |
| `region` | Region identifier | Yes | - |
| `endpoint` | Custom S3 endpoint | No | AWS default |
| `force_path_style` | Use path-style URLs | No | `false` |
| `use_compression` | Enable ZSTD compression | No | `true` |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CONFIG_PATH` | Path to config.yaml | `config/config.yaml` |
| `FILE_PROXY_DIR` | Base directory for files | `/data` |
| `PORT` | Server port | `3000` |

## API Endpoints

| Endpoint | Description | Status |
|----------|-------------|--------|
| `GET /health` | Health check | Existing |
| `GET /ls` | List directory | Existing |
| `GET /download` | Download file (direct) | Existing |
| `GET /get_s3_url` | Get S3 presigned URL | **NEW** ✨ |

## Next Steps

- See [API.md](API.md) for complete API documentation
- Check [config/config.yaml](config/config.yaml) for all configuration options
- Review server logs for debugging

## Performance Tips

1. **Enable Compression**: Set `use_compression: true` to reduce storage and bandwidth costs
2. **Reuse URLs**: The same file won't be re-uploaded if it already exists in S3
3. **Monitor Storage**: Keep track of your S3 bucket size and costs
4. **Cache URLs**: Store presigned URLs temporarily if you need to share them multiple times within the hour

## Security Notes

- ⚠️ Presigned URLs are valid for 1 hour
- ⚠️ Anyone with the URL can download the file during this time
- ⚠️ Keep your S3 credentials secure (never commit config.yaml with real credentials)
- ⚠️ Use HTTPS in production
- ⚠️ Consider setting up CORS policies on your S3 bucket if needed

