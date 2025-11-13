# File Proxy API Documentation

## Endpoints

### 1. Health Check
Check if the server is running.

**Endpoint:** `GET /health`

**Response:**
```
OK
```

---

### 2. List Files
List files and directories in a given directory.

**Endpoint:** `GET /ls`

**Query Parameters:**
- `dir` (string, required): Directory path to list (relative to base directory)

**Example:**
```bash
curl "http://localhost:3000/ls?dir=data/trades"
```

**Response:**
```json
[
  {
    "name": "file1.csv",
    "is_dir": false,
    "size": 1024
  },
  {
    "name": "subdir",
    "is_dir": true,
    "size": 4096
  }
]
```

---

### 3. Download File
Download a file directly from the server (compressed with ZSTD).

**Endpoint:** `GET /download`

**Query Parameters:**
- `file` (string, required): File path to download (relative to base directory)

**Example:**
```bash
curl "http://localhost:3000/download?file=data/trades/btc_usdt.csv" -o output.csv.zstd
```

**Response:**
- Binary data compressed with ZSTD
- Headers:
  - `Content-Type: application/octet-stream`
  - `Content-Encoding: zstd`
  - `Content-Disposition: attachment; filename="<filename>.zstd"`

**Decompression:**
```bash
# Decompress the downloaded file
zstd -d output.csv.zstd
```

---

### 4. Get S3 URL (NEW)
Get a presigned S3 URL for a file. If the file doesn't exist in S3, it will be uploaded first.

**Endpoint:** `GET /get_s3_url`

**Query Parameters:**
- `file` (string, required): File path (relative to base directory)

**Example:**
```bash
curl "http://localhost:3000/get_s3_url?file=data/trades/btc_usdt.csv"
```

**Response:**
```json
{
  "url": "https://your-bucket.r2.cloudflarestorage.com/data/trades/btc_usdt.csv.zstd?X-Amz-Algorithm=...",
  "uploaded": true,
  "compressed": true
}
```

**Response Fields:**
- `url` (string): Presigned URL valid for 1 hour
- `uploaded` (boolean): Whether the file was uploaded in this request (false if already existed)
- `compressed` (boolean): Whether the file is compressed with ZSTD

**Features:**
- **Automatic Upload**: If file doesn't exist in S3, it's uploaded automatically
- **Compression**: Files are compressed with ZSTD before upload (configurable)
- **Presigned URL**: Returns a secure, time-limited download URL
- **Idempotent**: Safe to call multiple times for the same file

**Use Cases:**
1. Share large files without direct server download
2. Offload bandwidth to S3/R2
3. Create shareable links for clients
4. Archive and distribute data efficiently

---

## Configuration

### Environment Variables

- `FILE_PROXY_DIR`: Base directory for file operations (default: `/data`)
- `PORT`: Server port (default: `3000`)
- `CONFIG_PATH`: Path to configuration file (default: `config/config.yaml`)

### Configuration File

The server requires a `config.yaml` file for S3 functionality.

**Example:**
```yaml
s3:
  provider: "r2"
  access_key_id: "YOUR_ACCESS_KEY"
  secret_access_key: "YOUR_SECRET_KEY"
  bucket: "your-bucket"
  region: "auto"
  endpoint: "https://account_id.r2.cloudflarestorage.com"
  force_path_style: false
  use_compression: true
```

**Configuration Steps:**
1. Copy example: `cp config/config.example.yaml config/config.yaml`
2. Edit `config.yaml` with your credentials
3. Run server: `CONFIG_PATH=config/config.yaml cargo run`

**Supported Providers:**
- Cloudflare R2
- Backblaze B2
- AWS S3
- MinIO
- Any S3-compatible storage

---

## Error Responses

All errors return appropriate HTTP status codes with a text message:

| Status Code | Error | Description |
|-------------|-------|-------------|
| 400 | Invalid file path | Path contains invalid characters or traversal attempts |
| 400 | Path is not a file | Requested path is a directory |
| 400 | Path is not a directory | Requested path is a file (for /ls) |
| 404 | File not found | File doesn't exist |
| 500 | Failed to read file | Internal error reading file |
| 500 | Failed to compress file | Compression error |
| 500 | Failed to upload to S3 | S3 upload failed |
| 500 | Failed to generate presigned URL | Presigning error |
| 503 | S3 is not configured | S3 config missing (for /get_s3_url) |

---

## Examples

### Python Client Example

```python
import requests
import zstandard as zstd

# List files
response = requests.get("http://localhost:3000/ls", params={"dir": "data"})
files = response.json()
print(files)

# Download file directly (compressed)
response = requests.get("http://localhost:3000/download", params={"file": "data/file.csv"})
compressed_data = response.content
# Decompress
dctx = zstd.ZstdDecompressor()
decompressed = dctx.decompress(compressed_data)

# Get S3 URL
response = requests.get("http://localhost:3000/get_s3_url", params={"file": "data/file.csv"})
result = response.json()
s3_url = result["url"]
print(f"S3 URL: {s3_url}")
print(f"Was uploaded: {result['uploaded']}")

# Download from S3 URL
s3_response = requests.get(s3_url)
s3_data = s3_response.content
# Note: If compressed=true, data is ZSTD compressed
if result["compressed"]:
    decompressed = dctx.decompress(s3_data)
```

### Bash Example

```bash
#!/bin/bash

BASE_URL="http://localhost:3000"

# List files
curl "$BASE_URL/ls?dir=data"

# Download and decompress
curl "$BASE_URL/download?file=data/trades.csv" -o trades.csv.zstd
zstd -d trades.csv.zstd

# Get S3 URL and download from S3
S3_URL=$(curl -s "$BASE_URL/get_s3_url?file=data/trades.csv" | jq -r '.url')
curl "$S3_URL" -o trades_from_s3.csv.zstd
zstd -d trades_from_s3.csv.zstd
```

---

## Security

### Path Traversal Protection

All file paths are validated to prevent directory traversal attacks:
- Leading slashes are removed
- Paths containing `..`, `./`, or `\.` are rejected
- Final paths are canonicalized and verified to be within the base directory

### S3 Presigned URLs

- URLs expire after 1 hour
- Only allow GET operations
- Use secure AWS Signature V4 signing

### Best Practices

1. **Restrict Base Directory**: Set `FILE_PROXY_DIR` to limit accessible files
2. **Use HTTPS**: Deploy behind a reverse proxy with TLS
3. **Firewall**: Restrict access to trusted IPs if possible
4. **Credentials**: Store S3 credentials securely (use environment variables or secrets manager)
5. **Monitor**: Log and monitor file access patterns

