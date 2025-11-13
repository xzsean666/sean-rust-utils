# HTTP Server Usage Guide

## Overview

The trade data processor now supports fetching data via HTTP in addition to SSH. This is more efficient and easier to configure.

## HTTP API Endpoints

The HTTP server should implement these two endpoints:

### 1. List Files
```
GET /ls?dir={path}
```
Returns a JSON array of file information:
```json
[
  {
    "name": "binance_mark_price_2025-11-05.jsonl",
    "is_dir": false,
    "size": 206266541
  }
]
```

### 2. Download File
```
GET /download?file={path}
```
Returns the raw file contents as bytes.

## Configuration

Create a configuration file (e.g., `config/mark-price-http.config.yaml`):

```yaml
data_sources:
  - data_type: "mark-price"
    
    # HTTP servers (new)
    http_servers:
      - base_url: "http://198.50.126.194:10048"
        input_base_path: "mark-price"
    
    # SSH servers (optional, can coexist with HTTP)
    # ssh_servers:
    #   - host: "127.0.0.1"
    #     port: 22
    #     username: "user"
    #     private_key_path: "./ssh_keys/key"
    #     input_base_path: "/data/mark-price"

output:
  path: "/mnt/b2fs/mark-price"
  name: "mark-price"
```

## Usage Examples

### List files for a specific date
```bash
curl "http://198.50.126.194:10048/ls?dir=mark-price/2025/11/05/"
```

### Download a file
```bash
curl -O -J "http://198.50.126.194:10048/download?file=mark-price/2025/11/05/binance_mark_price_2025-11-05.jsonl"
```

### Run the processor
```bash
./trade-data-processor \
  --config config/mark-price-http.config.yaml \
  --date 2025-11-05 \
  --data-type mark-price
```

## How it Works

1. The processor builds the path: `{input_base_path}/{year}/{month}/{day}`
   - Example: `mark-price/2025/11/05`

2. It calls the `/ls` endpoint to list all `.jsonl` files in that directory

3. It downloads all files in parallel using the `/download` endpoint

4. Data from all sources (HTTP and SSH if both configured) is merged and deduplicated

5. Forward-fill is applied to fill gaps

6. Results are written to Parquet files

## Benefits of HTTP vs SSH

- **Simpler**: No SSH keys or authentication setup required
- **Faster**: No SSH handshake overhead
- **Scalable**: HTTP servers can use CDNs and load balancers
- **Flexible**: Can mix HTTP and SSH sources in the same configuration
- **Portable**: Works across different network environments more easily

## Mixed Configuration

You can use both HTTP and SSH servers simultaneously:

```yaml
data_sources:
  - data_type: "mark-price"
    http_servers:
      - base_url: "http://198.50.126.194:10048"
        input_base_path: "mark-price"
    ssh_servers:
      - host: "127.0.0.1"
        port: 22
        username: "astrid"
        private_key_path: "./ssh_keys/sean"
        input_base_path: "/hdd16/trade/wss-collector/data/mark-price"
```

All sources will be processed and merged together with deduplication.

