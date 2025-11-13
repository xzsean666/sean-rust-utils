# S3 Helper 使用指南

S3 Helper 模块提供了一个统一的接口来与多个S3兼容的存储服务进行交互。

## 支持的存储服务

- **AWS S3** - Amazon Web Services S3
- **Backblaze B2** - Backblaze B2 Cloud Storage
- **Cloudflare R2** - Cloudflare R2 Storage
- **MinIO** - Self-hosted S3-compatible storage
- **其他S3兼容服务** - 任何支持S3 API的存储服务

## 配置

在你的YAML配置文件中添加S3配置:

### AWS S3 配置示例

```yaml
s3:
  provider: "aws"
  bucket: "my-trading-data"
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  region: "us-east-1"
  # AWS S3 不需要自定义endpoint
  # force_path_style默认为false（使用虚拟主机风格）
  base_path: "trade-data/mark-price"
```

### Backblaze B2 配置示例

```yaml
s3:
  provider: "b2"
  bucket: "my-b2-bucket"
  access_key_id: "your_b2_key_id"
  secret_access_key: "your_b2_application_key"
  region: "us-west-002"  # B2区域
  endpoint: "https://s3.us-west-002.backblazeb2.com"
  force_path_style: true
  base_path: "trade-data"
```

### Cloudflare R2 配置示例

```yaml
s3:
  provider: "r2"
  bucket: "my-r2-bucket"
  access_key_id: "your_r2_access_key_id"
  secret_access_key: "your_r2_secret_access_key"
  endpoint: "https://your-account-id.r2.cloudflarestorage.com"
  force_path_style: true
  base_path: "trading"
```

### MinIO 配置示例

```yaml
s3:
  provider: "generic"
  bucket: "trading-data"
  access_key_id: "minioadmin"
  secret_access_key: "minioadmin"
  endpoint: "http://localhost:9000"
  force_path_style: true
```

## Rust 代码使用示例

### 基本使用

```rust
use trade_data_processor::{S3Config, S3Helper};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 从配置创建S3客户端
    let s3_config = S3Config {
        provider: "aws".to_string(),
        bucket: "my-bucket".to_string(),
        access_key_id: "YOUR_KEY".to_string(),
        secret_access_key: "YOUR_SECRET".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: None,
        force_path_style: None,
        base_path: Some("data".to_string()),
    };
    
    let s3 = S3Helper::new(s3_config).await?;
    
    Ok(())
}
```

### 上传文件

```rust
// 上传本地文件到S3
let etag = s3.upload_file("local_file.parquet", "data/2025/11/10/file.parquet").await?;
println!("Uploaded with ETag: {}", etag);

// 上传字节数据
use bytes::Bytes;
let data = Bytes::from("Hello, S3!");
let etag = s3.upload_bytes(data, "data/hello.txt").await?;
```

### 下载文件

```rust
// 下载文件到本地
s3.download_file("data/2025/11/10/file.parquet", "local_file.parquet").await?;

// 下载为字节
let data = s3.download_bytes("data/hello.txt").await?;
let content = String::from_utf8(data.to_vec())?;
println!("Content: {}", content);
```

### 列出对象

```rust
// 列出指定前缀的对象（最多1000个）
let objects = s3.list_objects("data/2025/11/", Some(1000)).await?;
for key in objects {
    println!("Found: {}", key);
}

// 列出所有对象（自动分页）
let all_objects = s3.list_all_objects("data/2025/").await?;
println!("Total objects: {}", all_objects.len());
```

### 检查对象是否存在

```rust
let exists = s3.object_exists("data/2025/11/10/file.parquet").await?;
if exists {
    println!("File exists!");
}
```

### 删除对象

```rust
// 删除单个对象
s3.delete_object("data/old_file.parquet").await?;

// 批量删除
let keys = vec![
    "data/file1.parquet".to_string(),
    "data/file2.parquet".to_string(),
    "data/file3.parquet".to_string(),
];
let deleted_count = s3.delete_objects_batch(keys).await?;
println!("Deleted {} objects", deleted_count);
```

### 复制对象

```rust
// 在S3内部复制对象（同一个bucket）
s3.copy_object(
    "data/2025/11/09/file.parquet",
    "backup/2025/11/09/file.parquet"
).await?;
```

### 获取对象元数据

```rust
let (size, last_modified) = s3.get_object_metadata("data/file.parquet").await?;
println!("Size: {} bytes", size);
println!("Last modified: {}", last_modified);
```

### 批量上传文件

```rust
let files = vec![
    ("local1.parquet", "data/remote1.parquet".to_string()),
    ("local2.parquet", "data/remote2.parquet".to_string()),
    ("local3.parquet", "data/remote3.parquet".to_string()),
];

let uploaded_count = s3.upload_files_batch(files).await?;
println!("Uploaded {} files", uploaded_count);
```

### 文件夹同步功能

S3 Helper 提供了强大的文件夹同步功能，支持三种同步方向，使用本地KV数据库避免重复上传。

#### 同步方向

`sync_folder` 函数支持三种同步方向：

- **LocalToS3** - 单向同步：本地 → S3
- **S3ToLocal** - 单向同步：S3 → 本地
- **Bidirectional** - 双向同步：本地 ↔ S3

#### 单向同步：本地 → S3

```rust
use trade_data_processor::s3_helper::{SyncDirection, SyncOptions, SyncStats};

// 配置同步选项
let mut options = SyncOptions::default();
options.direction = SyncDirection::LocalToS3;  // 本地到S3
options.force = false;       // 是否强制重新检查所有文件
options.delete = false;      // 是否删除S3上不存在于本地的文件
options.dry_run = false;     // 是否只预览不实际操作

// 同步文件夹
let stats = s3.sync_folder(
    "./data",                // 本地文件夹
    "backups/data",          // S3前缀
    "./.sync_cache",         // 同步数据库路径
    options
).await?;

println!("Uploaded: {}", stats.files_uploaded);
println!("Skipped: {}", stats.files_skipped);
println!("Errors: {}", stats.errors);
```

#### 单向同步：S3 → 本地

```rust
use trade_data_processor::s3_helper::{SyncDirection, SyncOptions};

// 配置同步选项
let mut options = SyncOptions::default();
options.direction = SyncDirection::S3ToLocal;  // S3到本地
options.force = false;       // 是否强制重新检查所有文件
options.delete = false;      // 是否删除本地不存在于S3的文件
options.dry_run = false;     // 是否只预览不实际操作

// 同步文件夹（从S3下载到本地）
let stats = s3.sync_folder(
    "./data",                // 本地文件夹
    "backups/data",          // S3前缀
    "./.sync_cache",         // 同步数据库路径
    options
).await?;

println!("Downloaded: {}", stats.files_downloaded);
println!("Skipped: {}", stats.files_skipped);
println!("Errors: {}", stats.errors);
```

#### 双向同步（本地 ↔ S3）

```rust
use trade_data_processor::s3_helper::{SyncDirection, SyncOptions};

// 配置同步选项
let mut options = SyncOptions::default();
options.direction = SyncDirection::Bidirectional;  // 双向同步
options.force = false;
options.delete = true;       // 在双向同步中谨慎使用delete
options.dry_run = false;

// 双向同步：上传本地新文件，下载S3上更新的文件
let stats = s3.sync_folder(
    "./data",
    "backups/data",
    "./.sync_cache",
    options
).await?;

println!("Uploaded: {}", stats.files_uploaded);
println!("Downloaded: {}", stats.files_downloaded);
println!("Skipped: {}", stats.files_skipped);
```

#### 强制同步

强制同步会忽略本地缓存，重新检查所有文件：

```rust
let mut options = SyncOptions::default();
options.direction = SyncDirection::LocalToS3;
options.force = true;  // 强制重新检查

let stats = s3.sync_folder(
    "./data",
    "backups/data",
    "./.sync_cache",
    options
).await?;
```

#### 同步并删除

删除目标中不存在于源的文件（根据同步方向）：

```rust
let mut options = SyncOptions::default();
options.direction = SyncDirection::LocalToS3;  // 会删除S3上不存在于本地的文件
options.delete = true;  // 启用删除

let stats = s3.sync_folder(
    "./data",
    "backups/data",
    "./.sync_cache",
    options
).await?;

println!("Deleted: {}", stats.files_deleted);
```

**注意**：在 `S3ToLocal` 模式下使用 `delete = true` 会删除本地不存在于S3的文件，请谨慎使用！

#### 自定义排除规则

```rust
let mut options = SyncOptions::default();
options.direction = SyncDirection::Bidirectional;
options.exclude_patterns = vec![
    ".git".to_string(),
    "*.tmp".to_string(),
    "*.log".to_string(),
    "node_modules".to_string(),
];

let stats = s3.sync_folder(
    "./data",
    "backups/data",
    "./.sync_cache",
    options
).await?;
```

#### 干运行（预览）

在实际同步前预览将要执行的操作：

```rust
let mut options = SyncOptions::default();
options.direction = SyncDirection::LocalToS3;
options.dry_run = true;  // 只预览，不实际操作

let stats = s3.sync_folder(
    "./data",
    "backups/data",
    "./.sync_cache",
    options
).await?;

// 显示将要执行的操作，但不实际上传/下载
```

#### 同步功能特性

- **智能增量同步**：使用SHA256哈希和文件元数据判断文件是否改变
- **本地缓存数据库**：使用sled嵌入式数据库缓存文件状态，避免频繁访问S3
- **并行处理**：支持配置最大并行上传/下载数
- **错误恢复**：单个文件失败不影响其他文件的同步
- **详细统计**：提供完整的同步统计信息
- **排除规则**：支持通配符模式排除特定文件或目录

#### 同步数据库

同步功能使用本地KV数据库（sled）存储文件元数据：

- **存储内容**：文件路径、大小、修改时间、SHA256哈希、ETag、最后同步时间
- **自动管理**：数据库会自动创建和更新
- **持久化**：即使程序重启，同步状态也会保留
- **清理**：可以手动删除数据库目录强制重新同步所有文件

```rust
use trade_data_processor::s3_helper::SyncDatabase;

// 手动操作同步数据库
let db = SyncDatabase::open("./.sync_cache")?;

// 列出所有跟踪的文件
let files = db.list_all()?;
for file in files {
    println!("{}: {} bytes, hash: {}", file.path, file.size, file.hash);
}

// 清空数据库（下次同步会重新检查所有文件）
db.clear()?;
```

## 完整示例：备份Parquet文件到S3

```rust
use trade_data_processor::{S3Config, S3Helper};
use anyhow::Result;
use std::path::Path;

async fn backup_to_s3(
    local_file: &str,
    s3_config: S3Config,
    date: &str,
) -> Result<()> {
    // 创建S3客户端
    let s3 = S3Helper::new(s3_config).await?;
    
    // 构建S3路径
    let file_name = Path::new(local_file)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    let s3_key = format!("backups/{}/{}", date, file_name);
    
    // 上传文件
    println!("Uploading {} to s3://{}/{}", local_file, s3.bucket(), s3_key);
    let etag = s3.upload_file(local_file, &s3_key).await?;
    
    // 验证上传
    let exists = s3.object_exists(&s3_key).await?;
    if exists {
        println!("Upload successful! ETag: {}", etag);
        
        // 获取元数据
        let (size, last_modified) = s3.get_object_metadata(&s3_key).await?;
        println!("Size: {} bytes, Last modified: {}", size, last_modified);
    } else {
        println!("Upload verification failed!");
    }
    
    Ok(())
}
```

## 最佳实践

### 1. 路径风格设置

- **AWS S3**: 使用虚拟主机风格（`force_path_style: false` 或不设置）
- **B2/R2/MinIO**: 使用路径风格（`force_path_style: true`）

### 2. 区域设置

- **AWS S3**: 必须指定正确的区域（如 `us-east-1`）
- **B2**: 使用B2的区域代码（如 `us-west-002`）
- **R2**: 使用 `auto`
- **MinIO**: 可以使用任意值或不设置

### 3. Endpoint设置

- **AWS S3**: 不需要设置endpoint，SDK会自动选择
- **B2**: 必须设置，格式为 `https://s3.{region}.backblazeb2.com`
- **R2**: 必须设置，格式为 `https://{account-id}.r2.cloudflarestorage.com`
- **MinIO**: 必须设置为MinIO服务器地址

### 4. 错误处理

始终使用适当的错误处理：

```rust
match s3.upload_file("local.parquet", "remote.parquet").await {
    Ok(etag) => println!("Upload successful: {}", etag),
    Err(e) => eprintln!("Upload failed: {}", e),
}
```

### 5. 批量操作

对于大量文件操作，使用批量方法以提高性能：

```rust
// 好：批量上传
let files = vec![/* ... */];
s3.upload_files_batch(files).await?;

// 不好：逐个上传
for (local, remote) in files {
    s3.upload_file(local, &remote).await?;
}
```

### 6. 对象键命名

使用清晰的层级结构：

```
data/
  2025/
    11/
      10/
        1000PEPEUSDT_2025-11-10.parquet
        BTCUSDT_2025-11-10.parquet
```

### 7. 凭证安全

- 不要在代码中硬编码凭证
- 使用环境变量或配置文件
- 配置文件不要提交到Git仓库

```bash
# 环境变量方式
export S3_ACCESS_KEY_ID="your_key"
export S3_SECRET_ACCESS_KEY="your_secret"
```

## 故障排查

### 连接失败

1. 检查endpoint是否正确
2. 检查网络连接
3. 验证凭证是否有效
4. 确认region设置正确

### 权限错误

确保你的凭证有足够的权限：

- `s3:PutObject` - 上传
- `s3:GetObject` - 下载
- `s3:ListBucket` - 列表
- `s3:DeleteObject` - 删除

### 路径风格问题

如果遇到DNS解析错误，尝试设置 `force_path_style: true`

## 性能优化

### 1. 并行上传/下载

```rust
use futures::future::join_all;

let tasks: Vec<_> = files.iter().map(|(local, remote)| {
    let s3_clone = s3.clone();
    async move {
        s3_clone.upload_file(local, remote).await
    }
}).collect();

let results = join_all(tasks).await;
```

### 2. 使用批量操作

批量删除比单独删除快得多：

```rust
// 好
s3.delete_objects_batch(keys).await?;

// 慢
for key in keys {
    s3.delete_object(&key).await?;
}
```

### 3. 流式传输大文件

S3Helper内部使用ByteStream，自动支持流式传输大文件。

## 与其他模块集成

### 与ParquetWriter集成

```rust
use trade_data_processor::{ParquetWriter, ParquetWriterConfig, S3Helper};

// 1. 写入本地Parquet文件
let config = ParquetWriterConfig { /* ... */ };
let mut writer = ParquetWriter::new(config);
writer.write_rows(rows).await?;
writer.flush_buffer().await?;

// 2. 上传到S3
let s3 = S3Helper::new(s3_config).await?;
s3.upload_file("local_output.parquet", "s3/path/output.parquet").await?;
```

### 与DataMerger集成

```rust
// 处理数据并上传到S3
let merger = DataMerger::new(date);
// ... 添加数据 ...
let rows = merger.get_sorted_rows();

// 写入临时文件
let temp_file = "/tmp/merged_data.parquet";
// ... 写入parquet ...

// 上传到S3
let s3 = S3Helper::new(s3_config).await?;
let key = format!("merged/{}/data.parquet", date.format("%Y-%m-%d"));
s3.upload_file(temp_file, &key).await?;

// 清理临时文件
tokio::fs::remove_file(temp_file).await?;
```

## 示例脚本

查看项目中的示例脚本：

- `examples/s3_basic.rs` - S3基本操作示例
- `examples/s3_parquet_backup.rs` - 备份Parquet文件到S3
- `examples/s3_folder_sync.rs` - 文件夹同步基本示例
- `examples/s3_folder_sync_directions.rs` - 文件夹同步方向控制示例（新功能）

运行示例：

```bash
# 基本操作示例
cargo run --example s3_basic

# Parquet备份示例
cargo run --example s3_parquet_backup

# 文件夹同步基本示例
cargo run --example s3_folder_sync

# 文件夹同步方向控制示例（演示LocalToS3、S3ToLocal、Bidirectional）
cargo run --example s3_folder_sync_directions
```

## 相关文档

- [AWS SDK for Rust](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/welcome.html)
- [Backblaze B2 S3 API](https://www.backblaze.com/b2/docs/s3_compatible_api.html)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

