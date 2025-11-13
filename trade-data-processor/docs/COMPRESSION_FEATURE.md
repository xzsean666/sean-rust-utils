# S3 Sync 压缩功能

## 概述

S3Helper 的 `sync_folder` 功能现在支持 zstd 压缩。启用此功能后：
- **本地文件**：保持原始未压缩状态
- **S3 存储**：自动压缩存储，文件名添加 `.zst` 扩展名
- 上传时自动压缩，下载时自动解压

## 特性

- ✅ 使用高效的 zstd 压缩算法（压缩级别3，平衡速度和压缩率）
- ✅ 透明的压缩/解压过程
- ✅ 自动文件名管理（S3 上添加 `.zst` 后缀）
- ✅ 临时文件自动清理
- ✅ 完整的错误处理

## 使用方法

### 1. 代码中启用压缩

```rust
use trade_data_processor::s3_helper::{S3Helper, SyncOptions, SyncDirection};

let mut options = SyncOptions::default();
options.use_compression = true;  // 启用压缩

// 同步文件夹
let stats = s3_helper.sync_folder(
    "./data/local",      // 本地文件夹
    "data/remote",       // S3 前缀
    "./.sync_cache.db",  // 同步缓存数据库
    options
).await?;
```

### 2. 命令行示例

使用压缩同步文件到 R2：

```bash
# 启用压缩同步
cargo run --example r2_sync_mark_price -- \
    --config config/r2.config.yaml \
    --compress

# 结合其他选项使用
cargo run --example r2_sync_mark_price -- \
    --config config/r2.config.yaml \
    --compress \
    --dry-run    # 预览模式
```

### 3. 双向同步

压缩功能支持所有同步方向：

#### 本地 → S3（上传并压缩）
```rust
options.direction = SyncDirection::LocalToS3;
options.use_compression = true;
```

#### S3 → 本地（下载并解压）
```rust
options.direction = SyncDirection::S3ToLocal;
options.use_compression = true;
```

#### 双向同步
```rust
options.direction = SyncDirection::Bidirectional;
options.use_compression = true;
```

## 文件命名规则

启用压缩后，S3 文件名会自动添加 `.zst` 扩展名：

| 本地文件路径 | S3 对象键 |
|------------|----------|
| `data/2025/11/11/BTCUSDT.parquet` | `prefix/2025/11/11/BTCUSDT.parquet.zst` |
| `data/prices.json` | `prefix/prices.json.zst` |

## 性能考虑

### 压缩率示例

zstd 压缩对不同类型文件的效果：

- **Parquet 文件**：10-30% 额外压缩（Parquet 已经压缩）
- **JSON 文件**：70-85% 压缩（文本文件压缩效果好）
- **CSV 文件**：60-80% 压缩
- **日志文件**：70-90% 压缩

### 速度

- **压缩级别 3**：在速度和压缩率之间取得平衡
- **并行处理**：支持通过 `max_parallel` 设置并行上传/下载数量

## 注意事项

1. **一致性**：同步时保持 `use_compression` 设置一致
   - 不要在压缩和非压缩模式之间切换同一文件夹
   
2. **存储成本**：压缩可显著降低 S3 存储成本

3. **临时文件**：压缩/解压过程使用系统临时目录，自动清理

4. **兼容性**：与其他 S3 客户端配合使用时，需要手动处理 `.zst` 文件

## 完整示例

```rust
use trade_data_processor::s3_helper::{S3Helper, SyncOptions, SyncDirection};
use trade_data_processor::config::S3Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 配置 S3
    let config = S3Config {
        provider: "r2".to_string(),
        access_key_id: "your_access_key".to_string(),
        secret_access_key: "your_secret_key".to_string(),
        bucket: "your-bucket".to_string(),
        endpoint: Some("https://your-account.r2.cloudflarestorage.com".to_string()),
        region: Some("auto".to_string()),
        force_path_style: Some(true),
        // ... 其他配置
    };

    // 创建 S3Helper
    let s3_helper = S3Helper::new(config).await?;

    // 配置同步选项
    let mut options = SyncOptions {
        direction: SyncDirection::LocalToS3,
        force: false,
        delete: false,
        dry_run: false,
        exclude_patterns: vec![
            ".git".to_string(),
            "*.tmp".to_string(),
        ],
        max_parallel: 4,
        use_compression: true,  // 启用压缩
    };

    // 执行同步
    let stats = s3_helper.sync_folder(
        "./data/local",
        "data/remote",
        "./.sync_cache.db",
        options
    ).await?;

    println!("同步完成!");
    println!("- 上传: {} 个文件", stats.files_uploaded);
    println!("- 下载: {} 个文件", stats.files_downloaded);
    println!("- 跳过: {} 个文件", stats.files_skipped);
    println!("- 错误: {} 个", stats.errors);

    Ok(())
}
```

## 故障排除

### 问题：压缩文件无法下载

**原因**：S3 上的文件不是 `.zst` 格式

**解决**：确保上传时使用了 `use_compression = true`

### 问题：本地文件是压缩的

**原因**：可能手动下载了 `.zst` 文件

**解决**：使用 `sync_s3_to_local` 时启用 `use_compression`，会自动解压

### 问题：压缩率不理想

**原因**：某些文件类型已经压缩（如 Parquet）

**解决**：这是正常现象，可以选择性地对特定类型文件启用压缩

## 相关资源

- [zstd 官方文档](https://facebook.github.io/zstd/)
- [S3Helper 完整 API 文档](../src/s3_helper.rs)
- [R2 同步示例](../examples/r2_sync_mark_price.rs)

