# 压缩功能快速开始

## 快速使用

### 启用压缩同步

```rust
let mut options = SyncOptions::default();
options.use_compression = true;  // 启用 zstd 压缩
```

### 命令行使用

```bash
# 启用压缩
cargo run --example r2_sync_mark_price -- --config config/r2.config.yaml --compress

# 预览压缩效果（不实际上传）
cargo run --example r2_sync_mark_price -- --config config/r2.config.yaml --compress --dry-run
```

## 工作原理

```
本地文件夹              压缩上传              S3 存储
─────────────────      ─────────      ─────────────────
data/file.parquet  →   [zstd 压缩]  →  data/file.parquet.zst
data/data.json     →   [zstd 压缩]  →  data/data.json.zst


S3 存储              解压下载              本地文件夹
─────────────────      ─────────      ─────────────────
data/file.parquet.zst  →  [zstd 解压]  →  data/file.parquet
data/data.json.zst     →  [zstd 解压]  →  data/data.json
```

## 优势

✅ **节省存储成本**：JSON/CSV 文件可压缩 70-90%  
✅ **自动管理**：上传自动压缩，下载自动解压  
✅ **透明操作**：本地文件始终保持原始格式  
✅ **快速压缩**：zstd level 3 平衡速度和压缩率  

## 配置选项

```rust
pub struct SyncOptions {
    pub direction: SyncDirection,     // LocalToS3, S3ToLocal, Bidirectional
    pub force: bool,                  // 强制重新同步
    pub delete: bool,                 // 删除目标中不存在的文件
    pub dry_run: bool,                // 预览模式
    pub exclude_patterns: Vec<String>, // 排除模式
    pub max_parallel: usize,          // 并行数量
    pub use_compression: bool,        // 🆕 启用压缩
}
```

## 注意事项

⚠️ **保持一致性**：同一文件夹始终使用相同的 `use_compression` 设置  
⚠️ **文件命名**：S3 上的文件会自动添加 `.zst` 后缀  
⚠️ **Parquet 文件**：已经是压缩格式，额外压缩效果有限  

---

详细文档请参考：[COMPRESSION_FEATURE.md](./COMPRESSION_FEATURE.md)

