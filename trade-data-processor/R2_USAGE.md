# Cloudflare R2 使用指南

本指南介绍如何使用 Cloudflare R2 存储服务同步 mark-price 数据到云端。

## 目录

- [简介](#简介)
- [前置要求](#前置要求)
- [快速开始](#快速开始)
- [详细配置](#详细配置)
- [使用方法](#使用方法)
- [常见问题](#常见问题)

## 简介

Cloudflare R2 是一个 S3 兼容的对象存储服务，具有以下优势：
- ✅ **零出口费用** - 下载数据不收费
- ✅ **S3 兼容** - 可使用现有的 S3 工具和库
- ✅ **全球分发** - Cloudflare 的全球网络
- ✅ **价格便宜** - 比 AWS S3 便宜

本项目已集成 R2 支持，可以轻松同步数据到 R2。

## 前置要求

1. Cloudflare 账户
2. 已创建的 R2 bucket
3. R2 API Token（Access Key 和 Secret Key）

## 获取 R2 凭证

### 1. 登录 Cloudflare Dashboard

访问 [Cloudflare Dashboard](https://dash.cloudflare.com/)

### 2. 进入 R2 管理页面

左侧菜单 -> **R2** -> **Overview**

### 3. 获取 Account ID

在 R2 页面右侧可以看到你的 **Account ID**，记下这个 ID。

### 4. 创建 API Token

1. 点击 **Manage R2 API Tokens**
2. 点击 **Create API Token**
3. 填写 Token 名称，例如：`trade-data-sync`
4. 权限设置：
   - **Permissions**: `Admin Read & Write` 或根据需要设置
   - **Buckets**: 选择特定 bucket 或所有 buckets
5. 点击 **Create API Token**
6. **重要**: 保存显示的 **Access Key ID** 和 **Secret Access Key**（只显示一次）

### 5. 创建 R2 Bucket

如果还没有 bucket：
1. 在 R2 页面点击 **Create bucket**
2. 输入 bucket 名称（例如：`trading-data`）
3. 选择存储位置
4. 点击 **Create bucket**

## 配置 R2

### 方法 1: 使用环境变量（推荐）

创建 `.env` 文件或直接设置环境变量：

```bash
export R2_ACCESS_KEY_ID="your_access_key_id"
export R2_SECRET_ACCESS_KEY="your_secret_access_key"
export R2_BUCKET="your-bucket-name"
export R2_ACCOUNT_ID="your_account_id"
```

### 方法 2: 使用配置文件

复制示例配置文件：

```bash
cp config/r2.config.example.yaml config/r2.config.yaml
```

编辑 `config/r2.config.yaml` 并填入你的凭证：

```yaml
s3:
  provider: "r2"
  access_key_id: "your_access_key_id"
  secret_access_key: "your_secret_access_key"
  bucket: "your-bucket-name"
  region: "auto"
  endpoint: "https://your_account_id.r2.cloudflarestorage.com"
  force_path_style: false
```

## 使用示例

### 示例 1: 同步 mark-price 数据到 R2

我们提供了一个现成的示例程序来同步 `data/merged/mark-price/2025/11/11` 目录到 R2。

#### 干跑模式（预览，不实际上传）

```bash
cargo run --example r2_sync_mark_price
```

输出示例：
```
=== Cloudflare R2 Sync Example ===

Connecting to Cloudflare R2...
  Endpoint: https://xxxxx.r2.cloudflarestorage.com
  Bucket: trading-data

✓ Successfully connected to R2

🔍 DRY RUN MODE - No files will be uploaded
   Set DRY_RUN=false to perform actual sync

=== Syncing Local Directory to R2 ===
  Source: ./data/merged/mark-price/2025/11/11
  Destination: r2://trading-data/mark-price/2025/11/11

Starting sync...

=== Sync Statistics ===
  Files scanned:    156
  Files uploaded:   156
  Files skipped:    0
  Files deleted:    0
  Bytes uploaded:   45678 KB (44.61 MB)

✅ Dry run completed successfully!
```

#### 实际同步模式

```bash
DRY_RUN=false cargo run --example r2_sync_mark_price
```

### 示例 2: 使用脚本自动同步

创建一个同步脚本 `sync_to_r2.sh`：

```bash
#!/bin/bash

# 设置环境变量
export R2_ACCESS_KEY_ID="your_access_key_id"
export R2_SECRET_ACCESS_KEY="your_secret_access_key"
export R2_BUCKET="trading-data"
export R2_ACCOUNT_ID="your_account_id"

# 执行同步
DRY_RUN=false cargo run --example r2_sync_mark_price

# 检查结果
if [ $? -eq 0 ]; then
    echo "✅ 同步成功"
else
    echo "❌ 同步失败"
    exit 1
fi
```

赋予执行权限并运行：

```bash
chmod +x sync_to_r2.sh
./sync_to_r2.sh
```

### 示例 3: 定时同步（使用 cron）

编辑 crontab：

```bash
crontab -e
```

添加定时任务（每天凌晨 2 点同步）：

```cron
0 2 * * * cd /path/to/trade-data-processor && /path/to/sync_to_r2.sh >> /var/log/r2-sync.log 2>&1
```

## 高级用法

### 强制重新同步所有文件

如果需要强制重新上传所有文件（忽略缓存）：

修改示例代码中的 `SyncOptions`：

```rust
let mut options = SyncOptions::default();
options.dry_run = false;
options.force = true;  // 强制同步
```

### 同步时删除远程多余文件

如果希望保持远程和本地完全一致（删除远程多余的文件）：

```rust
let mut options = SyncOptions::default();
options.dry_run = false;
options.delete = true;  // 删除远程多余文件
```

### 双向同步

如果需要双向同步（本地 ↔ R2）：

```rust
let stats = s3_helper
    .sync_folder_bidirectional(local_folder, r2_prefix, db_path, options)
    .await?;
```

## R2 性能优化

### 并发上传

S3Helper 自动使用多线程上传，可以通过修改 `SyncOptions` 来调整：

```rust
let mut options = SyncOptions::default();
options.max_concurrent_uploads = 10;  // 最多 10 个并发上传
```

### 分片上传大文件

对于大文件（>100MB），自动使用分片上传以提高可靠性和速度。

### 缓存机制

同步操作使用本地 SQLite 缓存来跟踪已上传的文件，避免重复上传：
- 缓存位置：`./.r2_sync_cache/`
- 基于文件的 SHA256 哈希判断是否需要重新上传

## 常见问题

### Q: 如何查看 R2 中已上传的文件？

**方法 1**: 使用 Cloudflare Dashboard
- 访问 Dashboard -> R2 -> 选择你的 bucket -> Browse

**方法 2**: 使用代码列出文件

```rust
use trade_data_processor::{S3Config, S3Helper};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = S3Config { /* your config */ };
    let s3 = S3Helper::new(config).await?;
    
    let objects = s3.list_objects("mark-price/2025/11/11", None).await?;
    for obj in objects {
        println!("{}", obj);
    }
    
    Ok(())
}
```

### Q: 同步失败怎么办？

1. **检查凭证**: 确认 Access Key 和 Secret Key 正确
2. **检查网络**: 确保能访问 `*.r2.cloudflarestorage.com`
3. **检查权限**: 确认 API Token 有读写权限
4. **查看日志**: 运行时会输出详细的错误信息

### Q: 如何估算 R2 费用？

R2 定价（2024）：
- **存储**: $0.015/GB/月
- **Class A 操作**（写入）: $4.50/百万次请求
- **Class B 操作**（读取）: $0.36/百万次请求
- **出口流量**: **免费** 🎉

示例：存储 1TB 数据，每天写入 1000 个文件：
- 存储费用: 1000 GB × $0.015 = $15/月
- 写入费用: 30,000 次 × $4.50/1,000,000 = $0.135/月
- **总计**: 约 $15.14/月

### Q: R2 vs AWS S3 有什么区别？

| 特性 | Cloudflare R2 | AWS S3 |
|------|---------------|--------|
| 存储价格 | $0.015/GB/月 | $0.023/GB/月 |
| 出口流量 | **免费** ✅ | $0.09/GB ⚠️ |
| API 兼容 | S3 兼容 | 原生 S3 |
| 全球分发 | 是 | 是 |
| 最佳用途 | 高读取流量 | AWS 生态系统 |

**推荐**: 如果数据需要频繁下载，R2 更经济实惠。

### Q: 如何迁移现有 S3 数据到 R2？

使用我们的同步工具：

```bash
# 1. 从 S3 下载到本地
aws s3 sync s3://your-s3-bucket/path ./local-data

# 2. 从本地上传到 R2
DRY_RUN=false cargo run --example r2_sync_mark_price
```

或者使用 rclone 等工具直接迁移。

### Q: 可以使用自定义域名吗？

可以！R2 支持自定义域名：

1. 在 R2 Dashboard 中选择你的 bucket
2. 点击 **Settings** -> **Custom Domains**
3. 添加你的域名（例如：`data.yourdomain.com`）
4. 配置 DNS（按照提示操作）

配置后可以使用 `https://data.yourdomain.com/file.parquet` 访问文件。

## 相关链接

- [Cloudflare R2 官方文档](https://developers.cloudflare.com/r2/)
- [R2 定价](https://developers.cloudflare.com/r2/pricing/)
- [R2 API 文档](https://developers.cloudflare.com/r2/api/)
- [S3 API 兼容性](https://developers.cloudflare.com/r2/api/s3/api/)

## 技术支持

如有问题，请查看：
- 项目 [README.md](README.md)
- [S3_USAGE.md](S3_USAGE.md) - S3 通用使用指南
- GitHub Issues

---

**提示**: 首次使用建议先用干跑模式（dry run）测试，确认无误后再实际同步。

