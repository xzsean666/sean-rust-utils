# Trade Data Processor

一个用于从多台 SSH 服务器收集交易数据、合并去重并写入 Parquet 文件的工具。

## 功能特性

- **多源数据收集**: 通过 SSH/HTTP 从多台机器并行收集数据
- **智能去重**: 基于时间戳（秒级）自动去重，第一个数据源的数据优先
- **Forward-Fill**: 自动填充缺失的秒级数据，确保每一秒都有数据
- **Parquet 输出**: 高效的列式存储格式，便于后续分析
- **灵活配置**: YAML 配置文件支持多种数据类型和认证方式
- **S3 备份**: 支持多种 S3 兼容存储服务（AWS S3、Backblaze B2、Cloudflare R2 等）

## 安装

确保已安装 Rust 工具链，然后编译项目：

```bash
cargo build --release
```

编译后的二进制文件位于 `target/release/trade-data-processor`

## 配置文件

创建配置文件 `config.yaml`（可以参考 `config.example.yaml`）：

```yaml
data_sources:
  - data_type: "mark-price"
    input_base_path: "/hdd16/trade/wss-collector/data/mark-price"
    ssh_servers:
      - host: "192.168.1.100"
        port: 22
        username: "datauser"
        password: "your_password"
      - host: "192.168.1.101"
        username: "datauser"
        private_key_path: "/home/user/.ssh/id_rsa"

output:
  path: "/output/parquet/mark-price"
  name: "mark-price"
  batch_size: 10000
  use_temp_dir: false  # 可选，设置为 true 可提高写入性能
```

### 配置说明

**data_sources**: 数据源配置列表

- `data_type`: 数据类型标识（如 "mark-price"）
- `input_base_path`: 远程机器上的基础目录路径
- `ssh_servers`: SSH 服务器列表
  - `host`: 服务器地址
  - `port`: SSH 端口（可选，默认 22）
  - `username`: 用户名
  - `password`: 密码（可选，与 private_key_path 二选一）
  - `private_key_path`: SSH 私钥路径（可选）

**output**: 输出配置

- `path`: Parquet 文件输出基础路径
- `name`: 文件名前缀
- `batch_size`: 批量写入大小（可选，如果不设置则写入单个文件）
- `use_temp_dir`: 是否先写入到 `/tmp` 再复制到输出目录（可选，默认 false）
  - 设置为 `true` 时，文件会先写入到 `/tmp` 目录（通常更快），然后复制到最终输出目录
  - 适用于输出目录在较慢存储（如网络存储、机械硬盘）上的场景
  - 可以提高写入性能，减少对输出目录的 I/O 压力

## 使用方法

基本用法：

```bash
./target/release/trade-data-processor \
  --config config.yaml \
  --date 2025-11-06 \
  --data-type mark-price

cargo run -- \
  --config ./config/config.yaml \
  --date 2025-11-04 \
  --data-type mark-price
```

```

```

### 参数说明

- `--config, -c`: 配置文件路径
- `--date, -d`: 要处理的日期（格式：YYYY-MM-DD）
- `--data-type, -t`: 数据类型（需要在配置文件中定义）

## 工作流程

1. **读取配置**: 加载 YAML 配置文件
2. **构建路径**: 根据日期构建远程目录路径
   - 例如：`/hdd16/trade/wss-collector/data/mark-price/2025/11/06`
3. **SSH 连接**: 依次连接到配置的 SSH 服务器
4. **下载数据**: 下载目标日期目录下的所有 `.jsonl`文件
5. **数据合并**:
   - 第一个 SSH 服务器的数据作为基础
   - 后续服务器的数据填充缺失的秒数
   - 基于"E"字段（时间戳毫秒）进行秒级去重
6. **Forward-Fill**: 填充一天中缺失的秒级数据
7. **写入 Parquet**: 将处理后的数据写入 Parquet 文件

## 数据格式

输入的 JSONL 文件每行应该是一个 JSON 对象，必须包含"E"字段（时间戳毫秒）：

```json
{
  "E": 1762411870001,
  "P": 103412.27508104,
  "T": 1762416000000,
  "e": "markPriceUpdate",
  "i": 103355.12021739,
  "p": 103308.50797101,
  "r": 0.00008966,
  "s": "BTCUSDT"
}
```

## 输出结构

Parquet 文件会按照以下结构组织：

```
{output.path}/
  └── {year}/
      └── {month}/
          └── {day}/
              └── {name}_{date}_{timestamp}_{sequence}.parquet
```

例如：

```
/output/parquet/mark-price/
  └── 2025/
      └── 11/
          └── 06/
              └── mark-price_2025-11-06_1730889600000_000001.parquet
```

## 日志

程序使用 tracing 进行日志记录。可以通过环境变量控制日志级别：

```bash
RUST_LOG=debug ./target/release/trade-data-processor --config config.yaml --date 2025-11-06 --data-type mark-price
```

日志级别：

- `error`: 仅错误
- `warn`: 警告及以上
- `info`: 信息及以上（默认）
- `debug`: 调试及以上
- `trace`: 所有日志

## 故障排查

### SSH 连接失败

- 检查网络连接和防火墙设置
- 验证 SSH 凭据（用户名、密码或私钥）
- 确保私钥文件权限正确（chmod 600）

### 找不到远程目录

- 确认远程路径是否正确
- 检查日期格式和目录结构
- 验证用户权限

### 数据解析错误

- 检查 JSONL 文件格式
- 确保每行都是有效的 JSON
- 验证"E"字段存在且为数字

## S3 存储支持

本项目包含完整的 S3 Helper 模块，支持将数据备份到各种 S3 兼容的存储服务。

### 支持的存储服务

- **AWS S3** - Amazon Web Services S3
- **Backblaze B2** - 经济实惠的云存储（前 10GB 免费）
- **Cloudflare R2** - 零出口费用的对象存储
- **MinIO** - 自建 S3 兼容存储
- 其他任何 S3 兼容服务

### 快速开始

详细使用文档请参考：

- **[S3_USAGE.md](S3_USAGE.md)** - 完整的使用指南和代码示例
- **[config/s3.config.example.yaml](config/s3.config.example.yaml)** - S3 配置示例
- **[examples/s3_basic.rs](examples/s3_basic.rs)** - 基本操作示例
- **[examples/s3_parquet_backup.rs](examples/s3_parquet_backup.rs)** - Parquet 备份示例

### 基本用法示例

```rust
use trade_data_processor::{S3Config, S3Helper};

// 创建 S3 配置
let s3_config = S3Config {
    provider: "b2".to_string(),
    bucket: "my-bucket".to_string(),
    access_key_id: "your_key".to_string(),
    secret_access_key: "your_secret".to_string(),
    region: Some("us-west-002".to_string()),
    endpoint: Some("https://s3.us-west-002.backblazeb2.com".to_string()),
    force_path_style: Some(true),
    base_path: Some("trading-data".to_string()),
};

// 创建 S3 客户端
let s3 = S3Helper::new(s3_config).await?;

// 上传文件
s3.upload_file("local.parquet", "remote/path.parquet").await?;

// 下载文件
s3.download_file("remote/path.parquet", "local.parquet").await?;

// 列出对象
let objects = s3.list_objects("remote/", None).await?;
```

### 配置示例（YAML）

```yaml
s3:
  provider: "b2"  # 或 "aws", "r2", "generic"
  bucket: "my-trading-data"
  access_key_id: "your_key_id"
  secret_access_key: "your_secret_key"
  region: "us-west-002"
  endpoint: "https://s3.us-west-002.backblazeb2.com"
  force_path_style: true
  base_path: "trading-data/mark-price"
```

## 相关文档

- [HTTP 使用指南](HTTP_USAGE.md) - HTTP 数据源配置说明
- [代理使用指南](PROXY_USAGE.md) - HTTP 代理配置说明
- [临时目录使用](USE_TEMP_DIR.md) - 提高写入性能的配置
- [S3 使用指南](S3_USAGE.md) - S3 存储配置和使用

## 许可证

[根据项目需要添加许可证信息]
