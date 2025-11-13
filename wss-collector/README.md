# WSS Collector

一个用于从WebSocket收集数据并存储为Parquet文件的CLI工具。

## 功能特性

- ✅ 从WebSocket实时收集数据
- ✅ 自动存储为Parquet格式
- ✅ 按年/月/日自动组织文件结构
- ✅ 每天一个Parquet文件
- ✅ 无限重试机制，确保程序不会中断
- ✅ 自动处理连接断开和重连
- ✅ 指数退避重试策略
- ✅ **动态Schema推断**：自动根据JSON数据结构创建表结构
- ✅ **智能数据提取**：支持多种JSON格式（数组、嵌套对象等）
- ✅ **类型自动识别**：自动识别字符串、整数、浮点数、布尔值
- ✅ **Schema自动检测**：文件Schema变化时自动重建

## 文件组织结构

```
{path}/
├── 2025/
│   ├── 01/
│   │   ├── {name}_2025-01-01.parquet
│   │   ├── {name}_2025-01-02.parquet
│   │   └── ...
│   ├── 02/
│   │   └── ...
│   └── ...
└── ...
```

## 安装

确保已安装Rust工具链，然后克隆并构建项目：

```bash
git clone <repository-url>
cd wss-collector
cargo build --release
```

## 配置

创建一个YAML配置文件（例如 `config.yaml`）：

```yaml
# WebSocket URL
wss_url: "wss://stream.binance.com:9443/ws/btcusdt@trade"

# 数据存储路径
path: "./data"

# 文件名前缀
name: "binance_btcusdt"
```

### 配置参数说明

- `wss_url`: WebSocket服务器的URL
- `path`: Parquet文件的存储基础路径
- `name`: Parquet文件名的前缀

## 使用方法

运行程序：

```bash
# 使用开发模式
cargo run -- --config config.yaml

# 或使用发布版本
./target/release/wss-collector --config config.yaml
```

## Parquet数据结构

**自动推断Schema**：程序会自动从WebSocket返回的JSON数据中推断列结构。

### 支持的数据格式

程序能自动处理多种JSON格式：

1. **直接数组**：`[{...}, {...}]`
2. **嵌套data字段**：`{"data": [{...}, {...}]}`
3. **双层嵌套**：`{"data": {"data": [{...}, {...}]}}`
4. **单个对象**：`{"field1": "value1", "field2": 123}`

### 类型映射

| JSON类型 | Parquet类型 | 说明 |
|---------|------------|------|
| 数字（整数） | UInt64/Int64 | 根据正负自动选择 |
| 数字（浮点） | Float64 | 小数类型 |
| 字符串 | String | 文本类型 |
| 布尔值 | Boolean | true/false |
| 数字字符串 | Float64 | 如 "123.45" 会被识别为数字 |

### 示例

对于Binance标记价格数据：
```json
{
  "data": [
    {
      "e": "markPriceUpdate",
      "E": 1762152003000,
      "s": "BTCUSDT",
      "p": "107408.70000000",
      "P": "107454.08475604",
      "i": "107462.69782609",
      "r": "0.00005208",
      "T": 1762156800000
    }
  ]
}
```

会自动创建以下Schema：
| 字段 | 类型 | 说明 |
|------|------|------|
| E | UInt64 | 事件时间 |
| P | Float64 | 预估结算价 |
| T | UInt64 | 下次资金费时间 |
| e | String | 事件类型 |
| i | Float64 | 指数价格 |
| p | Float64 | 标记价格 |
| r | Float64 | 资金费率 |
| s | String | 交易对符号 |

## 错误处理

程序具有健壮的错误处理机制：

1. **连接失败**：使用指数退避策略自动重试（最长等待60秒）
2. **连接断开**：自动检测并重新连接
3. **写入失败**：记录错误但继续运行
4. **日期切换**：自动在新的一天创建新的Parquet文件
5. **文件损坏**：自动检测并重建损坏的Parquet文件
6. **Schema变更**：检测到Schema不匹配时自动删除旧文件并重建

## 日志

程序使用 `tracing` 库输出日志信息：

- `INFO`: 正常操作信息（连接、接收消息等）
- `WARN`: 警告信息（连接断开、重试等）
- `ERROR`: 错误信息（连接失败、写入失败等）

## 示例

### 收集币安交易数据

```yaml
wss_url: "wss://stream.binance.com:9443/ws/btcusdt@trade"
path: "./binance_data"
name: "btcusdt_trades"
```

### 收集深度数据

```yaml
wss_url: "wss://stream.binance.com:9443/ws/btcusdt@depth"
path: "./binance_data"
name: "btcusdt_depth"
```

## 停止程序

使用 `Ctrl+C` 停止程序。程序会在停止前正确关闭所有打开的Parquet文件。

## 技术栈

- **tokio**: 异步运行时
- **tokio-tungstenite**: WebSocket客户端
- **arrow/parquet**: Parquet文件读写
- **clap**: 命令行参数解析
- **serde/serde_yaml**: 配置文件解析
- **chrono**: 日期时间处理
- **tracing**: 日志记录

## 许可证

MIT

