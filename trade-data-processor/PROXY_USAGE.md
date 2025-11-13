# HTTP 代理配置指南

## 功能说明

HTTP 客户端现在支持代理配置，允许通过代理服务器进行 HTTP 连接。这对于在受限网络环境中访问远程数据服务很有用。

## 配置方法

在 YAML 配置文件中的 `http_servers` 部分添加 `proxy` 字段：

### 基础配置

```yaml
data_sources:
  - data_type: "mark-price"
    http_servers:
      - base_url: "http://data-server.example.com:8080"
        input_base_path: "mark-price"
        proxy: "http://proxy.example.com:3128"  # 添加代理 URL
```

## 代理 URL 格式

支持的代理协议：

| 协议 | 格式 | 说明 |
|------|------|------|
| HTTP | `http://proxy.example.com:8080` | 标准 HTTP 代理 |
| HTTPS | `https://proxy.example.com:8080` | 安全的 HTTPS 代理 |
| SOCKS5 | `socks5://proxy.example.com:1080` | SOCKS5 代理协议 |

### 完整示例

```yaml
data_sources:
  - data_type: "mark-price"
    http_servers:
      # 直接连接（无代理）
      - base_url: "http://direct-server.example.com:8080"
        input_base_path: "mark-price"

      # HTTP 代理
      - base_url: "http://data-server1.example.com:8080"
        input_base_path: "mark-price"
        proxy: "http://proxy.example.com:3128"

      # HTTPS 代理
      - base_url: "https://data-server2.example.com:8080"
        input_base_path: "mark-price"
        proxy: "https://secure-proxy.example.com:8443"

      # SOCKS5 代理
      - base_url: "http://data-server3.example.com:8080"
        input_base_path: "mark-price"
        proxy: "socks5://socks-proxy.example.com:1080"

output:
  path: "./data/parquet/mark-price"
  name: "mark-price"
```

## 特性

- ✅ 可选配置：如果不设置 `proxy` 字段或设为 `null`，将直接连接
- ✅ 灵活支持：支持 HTTP、HTTPS 和 SOCKS5 代理协议
- ✅ 日志记录：代理配置会在初始化时记录到日志中
- ✅ 错误处理：代理配置错误不会导致程序崩溃，只会记录警告

## 日志输出

当配置代理时，会看到如下日志：

```
INFO: HTTP client configured with proxy: http://proxy.example.com:3128
```

如果代理配置有问题，会看到：

```
DEBUG: Failed to configure proxy http://invalid-proxy: error details
```

## 故障排除

### 问题 1: 代理连接超时
- 检查代理服务器地址和端口是否正确
- 确保代理服务器可访问
- 考虑增加超时时间（当前设置为 600 秒）

### 问题 2: 代理认证失败
如果代理需要认证，请使用以下格式：

```yaml
proxy: "http://username:password@proxy.example.com:3128"
```

### 问题 3: 性能问题
- 确保代理服务器性能良好
- 检查网络连接质量
- 考虑使用本地代理缓存

## 代码实现

代理支持通过 `reqwest` 库的 `Proxy::all()` 方法实现。相关代码位置：

- **配置结构**：`src/config.rs` - `HttpConfig` 结构体
- **客户端初始化**：`src/http_client.rs` - `HttpClient::new()` 方法

## 测试

包含的单元测试：

```rust
#[test]
fn test_http_client_with_proxy() {
    let config = HttpConfig {
        base_url: "http://localhost:8080".to_string(),
        input_base_path: "/data/mark-price".to_string(),
        proxy: Some("http://proxy.example.com:8080".to_string()),
    };
    
    let client = HttpClient::new(config);
    assert_eq!(client.host_identifier(), "http://localhost:8080");
}
```

运行测试：

```bash
cargo test test_http_client_with_proxy -- --nocapture
```

