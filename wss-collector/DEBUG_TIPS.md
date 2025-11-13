# 调试指南 - WSS 连接卡住问题

## 问题症状
程序打印 `Using proxy: http://127.0.0.1:7897` 后卡住，没有继续执行。

## 原因分析

### 🔴 最可能的原因
1. **代理超时或无响应** - `http://127.0.0.1:7897` 可能响应缓慢或者无法连接到目标
2. **没有连接超时** - `connect_async()` 原来没有超时机制，会无限期等待

## ✅ 已修复的问题

我已经添加了 **30秒连接超时**，现在程序会在连接失败时快速返回错误。

## 🧪 调试步骤

### 1. 测试代理可达性
```bash
# 测试代理是否运行
nc -zv 127.0.0.1 7897

# 或使用 curl 测试
curl -x http://127.0.0.1:7897 http://www.google.com -v
```

### 2. 测试 WebSocket 直接连接（跳过代理）
临时修改 `config.yaml`，注释掉 proxy 行：
```yaml
proxy: # "http://127.0.0.1:7897"
```

然后运行：
```bash
cargo run -- --config config.yaml
```

如果直接连接成功，说明问题在代理配置。

### 3. 使用修改后的代码运行
新代码现在会：
- 打印 "Connecting with 30-second timeout..." 
- 如果 30 秒内连接超时，会快速返回错误而不是卡住
- 之后会进入指数退避重试

运行：
```bash
cargo run -- --config config.yaml
```

### 4. 检查防火墙/网络
```bash
# 检查是否可以 ping 通 Binance
ping -c 3 fstream.binance.com

# 检查 DNS 解析
nslookup fstream.binance.com
```

## 预期输出（修复后）

如果代理不可达，你应该会看到：
```
INFO wss_collector: Attempting to connect to WebSocket: wss://fstream.binance.com/stream?streams=!markPrice@arr
INFO wss_collector: Using proxy: http://127.0.0.1:7897
INFO wss_collector: Setting up proxy environment variables...
INFO wss_collector: Connecting with 30-second timeout...
ERROR wss_collector: Failed to connect to WebSocket (attempt 1): Connection closed
WARN wss_collector: Retrying in 1 seconds...
```

## 解决方案建议

### 方案 A: 如果使用代理
- 确保代理是运行的并且可访问
- 检查代理的日志
- 考虑增加超时时间（修改代码中的 `Duration::from_secs(30)` 为更大的值）

### 方案 B: 如果不需要代理
- 注释掉 `config.yaml` 中的 `proxy` 行
- 直接连接到 Binance

### 方案 C: 调试代理连接
添加环境变量控制日志级别：
```bash
RUST_LOG=debug cargo run -- --config config.yaml
```

## 代码改动

修改了 `src/main.rs` 中的 `connect_and_collect()` 函数：
- 导入 `timeout` 函数
- 使用 `tokio::time::timeout(Duration::from_secs(30), connect_async(...))` 包装连接
- 增加了更多日志输出便于调试
