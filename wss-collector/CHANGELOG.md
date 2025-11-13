# 更新日志

## [v2.0.0] - 2025-11-03

### 重大更新：动态Schema支持

#### 新增功能
- ✅ **动态Schema推断**：程序现在能自动从WebSocket返回的JSON数据中推断表结构，不再需要硬编码数据结构
- ✅ **智能数据提取**：支持多种JSON格式：
  - 直接数组：`[{...}, {...}]`
  - 嵌套data字段：`{"data": [{...}, {...}]}`
  - 双层嵌套：`{"data": {"data": [{...}, {...}]}}`
  - 单个对象：`{...}`
- ✅ **自动类型识别**：根据JSON值自动识别并转换为合适的Parquet类型：
  - String → Utf8
  - 整数 → UInt64/Int64
  - 浮点数 → Float64
  - 布尔值 → Boolean
  - 数字字符串 → Float64（如 "123.45"）
- ✅ **Schema自动检测**：检测到Schema不匹配时自动删除旧文件并重建
- ✅ **损坏文件处理**：自动检测并重建损坏的Parquet文件

#### 改进
- 移除了硬编码的`MarkPriceData`结构体
- 使用`HashMap<String, Value>`存储动态数据
- Schema在首次接收消息时自动推断
- 字段名按字母顺序排序，确保Schema一致性

#### 技术细节
- 数据缓冲机制：每100条记录或每天结束时批量写入
- 读取现有文件并追加新数据，实现真正的数据累积
- 完整的错误处理和日志记录

### 使用示例

现在程序可以处理任何WebSocket数据源，无需修改代码：

**Binance标记价格**：
```yaml
wss_url: "wss://fstream.binance.com/stream?streams=!markPrice@arr"
path: "./data"
name: "binance_mark_price"
```

**Binance交易数据**：
```yaml
wss_url: "wss://stream.binance.com:9443/ws/btcusdt@trade"
path: "./data"
name: "binance_btc_trades"
```

**任何其他WebSocket**：
只需配置URL，程序会自动识别数据结构！

### 向后兼容性

⚠️ **不兼容**：由于Schema结构完全改变，旧版本生成的Parquet文件不兼容。
建议删除旧文件或使用新目录。程序会自动检测并重建不兼容的文件。

## [v1.0.0] - 之前版本

- 基础WebSocket收集功能
- 固定Schema（timestamp + data字符串）
- 按日期组织文件
- 自动重连机制

