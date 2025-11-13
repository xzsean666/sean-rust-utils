# use_temp_dir 功能使用说明

## 功能简介

`use_temp_dir` 是一个可选配置项，用于优化 Parquet 文件写入性能。当启用此功能时，程序会先将文件写入到 `/tmp` 目录，然后复制到最终的输出目录。

## 使用场景

这个功能特别适用于以下场景：

1. **输出目录在较慢的存储上**
   - 网络存储（NFS、SMB等）
   - 机械硬盘（HDD）
   - 较慢的 SSD

2. **需要提高写入性能**
   - `/tmp` 通常位于内存文件系统（tmpfs）或更快的 SSD 上
   - 先写入 `/tmp` 可以更快完成 Parquet 文件的创建
   - 然后通过系统 `cp` 命令快速复制到最终位置

3. **减少对输出目录的 I/O 压力**
   - 写入过程不会长时间占用输出目录的 I/O
   - 复制操作是原子性的，避免文件写入过程中的不一致状态

## 配置方法

在配置文件的 `output` 部分添加 `use_temp_dir` 选项：

### 示例 1: 启用临时目录写入

```yaml
output:
  path: "/mnt/slow-storage/parquet/mark-price"
  name: "mark-price"
  batch_size: 10000
  use_temp_dir: true  # 启用临时目录写入
```

### 示例 2: 禁用临时目录写入（默认）

```yaml
output:
  path: "./data/parquet/mark-price"
  name: "mark-price"
  batch_size: 10000
  use_temp_dir: false  # 直接写入输出目录（默认行为）
```

### 示例 3: 省略配置（使用默认值）

```yaml
output:
  path: "./data/parquet/mark-price"
  name: "mark-price"
  batch_size: 10000
  # 不设置 use_temp_dir，默认值为 false
```

## 工作流程

当 `use_temp_dir: true` 时，写入流程如下：

1. **生成临时文件路径**
   - 格式：`/tmp/{filename}.{pid}.{timestamp}.tmp`
   - 使用进程 ID 和时间戳确保文件名唯一

2. **写入临时文件**
   - 在 `/tmp` 目录创建 Parquet 文件
   - 写入所有数据

3. **创建输出目录**
   - 如果输出目录不存在，自动创建

4. **复制文件**
   - 使用系统 `cp` 命令将文件从 `/tmp` 复制到输出目录
   - 设置正确的文件权限（0664）

5. **清理临时文件**
   - 删除 `/tmp` 中的临时文件

## 性能考虑

### 优点

- ✅ 写入速度快：`/tmp` 通常在更快的存储上
- ✅ 减少 I/O 阻塞：写入过程不占用输出目录
- ✅ 原子性操作：复制操作是原子的

### 注意事项

- ⚠️ 需要足够的 `/tmp` 空间：确保 `/tmp` 有足够的空间存储临时文件
- ⚠️ 额外复制开销：需要额外的复制操作（但通常比直接写入慢存储更快）
- ⚠️ 进程崩溃风险：如果程序在复制前崩溃，临时文件可能残留在 `/tmp` 中（会被系统定期清理）

## 建议

- **推荐启用**：如果输出目录在网络存储或较慢的硬盘上
- **不推荐启用**：如果输出目录已经在快速 SSD 上，可能没有明显性能提升
- **必须启用**：如果写入性能是瓶颈，且 `/tmp` 有足够空间

## 监控和调试

启用 `use_temp_dir` 后，日志会显示临时文件路径：

```
INFO Wrote 10000 records to /mnt/storage/parquet/mark-price/2025/11/06/file.parquet (via temp file /tmp/file.parquet.12345.1699123456789.tmp)
```

可以通过日志确认功能是否正常工作。

