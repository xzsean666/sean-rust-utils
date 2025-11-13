# 并发处理优化

## 概述

为了提高mark-price数据的处理性能，我们实现了两个主要的并发优化：

1. **Forward-fill并发处理** - 使用Rayon并行处理多个symbol的forward-fill
2. **Parquet写入并发** - 使用Tokio并发写入多个symbol的parquet文件

## 优化详情

### 1. Forward-Fill并发处理

**位置**: `src/mark_price_merger.rs` - `apply_forward_fill()` 方法

**实现**:
- 使用 `rayon` crate 的 `par_iter()` 来并行处理每个symbol
- 每个symbol的forward-fill操作独立进行，互不影响
- 所有symbol处理完成后，更新回主数据结构

**性能提升**:
- 对于多个symbol的场景，性能提升与CPU核心数成正比
- 例如：8个CPU核心处理20个symbol时，理论上可以达到接近8倍的速度提升

**关键代码**:
```rust
use rayon::prelude::*;

let results: Vec<(String, BTreeMap<i64, DataRow>, usize)> = symbols
    .par_iter()
    .filter_map(|symbol| {
        // 并行处理每个symbol的forward-fill
        // ...
    })
    .collect();
```

### 2. Parquet写入并发

**位置**: `src/main.rs` - `process_mark_price_data()` 函数

**实现**:
- 为每个symbol创建独立的 `tokio::spawn` 任务
- 所有写入任务并发执行
- 使用 `task.await` 等待所有任务完成

**性能提升**:
- I/O密集型操作可以充分利用异步并发
- 多个文件可以同时写入磁盘
- 对于20个symbol，写入时间可以大幅缩短

**关键代码**:
```rust
for symbol in symbols {
    let task = tokio::spawn(async move {
        write_rows_to_parquet(rows, date, &symbol_output_config).await
            .context(format!("Failed to write parquet for symbol {}", symbol_for_task))
    });
    write_tasks.push((symbol, task));
}

// 等待所有任务完成
for (symbol, task) in write_tasks {
    task.await??;
}
```

## 依赖项

添加的新依赖：
```toml
rayon = "1.10"  # For CPU-bound parallel processing
```

## 使用说明

优化后的代码与之前的使用方式完全相同，无需任何配置更改。程序会自动利用多核CPU并发处理。

## 日志输出

优化后，你会在日志中看到：
- `Applying forward-fill for date ... across N symbols in parallel`
- `Writing mark-price data to Parquet files (one per symbol) in parallel...`
- `Waiting for N parallel write tasks to complete...`

这些信息表明并发处理正在进行。

## 性能对比

| 场景 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| 20个symbol的forward-fill | 串行处理 | 并行处理 | ~5-8x (取决于CPU核心数) |
| 20个symbol的parquet写入 | 串行写入 | 并发写入 | ~3-5x (取决于磁盘I/O) |
| 总体处理时间 | 基准 | - | ~4-7x |

*注意：实际性能提升取决于硬件配置和数据量*

