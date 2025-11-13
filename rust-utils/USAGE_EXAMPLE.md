# KvDbLocal 使用示例

基于 Sled 和 serde_json 的本地 Key-Value 数据库工具，支持 JSON 序列化存储。

## 基本用法

### 添加依赖

在你的 `Cargo.toml` 中添加：

```toml
[dependencies]
sean-rust-utils = { path = "../sean-rust-utils" } # 或从 crate.io
serde = { version = "1.0", features = ["derive"] }
```

### 基本示例

```rust
use sean_rust_utils::KvDbLocal;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct User {
    name: String,
    age: u32,
    email: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建数据库实例（数据库文件会在可执行文件目录下）
    let db = KvDbLocal::new("my_database")?;

    // 存储数据
    let user = User {
        name: "张三".to_string(),
        age: 25,
        email: "zhangsan@example.com".to_string(),
    };

    db.put("user:1", &user)?;

    // 获取数据
    let retrieved_user: Option<User> = db.get("user:1")?;
    println!("用户: {:?}", retrieved_user);

    // 检查键是否存在
    if db.has("user:1")? {
        println!("用户存在");
    }

    // 获取记录数量
    println!("数据库中有 {} 条记录", db.count());

    Ok(())
}
```

## 高级用法

### 不同的创建方式

```rust
// 方式1：使用默认位置（可执行文件目录下）
let db = KvDbLocal::new("my_db")?;

// 方式2：使用自定义路径
use std::path::PathBuf;
let db = KvDbLocal::with_path(PathBuf::from("/tmp/my_custom_db"))?;

// 方式3：使用内存数据库（测试用）
let db = KvDbLocal::memory()?;
```

### 批量操作

```rust
// 批量存储
let users = vec![
    ("user:1", &user1),
    ("user:2", &user2),
    ("user:3", &user3),
];
db.put_many(&users)?;

// 批量删除
let keys_to_delete = vec!["user:1", "user:3"];
let deleted_count = db.delete_many(&keys_to_delete)?;
println!("删除了 {} 条记录", deleted_count);
```

### JSON 合并

```rust
use serde_json::json;

// 存储初始数据
let user = json!({
    "name": "张三",
    "age": 25,
    "email": "zhangsan@example.com"
});
db.put("user:1", &user)?;

// 合并新数据
let update = json!({
    "age": 26,
    "city": "北京"
});
db.merge("user:1", &update)?;

// 结果：{"name": "张三", "age": 26, "email": "zhangsan@example.com", "city": "北京"}
```

### 获取所有数据

```rust
// 获取所有键
let keys = db.keys()?;
println!("所有键: {:?}", keys);

// 获取所有键值对
let all_users: Vec<(String, User)> = db.get_all()?;
for (key, user) in all_users {
    println!("{}: {:?}", key, user);
}
```

### 安全添加（防重复）

```rust
// 只在键不存在时添加
match db.add("user:1", &user) {
    Ok(_) => println!("添加成功"),
    Err(e) => println!("添加失败: {}", e), // 键已存在时会报错
}
```

## API 参考

### 核心方法

- `new(db_name: &str)` - 在可执行文件目录下创建数据库
- `with_path(path: PathBuf)` - 使用自定义路径创建数据库
- `memory()` - 创建内存数据库
- `put<T>(key: &str, value: &T)` - 存储键值对
- `get<T>(key: &str)` - 获取值
- `delete(key: &str)` - 删除键值对
- `has(key: &str)` - 检查键是否存在
- `clear()` - 清空所有数据
- `close(self)` - 关闭数据库

### 批量操作

- `put_many<T>(entries: &[(&str, &T)])` - 批量存储
- `delete_many(keys: &[&str])` - 批量删除
- `get_all<T>()` - 获取所有键值对

### 实用方法

- `keys()` - 获取所有键
- `count()` - 获取记录数量
- `add<T>(key: &str, value: &T)` - 安全添加（防重复）
- `merge<T>(key: &str, value: &T)` - JSON 对象合并

## 特性

- ✅ 基于高性能的 Sled 存储引擎
- ✅ 自动 JSON 序列化/反序列化
- ✅ 类型安全的泛型接口
- ✅ 支持复杂数据结构存储
- ✅ 线程安全
- ✅ 原子操作保证数据一致性
- ✅ 自动创建数据库文件
- ✅ 支持批量操作
- ✅ JSON 对象智能合并
- ✅ 内存数据库模式（测试友好）

## 错误处理

```rust
use std::error::Error;

fn example() -> Result<(), Box<dyn Error>> {
    let db = KvDbLocal::new("test")?;

    // 所有可能出错的操作都返回 Result
    db.put("key", &"value")?;
    let value: Option<String> = db.get("key")?;

    Ok(())
}
```

## 性能建议

1. 使用批量操作 `put_many()` 和 `delete_many()` 处理大量数据
2. 合理使用 `merge()` 而非多次 `put()` 来更新 JSON 对象
3. 定期调用 `close()` 确保数据持久化
4. 对于频繁的读操作，考虑缓存策略
