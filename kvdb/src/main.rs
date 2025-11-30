use kvdb::KVDB;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Profile {
    name: String,
    age: u8,
    tags: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // KVDB 支持任何实现了 Serialize/DeserializeOwned 的类型。
    let db = KVDB::new("db/example1.db", "example")?;

    // 1) 基础类型
    db.put("bool", &true)?;
    db.put("i64", &12345_i64)?;
    db.put("text", &"hello kvdb".to_string())?;

    // 2) 复合类型（如 Vec/String）
    let words = vec!["red".to_string(), "green".to_string(), "blue".to_string()];
    db.put("colors", &words)?;

    // 3) 自定义结构体（需要 serde 派生）
    let profile = Profile {
        name: "Alice".to_string(),
        age: 30,
        tags: vec!["rust".into(), "sled".into()],
    };
    db.put("profile", &profile)?;

    // 读取示例
    let number: Option<i64> = db.get("i64")?;
    let text: Option<String> = db.get("text")?;
    let colors: Option<Vec<String>> = db.get("colors")?;
    let stored_profile: Option<Profile> = db.get("profile")?;

    println!("bool => {:?}", db.get::<bool>("bool")?);
    println!("i64 => {:?}", number);
    println!("text => {:?}", text);
    println!("colors => {:?}", colors);
    println!("profile => {:?}", stored_profile);

    // 4) 前缀查询示例（get_with_prefix）
    db.put("user:1", &"alice")?;
    db.put("user:2", &"bob")?;
    db.put("order:1", &"should be filtered out")?;

    let mut users: Vec<(String, String)> = db.get_with_prefix("user:")?;
    users.sort_by(|a, b| a.0.cmp(&b.0));
    println!("users with prefix 'user:' => {:?}", users);

    // 5) 批量写入 + get_all（offset/limit），放在单独的树中保持类型一致
    // 与上面的 `db` 共享同一个底层 sled 数据库，但使用另一个 tree。
    let items_db = db.with_tree("items")?;
    items_db.put_many(vec![
        ("item:1", 10u32),
        ("item:2", 20u32),
        ("item:3", 30u32),
    ])?;

    // 默认 get_all(None, None) 获取全部
    let all_items: Vec<(String, u32)> = items_db.get_all(None, None)?;
    println!("all items => {:?}", all_items);

    // 带偏移和限制的分页
    let paged: Vec<(String, u32)> = items_db.get_all(Some(1), Some(2))?;
    println!("paged items (offset=1, limit=2) => {:?}", paged);

    // 6) 备份与恢复
    let backup_path = db.backup_to_path("db/backups", 3)?;
    println!("backup created at {:?}", backup_path);

    KVDB::restore_from(&backup_path, "db/restore_example")?;
    let restored_db = KVDB::new("db/restore_example", "example")?;
    println!("restored text => {:?}", restored_db.get::<String>("text")?);

    Ok(())
}
