use sean_rust_utils::{KvDbLocal};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    name: String,
    age: u32,
    email: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 测试KvDbLocal功能 ===");

    // 创建数据库实例
    let db = KvDbLocal::new("test.db")?;

    // 测试基本的put/get操作
    let user = User {
        name: "张三".to_string(),
        age: 25,
        email: "zhangsan@example.com".to_string(),
    };

    println!("\n1. 测试put/get操作");
    db.put("user1", &user)?;
    println!("已存储用户: {:?}", user);

    let retrieved_user: Option<User> = db.get("user1")?;
    println!("获取的用户: {:?}", retrieved_user);

    // 测试has方法
    println!("\n2. 测试has方法");
    println!("user1是否存在: {}", db.has("user1")?);
    println!("user2是否存在: {}", db.has("user2")?);

    // 测试count方法
    println!("\n3. 测试count方法");
    println!("数据库中的记录数: {}", db.count());

    // 测试批量操作
    println!("\n4. 测试批量操作");
    let user2 = User {
        name: "李四".to_string(),
        age: 30,
        email: "lisi@example.com".to_string(),
    };
    let user3 = User {
        name: "王五".to_string(),
        age: 28,
        email: "wangwu@example.com".to_string(),
    };
    let users = vec![
        ("user2", &user2),
        ("user3", &user3),
    ];

    db.put_many(&users)?;
    println!("批量存储完成，现在有 {} 条记录", db.count());

    // 测试获取所有键
    println!("\n5. 测试keys方法");
    let keys = db.keys()?;
    println!("所有键: {:?}", keys);

    // 测试merge操作（JSON合并）
    println!("\n6. 测试merge操作");
    let update_data = serde_json::json!({
        "age": 26,
        "city": "北京"
    });
    
    db.merge("user1", &update_data)?;
    let updated_user: Option<serde_json::Value> = db.get("user1")?;
    println!("合并后的用户数据: {:?}", updated_user);

    // 测试delete操作
    println!("\n7. 测试delete操作");
    let deleted = db.delete("user3")?;
    println!("删除user3成功: {}", deleted);
    println!("删除后记录数: {}", db.count());

    // 测试add方法
    println!("\n8. 测试add方法");
    match db.add("user1", &user) {
        Ok(_) => println!("添加成功"),
        Err(e) => println!("添加失败（预期）: {}", e),
    }

    // 显示最终状态
    println!("\n9. 最终状态");
    println!("最终记录数: {}", db.count());
    let final_keys = db.keys()?;
    println!("最终的所有键: {:?}", final_keys);

    println!("\n=== 测试完成 ===");
    
    // 保留原有的load_test_url测试
    // println!("\n=== 测试load_test_url功能 ===");
    // let results = load_test_url("https://httpbin.org/get", 10, 5).await?;
    // println!("成功率: {:.2}%", results.success_rate_percent);
    
    Ok(())
}
