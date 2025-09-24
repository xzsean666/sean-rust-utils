use sean_rust_utils::{load_test_url};
use tokio; // Import the tokio crate

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // 保留原有的load_test_url测试
    println!("\n=== 测试load_test_url功能 ===");
    let results = load_test_url("https://httpbin.org/get", 10, 5).await?;
    println!("成功率: {:.2}%", results.success_rate_percent);
    
    Ok(())
}
