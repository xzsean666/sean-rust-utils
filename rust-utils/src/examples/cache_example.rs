use sean_rust_utils::kv_db_local::KvDbLocal;
use sean_rust_utils::kv_cache;
use tokio;

async fn expensive_computation(input: u64) -> Result<String, Box<dyn std::error::Error>> {
    println!("Performing expensive computation for: {}", input);
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    Ok(format!("Result for {}: {}", input, input * 2))
}

async fn another_computation(input1: String, input2: u64) -> Result<String, Box<dyn std::error::Error>> {
    println!("Performing another computation for: {} and {}", input1, input2);
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    Ok(format!("Another result for {}-{}: {}", input1, input2, input2 * 3))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = KvDbLocal::memory()?;
    let db_clone = db.clone();
    let db_clone2 = db.clone();

    println!("--- First call (should compute and cache) ---");
    let result1 = kv_cache!(db.clone(), "my_app", 10, expensive_computation, 5).await?;
    println!("Result: {}", result1);

    println!("\n--- Second call (should hit cache) ---");
    let result2 = kv_cache!(db.clone(), "my_app", 10, expensive_computation, 5).await?;
    println!("Result: {}", result2);

    println!("\n--- Third call with different args (should compute and cache) ---");
    let result3 = kv_cache!(db.clone(), "my_app", 10, expensive_computation, 10).await?;
    println!("Result: {}", result3);

    println!("\n--- Fourth call with different function (should compute and cache) ---");
    let result4 = kv_cache!(db_clone, "my_app", 10, another_computation, "test".to_string(), 7).await?;
    println!("Result: {}", result4);

    println!("\n--- Fifth call with different function (should hit cache) ---");
    let result5 = kv_cache!(db_clone2, "my_app", 10, another_computation, "test".to_string(), 7).await?;
    println!("Result: {}", result5);

    println!("\n--- Waiting for cache to expire (11 seconds) ---");
    tokio::time::sleep(tokio::time::Duration::from_secs(11)).await;

    println!("\n--- Sixth call after expiry (should re-compute) ---");
    let result6 = kv_cache!(db.clone(), "my_app", 10, expensive_computation, 5).await?;
    println!("Result: {}", result6);

    Ok(())
}
