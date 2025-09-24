use std::future::Future;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use crate::kv_db_local::KvDbLocal;
use std::time::{SystemTime, Duration};

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// Caches the result of an async function call, similar to JavaScript decorators.
/// 
/// This function implements a caching layer that stores function results in a key-value database.
/// If a cached result exists and hasn't expired, it returns the cached value. Otherwise, 
/// it executes the original function, caches the result, and returns it.
///
/// # Arguments
///
/// * `db` - A KvDbLocal instance for storing cached results
/// * `prefix` - A prefix for the cache key to avoid collisions
/// * `function_name` - The name of the function being cached (for key generation)
/// * `ttl` - Time-to-live in seconds for cached results
/// * `func` - The original async function to be cached
/// * `args` - Arguments to pass to the original function
///
/// # Type Parameters
///
/// * `F` - The function type that takes Args and returns a Future
/// * `Fut` - The Future type returned by the function
/// * `Args` - The type of arguments (must implement Serialize, Hash, Send)
/// * `R` - The return type (must implement Serialize, Deserialize, Clone)
///
/// # Returns
///
/// Returns `Result<R, Box<dyn std::error::Error>>` - either the cached result or 
/// the result from executing the function.
///
/// # Example
///
/// ```rust,ignore
/// use sean_rust_utils::kv_db_local::KvDbLocal;
/// use sean_rust_utils::kv_cache::cache_result;
///
/// async fn expensive_operation(input: u64) -> Result<String, Box<dyn std::error::Error>> {
///     // Simulate expensive computation
///     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
///     Ok(format!("Result: {}", input * 2))
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let db = KvDbLocal::memory()?;
///     
///     let result = cache_result(
///         db,
///         "my_app".to_string(),
///         "expensive_operation".to_string(),
///         300, // 5 minutes TTL
///         |args| async move { expensive_operation(args).await },
///         42u64,
///     ).await?;
///     
///     println!("Result: {}", result);
///     Ok(())
/// }
/// ```
pub async fn cache_result<
    F, 
    Fut, 
    Args, 
    R
>(
    db: KvDbLocal, // Sled数据库实例
    prefix: String, // 缓存键前缀
    function_name: String, // 函数名称
    ttl: u64, // 缓存生命周期，秒
    func: F, // 原始异步函数
    args: Args, // 原始函数的参数
) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce(Args) -> Fut,
    Fut: Future<Output = Result<R, Box<dyn std::error::Error>>>,
    Args: Serialize + Hash + Send + 'static,
    R: Serialize + DeserializeOwned + Clone + 'static,
{
    let args_hash = calculate_hash(&args);
    let cache_key = format!("{}:{}:{}", prefix, function_name, args_hash);

    // 尝试从缓存中获取
    if let Some(cached_data) = db.get::<CachedValue<R>>(&cache_key)
        .map_err(|e| format!("Failed to retrieve from cache for key '{}': {}", cache_key, e))? {
        if SystemTime::now().duration_since(cached_data.timestamp)
            .map_err(|e| format!("Time calculation error: {}", e))? < Duration::from_secs(ttl) {
            return Ok(cached_data.value);
        }
    }

    // 缓存未命中或过期，执行原始函数
    let result = func(args).await
        .map_err(|e| format!("Function '{}' execution failed: {}", function_name, e))?;

    // 存储结果到缓存
    db.put(&cache_key, &CachedValue { 
        value: result.clone(), // 需要R实现Clone
        timestamp: SystemTime::now(),
    }).map_err(|e| format!("Failed to store to cache for key '{}': {}", cache_key, e))?;

    Ok(result)
}

// 辅助结构体用于存储缓存值和时间戳
#[derive(Serialize, Deserialize, Clone)]
struct CachedValue<T> {
    value: T,
    timestamp: SystemTime,
}

/// A convenient macro for caching async function results.
/// 
/// This macro provides a simple interface for caching the results of async function calls.
/// It automatically handles cache key generation, TTL expiration, and result storage/retrieval.
///
/// # Syntax
///
/// ```rust,ignore
/// kv_cache!(db, prefix, ttl_seconds, function_name, arg1, arg2, ...)
/// ```
///
/// # Arguments
///
/// * `db` - A KvDbLocal instance for caching
/// * `prefix` - String prefix for cache keys to avoid collisions
/// * `ttl_seconds` - Time-to-live for cached results in seconds
/// * `function_name` - The async function to cache (must be a path)
/// * `args...` - Arguments to pass to the function (0-5 arguments supported)
///
/// # Examples
///
/// ```rust,ignore
/// use sean_rust_utils::{KvDbLocal, kv_cache};
///
/// async fn fetch_data(id: u64) -> Result<String, Box<dyn std::error::Error>> {
///     // Expensive operation like API call or database query
///     Ok(format!("data-{}", id))
/// }
///
/// async fn process_data(input: String, multiplier: u64) -> Result<String, Box<dyn std::error::Error>> {
///     Ok(format!("{}-{}", input, multiplier))
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let db = KvDbLocal::memory()?;
///
///     // Cache single argument function call for 5 minutes
///     let result1 = kv_cache!(db.clone(), "api", 300, fetch_data, 123).await?;
///
///     // Cache two argument function call for 1 hour
///     let result2 = kv_cache!(db.clone(), "processing", 3600, process_data, "input".to_string(), 42).await?;
///
///     Ok(())
/// }
/// ```
///
/// # Supported Argument Patterns
///
/// - Zero arguments: `kv_cache!(db, prefix, ttl, func)`
/// - Single argument: `kv_cache!(db, prefix, ttl, func, arg)`
/// - Two arguments: `kv_cache!(db, prefix, ttl, func, arg1, arg2)`
/// - Three arguments: `kv_cache!(db, prefix, ttl, func, arg1, arg2, arg3)`
/// - Four arguments: `kv_cache!(db, prefix, ttl, func, arg1, arg2, arg3, arg4)`
/// - Five arguments: `kv_cache!(db, prefix, ttl, func, arg1, arg2, arg3, arg4, arg5)`
///
/// # Notes
///
/// - Functions must be async and return `Result<T, Box<dyn std::error::Error>>`
/// - Arguments must implement `Serialize + Hash + Send + 'static`
/// - Return types must implement `Serialize + DeserializeOwned + Clone + 'static`
/// - Cache keys are generated using function name, arguments hash, and prefix
/// - Expired cache entries are automatically refreshed on next access
#[macro_export]
macro_rules! kv_cache {
    // Zero arguments version
    ($db:expr, $prefix:expr, $ttl:expr, $func:path) => {
        $crate::kv_cache::cache_result(
            $db,
            $prefix.to_string(),
            stringify!($func).to_string(),
            $ttl,
            |_args| async move {
                $func().await
            },
            (), // Empty tuple for no arguments
        )
    };
    // Single argument version
    ($db:expr, $prefix:expr, $ttl:expr, $func:path, $arg:expr) => {
        $crate::kv_cache::cache_result(
            $db,
            $prefix.to_string(),
            stringify!($func).to_string(),
            $ttl,
            |args| async move {
                $func(args).await
            },
            $arg,
        )
    };
    // Two arguments version
    ($db:expr, $prefix:expr, $ttl:expr, $func:path, $arg1:expr, $arg2:expr) => {
        $crate::kv_cache::cache_result(
            $db,
            $prefix.to_string(),
            stringify!($func).to_string(),
            $ttl,
            |args| async move {
                $func(args.0, args.1).await
            },
            ($arg1, $arg2),
        )
    };
    // Three arguments version
    ($db:expr, $prefix:expr, $ttl:expr, $func:path, $arg1:expr, $arg2:expr, $arg3:expr) => {
        $crate::kv_cache::cache_result(
            $db,
            $prefix.to_string(),
            stringify!($func).to_string(),
            $ttl,
            |args| async move {
                $func(args.0, args.1, args.2).await
            },
            ($arg1, $arg2, $arg3),
        )
    };
    // Four arguments version
    ($db:expr, $prefix:expr, $ttl:expr, $func:path, $arg1:expr, $arg2:expr, $arg3:expr, $arg4:expr) => {
        $crate::kv_cache::cache_result(
            $db,
            $prefix.to_string(),
            stringify!($func).to_string(),
            $ttl,
            |args| async move {
                $func(args.0, args.1, args.2, args.3).await
            },
            ($arg1, $arg2, $arg3, $arg4),
        )
    };
    // Five arguments version
    ($db:expr, $prefix:expr, $ttl:expr, $func:path, $arg1:expr, $arg2:expr, $arg3:expr, $arg4:expr, $arg5:expr) => {
        $crate::kv_cache::cache_result(
            $db,
            $prefix.to_string(),
            stringify!($func).to_string(),
            $ttl,
            |args| async move {
                $func(args.0, args.1, args.2, args.3, args.4).await
            },
            ($arg1, $arg2, $arg3, $arg4, $arg5),
        )
    };
}
