use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, Method, Response, StatusCode, header::HeaderMap, header::HeaderName, header::HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tokio::sync::Mutex;
use std::str::FromStr;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// 测试结果结构体
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TestResults {
    pub actual_duration_seconds: f64,
    pub requests_sent: u64,
    pub requests_completed: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub success_rate_percent: f64,
    pub actual_requests_per_second: f64,
}

/// HTTP请求配置结构体
#[derive(Debug, Clone)]
pub struct RequestConfig {
    pub url: String,
    pub method: Method,
    pub headers: HeaderMap,
    pub body: Option<String>,
    pub name: Option<String>, // 用于识别不同的请求
}

impl RequestConfig {
    /// 从简单URL创建GET请求配置
    pub fn from_url(url: &str) -> Self {
        Self {
            url: url.to_string(),
            method: Method::GET,
            headers: HeaderMap::new(),
            body: None,
            name: None,
        }
    }
    
    /// 从curl命令解析请求配置
    pub fn from_curl_command(curl_command: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut url = String::new();
        let mut method = Method::GET;
        let mut headers = HeaderMap::new();
        let mut body: Option<String> = None;
        
        // 移除curl命令开头，并分割参数
        let command = curl_command.trim_start_matches("curl").trim();
        let parts = parse_curl_args(command)?;
        
        // 解析各个参数
        let mut i = 0;
        while i < parts.len() {
            let part = &parts[i];
            
            match part.as_str() {
                "-X" | "--request" => {
                    if i + 1 < parts.len() {
                        method = Method::from_str(&parts[i + 1].to_uppercase())?;
                        i += 2;
                    } else {
                        i += 1;
                    }
                },
                "-H" | "--header" => {
                    if i + 1 < parts.len() {
                        let header = &parts[i + 1];
                        if let Some(colon_pos) = header.find(':') {
                            let name = header[..colon_pos].trim();
                            let value = header[colon_pos + 1..].trim();
                            
                            if let (Ok(header_name), Ok(header_value)) = (
                                HeaderName::from_str(name),
                                HeaderValue::from_str(value)
                            ) {
                                headers.insert(header_name, header_value);
                            }
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                },
                "-d" | "--data" | "--data-raw" => {
                    if i + 1 < parts.len() {
                        body = Some(parts[i + 1].clone());
                        // POST data implies POST method if not specified
                        if method == Method::GET {
                            method = Method::POST;
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                },
                _ => {
                    // 如果不是选项（不以-开头），则认为是URL
                    if !part.starts_with('-') && url.is_empty() {
                        url = part.clone();
                    }
                    i += 1;
                }
            }
        }
        
        if url.is_empty() {
            return Err("未找到URL".into());
        }
        
        // 如果有body但没有Content-Type，添加默认的
        if body.is_some() && !headers.contains_key("content-type") {
            headers.insert("content-type", HeaderValue::from_static("application/json"));
        }
        
        Ok(Self {
            url,
            method,
            headers,
            body,
            name: None,
        })
    }
    
    /// 设置请求名称
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }
}

/// 解析curl命令参数（增强版本，支持转义符号）
fn parse_curl_args(command: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut args = Vec::new();
    let mut current_arg = String::new();
    let mut in_quotes = false;
    let mut quote_char = '"';
    let mut escape_next = false;
    let mut i = 0;
    let chars: Vec<char> = command.chars().collect();
    
    while i < chars.len() {
        let ch = chars[i];
        
        if escape_next {
            // 处理转义字符
            match ch {
                'n' => current_arg.push('\n'),
                't' => current_arg.push('\t'),
                'r' => current_arg.push('\r'),
                '\\' => current_arg.push('\\'),
                '"' => current_arg.push('"'),
                '\'' => current_arg.push('\''),
                _ => {
                    // 对于其他字符，保持原样
                    current_arg.push(ch);
                }
            }
            escape_next = false;
        } else {
            match ch {
                '\\' => {
                    // 检查下一个字符来决定如何处理
                    if i + 1 < chars.len() {
                        let next_ch = chars[i + 1];
                        if in_quotes {
                            // 在引号内，处理转义
                            escape_next = true;
                        } else if next_ch == ' ' || next_ch == '\t' {
                            // 行继续符（反斜杠 + 空格）
                            i += 1; // 跳过下一个空格
                            while i + 1 < chars.len() && (chars[i + 1] == ' ' || chars[i + 1] == '\t') {
                                i += 1; // 跳过多余的空格
                            }
                        } else {
                            // 转义下一个字符
                            escape_next = true;
                        }
                    } else {
                        // 最后一个字符是反斜杠，直接添加
                        current_arg.push('\\');
                    }
                }
                '"' | '\'' => {
                    if !in_quotes {
                        in_quotes = true;
                        quote_char = ch;
                    } else if ch == quote_char {
                        in_quotes = false;
                    } else {
                        // 在其他类型的引号内，正常字符
                        current_arg.push(ch);
                    }
                }
                ' ' | '\t' => {
                    if in_quotes {
                        current_arg.push(ch);
                    } else if !current_arg.is_empty() {
                        args.push(current_arg.trim().to_string());
                        current_arg.clear();
                        
                        // 跳过多余的空格
                        while i + 1 < chars.len() && (chars[i + 1] == ' ' || chars[i + 1] == '\t') {
                            i += 1;
                        }
                    }
                }
                '\n' | '\r' => {
                    // 换行符处理：如果不在引号内，视为参数分隔
                    if in_quotes {
                        current_arg.push(ch);
                    } else if !current_arg.is_empty() {
                        args.push(current_arg.trim().to_string());
                        current_arg.clear();
                    }
                }
                _ => {
                    current_arg.push(ch);
                }
            }
        }
        
        i += 1;
    }
    
    if !current_arg.is_empty() {
        args.push(current_arg.trim().to_string());
    }
    
    Ok(args)
}

/// 判断响应是否成功：
/// 1) HTTP 状态码为 2xx
/// 2) 如果是 JSON 且包含 GraphQL 风格的非空 errors 数组，则视为失败
async fn response_is_success(response: Response) -> bool {
    if !response.status().is_success() {
        return false;
    }

    // 尝试解析为 JSON，若不是 JSON 或没有 errors 字段，则按成功处理
    match response.json::<Value>().await {
        Ok(Value::Object(map)) => {
            if let Some(errors) = map.get("errors").and_then(|v| v.as_array()) {
                return errors.is_empty();
            }
            true
        }
        Ok(_) => true,
        Err(_) => true,
    }
}

/// 单次请求日志
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestLog {
    pub config_name: String,
    pub method: String,
    pub url: String,
    pub status: Option<u16>,
    pub success: bool,
    pub duration_ms: u128,
    pub error: Option<String>,
    pub response_excerpt: Option<String>,
    pub response_content_key: String, // 响应内容标识key
}

/// 响应状态统计项
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseStat {
    pub count: u64,
    pub percentage: f64,
}

/// 响应归集统计
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseSummary {
    pub total_requests: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub success_rate: f64,
    pub http_status_distribution: HashMap<String, ResponseStat>,
    pub error_distribution: HashMap<String, ResponseStat>,
    pub response_content_distribution: HashMap<String, ResponseStat>, // 按响应内容分类
    pub response_time_stats: ResponseTimeStats,
}

/// 响应时间统计
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseTimeStats {
    pub min_ms: u128,
    pub max_ms: u128,
    pub avg_ms: f64,
    pub p50_ms: u128,
    pub p90_ms: u128,
    pub p95_ms: u128,
    pub p99_ms: u128,
}

/// 根据状态码和响应文本判断是否成功（与 GraphQL 错误规则一致）
fn response_text_is_success(status: StatusCode, body_text: &str) -> bool {
    if !status.is_success() {
        return false;
    }
    // 尝试解析 JSON 并检查 errors 字段
    match serde_json::from_str::<Value>(body_text) {
        Ok(Value::Object(map)) => {
            if let Some(errors) = map.get("errors").and_then(|v| v.as_array()) {
                return errors.is_empty();
            }
            true
        }
        _ => true,
    }
}

/// 对单个URL执行负载测试
/// 
/// # 参数
/// * `url` - 要测试的目标URL
/// * `requests` - 总请求数
/// * `duration` - 测试持续时间（秒）
/// 
/// # 返回值
/// * `Result<TestResults, Box<dyn std::error::Error>>` - 测试结果或错误
/// 
/// # 示例
/// ```rust
/// use load_test_url::load_test_url;
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let results = load_test_url("https://httpbin.org/get", 100, 10).await?;
///     println!("成功率: {:.2}%", results.success_rate_percent);
///     Ok(())
/// }
/// ```
pub async fn load_test_url(
    url: &str,
    requests: u64,
    duration: u64,
) -> Result<TestResults, Box<dyn std::error::Error>> {
    println!("🚀 开始负载测试");
    println!("📊 目标URL: {}", url);
    println!("⏱️  持续时间: {} 秒", duration);
    println!("🔢 总请求数: {}", requests);
    
    // 计算并发频率
    let requests_per_second = requests as f64 / duration as f64;
    let interval = Duration::from_secs_f64(1.0 / requests_per_second);
    
    println!("⚡ 计算得出的请求频率: {:.2} 请求/秒", requests_per_second);
    println!("📏 请求间隔: {:.2} 毫秒", interval.as_millis());
    println!();

    // 创建HTTP客户端
    let client = Arc::new(Client::new());
    
    // 统计计数器
    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));
    let total_sent = Arc::new(AtomicU64::new(0));
    
    // 进度条
    let pb = ProgressBar::new(requests);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({per_sec}) [{eta}] 成功:{msg}")
            .unwrap()
            .progress_chars("#>-")
    );

    let start_time = Instant::now();
    
    // 创建任务列表
    let mut tasks = Vec::new();
    let mut interval_timer = time::interval(interval);
    interval_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    
    let deadline = start_time + Duration::from_secs(duration);
    
    // 启动并发请求任务
    for _ in 0..std::cmp::min(requests, 50) {
        let client = client.clone();
        let url = url.to_string();
        let success_count = success_count.clone();
        let failure_count = failure_count.clone();
        let total_sent = total_sent.clone();
        let pb = pb.clone();
        
        let task = tokio::spawn(async move {
            // 发送请求
            total_sent.fetch_add(1, Ordering::Relaxed);
            
            match client.get(&url).timeout(Duration::from_secs(10)).send().await {
                Ok(response) => {
                    if response_is_success(response).await {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failure_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    failure_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            let success = success_count.load(Ordering::Relaxed);
            let total = success + failure_count.load(Ordering::Relaxed);
            let success_rate = if total > 0 { success as f64 / total as f64 * 100.0 } else { 0.0 };
            
            pb.set_message(format!("{} ({:.1}%)", success, success_rate));
            pb.inc(1);
        });
        
        tasks.push(task);
    }
    
    // 控制发送频率，启动剩余请求
    let mut remaining_requests = requests.saturating_sub(50);
    while remaining_requests > 0 && Instant::now() < deadline {
        interval_timer.tick().await;
        
        if Instant::now() >= deadline {
            break;
        }
        
        let client = client.clone();
        let url = url.to_string();
        let success_count = success_count.clone();
        let failure_count = failure_count.clone();
        let total_sent = total_sent.clone();
        let pb = pb.clone();
        
        let task = tokio::spawn(async move {
            total_sent.fetch_add(1, Ordering::Relaxed);
            
            match client.get(&url).timeout(Duration::from_secs(10)).send().await {
                Ok(response) => {
                    if response_is_success(response).await {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failure_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    failure_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            let success = success_count.load(Ordering::Relaxed);
            let total = success + failure_count.load(Ordering::Relaxed);
            let success_rate = if total > 0 { success as f64 / total as f64 * 100.0 } else { 0.0 };
            
            pb.set_message(format!("{} ({:.1}%)", success, success_rate));
            pb.inc(1);
        });
        
        tasks.push(task);
        remaining_requests -= 1;
    }
    
    // 等待所有任务完成或超时
    for task in tasks {
        let _ = task.await;
    }
    
    pb.finish();
    
    // 最终统计
    let elapsed = start_time.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let failure = failure_count.load(Ordering::Relaxed);
    let total_completed = success + failure;
    let sent = total_sent.load(Ordering::Relaxed);
    
    println!("\n📈 测试结果:");
    println!("⏱️  实际执行时间: {:.2} 秒", elapsed.as_secs_f64());
    println!("📤 发送请求数: {}", sent);
    println!("📥 完成请求数: {}", total_completed);
    println!("✅ 成功请求数: {}", success);
    println!("❌ 失败请求数: {}", failure);
    
    let success_rate = if total_completed > 0 { success as f64 / total_completed as f64 * 100.0 } else { 0.0 };
    let actual_rps = if elapsed.as_secs_f64() > 0.0 { total_completed as f64 / elapsed.as_secs_f64() } else { 0.0 };
    
    if total_completed > 0 {
        println!("📊 成功率: {:.2}%", success_rate);
        println!("⚡ 实际请求频率: {:.2} 请求/秒", actual_rps);
    }
    
    // 返回测试结果
    Ok(TestResults {
        actual_duration_seconds: elapsed.as_secs_f64(),
        requests_sent: sent,
        requests_completed: total_completed,
        successful_requests: success,
        failed_requests: failure,
        success_rate_percent: success_rate,
        actual_requests_per_second: actual_rps,
    })
}

/// 对单个URL执行静默负载测试（不显示进度条和控制台输出）
/// 
/// # 参数
/// * `url` - 要测试的目标URL
/// * `requests` - 总请求数
/// * `duration` - 测试持续时间（秒）
/// 
/// # 返回值
/// * `Result<TestResults, Box<dyn std::error::Error>>` - 测试结果或错误
pub async fn load_test_url_silent(
    url: &str,
    requests: u64,
    duration: u64,
) -> Result<TestResults, Box<dyn std::error::Error>> {
    // 计算并发频率
    let requests_per_second = requests as f64 / duration as f64;
    let interval = Duration::from_secs_f64(1.0 / requests_per_second);

    // 创建HTTP客户端
    let client = Arc::new(Client::new());
    
    // 统计计数器
    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));
    let total_sent = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();
    
    // 创建任务列表
    let mut tasks = Vec::new();
    let mut interval_timer = time::interval(interval);
    interval_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    
    let deadline = start_time + Duration::from_secs(duration);
    
    // 启动并发请求任务
    for _ in 0..std::cmp::min(requests, 50) {
        let client = client.clone();
        let url = url.to_string();
        let success_count = success_count.clone();
        let failure_count = failure_count.clone();
        let total_sent = total_sent.clone();
        
        let task = tokio::spawn(async move {
            // 发送请求
            total_sent.fetch_add(1, Ordering::Relaxed);
            
            match client.get(&url).timeout(Duration::from_secs(10)).send().await {
                Ok(response) => {
                    if response_is_success(response).await {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failure_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    failure_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        
        tasks.push(task);
    }
    
    // 控制发送频率，启动剩余请求
    let mut remaining_requests = requests.saturating_sub(50);
    while remaining_requests > 0 && Instant::now() < deadline {
        interval_timer.tick().await;
        
        if Instant::now() >= deadline {
            break;
        }
        
        let client = client.clone();
        let url = url.to_string();
        let success_count = success_count.clone();
        let failure_count = failure_count.clone();
        let total_sent = total_sent.clone();
        
        let task = tokio::spawn(async move {
            total_sent.fetch_add(1, Ordering::Relaxed);
            
            match client.get(&url).timeout(Duration::from_secs(10)).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failure_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    failure_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        
        tasks.push(task);
        remaining_requests -= 1;
    }
    
    // 等待所有任务完成或超时
    for task in tasks {
        let _ = task.await;
    }
    
    // 最终统计
    let elapsed = start_time.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let failure = failure_count.load(Ordering::Relaxed);
    let total_completed = success + failure;
    let sent = total_sent.load(Ordering::Relaxed);
    
    let success_rate = if total_completed > 0 { success as f64 / total_completed as f64 * 100.0 } else { 0.0 };
    let actual_rps = if elapsed.as_secs_f64() > 0.0 { total_completed as f64 / elapsed.as_secs_f64() } else { 0.0 };
    
    // 返回测试结果
    Ok(TestResults {
        actual_duration_seconds: elapsed.as_secs_f64(),
        requests_sent: sent,
        requests_completed: total_completed,
        successful_requests: success,
        failed_requests: failure,
        success_rate_percent: success_rate,
        actual_requests_per_second: actual_rps,
    })
}

/// 对多个请求配置执行负载测试
/// 
/// # 参数
/// * `requests_configs` - 请求配置数组
/// * `requests_per_config` - 每个配置的请求数
/// * `duration` - 测试持续时间（秒）
/// 
/// # 返回值
/// * `Result<LoadTestOutput, Box<dyn std::error::Error>>` - 测试结果或错误
/// 分析请求日志生成响应归集统计
pub fn analyze_response_logs(request_logs: &[RequestLog]) -> ResponseSummary {
    if request_logs.is_empty() {
        return ResponseSummary {
            total_requests: 0,
            success_count: 0,
            failure_count: 0,
            success_rate: 0.0,
            http_status_distribution: HashMap::new(),
            error_distribution: HashMap::new(),
            response_content_distribution: HashMap::new(),
            response_time_stats: ResponseTimeStats {
                min_ms: 0,
                max_ms: 0,
                avg_ms: 0.0,
                p50_ms: 0,
                p90_ms: 0,
                p95_ms: 0,
                p99_ms: 0,
            },
        };
    }

    let total_requests = request_logs.len() as u64;
    let success_count = request_logs.iter().filter(|log| log.success).count() as u64;
    let failure_count = total_requests - success_count;
    let success_rate = if total_requests > 0 { 
        success_count as f64 / total_requests as f64 * 100.0 
    } else { 
        0.0 
    };

    // 统计HTTP状态码分布
    let mut status_counts: HashMap<String, u64> = HashMap::new();
    for log in request_logs {
        let status_key = if let Some(status) = log.status {
            format!("{} {}", status, get_status_description(status))
        } else {
            "Network Error".to_string()
        };
        *status_counts.entry(status_key).or_insert(0) += 1;
    }

    let http_status_distribution: HashMap<String, ResponseStat> = status_counts
        .into_iter()
        .map(|(status, count)| {
            let percentage = count as f64 / total_requests as f64 * 100.0;
            (status, ResponseStat { count, percentage })
        })
        .collect();

    // 统计错误分布
    let mut error_counts: HashMap<String, u64> = HashMap::new();
    for log in request_logs {
        if !log.success {
            let error_key = if let Some(error) = &log.error {
                // 网络错误等
                let simplified_error: String = error.chars().take(100).collect();
                simplified_error
            } else if let Some(status) = log.status {
                // HTTP状态码成功但业务逻辑失败（如GraphQL errors）
                if status == 200 {
                    // 尝试从响应内容中提取错误信息
                    if let Some(excerpt) = &log.response_excerpt {
                        extract_business_error_from_response(excerpt)
                    } else {
                        "HTTP 200 Business Logic Error".to_string()
                    }
                } else {
                    format!("HTTP {}", status)
                }
            } else {
                "Unknown Error".to_string()
            };
            *error_counts.entry(error_key).or_insert(0) += 1;
        }
    }

    let error_distribution: HashMap<String, ResponseStat> = error_counts
        .into_iter()
        .map(|(error, count)| {
            let percentage = count as f64 / total_requests as f64 * 100.0;
            (error, ResponseStat { count, percentage })
        })
        .collect();

    // 统计响应内容分布 (只针对失败请求)
    let mut response_content_counts: HashMap<String, u64> = HashMap::new();
    for log in request_logs {
        if !log.success {
            *response_content_counts.entry(log.response_content_key.clone()).or_insert(0) += 1;
        }
    }

    let response_content_distribution: HashMap<String, ResponseStat> = response_content_counts
        .into_iter()
        .map(|(content_key, count)| {
            let percentage = count as f64 / total_requests as f64 * 100.0;
            (content_key, ResponseStat { count, percentage })
        })
        .collect();

    // 计算响应时间统计
    let mut durations: Vec<u128> = request_logs.iter().map(|log| log.duration_ms).collect();
    durations.sort_unstable();

    let min_ms = durations.first().copied().unwrap_or(0);
    let max_ms = durations.last().copied().unwrap_or(0);
    let avg_ms = if !durations.is_empty() {
        durations.iter().sum::<u128>() as f64 / durations.len() as f64
    } else {
        0.0
    };

    let percentile = |p: f64| -> u128 {
        if durations.is_empty() { return 0; }
        let index = ((durations.len() as f64 * p / 100.0) as usize).min(durations.len() - 1);
        durations[index]
    };

    let response_time_stats = ResponseTimeStats {
        min_ms,
        max_ms,
        avg_ms,
        p50_ms: percentile(50.0),
        p90_ms: percentile(90.0),
        p95_ms: percentile(95.0),
        p99_ms: percentile(99.0),
    };

    ResponseSummary {
        total_requests,
        success_count,
        failure_count,
        success_rate,
        http_status_distribution,
        error_distribution,
        response_content_distribution,
        response_time_stats,
    }
}

/// 从响应内容中提取业务逻辑错误信息（如GraphQL errors）
fn extract_business_error_from_response(response_text: &str) -> String {
    // 尝试解析JSON并查找错误信息
    if let Ok(json_value) = serde_json::from_str::<Value>(response_text) {
        if let Value::Object(obj) = &json_value {
            // 检查GraphQL errors格式
            if let Some(errors) = obj.get("errors").and_then(|v| v.as_array()) {
                if let Some(first_error) = errors.first() {
                    if let Some(message) = first_error.get("message").and_then(|v| v.as_str()) {
                        return format!("GraphQL Error: {}", message);
                    }
                }
                return "GraphQL Error: Unknown".to_string();
            }
            
            // 检查其他常见错误格式
            if let Some(error_msg) = obj.get("error").and_then(|v| v.as_str()) {
                return format!("API Error: {}", error_msg);
            }
            if let Some(message) = obj.get("message").and_then(|v| v.as_str()) {
                return format!("Message: {}", message);
            }
        }
    }
    
    // 如果无法解析，返回通用错误
    "HTTP 200 Business Logic Error".to_string()
}

/// 生成响应内容的标识key
fn generate_response_content_key(status_code: Option<u16>, response_text: &str, error: Option<&String>) -> String {
    // 错误情况
    if let Some(err) = error {
        return format!("ERROR: {}", &err[..std::cmp::min(50, err.len())]);
    }

    let status = status_code.unwrap_or(0);
    
    // 空响应
    if response_text.is_empty() {
        return format!("HTTP {} Empty", status);
    }

    // 使用响应内容的hash作为key，但显示前100个字符作为识别
    let mut hasher = DefaultHasher::new();
    response_text.hash(&mut hasher);
    let hash = hasher.finish();
    
    // 截取响应内容前100个字符作为展示
    let display_content: String = response_text.chars()
        .filter(|&c| c != '\n' && c != '\r' && c != '\t')
        .take(100)
        .collect();
    
    format!("HTTP {} - {} (#{:x})", status, display_content, hash & 0xFFFF)
}


/// 获取HTTP状态码描述
fn get_status_description(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Other",
    }
}

/// 多请求负载测试返回结构
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoadTestOutput {
    pub results: TestResults,
    pub request_logs: Vec<RequestLog>,
    pub response_summary: ResponseSummary,
}

pub async fn load_test_requests(
    request_configs: &[RequestConfig],
    requests_per_config: u64,
    duration: u64,
) -> Result<LoadTestOutput, Box<dyn std::error::Error>> {
    if request_configs.is_empty() {
        return Err("请求配置数组不能为空".into());
    }
    
    let total_requests = requests_per_config * request_configs.len() as u64;
    
    println!("🚀 开始多请求负载测试");
    println!("📊 请求配置数: {}", request_configs.len());
    for (i, config) in request_configs.iter().enumerate() {
        let default_name = format!("Request{}", i + 1);
        let name = config.name.as_ref().unwrap_or(&default_name);
        println!("   [{}] {} {} {}", i + 1, config.method, config.url, name);
    }
    println!("⏱️  持续时间: {} 秒", duration);
    println!("🔢 每个配置请求数: {}", requests_per_config);
    println!("🔢 总请求数: {}", total_requests);
    
    // 计算每个配置的请求频率（每个配置独立计算）
    let requests_per_second_per_config = requests_per_config as f64 / duration as f64;
    let interval_per_config = Duration::from_secs_f64(1.0 / requests_per_second_per_config);
    let total_requests_per_second = requests_per_second_per_config * request_configs.len() as f64;
    
    println!("⚡ 每个配置请求频率: {:.2} 请求/秒", requests_per_second_per_config);
    println!("⚡ 总体请求频率: {:.2} 请求/秒", total_requests_per_second);
    println!("📏 每个配置请求间隔: {:.2} 毫秒", interval_per_config.as_millis());
    println!();

    // 创建HTTP客户端
    let client = Arc::new(Client::new());
    
    // 统计计数器
    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));
    let total_sent = Arc::new(AtomicU64::new(0));
    let request_logs: Arc<Mutex<Vec<RequestLog>>> = Arc::new(Mutex::new(Vec::with_capacity(
        (requests_per_config as usize) * request_configs.len(),
    )));
    
    // 进度条
    let pb = ProgressBar::new(total_requests);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({per_sec}) [{eta}] 成功:{msg}")
            .unwrap()
            .progress_chars("#>-")
    );

    let start_time = Instant::now();
    
    // 为每个配置创建独立的任务组
    let mut config_tasks = Vec::new();
    
    for (_config_index, config) in request_configs.iter().enumerate() {
        let client = client.clone();
        let request_config = config.clone();
        let success_count = success_count.clone();
        let failure_count = failure_count.clone();
        let total_sent = total_sent.clone();
        let pb = pb.clone();
        let request_logs = request_logs.clone();
        
        // 为每个配置创建独立的任务
        let config_task = tokio::spawn(async move {
            let mut interval_timer = time::interval(interval_per_config);
            interval_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            
            let deadline = start_time + Duration::from_secs(duration);
            let mut request_tasks = Vec::new();
            
            // 为当前配置发送所有请求
            for _ in 0..requests_per_config {
                // 等待下一个发送时机
                interval_timer.tick().await;
                
                // 检查是否已超时
                if Instant::now() >= deadline {
                    break;
                }
                
                let client = client.clone();
                let request_config = request_config.clone();
                let success_count = success_count.clone();
                let failure_count = failure_count.clone();
                let total_sent = total_sent.clone();
                let pb = pb.clone();
                let request_logs = request_logs.clone();
                
                let request_task = tokio::spawn(async move {
                    total_sent.fetch_add(1, Ordering::Relaxed);
                    let started = Instant::now();
                    
                    let mut request = client.request(request_config.method.clone(), &request_config.url)
                        .headers(request_config.headers)
                        .timeout(Duration::from_secs(10));
                        
                    if let Some(body) = request_config.body {
                        request = request.body(body);
                    }
                    
                    match request.send().await {
                        Ok(response) => {
                            let status = response.status();
                            let text = response.text().await.unwrap_or_default();
                            let success = response_text_is_success(status, &text);
                            if success {
                                success_count.fetch_add(1, Ordering::Relaxed);
                            } else {
                                failure_count.fetch_add(1, Ordering::Relaxed);
                            }
                            let duration_ms = started.elapsed().as_millis();
                            let default_name = "Unknown".to_string();
                            let name = request_config.name.as_ref().unwrap_or(&default_name).clone();
                            let excerpt = if text.is_empty() { None } else {
                                let truncated: String = text.chars().take(500).collect();
                                Some(truncated)
                            };
                            
                            // 生成响应内容key
                            let response_content_key = generate_response_content_key(Some(status.as_u16()), &text, None);
                            
                            let log = RequestLog {
                                config_name: name,
                                method: request_config.method.to_string(),
                                url: request_config.url.clone(),
                                status: Some(status.as_u16()),
                                success,
                                duration_ms,
                                error: None,
                                response_excerpt: excerpt,
                                response_content_key,
                            };
                            let mut guard = request_logs.lock().await;
                            guard.push(log);
                        }
                        Err(err) => {
                            failure_count.fetch_add(1, Ordering::Relaxed);
                            let duration_ms = started.elapsed().as_millis();
                            let default_name = "Unknown".to_string();
                            let name = request_config.name.as_ref().unwrap_or(&default_name).clone();
                            
                            // 生成错误响应key
                            let response_content_key = generate_response_content_key(None, "", Some(&err.to_string()));
                            
                            let log = RequestLog {
                                config_name: name,
                                method: request_config.method.to_string(),
                                url: request_config.url.clone(),
                                status: None,
                                success: false,
                                duration_ms,
                                error: Some(err.to_string()),
                                response_excerpt: None,
                                response_content_key,
                            };
                            let mut guard = request_logs.lock().await;
                            guard.push(log);
                        }
                    }
                    
                    let success = success_count.load(Ordering::Relaxed);
                    let total = success + failure_count.load(Ordering::Relaxed);
                    let success_rate = if total > 0 { success as f64 / total as f64 * 100.0 } else { 0.0 };
                    
                    pb.set_message(format!("{} ({:.1}%)", success, success_rate));
                    pb.inc(1);
                });
                
                request_tasks.push(request_task);
            }
            
            // 等待当前配置的所有请求完成
            for task in request_tasks {
                let _ = task.await;
            }
        });
        
        config_tasks.push(config_task);
    }
    
    // 等待所有配置任务完成
    for task in config_tasks {
        let _ = task.await;
    }
    
    pb.finish();
    
    // 最终统计
    let elapsed = start_time.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let failure = failure_count.load(Ordering::Relaxed);
    let total_completed = success + failure;
    let sent = total_sent.load(Ordering::Relaxed);
    
    println!("\n📈 测试结果:");
    println!("⏱️  实际执行时间: {:.2} 秒", elapsed.as_secs_f64());
    println!("📤 发送请求数: {}", sent);
    println!("📥 完成请求数: {}", total_completed);
    println!("✅ 成功请求数: {}", success);
    println!("❌ 失败请求数: {}", failure);
    
    let success_rate = if total_completed > 0 { success as f64 / total_completed as f64 * 100.0 } else { 0.0 };
    let actual_rps = if elapsed.as_secs_f64() > 0.0 { total_completed as f64 / elapsed.as_secs_f64() } else { 0.0 };
    
    if total_completed > 0 {
        println!("📊 成功率: {:.2}%", success_rate);
        println!("⚡ 实际请求频率: {:.2} 请求/秒", actual_rps);
    }
    
    // 获取请求日志并生成响应摘要
    let logs = request_logs.lock().await.to_vec();
    let response_summary = analyze_response_logs(&logs);
    
    // 返回测试结果
    Ok(LoadTestOutput {
        results: TestResults {
            actual_duration_seconds: elapsed.as_secs_f64(),
            requests_sent: sent,
            requests_completed: total_completed,
            successful_requests: success,
            failed_requests: failure,
            success_rate_percent: success_rate,
            actual_requests_per_second: actual_rps,
        },
        request_logs: logs,
        response_summary,
    })
}
