use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use crate::test_utils::load_test_url::RequestConfig;
use std::time::Duration;

pub async fn curl_to_reqwest(curl_command: &str) -> Result<Value> {
    // 使用已有的 RequestConfig::from_curl_command 解析 curl 命令
    let request_config = RequestConfig::from_curl_command(curl_command)
        .map_err(|e| anyhow::anyhow!("Failed to parse curl command: {}", e))?;
    
    // 创建 HTTP 客户端
    let client = Client::new();
    
    // 构建请求
    let mut request = client.request(request_config.method, &request_config.url)
        .headers(request_config.headers)
        .timeout(Duration::from_secs(30));
    
    // 添加请求体（如果有）
    if let Some(body) = request_config.body {
        request = request.body(body);
    }
    
    // 发送请求并获取响应
    let response = request.send().await?;
    let response_text = response.text().await?;
    
    // 尝试解析为 JSON，如果失败则返回字符串包装的 JSON
    match serde_json::from_str::<Value>(&response_text) {
        Ok(json_value) => Ok(json_value),
        Err(_) => Ok(Value::String(response_text)),
    }
}