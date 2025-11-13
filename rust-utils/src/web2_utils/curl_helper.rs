use anyhow::Result;
use reqwest::{Client, Method, header::{HeaderMap, HeaderName, HeaderValue}};
use serde_json::Value;
use std::time::Duration;
use std::str::FromStr;

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
    let parts = Self::parse_curl_args(command)?;
    
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
}

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