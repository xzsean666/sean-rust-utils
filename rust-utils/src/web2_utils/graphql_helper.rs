use std::collections::HashMap;
use reqwest::{Client, Error as ReqwestError, header::{HeaderMap, HeaderValue}};
use serde::{Deserialize};
use serde_json::{json, Value};
use std::fmt::{self, Display, Formatter};

#[derive(Debug)]
pub enum GraphQLHelperError {
    Reqwest(ReqwestError),
    Io(std::io::Error),
    GraphQL(String),
}

impl Display for GraphQLHelperError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            GraphQLHelperError::Reqwest(e) => write!(f, "Reqwest Error: {}", e),
            GraphQLHelperError::Io(e) => write!(f, "IO Error: {}", e),
            GraphQLHelperError::GraphQL(e) => write!(f, "GraphQL Error: {}", e),
        }
    }
}

impl std::error::Error for GraphQLHelperError {}

impl From<ReqwestError> for GraphQLHelperError {
    fn from(err: ReqwestError) -> Self {
        GraphQLHelperError::Reqwest(err)
    }
}

impl From<std::io::Error> for GraphQLHelperError {
    fn from(err: std::io::Error) -> Self {
        GraphQLHelperError::Io(err)
    }
}

#[derive(Debug, Deserialize)]
pub struct GraphQLError {
    pub message: String,
}

#[derive(Deserialize)]
pub struct GraphQLResponse {
    pub data: Option<Value>,
    pub errors: Option<Vec<GraphQLError>>,
}

pub struct GraphQLHelper {
    client: Client,
    endpoint: String,
}

impl GraphQLHelper {
    fn process_headers(headers_value: Value) -> HeaderMap {
        let mut header_map = HeaderMap::new();
        header_map.insert("Content-Type", HeaderValue::from_static("application/json"));

        if let Some(headers_obj) = headers_value.as_object() {
            for (key, value) in headers_obj {
                if let Some(s) = value.as_str() {
                    if let Ok(header_value) = HeaderValue::from_str(s) {
                        if let Ok(header_name) = reqwest::header::HeaderName::from_bytes(key.as_bytes()) {
                            header_map.insert(header_name, header_value);
                        } else {
                            eprintln!("Warning: Invalid header name '{}'", key);
                        }
                    } else {
                        eprintln!("Warning: Invalid header value for key '{}'", key);
                    }
                } else {
                    eprintln!("Warning: Header value for key '{}' is not a string", key);
                }
            }
        }
        header_map
    }

    pub fn new(
        endpoint: String,
        headers: Option<Value>,
    ) -> Self {
        let mut default_headers = HeaderMap::new();
        default_headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        if let Some(h) = headers {
            let processed_headers = GraphQLHelper::process_headers(h);
            for (key, value) in processed_headers.iter() {
                default_headers.insert(key.clone(), value.clone());
            }
        }

        let client = Client::builder()
            .default_headers(default_headers)
            .build()
            .expect("Failed to create reqwest client");

        GraphQLHelper { client, endpoint }
    }

    /**
     * 执行 GraphQL 查询
     * @param query GraphQL 查询语句
     * @param variables 查询变量
     * @returns 查询结果
     */
    pub async fn query(
        &self,
        query: String,
        variables: Option<HashMap<String, Value>>,
    ) -> Result<Value, GraphQLHelperError> {
        let request_body = json!({ "query": query, "variables": variables });

        let response = self.client
            .post(&self.endpoint)
            .json(&request_body)
            .send()
            .await?;

        let graphql_response: GraphQLResponse = response.json().await?;

        if let Some(errors) = graphql_response.errors {
            return Err(GraphQLHelperError::GraphQL(format!(
                "GraphQL Errors: {}",
                errors.into_iter().map(|e| e.message).collect::<Vec<_>>().join(", ")
            )));
        }

        graphql_response.data.ok_or_else(|| {
            GraphQLHelperError::GraphQL("GraphQL response data is empty".to_string())
        })
    }

    /**
     * 处理变量，自动序列化需要的值
     * @param variables 变量对象
     * @returns 处理后的变量对象
     */
    fn process_variables(
        &self,
        variables: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        variables
            .into_iter()
            .map(|(key, value)| {
                if value.is_object() && !value.is_array() {
                    (key, json!(value.to_string()))
                } else {
                    (key, value)
                }
            })
            .collect()
    }

    /**
     * 执行 GraphQL 变更操作
     * @param mutation 变更操作语句
     * @param variables 变更变量
     * @returns 变更结果
     */
    pub async fn mutate(
        &self,
        mutation: String,
        variables: Option<HashMap<String, Value>>,
    ) -> Result<Value, GraphQLHelperError> {
        let processed_variables = variables.map(|v| self.process_variables(v));
        let request_body = json!({ "query": mutation, "variables": processed_variables });

        let response = self.client
            .post(&self.endpoint)
            .json(&request_body)
            .send()
            .await?;

        let graphql_response: GraphQLResponse = response.json().await?;

        if let Some(errors) = graphql_response.errors {
            return Err(GraphQLHelperError::GraphQL(format!(
                "GraphQL Errors: {}",
                errors.into_iter().map(|e| e.message).collect::<Vec<_>>().join(", ")
            )));
        }

        graphql_response.data.ok_or_else(|| {
            GraphQLHelperError::GraphQL("GraphQL response data is empty".to_string())
        })
    }

    /**
     * 更新 GraphQL 客户端的请求头
     * @param headers 新的请求头
     */
    pub fn set_headers(&mut self, headers: Value) {
        let mut default_headers = HeaderMap::new();
        default_headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        let processed_headers = GraphQLHelper::process_headers(headers);
        for (key, value) in processed_headers.iter() {
            default_headers.insert(key.clone(), value.clone());
        }

        self.client = Client::builder()
            .default_headers(default_headers)
            .build()
            .expect("Failed to create reqwest client");
    }

    /**
     * 获取当前的 GraphQL 客户端实例
     * @returns GraphQL 客户端实例
     */
    pub fn get_client(&self) -> &Client {
        &self.client
    }
}
