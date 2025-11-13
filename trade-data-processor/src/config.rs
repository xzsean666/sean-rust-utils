//! Configuration module for trade data processor
//!
//! This module defines the configuration structure for SSH connections,
//! input/output paths, and merge strategies.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// SSH server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshConfig {
    pub host: String,
    pub port: Option<u16>,
    pub username: String,
    pub password: Option<String>,
    pub private_key_path: Option<String>,
    /// Base input directory on this remote machine (e.g., "/hdd16/trade/wss-collector/data/mark-price/")
    pub input_base_path: String,
}

/// HTTP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Base URL of the HTTP server (e.g., "http://198.50.126.194:10048")
    pub base_url: String,
    /// Base input directory path (e.g., "mark-price")
    /// This will be appended to API endpoints
    pub input_base_path: String,
    /// Optional proxy URL for HTTP requests (e.g., "http://proxy.example.com:8080")
    #[serde(default)]
    pub proxy: Option<String>,
}

/// Local file configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalFileConfig {
    /// Base path for local files (e.g., "/data/mark-price")
    /// The program will append /{year}/{month}/{day} automatically
    pub base_path: String,
}

/// S3-compatible storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// Provider type: "aws", "b2" (Backblaze B2), "r2" (Cloudflare R2), or "generic"
    pub provider: String,
    /// S3 bucket name
    pub bucket: String,
    /// Access key ID
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Region (optional, will use provider defaults if not specified)
    #[serde(default)]
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services
    /// Examples:
    /// - Backblaze B2: "https://s3.us-west-002.backblazeb2.com"
    /// - Cloudflare R2: "https://<account-id>.r2.cloudflarestorage.com"
    /// - MinIO: "http://localhost:9000"
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Force path-style addressing (true for most S3-compatible services)
    /// AWS S3 uses virtual-hosted-style by default (false)
    #[serde(default)]
    pub force_path_style: Option<bool>,
    /// Base path prefix for storing data (e.g., "trade-data/mark-price")
    #[serde(default)]
    pub base_path: Option<String>,
    /// Local directory path for sync operations
    #[serde(default)]
    pub local_path: Option<String>,
    /// Remote prefix (destination path in S3 bucket) for sync operations
    #[serde(default)]
    pub remote_prefix: Option<String>,
    /// Cache database path for sync state tracking
    #[serde(default)]
    pub cache_db_path: Option<String>,
    /// Sync direction: "local_to_s3", "s3_to_local", or "bidirectional"
    #[serde(default)]
    pub sync_direction: Option<String>,
}

/// Data source configuration for a specific data type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    /// Type name (e.g., "mark-price")
    pub data_type: String,
    /// List of SSH server configurations to pull data from (optional)
    #[serde(default)]
    pub ssh_servers: Vec<SshConfig>,
    /// List of HTTP server configurations to pull data from (optional)
    #[serde(default)]
    pub http_servers: Vec<HttpConfig>,
    /// List of local file paths to read data from (optional)
    #[serde(default)]
    pub local_files: Vec<LocalFileConfig>,
}

/// Output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Base output path for parquet files
    pub path: String,
    /// Name prefix for parquet files
    pub name: String,
    /// Batch size for writing parquet files
    /// If None, all data will be written to a single file
    /// If Some(n), data will be split into multiple files with batch size n
    pub batch_size: Option<usize>,
    /// Whether to write to /tmp first and then copy to output directory
    /// This can improve performance by writing to faster storage first
    #[serde(default)]
    pub use_temp_dir: bool,
}

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Data source configurations
    pub data_sources: Vec<DataSourceConfig>,
    /// Output configuration
    pub output: OutputConfig,
}

impl Config {
    /// Load configuration from YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .context(format!("Failed to read config file: {:?}", path.as_ref()))?;
        
        let config: Config = serde_yaml::from_str(&content)
            .context("Failed to parse config YAML")?;
        
        Ok(config)
    }
    
    /// Find data source configuration by data type
    pub fn find_data_source(&self, data_type: &str) -> Option<&DataSourceConfig> {
        self.data_sources.iter()
            .find(|ds| ds.data_type == data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parsing() {
        let yaml = r#"
data_sources:
  - data_type: "mark-price"
    ssh_servers:
      - host: "192.168.1.100"
        port: 22
        username: "user1"
        password: "pass1"
        input_base_path: "/hdd16/trade/wss-collector/data/mark-price/"
      - host: "192.168.1.101"
        username: "user2"
        private_key_path: "/home/user/.ssh/id_rsa"
        input_base_path: "/data/mark-price/"

output:
  path: "/output/parquet"
  name: "mark-price"
  batch_size: 5000
"#;
        
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.data_sources.len(), 1);
        assert_eq!(config.data_sources[0].ssh_servers.len(), 2);
        assert_eq!(config.output.batch_size, Some(5000));
    }
    
    #[test]
    fn test_config_parsing_no_batch_size() {
        let yaml = r#"
data_sources:
  - data_type: "mark-price"
    ssh_servers:
      - host: "192.168.1.100"
        username: "user1"
        password: "pass1"
        input_base_path: "/data/mark-price/"

output:
  path: "/output/parquet"
  name: "mark-price"
"#;
        
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.data_sources.len(), 1);
        assert_eq!(config.output.batch_size, None);
    }
}

