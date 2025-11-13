//! HTTP client module for fetching remote files
//!
//! This module handles HTTP connections and file downloads from remote servers
//! via HTTP API endpoints.

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::{info, debug, error};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use indicatif::{ProgressBar, ProgressStyle};


use crate::config::HttpConfig;
use urlencoding::encode;

/// Magic bytes for zstd format (0x28, 0xB5, 0x2F, 0xFD)
const ZSTD_MAGIC: &[u8] = &[0x28, 0xB5, 0x2F, 0xFD];

/// File list response from the HTTP server
#[derive(Debug, Deserialize)]
struct FileInfo {
    name: String,
    is_dir: bool,
    #[allow(dead_code)]
    size: u64,
}

/// S3 URL response from the HTTP server
#[derive(Debug, Deserialize)]
struct S3UrlResponse {
    url: String,
    #[allow(dead_code)]
    uploaded: bool,
    compressed: bool,
    #[allow(dead_code)]
    md5: String,
}

/// HTTP client wrapper for file operations
pub struct HttpClient {
    config: HttpConfig,
    client: reqwest::Client,
}

impl HttpClient {
    /// Create a new HTTP client
    pub fn new(config: HttpConfig) -> Self {
        let mut builder = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(6000)); // 10 minute timeout for large files
        
        // Configure proxy if provided
        if let Some(proxy_url) = &config.proxy {
            match reqwest::Proxy::all(proxy_url) {
                Ok(proxy) => {
                    builder = builder.proxy(proxy);
                    info!("HTTP client configured with proxy: {}", proxy_url);
                }
                Err(e) => {
                    error!("Failed to configure proxy {}: {}", proxy_url, e);
                    panic!("Failed to configure proxy: {}", e);
                }
            }
        }
        
        let client = builder
            .build()
            .expect("Failed to create HTTP client");
        
        Self {
            config,
            client,
        }
    }

    /// Check if proxy is available and working
    /// 
    /// This method tries to access a test URL through the proxy to verify connectivity.
    /// If the proxy is not available or not working, this will return an error.
    /// The test URL used is: http://clients3.google.com/generate_204
    pub async fn check_proxy_availability(&self) -> Result<()> {
        // Only check if proxy is configured
        if self.config.proxy.is_none() {
            return Ok(());
        }

        let proxy_url = self.config.proxy.as_ref().unwrap();
        let check_url = "http://clients3.google.com/generate_204";
        
        info!("Checking proxy availability: {} via {}", check_url, proxy_url);
        
        // Create a request with 5 second timeout for the check
        let timeout = std::time::Duration::from_secs(5);
        let response = self.client
            .get(check_url)
            .timeout(timeout)
            .send()
            .await;
        
        match response {
            Ok(resp) => {
                if resp.status().is_success() || resp.status().as_u16() == 204 {
                    info!("✅ Proxy check passed: proxy is available and working");
                    Ok(())
                } else {
                    let err_msg = format!(
                        "❌ Proxy check failed: proxy returned status {} when accessing {}",
                        resp.status(),
                        check_url
                    );
                    error!("{}", err_msg);
                    anyhow::bail!(err_msg);
                }
            }
            Err(e) => {
                let err_msg = format!(
                    "❌ Proxy check failed: cannot access {} through proxy {}: {}",
                    check_url,
                    proxy_url,
                    e
                );
                error!("{}", err_msg);
                anyhow::bail!(err_msg);
            }
        }
    }

    /// Decompress data based on encoding
    /// 
    /// Handles zstd, gzip, and other compressions
    fn decompress_if_needed(data: Vec<u8>) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(data);
        }
        
        // Check for zstd
        if data.len() >= 4 && &data[0..4] == ZSTD_MAGIC {
            debug!("Detected zstd compressed data, decompressing...");
            let decompressed = zstd::decode_all(&data[..])
                .context("Failed to decompress zstd data")?;
            debug!("Decompressed {} bytes to {} bytes", data.len(), decompressed.len());
            return Ok(decompressed);
        }
        
        // Check for gzip (magic bytes: 0x1f 0x8b)
        if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
            debug!("Detected gzip compressed data, decompressing...");
            let mut decoder = flate2::read::GzDecoder::new(&data[..]);
            let mut decompressed = Vec::new();
            use std::io::Read;
            decoder.read_to_end(&mut decompressed)
                .context("Failed to decompress gzip data")?;
            debug!("Decompressed {} bytes to {} bytes", data.len(), decompressed.len());
            return Ok(decompressed);
        }
        
        debug!("Data is not compressed, using as-is (first 8 bytes: {:02X?})", 
            &data.iter().take(8).collect::<Vec<_>>());
        Ok(data)
    }

    /// Get S3 presigned URL for a file
    /// 
    /// Makes a request to: {base_url}/get_s3_url?file={path}
    /// Returns S3 presigned URL and metadata
    async fn get_s3_url(&self, remote_path: &str) -> Result<S3UrlResponse> {
        let encoded_path = encode(remote_path);
        let url = format!("{}/get_s3_url?file={}", self.config.base_url.trim_end_matches('/'), encoded_path);
        
        debug!("Getting S3 URL from HTTP endpoint: {}", url);
        
        let response = self.client.get(&url)
            .send()
            .await
            .context(format!("Failed to send HTTP request to {}", url))?;
        
        if !response.status().is_success() {
            anyhow::bail!("HTTP request failed with status: {} for {}", response.status(), url);
        }
        
        let s3_response: S3UrlResponse = response.json()
            .await
            .context("Failed to parse S3 URL response")?;
        
        debug!("Got S3 URL for {}: compressed={}", remote_path, s3_response.compressed);
        Ok(s3_response)
    }

    /// Download file from S3 using presigned URL
    /// 
    /// Downloads file from S3 and decompresses if needed
    async fn download_from_s3(&self, s3_url: &str, is_compressed: bool) -> Result<Vec<u8>> {
        debug!("Downloading from S3: {}", s3_url);
        
        let response = self.client.get(s3_url)
            .send()
            .await
            .context(format!("Failed to download from S3: {}", s3_url))?;
        
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("S3 download failed with status: {} for {}", status, s3_url);
        }
        
        let data = response.bytes()
            .await
            .context("Failed to read S3 response bytes")?
            .to_vec();
        
        debug!("Downloaded {} bytes from S3", data.len());
        
        // Decompress if needed
        if is_compressed {
            let original_size = data.len();
            let decompressed = Self::decompress_if_needed(data)?;
            if decompressed.len() != original_size {
                info!("Downloaded and decompressed {} bytes (compressed: {} bytes) from S3", 
                    decompressed.len(), original_size);
            }
            Ok(decompressed)
        } else {
            Ok(data)
        }
    }

    /// List files in a remote directory
    /// 
    /// Makes a request to: {base_url}/ls?dir={path}
    /// Returns list of file names (excluding directories)
    pub async fn list_files(&self, remote_path: &str) -> Result<Vec<String>> {
        let encoded_path = encode(remote_path);
        let url = format!("{}/ls?dir={}", self.config.base_url.trim_end_matches('/'), encoded_path);
        
        debug!("Listing files from HTTP endpoint: {}", url);
        
        let response = self.client.get(&url)
            .send()
            .await
            .context(format!("Failed to send HTTP request to {}", url))?;
        
        if !response.status().is_success() {
            anyhow::bail!("HTTP request failed with status: {}", response.status());
        }
        
        let files: Vec<FileInfo> = response.json()
            .await
            .context("Failed to parse JSON response")?;
        
        // Filter out directories and extract file names
        let file_names: Vec<String> = files
            .into_iter()
            .filter(|f| !f.is_dir)
            .map(|f| f.name)
            .collect();
        
        debug!("Found {} files in {}", file_names.len(), remote_path);
        Ok(file_names)
    }

    /// Download a file from the remote server
    /// 
    /// First tries to get S3 presigned URL via {base_url}/get_s3_url?file={path}
    /// and download from S3. If that fails, falls back to direct download via
    /// {base_url}/download?file={path}
    /// Returns file contents as bytes (automatically decompresses if zstd)
    pub async fn download_file(&self, remote_path: &str) -> Result<Vec<u8>> {
        // Try S3 download first (default behavior)
        match self.get_s3_url(remote_path).await {
            Ok(s3_response) => {
                info!("Using S3 download for {}", remote_path);
                match self.download_from_s3(&s3_response.url, s3_response.compressed).await {
                    Ok(data) => {
                        info!("Successfully downloaded {} bytes from S3 for {}", data.len(), remote_path);
                        return Ok(data);
                    }
                    Err(e) => {
                        // S3 download failed, log and fall back to direct download
                        info!("S3 download failed for {}: {}, falling back to direct download", remote_path, e);
                    }
                }
            }
            Err(e) => {
                // Could not get S3 URL, log and fall back to direct download
                info!("Could not get S3 URL for {}: {}, falling back to direct download", remote_path, e);
            }
        }
        
        // Fallback: direct download from HTTP server
        let encoded_path = encode(remote_path);
        let url = format!("{}/download?file={}", self.config.base_url.trim_end_matches('/'), encoded_path);
        
        debug!("Downloading file from HTTP endpoint: {}", url);
        debug!("Original path: {}, Encoded path: {}", remote_path, encoded_path);
        
        let response = self.client.get(&url)
            .send()
            .await
            .context(format!("Failed to send HTTP request to {}", url))?;
        
        let status = response.status();
        if !status.is_success() {
            let headers = format!("{:?}", response.headers());
            anyhow::bail!("HTTP request failed with status: {} for {}\nHeaders: {}", status, url, headers);
        }
        
        // Log response headers for debugging
        debug!("Response headers: {:?}", response.headers());
        
        let data = response.bytes()
            .await
            .context(format!("Failed to read response bytes from {}", url))?
            .to_vec();
        
        debug!("Received {} bytes from {}", data.len(), remote_path);
        
        let original_size = data.len();
        let data = Self::decompress_if_needed(data)?;
        
        if data.len() != original_size {
            info!("Downloaded and decompressed {} bytes (compressed: {} bytes) from {}", data.len(), original_size, remote_path);
        } else {
            info!("Downloaded {} bytes from {}", data.len(), remote_path);
        }
        
        Ok(data)
    }

    /// Download multiple files concurrently
    /// 
    /// Downloads multiple files in parallel.
    /// First tries S3 download, falls back to direct HTTP download if S3 fails.
    /// Automatically decompresses zstd compressed data.
    /// Returns a vector of (file_path, file_contents) tuples.
    pub async fn download_files_parallel(&self, file_paths: Vec<String>) -> Result<Vec<(String, Vec<u8>)>> {
        let total_files = file_paths.len();
        let completed = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        
        info!("Starting parallel download of {} files (S3 with fallback)", total_files);
        
        // Create progress bar
        let progress_bar = ProgressBar::new(total_files as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files - {msg}")
                .unwrap()
                .progress_chars("█▓▒░  ")
        );
        let progress_bar = Arc::new(progress_bar);
        
        for file_path in file_paths {
            let client = self.client.clone();
            let base_url = self.config.base_url.clone();
            let completed = Arc::clone(&completed);
            let progress_bar = Arc::clone(&progress_bar);
            
            let handle = tokio::spawn(async move {
                let file_name = file_path.split('/').last().unwrap_or(&file_path);
                let download_source: &str;
                let data: Vec<u8>;
                let mut original_size: usize;
                
                // Try S3 download first
                let encoded_path = encode(&file_path);
                let s3_url_endpoint = format!("{}/get_s3_url?file={}", base_url.trim_end_matches('/'), encoded_path);
                
                match client.get(&s3_url_endpoint).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        // Got S3 URL, try to download from S3
                        match resp.json::<S3UrlResponse>().await {
                            Ok(s3_response) => {
                                info!("Using S3 download for {}", file_name);
                                match client.get(&s3_response.url).send().await {
                                    Ok(s3_resp) if s3_resp.status().is_success() => {
                                        match s3_resp.bytes().await {
                                            Ok(bytes) => {
                                                let bytes_vec = bytes.to_vec();
                                                original_size = bytes_vec.len();
                                                
                                                // Decompress if needed
                                                match s3_response.compressed {
                                                    true => {
                                                        match HttpClient::decompress_if_needed(bytes_vec) {
                                                            Ok(d) => {
                                                                data = d;
                                                                download_source = "S3";
                                                            }
                                                            Err(e) => {
                                                                let err_msg = format!("S3 decompression failed for {}: {}, falling back to direct download", file_name, e);
                                                                info!("{}", err_msg);
                                                                // Fall back to direct download
                                                                match Self::download_direct(&client, &base_url, &file_path).await {
                                                                    Ok((d, os)) => {
                                                                        data = d;
                                                                        original_size = os;
                                                                        download_source = "HTTP";
                                                                    }
                                                                    Err(e) => return Err(e),
                                                                }
                                                            }
                                                        }
                                                    }
                                                    false => {
                                                        data = bytes_vec;
                                                        download_source = "S3";
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                info!("Failed to read S3 response for {}: {}, falling back to direct download", file_name, e);
                                                match Self::download_direct(&client, &base_url, &file_path).await {
                                                    Ok((d, os)) => {
                                                        data = d;
                                                        original_size = os;
                                                        download_source = "HTTP";
                                                    }
                                                    Err(e) => return Err(e),
                                                }
                                            }
                                        }
                                    }
                                    Ok(s3_resp) => {
                                        info!("S3 download failed with status {} for {}, falling back to direct download", s3_resp.status(), file_name);
                                        match Self::download_direct(&client, &base_url, &file_path).await {
                                            Ok((d, os)) => {
                                                data = d;
                                                original_size = os;
                                                download_source = "HTTP";
                                            }
                                            Err(e) => return Err(e),
                                        }
                                    }
                                    Err(e) => {
                                        info!("S3 request failed for {}: {}, falling back to direct download", file_name, e);
                                        match Self::download_direct(&client, &base_url, &file_path).await {
                                            Ok((d, os)) => {
                                                data = d;
                                                original_size = os;
                                                download_source = "HTTP";
                                            }
                                            Err(e) => return Err(e),
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                info!("Failed to parse S3 URL response for {}: {}, falling back to direct download", file_name, e);
                                match Self::download_direct(&client, &base_url, &file_path).await {
                                    Ok((d, os)) => {
                                        data = d;
                                        original_size = os;
                                        download_source = "HTTP";
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                        }
                    }
                    _ => {
                        // Could not get S3 URL, fall back to direct download
                        info!("Could not get S3 URL for {}, falling back to direct download", file_name);
                        match Self::download_direct(&client, &base_url, &file_path).await {
                            Ok((d, os)) => {
                                data = d;
                                original_size = os;
                                download_source = "HTTP";
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                
                let count = completed.fetch_add(1, Ordering::SeqCst) + 1;
                
                if data.len() != original_size {
                    progress_bar.set_message(format!("{} ({} -> {} bytes via {})", file_name, original_size, data.len(), download_source));
                    info!("Downloaded and decompressed {} bytes (compressed: {} bytes) from {} via {} [{}/{} files completed]", 
                        data.len(),
                        original_size,
                        file_name,
                        download_source,
                        count,
                        total_files
                    );
                } else {
                    progress_bar.set_message(format!("{} ({} bytes via {})", file_name, data.len(), download_source));
                    info!("Downloaded {} bytes from {} via {} [{}/{} files completed]", 
                        data.len(), 
                        file_name,
                        download_source,
                        count,
                        total_files
                    );
                }
                
                progress_bar.inc(1);
                
                Ok::<(String, Vec<u8>), anyhow::Error>((file_path, data))
            });
            
            handles.push(handle);
        }
        
        let mut results = vec![];
        for handle in handles {
            match handle.await {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => {
                    error!("Parallel download FAILED: {}", e);
                    progress_bar.finish_with_message("Download failed!");
                    return Err(e);
                }
                Err(e) => {
                    progress_bar.finish_with_message("Download failed!");
                    return Err(anyhow::anyhow!("Download task failed: {}", e));
                }
            }
        }
        
        progress_bar.finish_with_message(format!("Completed: {} files", results.len()));
        info!("Completed parallel download: {} files succeeded", results.len());
        Ok(results)
    }

    /// Helper function for direct HTTP download (used as fallback)
    async fn download_direct(client: &reqwest::Client, base_url: &str, file_path: &str) -> Result<(Vec<u8>, usize)> {
        let encoded_path = encode(file_path);
        let url = format!("{}/download?file={}", base_url.trim_end_matches('/'), encoded_path);
        
        debug!("Direct download from HTTP endpoint: {}", url);
        
        let response = client.get(&url)
            .send()
            .await
            .context(format!("Failed to send HTTP request to {}", url))?;
        
        let status = response.status();
        if !status.is_success() {
            let headers = format!("{:?}", response.headers());
            anyhow::bail!("HTTP request failed with status: {} for {}\nHeaders: {}", status, url, headers);
        }
        
        debug!("Response headers: {:?}", response.headers());
        
        let data = response.bytes()
            .await
            .context(format!("Failed to read response bytes from {}", url))?
            .to_vec();
        
        debug!("Received {} bytes from {}", data.len(), file_path);
        
        let original_size = data.len();
        let data = HttpClient::decompress_if_needed(data)?;
        
        Ok((data, original_size))
    }

    /// Check if a remote path exists
    /// 
    /// Tries to list the directory to check if it exists
    pub async fn path_exists(&self, remote_path: &str) -> Result<bool> {
        let encoded_path = encode(remote_path);
        let url = format!("{}/ls?dir={}", self.config.base_url.trim_end_matches('/'), encoded_path);
        
        debug!("Checking if path exists: {}", url);
        
        let response = self.client.get(&url)
            .send()
            .await
            .context(format!("Failed to send HTTP request to {}", url))?;
        
        let exists = response.status().is_success();
        debug!("Path {} exists: {}", remote_path, exists);
        Ok(exists)
    }

    /// Get the host identifier for logging
    pub fn host_identifier(&self) -> String {
        self.config.base_url.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_creation() {
        let config = HttpConfig {
            base_url: "http://localhost:8080".to_string(),
            input_base_path: "/data/mark-price".to_string(),
            proxy: None,
        };
        
        let client = HttpClient::new(config);
        assert_eq!(client.host_identifier(), "http://localhost:8080");
    }
    
    #[test]
    fn test_http_client_with_proxy() {
        let config = HttpConfig {
            base_url: "http://localhost:8080".to_string(),
            input_base_path: "/data/mark-price".to_string(),
            proxy: Some("http://proxy.example.com:8080".to_string()),
        };
        
        let client = HttpClient::new(config);
        assert_eq!(client.host_identifier(), "http://localhost:8080");
    }
}

