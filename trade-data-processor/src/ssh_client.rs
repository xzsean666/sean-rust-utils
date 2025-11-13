//! SSH client module for fetching remote files
//!
//! This module handles SSH connections and file downloads from remote servers.
//! Features connection pooling for better performance when downloading multiple files.
//! Uses SFTP protocol for efficient file transfers combined with zstd compression
//! for optimal bandwidth usage and transfer speed.

use anyhow::{Result, bail};
use tracing::{info, debug, error};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::net::TcpStream;
use std::io::Read;
use ssh2::Session;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::config::SshConfig;

/// SSH client wrapper with connection pooling for file operations
pub struct SshClient {
    config: SshConfig,
    /// Shared connection (reused across operations on the same server)
    connection: Arc<Mutex<Option<Arc<Session>>>>,
}

impl SshClient {
    /// Create a new SSH client
    pub fn new(config: SshConfig) -> Self {
        Self {
            config,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    /// Connect to the SSH server (with reuse if already connected)
    async fn get_connection(&self) -> Result<Arc<Session>> {
        let mut conn_guard = self.connection.lock().await;
        
        if let Some(session) = conn_guard.as_ref() {
            // Connection exists, reuse it
            debug!("Reusing existing SSH connection");
            return Ok(session.clone());
        }

        // Create new connection in a blocking task
        let config = self.config.clone();
        let session = tokio::task::spawn_blocking(move || {
            Self::create_connection(&config)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Task failed: {}", e))??;

        let session = Arc::new(session);
        *conn_guard = Some(session.clone());
        Ok(session)
    }

    /// Create SSH connection (blocking)
    fn create_connection(config: &SshConfig) -> Result<Session> {
        let port = config.port.unwrap_or(22);
        let address = format!("{}:{}", config.host, port);
        
        info!("Establishing SSH connection to: {}", address);
        
        // Create TCP connection
        let tcp = TcpStream::connect(&address)
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", address, e))?;
        
        // Create SSH session
        let mut session = Session::new()
            .map_err(|e| anyhow::anyhow!("Failed to create SSH session: {:?}", e))?;
        
        session.set_tcp_stream(tcp);
        session.handshake()
            .map_err(|e| anyhow::anyhow!("SSH handshake failed: {:?}", e))?;

        // Authenticate
        if let Some(ref key_path) = config.private_key_path {
            debug!("Using private key authentication: {}", key_path);
            session.userauth_pubkey_file(
                &config.username,
                None,
                std::path::Path::new(key_path),
                config.password.as_deref(),
            )
            .map_err(|e| anyhow::anyhow!("Failed to authenticate with key: {:?}", e))?;
        } else if let Some(ref password) = config.password {
            debug!("Using password authentication");
            session.userauth_password(&config.username, password)
                .map_err(|e| anyhow::anyhow!("Failed to authenticate with password: {:?}", e))?;
        } else {
            bail!("No authentication method provided (password or private_key_path required)");
        }

        info!("Successfully connected to {}", config.host);
        Ok(session)
    }

    /// List files in a remote directory
    pub async fn list_files(&self, remote_path: &str) -> Result<Vec<String>> {
        let session = self.get_connection().await?;
        let remote_path = remote_path.to_string();
        let remote_path_log = remote_path.clone();
        
        debug!("Listing files in remote directory: {}", remote_path_log);
        
        let files = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let mut channel = session.channel_session()
                .map_err(|e| anyhow::anyhow!("Failed to open channel: {:?}", e))?;
            
            let command = format!("ls -1 {}", remote_path);
            channel.exec(&command)
                .map_err(|e| anyhow::anyhow!("Failed to execute command: {:?}", e))?;
            
            let mut output = String::new();
            channel.read_to_string(&mut output)
                .map_err(|e| anyhow::anyhow!("Failed to read command output: {:?}", e))?;
            
            let files: Vec<String> = output
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|line| line.trim().to_string())
                .collect();
            
            Ok(files)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Task failed: {}", e))??;
        
        debug!("Found {} files in {}", files.len(), remote_path_log);
        Ok(files)
    }

    /// Decompress zstd data
    fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>> {
        // Check if data looks like zstd format (starts with 0x28, 0xB5, 0x2F, 0xFD)
        let is_zstd = data.len() >= 4 && &data[0..4] == &[0x28, 0xB5, 0x2F, 0xFD];
        
        if !is_zstd {
            // Not zstd format, return as-is
            info!("*** Data is NOT zstd format, returning as-is ({} bytes) ***", data.len());
            return Ok(data.to_vec());
        }
        
        info!("*** Attempting zstd decompression on {} bytes ***", data.len());
        let decompressed = zstd::decode_all(data)
            .map_err(|e| anyhow::anyhow!("Zstd decompression failed: {}", e))?;
        info!("*** Successfully decompressed zstd data to {} bytes ***", decompressed.len());
        Ok(decompressed)
    }

    /// Download a file from the remote server via SFTP with zstd compression
    /// 
    /// Strategy:
    /// 1. Compress the file remotely with zstd and save to /tmp
    /// 2. Download the compressed file via SFTP
    /// 3. Decompress the data locally
    /// 4. Clean up temporary file on remote server
    pub async fn download_file(&self, remote_path: &str) -> Result<Vec<u8>> {
        let session = self.get_connection().await?;
        let remote_path = remote_path.to_string();
        
        debug!("Attempting to download via SFTP with compression: {}", remote_path);
        
        // Generate a unique temporary file name in /tmp
        let pid = std::process::id();
        let temp_filename = format!(".tmp_{}.zst", pid);
        let temp_path = format!("/tmp/{}", temp_filename);
        
        // Compress the file remotely and save to /tmp
        let compress_cmd = format!("zstd -q -f '{}' -o '{}' || cp '{}' '{}'", remote_path, temp_path, remote_path, temp_path);
        debug!("Executing compression command: {}", compress_cmd);
        
        // Execute compression in blocking task
        let remote_path_clone = remote_path.clone();
        let temp_path_clone = temp_path.clone();
        let session_clone = session.clone();
        
        tokio::task::spawn_blocking(move || {
            let mut channel = session_clone.channel_session()
                .map_err(|e| anyhow::anyhow!("Failed to open channel: {:?}", e))?;
            
            channel.exec(&compress_cmd)
                .map_err(|e| anyhow::anyhow!("Failed to execute command: {:?}", e))?;
            
            let exit_status = channel.exit_status()
                .map_err(|e| anyhow::anyhow!("Failed to get exit status: {:?}", e))?;
            
            if exit_status != 0 {
                bail!("Failed to compress file: exit code {}", exit_status);
            }
            
            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("Task failed: {}", e))??;
        
        // Download compressed file via SFTP
        debug!("Downloading compressed file via SFTP: {}", temp_path);
        
        let session_clone = session.clone();
        let compressed_data = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let sftp = session_clone.sftp()
                .map_err(|e| anyhow::anyhow!("Failed to open SFTP channel: {:?}", e))?;
            
            let mut file = sftp.open(std::path::Path::new(&temp_path_clone))
                .map_err(|e| anyhow::anyhow!("Failed to open file via SFTP: {:?}", e))?;
            
            let mut data = Vec::new();
            file.read_to_end(&mut data)
                .map_err(|e| anyhow::anyhow!("Failed to read file: {:?}", e))?;
            
            Ok(data)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Task failed: {}", e))??;
        
        info!("Downloaded {} bytes from {} via SFTP", compressed_data.len(), remote_path_clone);
        
        // Decompress locally
        let decompressed = Self::decompress_zstd(&compressed_data)
            .map_err(|e| anyhow::anyhow!("Decompression failed: {}", e))?;
        
        if decompressed.len() != compressed_data.len() {
            info!("Decompressed to {} bytes (compression ratio: {:.1}%)", 
                decompressed.len(), 
                (compressed_data.len() as f64 / decompressed.len() as f64) * 100.0);
        } else {
            debug!("File was not compressed, downloaded as-is");
        }
        
        // Clean up temporary file on remote server
        debug!("Cleaning up temporary file: {}", temp_path);
        let temp_path_cleanup = temp_path.clone();
        let session_clone = session.clone();
        
        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(mut channel) = session_clone.channel_session() {
                let cleanup_cmd = format!("rm -f '{}'", temp_path_cleanup);
                let _ = channel.exec(&cleanup_cmd);
            }
        })
        .await;
        
        Ok(decompressed)
    }

    /// Download multiple files concurrently with zstd compression via SFTP
    /// 
    /// Downloads multiple files in parallel with zstd compression using SFTP.
    /// Compresses files in /tmp, downloads via SFTP, decompresses locally.
    pub async fn download_files_parallel(&self, file_paths: Vec<String>) -> Result<Vec<(String, Vec<u8>)>> {
        let total_files = file_paths.len();
        let completed = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        
        info!("Starting parallel download of {} files (SFTP method)", total_files);
        
        for file_path in file_paths {
            let session = self.get_connection().await?;
            let completed = Arc::clone(&completed);
            
            let handle = tokio::spawn(async move {
                debug!("Downloading file in parallel: {}", file_path);
                
                // Generate unique temporary file name
                let pid = std::process::id();
                let temp_filename = format!(".tmp_{}.zst", pid);
                let temp_path = format!("/tmp/{}", temp_filename);
                
                // Compress the file remotely and save to /tmp
                let compress_cmd = format!("zstd -q -f '{}' -o '{}' || cp '{}' '{}'", file_path, temp_path, file_path, temp_path);
                debug!("Executing compression command: {}", compress_cmd);
                
                let file_path_for_error = file_path.clone();
                let file_path_for_info = file_path.clone();
                let session_clone = session.clone();
                tokio::task::spawn_blocking(move || {
                    let mut channel = session_clone.channel_session()
                        .map_err(|e| anyhow::anyhow!("Failed to open channel: {:?}", e))?;
                    
                    channel.exec(&compress_cmd)
                        .map_err(|e| anyhow::anyhow!("Failed to execute command: {:?}", e))?;
                    
                    let exit_status = channel.exit_status()
                        .map_err(|e| anyhow::anyhow!("Failed to get exit status: {:?}", e))?;
                    
                    if exit_status != 0 {
                        return Err(anyhow::anyhow!("Failed to compress {}: exit code {}", file_path_for_error, exit_status));
                    }
                    
                    Ok::<(), anyhow::Error>(())
                })
                .await
                .map_err(|e| anyhow::anyhow!("Task failed: {}", e))??;
                
                // Download compressed file via SFTP
                debug!("Downloading compressed file via SFTP: {}", temp_path);
                let session_clone = session.clone();
                let temp_path_clone = temp_path.clone();
                
                let compressed_data = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                    let sftp = session_clone.sftp()
                        .map_err(|e| anyhow::anyhow!("Failed to open SFTP channel: {:?}", e))?;
                    
                    let mut file = sftp.open(std::path::Path::new(&temp_path_clone))
                        .map_err(|e| anyhow::anyhow!("Failed to open file via SFTP: {:?}", e))?;
                    
                    let mut data = Vec::new();
                    file.read_to_end(&mut data)
                        .map_err(|e| anyhow::anyhow!("Failed to read file: {:?}", e))?;
                    
                    Ok(data)
                })
                .await
                .map_err(|e| anyhow::anyhow!("Task failed: {}", e))??;
                
                let count = completed.fetch_add(1, Ordering::SeqCst) + 1;
                info!("Downloaded {} bytes from {} via SFTP [{}/{} files completed]", 
                    compressed_data.len(), 
                    file_path_for_info.split('/').last().unwrap_or(&file_path_for_info),
                    count,
                    total_files
                );
                
                // Debug: Check first few bytes
                let first_bytes = if compressed_data.len() >= 4 {
                    format!("{:02X} {:02X} {:02X} {:02X}", 
                        compressed_data[0], compressed_data[1], compressed_data[2], compressed_data[3])
                } else {
                    "< 4 bytes".to_string()
                };
                info!("*** First 4 bytes of downloaded data (HEX): {} ***", first_bytes);
                
                // Check if it's zstd format
                let is_zstd = compressed_data.len() >= 4 && &compressed_data[0..4] == &[0x28, 0xB5, 0x2F, 0xFD];
                info!("*** Is zstd format: {} ***", is_zstd);
                
                // Decompress locally
                let decompressed = Self::decompress_zstd(&compressed_data)
                    .map_err(|e| anyhow::anyhow!("Decompression failed for {}: {}", file_path, e))?;
                
                if decompressed.len() != compressed_data.len() {
                    info!("Decompressed to {} bytes (ratio: {:.1}%)", 
                        decompressed.len(), 
                        (compressed_data.len() as f64 / decompressed.len() as f64) * 100.0);
                } else {
                    debug!("File was not compressed (same size: {} bytes)", decompressed.len());
                }
                
                // Log first 100 bytes for debugging
                let preview = String::from_utf8_lossy(&decompressed[..decompressed.len().min(100)]);
                debug!("First 100 bytes of decompressed data: {:?}", preview);
                
                // Log full content for debugging (for small files)
                if decompressed.len() < 5000 {
                    let full_content = String::from_utf8_lossy(&decompressed);
                    info!("*** FULL DECOMPRESSED CONTENT ({} bytes) for {}: ***\n{}\n*** END ***", 
                        decompressed.len(), file_path, full_content);
                } else {
                    info!("*** Decompressed file too large ({} bytes), showing hex dump of first 500 bytes ***", decompressed.len());
                    let hex_preview: String = decompressed[..decompressed.len().min(500)]
                        .iter()
                        .enumerate()
                        .map(|(i, b)| {
                            if i % 32 == 0 && i != 0 {
                                format!("\n{:04X}: {:02X}", i, b)
                            } else {
                                format!("{:02X}", b)
                            }
                        })
                        .collect::<String>();
                    info!("HEX: {}", hex_preview);
                }
                
                // Clean up temporary file on remote server
                debug!("Cleaning up temporary file: {}", temp_path);
                let session_clone = session.clone();
                let temp_path_cleanup = temp_path.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(mut channel) = session_clone.channel_session() {
                        let cleanup_cmd = format!("rm -f '{}'", temp_path_cleanup);
                        let _ = channel.exec(&cleanup_cmd);
                    }
                })
                .await;
                
                Ok((file_path, decompressed))
            });
            
            handles.push(handle);
        }
        
        let mut results = vec![];
        for handle in handles {
            match handle.await {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => {
                    // Log failures and return error
                    error!("Parallel download FAILED: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    // Task panic or join error
                    return Err(anyhow::anyhow!("Download task failed: {}", e));
                }
            }
        }
        
        info!("Completed parallel download: {} files succeeded", results.len());
        Ok(results)
    }

    /// Check if a remote path exists
    pub async fn path_exists(&self, remote_path: &str) -> Result<bool> {
        let session = self.get_connection().await?;
        let remote_path = remote_path.to_string();
        let remote_path_log = remote_path.clone();
        
        // Use test command to check if path exists
        let exists = tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut channel = session.channel_session()
                .map_err(|e| anyhow::anyhow!("Failed to open channel: {:?}", e))?;
            
            let command = format!("test -e {} && echo 'exists' || echo 'not_exists'", remote_path);
            channel.exec(&command)
                .map_err(|e| anyhow::anyhow!("Failed to execute command: {:?}", e))?;
            
            let mut output = String::new();
            channel.read_to_string(&mut output)
                .map_err(|e| anyhow::anyhow!("Failed to read command output: {:?}", e))?;
            
            let exists = output.trim() == "exists";
            Ok(exists)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Task failed: {}", e))??;
        
        debug!("Path {} exists: {}", remote_path_log, exists);
        Ok(exists)
    }

    /// Get the host identifier for logging
    pub fn host_identifier(&self) -> String {
        format!("{}@{}", self.config.username, self.config.host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssh_client_creation() {
        let config = SshConfig {
            host: "localhost".to_string(),
            port: Some(22),
            username: "user".to_string(),
            password: Some("pass".to_string()),
            private_key_path: None,
        };
        
        let client = SshClient::new(config);
        assert_eq!(client.host_identifier(), "user@localhost");
    }
}
