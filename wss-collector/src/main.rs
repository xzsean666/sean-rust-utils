use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{error, info, warn};
use wss_collector::{connect_wss_stream, extract_data_array, convert_to_rows, FilterCondition, 
                     ParquetWriter, ParquetWriterConfig, JsonWriter, JsonWriterConfig, 
                     Writer, WriterType};

#[derive(Parser, Debug)]
#[command(author, version, about = "WebSocket data collector to Parquet", long_about = None)]
struct Args {
    /// Path to YAML configuration file
    #[arg(short, long)]
    config: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    /// WebSocket URL to connect to
    wss_url: String,
    /// Base path for storing parquet/json files
    path: String,
    /// Name prefix for output files
    name: String,
    /// Optional HTTP proxy URL (e.g., http://proxy.example.com:8080)
    #[serde(default)]
    proxy: Option<String>,
    /// Writer type: "parquet" (default) or "json"
    #[serde(default)]
    writer_type: WriterType,
    /// Batch size - number of records to buffer before writing to file (only for parquet)
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    /// Optional filter conditions - if not specified, all data is written
    #[serde(default)]
    filter: Vec<FilterCondition>,
}

fn default_batch_size() -> usize {
    1000
}

impl Config {
    /// Convert generic Config to ParquetWriterConfig
    fn to_parquet_config(&self) -> ParquetWriterConfig {
        ParquetWriterConfig {
            path: self.path.clone(),
            name: self.name.clone(),
            batch_size: self.batch_size,
            filter: self.filter.clone(),
            date: None,
        }
    }

    /// Convert generic Config to JsonWriterConfig
    fn to_json_config(&self) -> JsonWriterConfig {
        JsonWriterConfig {
            path: self.path.clone(),
            name: self.name.clone(),
            filter: self.filter.clone(),
        }
    }

    /// Create appropriate writer based on configuration
    fn create_writer(&self) -> Box<dyn Writer> {
        match self.writer_type {
            WriterType::Parquet => {
                info!("Using Parquet writer with batch size: {}", self.batch_size);
                Box::new(ParquetWriter::new(self.to_parquet_config()))
            }
            WriterType::Json => {
                info!("Using JSON writer (line-by-line append mode)");
                Box::new(JsonWriter::new(self.to_json_config()))
            }
        }
    }
}

async fn connect_and_collect(config: Config) {
    let mut writer = config.create_writer();
    let mut retry_count = 0u64;
    let mut backoff_seconds = 1u64;

    loop {
        info!("Attempting to connect to WebSocket: {}", config.wss_url);
        if let Some(ref proxy) = config.proxy {
            info!("Using proxy: {}", proxy);
        }

        let connect_result = connect_wss_stream(
            &config.wss_url,
            config.proxy.as_deref()
        ).await;

        match connect_result {
            Ok(ws_stream) => {
                info!("Successfully connected to WebSocket");
                retry_count = 0;
                backoff_seconds = 1;

                let (mut write, mut read) = ws_stream.split();

                // Optional: Send a ping periodically to keep connection alive
                let ping_handle = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        if write.send(tokio_tungstenite::tungstenite::Message::Ping(vec![])).await.is_err() {
                            break;
                        }
                    }
                });

                // Read messages from WebSocket
                while let Some(message) = read.next().await {
                    match message {
                        Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                            info!("Received message: {} bytes", text.len());
                            
                            // Parse WebSocket message and extract data
                            match extract_data_array(&text) {
                                Ok(data_array) => {
                                    // Convert to data rows
                                    let rows = convert_to_rows(data_array);
                                    
                                    if rows.is_empty() {
                                        warn!("No valid rows extracted from message");
                                        continue;
                                    }
                                    
                                    // Write rows using the writer
                                    if let Err(e) = writer.write_rows(rows).await {
                                        error!("Failed to write rows: {}", e);
                                        error!("Message content: {}", &text[..text.len().min(200)]);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse message: {}", e);
                                    error!("Message content: {}", &text[..text.len().min(200)]);
                                }
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                            info!("Received binary message: {} bytes", data.len());
                            
                            // Convert binary to string representation
                            let text = String::from_utf8_lossy(&data).to_string();
                            
                            // Parse WebSocket message and extract data
                            match extract_data_array(&text) {
                                Ok(data_array) => {
                                    // Convert to data rows
                                    let rows = convert_to_rows(data_array);
                                    
                                    if rows.is_empty() {
                                        warn!("No valid rows extracted from binary message");
                                        continue;
                                    }
                                    
                                    // Write rows using the writer
                                    if let Err(e) = writer.write_rows(rows).await {
                                        error!("Failed to write rows from binary message: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse binary message: {}", e);
                                }
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Ping(_)) | Ok(tokio_tungstenite::tungstenite::Message::Pong(_)) => {
                            // Ignore ping/pong messages
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                            warn!("WebSocket connection closed by server");
                            // Flush buffer before reconnecting
                            if let Err(e) = writer.flush_buffer().await {
                                error!("Failed to flush buffer on close: {}", e);
                            }
                            break;
                        }
                        Err(e) => {
                            error!("WebSocket error: {}", e);
                            // Flush buffer before reconnecting
                            if let Err(e) = writer.flush_buffer().await {
                                error!("Failed to flush buffer on error: {}", e);
                            }
                            break;
                        }
                        _ => {}
                    }
                }

                ping_handle.abort();
                warn!("WebSocket stream ended, will reconnect...");
            }
            Err(e) => {
                retry_count += 1;
                error!("Failed to connect to WebSocket (attempt {}): {}", retry_count, e);
            }
        }

        // Exponential backoff with max of 60 seconds
        let sleep_duration = backoff_seconds.min(60);
        warn!("Retrying in {} seconds...", sleep_duration);
        tokio::time::sleep(Duration::from_secs(sleep_duration)).await;
        
        backoff_seconds = (backoff_seconds * 2).min(60);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    info!("Loading configuration from: {:?}", args.config);

    // Load configuration
    let config_content = fs::read_to_string(&args.config)
        .context(format!("Failed to read config file: {:?}", args.config))?;

    let config: Config = serde_yaml::from_str(&config_content)
        .context("Failed to parse YAML configuration")?;

    info!("Configuration loaded successfully");
    info!("WSS URL: {}", config.wss_url);
    info!("Storage path: {}", config.path);
    info!("Name prefix: {}", config.name);
    info!("Writer type: {:?}", config.writer_type);

    // Start collecting data
    connect_and_collect(config).await;

    Ok(())
}
