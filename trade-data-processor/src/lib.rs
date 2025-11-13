//! Trade Data Processor Library
//!
//! This library provides functionality for collecting trade data from multiple
//! SSH servers, merging the data with deduplication and forward-fill logic,
//! and writing the results to Parquet files.
//!
//! Supports multiple data types:
//! - Generic data: DataMerger
//! - Mark-price: MarkPriceMerger

pub mod config;
pub mod ssh_client;
pub mod http_client;
pub mod data_merger;
pub mod mark_price_merger;
pub mod writer;
pub mod parquet_writer;
pub mod s3_helper;

// Re-export commonly used types
pub use config::{Config, DataSourceConfig, OutputConfig, SshConfig, HttpConfig, LocalFileConfig, S3Config};
pub use ssh_client::SshClient;
pub use http_client::HttpClient;
pub use data_merger::DataMerger;
pub use mark_price_merger::MarkPriceMerger;
pub use writer::{Writer, DataRow};
pub use parquet_writer::{ParquetWriter, ParquetWriterConfig};
pub use s3_helper::{S3Helper, S3Provider, SyncDirection, SyncOptions, SyncStats, SyncDatabase, FileMetadata};

