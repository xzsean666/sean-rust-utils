//! Trade Data Processor
//!
//! A tool for collecting trade data from multiple SSH servers, merging with
//! deduplication and forward-fill, and writing to Parquet files.

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use clap::Parser;
use std::path::PathBuf;
use tracing::{info, error, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use trade_data_processor::{
    Config, DataMerger, MarkPriceMerger, ParquetWriter, ParquetWriterConfig, SshClient, HttpClient, Writer, DataRow,
};
use std::fs;
use std::path::Path;

/// CLI arguments
#[derive(Parser, Debug)]
#[command(name = "trade-data-processor")]
#[command(about = "Process trade data from multiple SSH sources", long_about = None)]
struct Args {
    /// Path to the configuration YAML file
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,

    /// Date to process (format: YYYY-MM-DD)
    #[arg(short, long, value_name = "DATE")]
    date: String,

    /// Data type to process (e.g., "mark-price")
    #[arg(short = 't', long, value_name = "TYPE")]
    data_type: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    // Parse CLI arguments
    let args = Args::parse();

    // Load configuration
    info!("Loading configuration from {:?}", args.config);
    let config = Config::from_file(&args.config)
        .context("Failed to load configuration")?;

    // Parse date
    let date = NaiveDate::parse_from_str(&args.date, "%Y-%m-%d")
        .context(format!("Failed to parse date: {}", args.date))?;
    info!("Processing date: {}", date);

    // Find data source configuration
    let data_source = config.find_data_source(&args.data_type)
        .context(format!("Data type '{}' not found in configuration", args.data_type))?;

    info!("Processing data type: {}", args.data_type);
    info!("Number of SSH servers: {}", data_source.ssh_servers.len());

    // Process data based on data type
    process_data(date, data_source, &config.output, &args.data_type).await?;

    info!("Processing completed successfully!");
    Ok(())
}

/// Main processing logic - routes to appropriate merger based on data type
async fn process_data(
    date: NaiveDate,
    data_source: &trade_data_processor::DataSourceConfig,
    output_config: &trade_data_processor::OutputConfig,
    data_type: &str,
) -> Result<()> {
    // Build the date path components
    let year = date.format("%Y").to_string();
    let month = date.format("%m").to_string();
    let day = date.format("%d").to_string();

    // Route to appropriate processor based on data type
    match data_type.to_lowercase().as_str() {
        "mark-price" => {
            info!("Using MarkPriceMerger for mark-price data");
            process_mark_price_data(date, data_source, output_config, &year, &month, &day).await
        }
        _ => {
            info!("Using generic DataMerger for data type: {}", data_type);
            process_generic_data(date, data_source, output_config, &year, &month, &day).await
        }
    }
}

/// Process mark-price specific data
async fn process_mark_price_data(
    date: NaiveDate,
    data_source: &trade_data_processor::DataSourceConfig,
    output_config: &trade_data_processor::OutputConfig,
    year: &str,
    month: &str,
    day: &str,
) -> Result<()> {
    let mut merger = MarkPriceMerger::new(date);

    // Process each local file source
    for (idx, local_config) in data_source.local_files.iter().enumerate() {
        let source_name = format!("local-{}", idx + 1);
        info!("Processing mark-price source: {}", source_name);

        // Build the local path (e.g., "/data/mark-price/2025/11/06")
        let local_dir = format!(
            "{}/{}/{}/{}",
            local_config.base_path.trim_end_matches('/'),
            year, month, day
        );
        info!("Local directory: {}", local_dir);

        // Check if local directory exists
        let local_path = Path::new(&local_dir);
        if !local_path.exists() {
            warn!("Local directory does not exist: {}, skipping", local_dir);
            continue;
        }

        if !local_path.is_dir() {
            warn!("Path is not a directory: {}, skipping", local_dir);
            continue;
        }

        // List JSONL files in the local directory
        let entries = match fs::read_dir(&local_dir) {
            Ok(entries) => entries,
            Err(e) => {
                error!("Failed to read local directory {}: {}", local_dir, e);
                continue;
            }
        };

        let mut jsonl_files = Vec::new();
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() {
                    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                        if file_name.ends_with(".jsonl") {
                            jsonl_files.push(path);
                        }
                    }
                }
            }
        }

        info!("Found {} JSONL files in {}", jsonl_files.len(), local_dir);

        // Process each JSONL file
        for file_path in jsonl_files {
            let file_name = file_path.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");
            
            match fs::read_to_string(&file_path) {
                Ok(content) => {
                    match merger.add_jsonl_data(&content, &source_name) {
                        Ok(count) => {
                            info!("Added {} mark-price records from local file {}", count, file_name);
                        }
                        Err(e) => {
                            error!("Failed to parse mark-price data from local file {}: {}", file_name, e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to read local file {:?}: {}", file_path, e);
                    continue;
                }
            }
        }
    }

    // Process each SSH server
    for (idx, ssh_config) in data_source.ssh_servers.iter().enumerate() {
        let source_name = format!("ssh-{}-{}", idx + 1, ssh_config.host);
        info!("Processing mark-price source: {}", source_name);

        // Build the remote path
        let remote_dir = format!(
            "{}/{}/{}/{}",
            ssh_config.input_base_path.trim_end_matches('/'),
            year, month, day
        );
        info!("Remote directory for {}: {}", ssh_config.host, remote_dir);

        let client = SshClient::new(ssh_config.clone());

        // Check if remote directory exists
        match client.path_exists(&remote_dir).await {
            Ok(true) => {
                info!("Remote directory exists: {}", remote_dir);
            }
            Ok(false) => {
                warn!("Remote directory does not exist: {} on {}, skipping", remote_dir, ssh_config.host);
                continue;
            }
            Err(e) => {
                error!("Failed to check remote directory on {}: {}", ssh_config.host, e);
                continue;
            }
        }

        // List JSONL files in the remote directory
        let files = match client.list_files(&remote_dir).await {
            Ok(files) => files,
            Err(e) => {
                error!("Failed to list files on {}: {}", ssh_config.host, e);
                continue;
            }
        };

        let jsonl_files: Vec<String> = files
            .into_iter()
            .filter(|f| f.ends_with(".jsonl"))
            .collect();

        info!("Found {} JSONL files on {}", jsonl_files.len(), ssh_config.host);

        // Build full paths for parallel download
        let remote_file_paths: Vec<String> = jsonl_files
            .iter()
            .map(|file| format!("{}/{}", remote_dir, file))
            .collect();

        // Download all files in parallel
        info!("Starting parallel download of {} files from {}", remote_file_paths.len(), ssh_config.host);
        match client.download_files_parallel(remote_file_paths.clone()).await {
            Ok(downloaded_files) => {
                info!("Successfully downloaded {} files from {}", downloaded_files.len(), ssh_config.host);
                
                // Process each downloaded file
                for (remote_file_path, content) in downloaded_files {
                    let content_str = String::from_utf8_lossy(&content);
                    
                    match merger.add_jsonl_data(&content_str, &source_name) {
                        Ok(count) => {
                            let file_name = remote_file_path.split('/').last().unwrap_or(&remote_file_path);
                            info!("Added {} mark-price records from {}", count, file_name);
                        }
                        Err(e) => {
                            error!("Failed to parse mark-price data from {}: {}", remote_file_path, e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to download files from {}: {}", ssh_config.host, e);
                continue;
            }
        }
    }

    // Process each HTTP server
    for (idx, http_config) in data_source.http_servers.iter().enumerate() {
        let source_name = format!("http-{}-{}", idx + 1, http_config.base_url);
        info!("Processing mark-price source: {}", source_name);

        // Build the remote path (e.g., "mark-price/2025/11/06")
        let remote_dir = format!(
            "{}/{}/{}/{}",
            http_config.input_base_path.trim_end_matches('/'),
            year, month, day
        );
        info!("Remote directory for {}: {}", http_config.base_url, remote_dir);

        let client = HttpClient::new(http_config.clone());

        // Check proxy availability if configured (fail fast if proxy is not working)
        client.check_proxy_availability().await
            .context("Proxy availability check failed - aborting")?;

        // Check if remote directory exists
        match client.path_exists(&remote_dir).await {
            Ok(true) => {
                info!("Remote directory exists: {}", remote_dir);
            }
            Ok(false) => {
                warn!("Remote directory does not exist: {} on {}, skipping", remote_dir, http_config.base_url);
                continue;
            }
            Err(e) => {
                error!("Failed to check remote directory on {}: {}", http_config.base_url, e);
                continue;
            }
        }

        // List JSONL files in the remote directory
        let files = match client.list_files(&remote_dir).await {
            Ok(files) => files,
            Err(e) => {
                error!("Failed to list files on {}: {}", http_config.base_url, e);
                continue;
            }
        };

        let jsonl_files: Vec<String> = files
            .into_iter()
            .filter(|f| f.ends_with(".jsonl"))
            .collect();

        info!("Found {} JSONL files on {}", jsonl_files.len(), http_config.base_url);

        // Build full paths for parallel download
        let remote_file_paths: Vec<String> = jsonl_files
            .iter()
            .map(|file| format!("{}/{}", remote_dir, file))
            .collect();

        // Download all files in parallel
        info!("Starting parallel download of {} files from {}", remote_file_paths.len(), http_config.base_url);
        match client.download_files_parallel(remote_file_paths.clone()).await {
            Ok(downloaded_files) => {
                info!("Successfully downloaded {} files from {}", downloaded_files.len(), http_config.base_url);
                
                // Process each downloaded file
                for (remote_file_path, content) in downloaded_files {
                    let content_str = String::from_utf8_lossy(&content);
                    
                    match merger.add_jsonl_data(&content_str, &source_name) {
                        Ok(count) => {
                            let file_name = remote_file_path.split('/').last().unwrap_or(&remote_file_path);
                            info!("Added {} mark-price records from {}", count, file_name);
                        }
                        Err(e) => {
                            error!("Failed to parse mark-price data from {}: {}", remote_file_path, e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to download files from {}: {}", http_config.base_url, e);
                continue;
            }
        }
    }

    // Check if we have any data
    if merger.is_empty() {
        bail!("No mark-price data collected from any source");
    }

    let symbols = merger.get_symbols();
    info!("Collected data for {} symbols: {:?}", symbols.len(), symbols);

    // Log statistics before forward-fill
    for symbol in &symbols {
        info!("Symbol {} - unique seconds before forward-fill: {}", symbol, merger.len_for_symbol(symbol));
    }

    // Apply forward-fill
    merger.apply_forward_fill()
        .context("Failed to apply forward-fill")?;

    // Log statistics after forward-fill
    for symbol in &symbols {
        info!("Symbol {} - total seconds after forward-fill: {}", symbol, merger.len_for_symbol(symbol));
    }

    // Write to Parquet - one file per symbol in the same directory (in parallel)
    info!("Writing mark-price data to Parquet files (one per symbol) in parallel...");
    
    // Prepare write tasks for parallel execution
    let mut write_tasks = Vec::new();
    
    for symbol in symbols {
        let rows = merger.get_sorted_rows_for_symbol(&symbol)
            .context(format!("Failed to get rows for symbol {}", symbol))?;
        
        info!("Preparing to write {} rows for symbol {}", rows.len(), symbol);
        
        // Create a modified output config with symbol-specific name only (same path)
        let symbol_output_config = trade_data_processor::OutputConfig {
            path: output_config.path.clone(),
            name: format!("{}-{}", output_config.name, symbol),
            batch_size: output_config.batch_size,
            use_temp_dir: output_config.use_temp_dir,
        };
        
        // Clone symbol for use in the spawned task
        let symbol_for_task = symbol.clone();
        
        // Spawn a task for each symbol's parquet write
        let task = tokio::spawn(async move {
            write_rows_to_parquet(rows, date, &symbol_output_config).await
                .context(format!("Failed to write parquet for symbol {}", symbol_for_task))
        });
        
        write_tasks.push((symbol, task));
    }
    
    // Wait for all write tasks to complete and collect results
    let total_tasks = write_tasks.len();
    info!("Waiting for {} parallel write tasks to complete...", total_tasks);
    for (symbol, task) in write_tasks {
        match task.await {
            Ok(Ok(())) => {
                info!("Successfully wrote parquet file for symbol {}", symbol);
            }
            Ok(Err(e)) => {
                error!("Failed to write parquet for symbol {}: {}", symbol, e);
                return Err(e);
            }
            Err(e) => {
                error!("Task panicked for symbol {}: {}", symbol, e);
                return Err(anyhow::anyhow!("Task panicked for symbol {}: {}", symbol, e));
            }
        }
    }
    
    info!("All {} parquet files written successfully", total_tasks);

    Ok(())
}

/// Process generic data
async fn process_generic_data(
    date: NaiveDate,
    data_source: &trade_data_processor::DataSourceConfig,
    output_config: &trade_data_processor::OutputConfig,
    year: &str,
    month: &str,
    day: &str,
) -> Result<()> {
    let mut merger = DataMerger::new(date);

    // Process each local file source
    for (idx, local_config) in data_source.local_files.iter().enumerate() {
        let source_name = format!("local-{}", idx + 1);
        info!("Processing generic data source: {}", source_name);

        // Build the local path (e.g., "/data/mark-price/2025/11/06")
        let local_dir = format!(
            "{}/{}/{}/{}",
            local_config.base_path.trim_end_matches('/'),
            year, month, day
        );
        info!("Local directory: {}", local_dir);

        // Check if local directory exists
        let local_path = Path::new(&local_dir);
        if !local_path.exists() {
            warn!("Local directory does not exist: {}, skipping", local_dir);
            continue;
        }

        if !local_path.is_dir() {
            warn!("Path is not a directory: {}, skipping", local_dir);
            continue;
        }

        // List JSONL files in the local directory
        let entries = match fs::read_dir(&local_dir) {
            Ok(entries) => entries,
            Err(e) => {
                error!("Failed to read local directory {}: {}", local_dir, e);
                continue;
            }
        };

        let mut jsonl_files = Vec::new();
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() {
                    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                        if file_name.ends_with(".jsonl") {
                            jsonl_files.push(path);
                        }
                    }
                }
            }
        }

        info!("Found {} JSONL files in {}", jsonl_files.len(), local_dir);

        // Process each JSONL file
        for file_path in jsonl_files {
            let file_name = file_path.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");
            
            match fs::read_to_string(&file_path) {
                Ok(content) => {
                    match merger.add_jsonl_data(&content, &source_name) {
                        Ok(count) => {
                            info!("Added {} records from local file {}", count, file_name);
                        }
                        Err(e) => {
                            error!("Failed to parse data from local file {}: {}", file_name, e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to read local file {:?}: {}", file_path, e);
                    continue;
                }
            }
        }
    }

    // Process each SSH server
    for (idx, ssh_config) in data_source.ssh_servers.iter().enumerate() {
        let source_name = format!("ssh-{}-{}", idx + 1, ssh_config.host);
        info!("Processing generic data source: {}", source_name);

        // Build the remote path
        let remote_dir = format!(
            "{}/{}/{}/{}",
            ssh_config.input_base_path.trim_end_matches('/'),
            year, month, day
        );
        info!("Remote directory for {}: {}", ssh_config.host, remote_dir);

        let client = SshClient::new(ssh_config.clone());

        // Check if remote directory exists
        match client.path_exists(&remote_dir).await {
            Ok(true) => {
                info!("Remote directory exists: {}", remote_dir);
            }
            Ok(false) => {
                warn!("Remote directory does not exist: {} on {}, skipping", remote_dir, ssh_config.host);
                continue;
            }
            Err(e) => {
                error!("Failed to check remote directory on {}: {}", ssh_config.host, e);
                continue;
            }
        }

        // List JSONL files in the remote directory
        let files = match client.list_files(&remote_dir).await {
            Ok(files) => files,
            Err(e) => {
                error!("Failed to list files on {}: {}", ssh_config.host, e);
                continue;
            }
        };

        let jsonl_files: Vec<String> = files
            .into_iter()
            .filter(|f| f.ends_with(".jsonl"))
            .collect();

        info!("Found {} JSONL files on {}", jsonl_files.len(), ssh_config.host);

        // Build full paths for parallel download
        let remote_file_paths: Vec<String> = jsonl_files
            .iter()
            .map(|file| format!("{}/{}", remote_dir, file))
            .collect();

        // Download all files in parallel
        info!("Starting parallel download of {} files from {}", remote_file_paths.len(), ssh_config.host);
        match client.download_files_parallel(remote_file_paths.clone()).await {
            Ok(downloaded_files) => {
                info!("Successfully downloaded {} files from {}", downloaded_files.len(), ssh_config.host);
                
                // Process each downloaded file
                for (remote_file_path, content) in downloaded_files {
                    let content_str = String::from_utf8_lossy(&content);
                    
                    match merger.add_jsonl_data(&content_str, &source_name) {
                        Ok(count) => {
                            let file_name = remote_file_path.split('/').last().unwrap_or(&remote_file_path);
                            info!("Added {} records from {}", count, file_name);
                        }
                        Err(e) => {
                            error!("Failed to parse data from {}: {}", remote_file_path, e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to download files from {}: {}", ssh_config.host, e);
                continue;
            }
        }
    }

    // Process each HTTP server
    for (idx, http_config) in data_source.http_servers.iter().enumerate() {
        let source_name = format!("http-{}-{}", idx + 1, http_config.base_url);
        info!("Processing generic data source: {}", source_name);

        // Build the remote path (e.g., "mark-price/2025/11/06")
        let remote_dir = format!(
            "{}/{}/{}/{}",
            http_config.input_base_path.trim_end_matches('/'),
            year, month, day
        );
        info!("Remote directory for {}: {}", http_config.base_url, remote_dir);

        let client = HttpClient::new(http_config.clone());

        // Check proxy availability if configured (fail fast if proxy is not working)
        client.check_proxy_availability().await
            .context("Proxy availability check failed - aborting")?;

        // Check if remote directory exists
        match client.path_exists(&remote_dir).await {
            Ok(true) => {
                info!("Remote directory exists: {}", remote_dir);
            }
            Ok(false) => {
                warn!("Remote directory does not exist: {} on {}, skipping", remote_dir, http_config.base_url);
                continue;
            }
            Err(e) => {
                error!("Failed to check remote directory on {}: {}", http_config.base_url, e);
                continue;
            }
        }

        // List JSONL files in the remote directory
        let files = match client.list_files(&remote_dir).await {
            Ok(files) => files,
            Err(e) => {
                error!("Failed to list files on {}: {}", http_config.base_url, e);
                continue;
            }
        };

        let jsonl_files: Vec<String> = files
            .into_iter()
            .filter(|f| f.ends_with(".jsonl"))
            .collect();

        info!("Found {} JSONL files on {}", jsonl_files.len(), http_config.base_url);

        // Build full paths for parallel download
        let remote_file_paths: Vec<String> = jsonl_files
            .iter()
            .map(|file| format!("{}/{}", remote_dir, file))
            .collect();

        // Download all files in parallel
        info!("Starting parallel download of {} files from {}", remote_file_paths.len(), http_config.base_url);
        match client.download_files_parallel(remote_file_paths.clone()).await {
            Ok(downloaded_files) => {
                info!("Successfully downloaded {} files from {}", downloaded_files.len(), http_config.base_url);
                
                // Process each downloaded file
                for (remote_file_path, content) in downloaded_files {
                    let content_str = String::from_utf8_lossy(&content);
                    
                    match merger.add_jsonl_data(&content_str, &source_name) {
                        Ok(count) => {
                            let file_name = remote_file_path.split('/').last().unwrap_or(&remote_file_path);
                            info!("Added {} records from {}", count, file_name);
                        }
                        Err(e) => {
                            error!("Failed to parse data from {}: {}", remote_file_path, e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to download files from {}: {}", http_config.base_url, e);
                continue;
            }
        }
    }

    // Check if we have any data
    if merger.is_empty() {
        bail!("No data collected from any source");
    }

    info!("Total unique seconds before forward-fill: {}", merger.len());

    // Apply forward-fill
    merger.apply_forward_fill()
        .context("Failed to apply forward-fill")?;

    info!("Total seconds after forward-fill: {}", merger.len());

    // Write to Parquet
    info!("Writing data to Parquet file...");
    let rows = merger.get_sorted_rows();
    write_rows_to_parquet(rows, date, output_config).await?;

    Ok(())
}

/// Write data rows to Parquet file (generic helper function)
async fn write_rows_to_parquet(
    rows: Vec<DataRow>,
    date: NaiveDate,
    output_config: &trade_data_processor::OutputConfig,
) -> Result<()> {
    info!("Writing {} rows to Parquet", rows.len());

    match output_config.batch_size {
        // If batch_size is None, write all data to a single file
        None => {
            info!("Writing all data to a single file");
            
            // Create Parquet writer configuration
            let parquet_config = ParquetWriterConfig {
                path: output_config.path.clone(),
                name: output_config.name.clone(),
                batch_size: rows.len(), // Use total rows as batch size
                has_batch_config: false, // No batch_size configured
                filter: Vec::new(), // No filtering
                date: Some(date),
                use_temp_dir: output_config.use_temp_dir,
            };

            // Create writer
            let mut writer = ParquetWriter::new(parquet_config);

            // Write all rows at once
            writer.write_rows(rows).await
                .context("Failed to write data")?;

            // Flush
            writer.flush_buffer().await
                .context("Failed to flush final buffer")?;
        }
        
        // If batch_size is Some(n), split data into multiple files
        Some(batch_size) => {
            info!("Writing data in batches of {} rows", batch_size);
            
            // Create Parquet writer configuration
            let parquet_config = ParquetWriterConfig {
                path: output_config.path.clone(),
                name: output_config.name.clone(),
                batch_size,
                has_batch_config: true, // Batch_size explicitly configured
                filter: Vec::new(), // No filtering
                date: Some(date),
                use_temp_dir: output_config.use_temp_dir,
            };

            // Create writer
            let mut writer = ParquetWriter::new(parquet_config);

            // Write in batches
            for (batch_idx, chunk) in rows.chunks(batch_size).enumerate() {
                info!("Writing batch {} ({} rows)", batch_idx + 1, chunk.len());
                writer.write_rows(chunk.to_vec()).await
                    .context(format!("Failed to write batch {}", batch_idx + 1))?;
            }

            // Flush any remaining data
            writer.flush_buffer().await
                .context("Failed to flush final buffer")?;
        }
    }

    info!("Successfully wrote all data to Parquet");
    Ok(())
}
