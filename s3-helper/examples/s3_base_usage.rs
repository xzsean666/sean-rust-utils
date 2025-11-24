// Minimal end-to-end demo for `s3_base` library module.
// Usage: cargo run --example s3_base_usage -- [config_path] [file_to_upload]
// Defaults: config/config.yaml and Cargo.toml
use s3_helper::s3_base;

use serde::Deserialize;
use sled::Db;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Deserialize)]
struct AppConfig {
    s3: s3_base::S3Config,
    #[serde(default)]
    cache_db_path: Option<PathBuf>,
}

/// Upload a file with optional caching and print a 15-minute presigned URL.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let (config_path, file_path) = parse_args();
    let config = load_config(&config_path)?;
    let db = open_db(config.cache_db_path)?;

    let helper = s3_base::S3Helper::from_config(config.s3, db.map(Arc::new)).await?;

    let index_key = file_path
        .file_name()
        .and_then(|name: &std::ffi::OsStr| name.to_str());
    // let index_key = Some("666");
    let uploaded_md5 = helper.upload_file(file_path.as_path(), index_key).await?;

    let expires_in_seconds = 15 * 60;
    let presigned_url = helper.presign(&uploaded_md5, expires_in_seconds).await?;
    println!("Uploaded {} with key {}", file_path.display(), uploaded_md5);
    println!("Presigned URL (15 minutes): {}", presigned_url);

    // let presigned_url_index = helper
    //     .presign_by_index_key("666", expires_in_seconds)
    //     .await?;
    // println!(
    //     "presigned_url_index URL (15 minutes): {}",
    //     presigned_url_index
    // );
    let localrecord = helper.get_file_metadata_by_md5("2f6789717b6fcd4f601633ec3a388437")?;
    println!("localrecord: {:?}", localrecord);
    let localrecordindexkey = helper.get_file_metadata_by_index_key("666")?;
    println!("localrecordindexkey: {:?}", localrecordindexkey);

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Ignore error if another subscriber was already set by the caller.
    let _ = fmt().with_env_filter(filter).with_target(false).try_init();
}

fn parse_args() -> (PathBuf, PathBuf) {
    let mut args = env::args().skip(1);
    let config_path = args
        .next()
        .map(PathBuf::from)
        .or_else(|| env::var("CONFIG_PATH").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("config/config.yaml"));

    let file_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("Cargo.toml"));

    (config_path, file_path)
}

fn load_config(path: &Path) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let config: AppConfig = serde_yaml::from_str(&contents)?;
    Ok(config)
}

fn open_db(path: Option<PathBuf>) -> Result<Option<Db>, Box<dyn std::error::Error>> {
    let path = path.unwrap_or_else(|| PathBuf::from("db/.s3_helper_cache.db"));

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let db = sled::open(path)?;
    Ok(Some(db))
}
