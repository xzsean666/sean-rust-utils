use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use tokio::fs::{File, read_dir};
use tokio::io::AsyncReadExt;
use tracing::{error, info, warn};
use aws_sdk_s3::Client as S3Client;

mod s3;
use s3::{S3Config, S3UrlResponse};

// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "file-proxy")]
#[command(about = "A file proxy server with S3 integration", long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config/config.yaml")]
    config: String,

    /// Base directory for file operations
    #[arg(short, long, env = "FILE_PROXY_DIR", default_value = "/data")]
    dir: String,

    /// Server port
    #[arg(short, long, env = "PORT", default_value = "3000")]
    port: u16,

    /// Database path
    #[arg(long, env = "DB_PATH", default_value = "./db/file-proxy.db")]
    db_path: String,
}

// Configuration structures
#[derive(Debug, Deserialize, Clone)]
struct Config {
    s3: Option<S3Config>,
}

#[derive(Clone)]
struct AppState {
    base_dir: Arc<PathBuf>,
    s3_client: Option<Arc<S3Client>>,
    s3_config: Option<Arc<S3Config>>,
    db: Option<Arc<Db>>,
}

#[derive(Deserialize)]
struct DownloadQuery {
    file: String,
}

#[derive(Deserialize)]
struct ListQuery {
    dir: String,
}

#[derive(Deserialize)]
struct S3UrlQuery {
    file: String,
    #[serde(default)]
    update: bool,
}

#[derive(Serialize, Debug)]
struct FileInfo {
    name: String,
    is_dir: bool,
    size: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();

    info!("Starting file-proxy server with base directory: {}", args.dir);
    info!("Using configuration file: {}", args.config);

    // Load config if it exists
    let config = load_config(&args.config).await;
    
    // Initialize database
    let db = match sled::open(&args.db_path) {
        Ok(db) => {
            info!("Database initialized successfully at: {}", args.db_path);
            Some(Arc::new(db))
        }
        Err(e) => {
            warn!("Failed to initialize database: {}", e);
            None
        }
    };
    
    // Initialize S3 client if config is available
    let (s3_client, s3_config) = if let Some(cfg) = config.as_ref().and_then(|c| c.s3.clone()) {
        match s3::create_s3_client(&cfg).await {
            Ok(client) => {
                info!("S3 client initialized successfully for provider: {}", cfg.provider);
                (Some(Arc::new(client)), Some(Arc::new(cfg)))
            }
            Err(e) => {
                warn!("Failed to initialize S3 client: {}", e);
                (None, None)
            }
        }
    } else {
        info!("No S3 configuration found, S3 features will be disabled");
        (None, None)
    };

    let state = AppState {
        base_dir: Arc::new(PathBuf::from(&args.dir)),
        s3_client,
        s3_config,
        db,
    };

    // Build router
    let app = Router::new()
        .route("/download", get(download_file))
        .route("/ls", get(list_files))
        .route("/get_s3_url", get(get_s3_url))
        .route("/health", get(health_check))
        .with_state(state);

    let bind_addr = format!("0.0.0.0:{}", args.port);
    
    // Start server
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("Server running on http://0.0.0.0:{}", args.port);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "OK"
}

async fn load_config(path: &str) -> Option<Config> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => match serde_yaml::from_str::<Config>(&content) {
            Ok(config) => Some(config),
            Err(e) => {
                warn!("Failed to parse config file {}: {}", path, e);
                None
            }
        },
        Err(e) => {
            warn!("Failed to read config file {}: {}", path, e);
            None
        }
    }
}


async fn get_s3_url(
    axum::extract::State(state): axum::extract::State<AppState>,
    Query(query): Query<S3UrlQuery>,
) -> Result<axum::Json<S3UrlResponse>, AppError> {
    // Check if S3 is configured
    info!("get_s3_url called for file: {}", query.file);
    
    if state.s3_client.is_none() {
        error!("S3 client not configured");
        return Err(AppError::S3NotConfigured);
    }
    if state.s3_config.is_none() {
        error!("S3 config not configured");
        return Err(AppError::S3NotConfigured);
    }
    if state.db.is_none() {
        error!("Database not configured");
        return Err(AppError::DatabaseNotConfigured);
    }
    
    let s3_client = state.s3_client.as_ref().unwrap();
    let s3_config = state.s3_config.as_ref().unwrap();
    let db = state.db.as_ref().unwrap();

    // Validate file path to prevent directory traversal
    let file_path = validate_path(&state.base_dir, &query.file)?;

    // Check if file exists locally
    if !file_path.exists() {
        error!("File does not exist: {:?}", file_path);
        return Err(AppError::FileNotFound);
    }

    if !file_path.is_file() {
        error!("Path is not a file: {:?}", file_path);
        return Err(AppError::NotAFile);
    }
    
    info!("File validated successfully: {:?}", file_path);

    // Use S3 module to handle URL generation
    let response = s3::handle_get_s3_url(
        s3_client.clone(),
        s3_config.clone(),
        db.clone(),
        &file_path,
        &query.file,
        query.update,
    )
    .await
    .map_err(AppError::from_s3_error)?;

    Ok(axum::Json(response))
}


async fn list_files(
    axum::extract::State(state): axum::extract::State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<axum::Json<Vec<FileInfo>>, AppError> {
    // Validate directory path to prevent directory traversal
    let dir_path = validate_path(&state.base_dir, &query.dir)?;

    info!("Attempting to list directory: {:?}", dir_path);

    // Check if path exists
    if !dir_path.exists() {
        error!("Directory does not exist: {:?}", dir_path);
        return Err(AppError::FileNotFound);
    }

    // Check if it's a directory
    if !dir_path.is_dir() {
        error!("Path is not a directory: {:?}", dir_path);
        return Err(AppError::NotADirectory);
    }

    // Read directory contents
    let mut files = Vec::new();
    let mut entries = read_dir(&dir_path)
        .await
        .map_err(|e| {
            error!("Failed to read directory {:?}: {}", dir_path, e);
            AppError::FileReadError
        })?;

    while let Some(entry) = entries.next_entry()
        .await
        .map_err(|e| {
            error!("Failed to read directory entry: {}", e);
            AppError::FileReadError
        })? {
        let metadata = entry.metadata()
            .await
            .map_err(|e| {
                error!("Failed to get file metadata: {}", e);
                AppError::FileReadError
            })?;

        let file_name = entry.file_name();
        let name = file_name.to_string_lossy().to_string();

        files.push(FileInfo {
            name,
            is_dir: metadata.is_dir(),
            size: metadata.len(),
        });
    }

    // Sort files by name
    files.sort_by(|a, b| a.name.cmp(&b.name));

    info!("Listed directory: {}, found {} items", query.dir, files.len());

    Ok(axum::Json(files))
}

async fn download_file(
    axum::extract::State(state): axum::extract::State<AppState>,
    Query(query): Query<DownloadQuery>,
) -> Result<Response, AppError> {
    // Validate file path to prevent directory traversal
    let file_path = validate_path(&state.base_dir, &query.file)?;

    // Check if file exists
    if !file_path.exists() {
        return Err(AppError::FileNotFound);
    }

    // Check if it's a file (not a directory)
    if !file_path.is_file() {
        return Err(AppError::NotAFile);
    }

    // Read file
    let mut file = File::open(&file_path)
        .await
        .map_err(|e| {
            error!("Failed to open file: {}", e);
            AppError::FileReadError
        })?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .await
        .map_err(|e| {
            error!("Failed to read file: {}", e);
            AppError::FileReadError
        })?;

    // Compress with ZSTD
    let compressed = zstd::encode_all(buffer.as_slice(), 19)
        .map_err(|e| {
            error!("Failed to compress file: {}", e);
            AppError::CompressionError
        })?;

    info!(
        "Downloaded file: {}, original size: {}, compressed size: {}",
        query.file,
        buffer.len(),
        compressed.len()
    );

    Ok((
        StatusCode::OK,
        [
            ("Content-Type", "application/octet-stream"),
            ("Content-Encoding", "zstd"),
            (
                "Content-Disposition",
                &format!(
                    "attachment; filename=\"{}.zstd\"",
                    file_path.file_name().unwrap().to_string_lossy()
                ),
            ),
        ],
        compressed,
    )
        .into_response())
}

/// Validate and normalize the path to prevent directory traversal attacks
fn validate_path(base_dir: &StdPath, requested_path: &str) -> Result<PathBuf, AppError> {
    // Remove leading slashes
    let cleaned_path = requested_path.trim_start_matches('/');

    // Reject paths with .. or .
    if cleaned_path.contains("..") || cleaned_path.contains("./") || cleaned_path.contains("\\.") {
        return Err(AppError::InvalidPath);
    }

    let full_path = base_dir.join(cleaned_path);

    // Ensure the resolved path is within base_dir
    let canonical_base = std::fs::canonicalize(base_dir)
        .map_err(|_| AppError::InvalidPath)?;
    
    let canonical_full = std::fs::canonicalize(&full_path)
        .map_err(|_| AppError::FileNotFound)?;

    if !canonical_full.starts_with(&canonical_base) {
        return Err(AppError::InvalidPath);
    }

    Ok(full_path)
}

// Custom error type
#[derive(Debug)]
enum AppError {
    FileNotFound,
    InvalidPath,
    NotAFile,
    NotADirectory,
    FileReadError,
    CompressionError,
    S3NotConfigured,
    S3UploadError,
    S3PresignError,
    DatabaseNotConfigured,
    DatabaseError,
}

impl AppError {
    fn from_s3_error(err: s3::S3Error) -> Self {
        match err {
            s3::S3Error::FileReadError => AppError::FileReadError,
            s3::S3Error::CompressionError => AppError::CompressionError,
            s3::S3Error::UploadFailed(_) => AppError::S3UploadError,
            s3::S3Error::PresignFailed(_) => AppError::S3PresignError,
            s3::S3Error::DatabaseError(_) => AppError::DatabaseError,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::FileNotFound => (StatusCode::NOT_FOUND, "File not found"),
            AppError::InvalidPath => (StatusCode::BAD_REQUEST, "Invalid file path"),
            AppError::NotAFile => (StatusCode::BAD_REQUEST, "Path is not a file"),
            AppError::NotADirectory => (StatusCode::BAD_REQUEST, "Path is not a directory"),
            AppError::FileReadError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to read file",
            ),
            AppError::CompressionError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to compress file",
            ),
            AppError::S3NotConfigured => (
                StatusCode::SERVICE_UNAVAILABLE,
                "S3 is not configured",
            ),
            AppError::S3UploadError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to upload to S3",
            ),
            AppError::S3PresignError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to generate presigned URL",
            ),
            AppError::DatabaseNotConfigured => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Database is not configured",
            ),
            AppError::DatabaseError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Database error",
            ),
        };

        // Log the error
        error!("API Error: {} - {}", status, message);
        
        // Return JSON response
        let body = axum::Json(serde_json::json!({
            "error": message,
            "status": status.as_u16()
        }));
        
        (status, body).into_response()
    }
}
