//! WebSocket connection library with proxy support
//!
//! This library provides utilities for establishing WebSocket connections
//! with optional HTTP proxy support.

use anyhow::{Context, Result};
use futures_util::{Sink, Stream};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{info, error};
use url::Url;

/// Enum to hold different stream types for proxied or direct connections
pub enum ProxyStream {
    Plain(WebSocketStream<TcpStream>),
    Tls(WebSocketStream<tokio_native_tls::TlsStream<TcpStream>>),
    Direct(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

impl ProxyStream {
    /// Split the stream into a sink and stream
    pub fn split(self) -> (
        futures_util::stream::SplitSink<Self, Message>,
        futures_util::stream::SplitStream<Self>,
    ) {
        futures_util::StreamExt::split(self)
    }
}

impl Stream for ProxyStream {
    type Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        match &mut *self {
            ProxyStream::Plain(s) => Pin::new(s).poll_next(cx),
            ProxyStream::Tls(s) => Pin::new(s).poll_next(cx),
            ProxyStream::Direct(s) => Pin::new(s).poll_next(cx),
        }
    }
}

impl Sink<Message> for ProxyStream {
    type Error = tokio_tungstenite::tungstenite::Error;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        match &mut *self {
            ProxyStream::Plain(s) => Pin::new(s).poll_ready(cx),
            ProxyStream::Tls(s) => Pin::new(s).poll_ready(cx),
            ProxyStream::Direct(s) => Pin::new(s).poll_ready(cx),
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> std::result::Result<(), Self::Error> {
        match &mut *self {
            ProxyStream::Plain(s) => Pin::new(s).start_send(item),
            ProxyStream::Tls(s) => Pin::new(s).start_send(item),
            ProxyStream::Direct(s) => Pin::new(s).start_send(item),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        match &mut *self {
            ProxyStream::Plain(s) => Pin::new(s).poll_flush(cx),
            ProxyStream::Tls(s) => Pin::new(s).poll_flush(cx),
            ProxyStream::Direct(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        match &mut *self {
            ProxyStream::Plain(s) => Pin::new(s).poll_close(cx),
            ProxyStream::Tls(s) => Pin::new(s).poll_close(cx),
            ProxyStream::Direct(s) => Pin::new(s).poll_close(cx),
        }
    }
}

/// Connect to a remote host through an HTTP proxy using CONNECT tunneling
async fn connect_through_proxy(
    proxy_url: &str,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream> {
    let proxy = Url::parse(proxy_url)
        .context("Failed to parse proxy URL")?;
    
    let proxy_host = proxy.host_str()
        .context("Proxy URL must have a host")?;
    let proxy_port = proxy.port().unwrap_or(8080);
    
    info!("Connecting to proxy {}:{}", proxy_host, proxy_port);
    let mut stream = TcpStream::connect((proxy_host, proxy_port)).await
        .context("Failed to connect to proxy")?;
    
    // Send HTTP CONNECT request
    let connect_req = format!(
        "CONNECT {}:{} HTTP/1.1\r\n\
         Host: {}:{}\r\n\
         \r\n",
        target_host, target_port, target_host, target_port
    );
    
    stream.write_all(connect_req.as_bytes()).await
        .context("Failed to send CONNECT request")?;
    stream.flush().await
        .context("Failed to flush CONNECT request")?;
    
    // Read response - read until we find end of HTTP headers (\r\n\r\n)
    let mut response = Vec::new();
    let mut buf = [0u8; 1];
    let mut consecutive_newlines = 0;
    
    // Read response with timeout
    let read_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        async {
            loop {
                let n = stream.read(&mut buf).await
                    .context("Failed to read CONNECT response")?;
                if n == 0 {
                    break;
                }
                response.push(buf[0]);
                
                // Check for end of HTTP headers
                if buf[0] == b'\n' {
                    consecutive_newlines += 1;
                    if consecutive_newlines == 2 {
                        break;
                    }
                } else if buf[0] != b'\r' {
                    consecutive_newlines = 0;
                }
                
                // Safety limit
                if response.len() > 4096 {
                    break;
                }
            }
            Ok::<_, anyhow::Error>(())
        }
    ).await;
    
    if let Err(_) = read_result {
        anyhow::bail!("Timeout reading proxy CONNECT response");
    }
    read_result??;
    
    let response_str = String::from_utf8_lossy(&response);
    info!("Proxy response: {}", response_str.lines().next().unwrap_or("(empty)"));
    
    if !response_str.starts_with("HTTP/1.1 200") && !response_str.starts_with("HTTP/1.0 200") {
        anyhow::bail!("Proxy CONNECT failed: {}", response_str.lines().next().unwrap_or("Unknown error"));
    }
    
    info!("Proxy CONNECT successful");
    Ok(stream)
}

/// Connect to a WebSocket with optional HTTP proxy support
pub async fn connect_wss_stream(
    wss_url: &str,
    proxy: Option<&str>,
) -> Result<ProxyStream> {
    let url = Url::parse(wss_url)
        .context("Failed to parse WebSocket URL")?;
    
    let scheme = url.scheme();
    let host = url.host_str()
        .context("WebSocket URL must have a host")?;
    let port = url.port().unwrap_or(if scheme == "wss" { 443 } else { 80 });
    
    if let Some(proxy_url) = proxy {
        info!("Using HTTP proxy: {}", proxy_url);
        
        // Connect through proxy
        let tcp_stream = connect_through_proxy(proxy_url, host, port).await?;
        
        // Upgrade to TLS if needed
        if scheme == "wss" {
            info!("Upgrading to TLS for wss://{}", host);
            
            // Build TLS connector with proper configuration
            let connector = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(false)  // Keep security enabled
                .danger_accept_invalid_hostnames(false)
                .build()
                .context("Failed to build TLS connector")?;
            let connector = tokio_native_tls::TlsConnector::from(connector);
            
            info!("Establishing TLS connection to: {}", host);
            match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                connector.connect(host, tcp_stream)
            ).await {
                Ok(Ok(tls_stream)) => {
                    info!("TLS connection established successfully");
                    
                    // Perform WebSocket handshake on TLS stream
                    info!("Performing WebSocket handshake");
                    let (ws_stream, _) = tokio_tungstenite::client_async(wss_url, tls_stream).await
                        .context("WebSocket handshake failed")?;
                    
                    info!("WebSocket handshake successful");
                    Ok(ProxyStream::Tls(ws_stream))
                }
                Ok(Err(e)) => {
                    error!("TLS handshake error: {:?}", e);
                    error!("TLS error details: {}", e);
                    if let Some(source) = std::error::Error::source(&e) {
                        error!("TLS error source: {}", source);
                    }
                    Err(anyhow::anyhow!("Failed to establish TLS connection: {}", e))
                }
                Err(_) => {
                    error!("TLS connection timeout (10s)");
                    Err(anyhow::anyhow!("TLS connection timeout"))
                }
            }
        } else {
            // Perform WebSocket handshake on plain TCP stream
            info!("Performing WebSocket handshake");
            let (ws_stream, _) = tokio_tungstenite::client_async(wss_url, tcp_stream).await
                .context("WebSocket handshake failed")?;
            
            Ok(ProxyStream::Plain(ws_stream))
        }
    } else {
        // No proxy, connect directly
        info!("Connecting directly to WebSocket: {}", wss_url);
        info!("Target host: {}:{} (scheme: {})", host, port, scheme);
        
        // Add timeout and better error reporting for direct connection
        let connect_result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            connect_async(wss_url)
        ).await;
        
        match connect_result {
            Ok(Ok((ws_stream, _))) => {
                info!("WebSocket connection successful");
                Ok(ProxyStream::Direct(ws_stream))
            }
            Ok(Err(e)) => {
                error!("WebSocket handshake error: {:?}", e);
                error!("Error details: {}", e);
                if let Some(source) = std::error::Error::source(&e) {
                    error!("Error source: {}", source);
                }
                Err(anyhow::anyhow!("WebSocket handshake failed: {}", e))
            }
            Err(_) => {
                error!("WebSocket connection timeout (10s) - possible network/firewall issue");
                Err(anyhow::anyhow!("WebSocket connection timeout - possible firewall or network block"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_stream_creation() {
        // This is a placeholder test
        // Real tests would require async runtime and mocking
    }
}
