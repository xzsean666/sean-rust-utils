# WebSocket Direct Connection Issue - Debug Report

## Problem Summary

You reported that:
- ✅ WebSocket connections **work with HTTP proxy** enabled
- ❌ WebSocket connections **fail without proxy** on direct connections

## Root Cause Identified

**Error:** `TLS support not compiled in`

The error message revealed that when attempting to use `tokio_tungstenite::connect_async()` for direct connections, the library was compiled **without TLS support**.

### Why Proxy Worked but Direct Failed

1. **With Proxy (Working):**
   - Code explicitly used `native_tls::TlsConnector` to handle TLS
   - Then performed WebSocket handshake with manual TLS stream
   - This worked because native-tls was available

2. **Without Proxy (Failing):**
   - Code called `connect_async()` which needs TLS feature compiled in
   - `tokio-tungstenite` was missing the `native-tls` feature flag
   - Connection failed at TLS initialization

## Solution Applied

**Modified:** `Cargo.toml`

```toml
# Before:
tokio-tungstenite = "0.24"

# After:
tokio-tungstenite = { version = "0.24", features = ["native-tls"] }
```

This enables TLS support in `tokio-tungstenite` for direct connections.

## Enhanced Error Logging

Also updated `src/wss_stream.rs` to provide better error diagnostics:
- Added detailed error messages showing exact failure reason
- Added timeout handling (10 seconds) to detect network/firewall blocks
- Added error source chain information for debugging

## Testing Instructions

1. Rebuild the project:
   ```bash
   cargo build --release
   ```

2. Test direct connection (without proxy):
   ```bash
   ./release/linux-x64/wss-collector --config config.yaml
   ```

3. Verify successful connection - you should see:
   ```
   INFO wss_collector::wss_stream: Connecting directly to WebSocket: ...
   INFO wss_collector::wss_stream: WebSocket connection successful
   ```

## Why Network Tests Showed Interesting Results

Your earlier diagnostics revealed:
- ✅ DNS resolution: OK
- ❌ ICMP ping: Failed (100% packet loss)
- ✅ HTTPS/TLS connection: OK

This is a **typical enterprise/cloud environment pattern**:
- Some networks block ICMP but allow TCP/443
- HTTP/HTTPS works fine
- WebSocket was failing only due to missing TLS compilation flag, not network issues

## Configuration Options

You can now choose to:

1. **Use direct connection (recommended):**
   ```yaml
   # config.yaml
   wss_url: "wss://fstream.binance.com/stream?streams=!markPrice@arr"
   # Remove or comment out the proxy line
   ```

2. **Keep using proxy (still works):**
   ```yaml
   proxy: "http://astrid:password@127.0.0.1:13128"
   ```

Both will now work. Direct connection is more efficient (lower latency, no intermediate proxy overhead).

## Performance Impact

- **Direct connection:** Lower latency, fewer hops, less resource overhead
- **Proxy connection:** Higher latency, but useful if you need traffic routing/inspection

## Summary

The issue was a simple but subtle dependency configuration problem. The fix requires just one line change in `Cargo.toml` and a rebuild. All WebSocket connections should now work without requiring a proxy.





