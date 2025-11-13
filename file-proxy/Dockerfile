# Runtime stage - use pre-built binary
FROM ubuntu:24.04

WORKDIR /app

# Use Alibaba Cloud mirror sources to fix network issues
RUN sed -i 's|archive.ubuntu.com|mirrors.aliyun.com|g' /etc/apt/sources.list.d/ubuntu.sources && \
    sed -i 's|security.ubuntu.com|mirrors.aliyun.com|g' /etc/apt/sources.list.d/ubuntu.sources

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built binary
COPY release/linux-x64/file-proxy /app/app

# Create data directory for output files and set permissions
RUN mkdir -p /app/data && chmod 755 /app/app && chmod 755 /app/data


