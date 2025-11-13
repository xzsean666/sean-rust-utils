# Runtime stage - use pre-built binary
FROM debian:bookworm-slim

WORKDIR /app

# Use Alibaba Cloud mirror sources to fix network issues
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's|security.debian.org|mirrors.aliyun.com/debian-security|g' /etc/apt/sources.list.d/debian.sources

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built binary
COPY release/linux-x64/wss-collector /app/wss-collector

# Create data directory for output files
RUN mkdir -p /app/data


