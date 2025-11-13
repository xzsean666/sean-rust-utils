#!/bin/bash

# Test script for the new /get_s3_url endpoint
# Usage: ./test_s3_url.sh [file_path]

set -e

BASE_URL="${BASE_URL:-http://localhost:3000}"
FILE_PATH="${1:-test.txt}"

echo "================================================"
echo "Testing /get_s3_url endpoint"
echo "================================================"
echo ""
echo "Configuration:"
echo "  Base URL: $BASE_URL"
echo "  File Path: $FILE_PATH"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if server is running
echo -n "Checking if server is running... "
if curl -s "$BASE_URL/health" > /dev/null 2>&1; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "Error: Server is not responding at $BASE_URL"
    echo "Please start the server first:"
    echo "  CONFIG_PATH=config/config.yaml cargo run"
    exit 1
fi
echo ""

# Test /get_s3_url endpoint
echo "Testing /get_s3_url endpoint..."
echo "URL: $BASE_URL/get_s3_url?file=$FILE_PATH"
echo ""

RESPONSE=$(curl -s -w "\n%{http_code}" "$BASE_URL/get_s3_url?file=$FILE_PATH")
HTTP_CODE=$(echo "$RESPONSE" | tail -n 1)
BODY=$(echo "$RESPONSE" | head -n -1)

echo "HTTP Status Code: $HTTP_CODE"
echo ""

if [ "$HTTP_CODE" -eq 200 ]; then
    echo -e "${GREEN}✓ Success!${NC}"
    echo ""
    echo "Response:"
    echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
    echo ""
    
    # Extract URL if jq is available
    if command -v jq &> /dev/null; then
        S3_URL=$(echo "$BODY" | jq -r '.url')
        UPLOADED=$(echo "$BODY" | jq -r '.uploaded')
        COMPRESSED=$(echo "$BODY" | jq -r '.compressed')
        
        echo "Details:"
        echo "  URL: $S3_URL"
        echo "  Uploaded: $UPLOADED"
        echo "  Compressed: $COMPRESSED"
        echo ""
        
        # Ask if user wants to test the URL
        read -p "Test downloading from S3 URL? (y/n) " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            OUTPUT_FILE="downloaded_$(basename $FILE_PATH)"
            if [ "$COMPRESSED" = "true" ]; then
                OUTPUT_FILE="${OUTPUT_FILE}.zstd"
            fi
            
            echo "Downloading from S3..."
            if curl -s "$S3_URL" -o "$OUTPUT_FILE"; then
                echo -e "${GREEN}✓ Downloaded to $OUTPUT_FILE${NC}"
                
                if [ "$COMPRESSED" = "true" ]; then
                    if command -v zstd &> /dev/null; then
                        read -p "Decompress file? (y/n) " -n 1 -r
                        echo ""
                        if [[ $REPLY =~ ^[Yy]$ ]]; then
                            zstd -d "$OUTPUT_FILE" -o "${OUTPUT_FILE%.zstd}"
                            echo -e "${GREEN}✓ Decompressed to ${OUTPUT_FILE%.zstd}${NC}"
                        fi
                    else
                        echo -e "${YELLOW}Note: Install zstd to decompress: sudo apt install zstd${NC}"
                    fi
                fi
            else
                echo -e "${RED}✗ Download failed${NC}"
            fi
        fi
    fi
    
elif [ "$HTTP_CODE" -eq 503 ]; then
    echo -e "${RED}✗ S3 Not Configured${NC}"
    echo ""
    echo "$BODY"
    echo ""
    echo "To enable S3 features:"
    echo "  1. Copy config: cp config/config.example.yaml config/config.yaml"
    echo "  2. Edit config.yaml with your credentials"
    echo "  3. Restart server: CONFIG_PATH=config/config.yaml cargo run"
    
elif [ "$HTTP_CODE" -eq 404 ]; then
    echo -e "${RED}✗ File Not Found${NC}"
    echo ""
    echo "$BODY"
    echo ""
    echo "Make sure the file exists in FILE_PROXY_DIR"
    echo "Current file path: $FILE_PATH"
    
else
    echo -e "${RED}✗ Error${NC}"
    echo ""
    echo "$BODY"
fi

echo ""
echo "================================================"
echo "Test completed"
echo "================================================"

