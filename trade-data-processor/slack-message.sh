#!/bin/bash

#############################################################################
# Slack Webhook Message Sender
# 用途: 通过 Slack Webhook 发送消息到 Slack
# 使用方式:
#   1. 使用环境变量（推荐）:
#      export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
#      ./slack-message.sh "你的消息内容"
#
#   2. 临时设置:
#      SLACK_WEBHOOK_URL="https://hooks.slack.com/..." ./slack-message.sh "消息"
#
#   3. 在脚本中配置默认值（下面的 DEFAULT_WEBHOOK_URL）
#
#############################################################################

# ==================== 配置 ====================
# 默认 Webhook URL（可选，如果设置了环境变量则优先使用环境变量）
DEFAULT_WEBHOOK_URL=""

# ==================== 颜色定义 ====================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ==================== 日志函数 ====================
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# ==================== 发送消息 ====================
send_message() {
    local message_text="$1"
    local webhook_url="$2"
    
    # 添加主机名信息
    if [[ -n "$HOST_NAME" ]]; then
        message_text="📍 *Host:* $HOST_NAME\n$message_text"
    fi
    
    # 构建 JSON payload
    local payload="{\"text\": \"$message_text\"}"
    
    log_info "Sending message to Slack..."
    
    # 发送请求
    local response
    response=$(curl -s -w "\n%{http_code}" \
        -X POST \
        -H 'Content-Type: application/json' \
        --data "$payload" \
        "$webhook_url" \
        --connect-timeout 5 \
        --max-time 10)
    
    # 提取 HTTP 状态码
    local http_code
    http_code=$(echo "$response" | tail -n 1)
    
    # 验证响应
    if [[ "$http_code" == "200" ]]; then
        log_info "Message sent to Slack successfully ✓"
        return 0
    else
        log_error "Failed to send message to Slack (HTTP $http_code)"
        return 1
    fi
}

# ==================== 脚本入口 ====================
main() {
    local message_text="$1"
    local webhook_url="${SLACK_WEBHOOK_URL:-$DEFAULT_WEBHOOK_URL}"
    
    # 验证参数
    if [[ -z "$message_text" ]]; then
        log_error "Usage: $0 \"message_text\""
        log_error "Example: $0 \"Server is running!\""
        log_error ""
        log_error "Set webhook URL via:"
        log_error "  export SLACK_WEBHOOK_URL=\"https://hooks.slack.com/services/xxx\""
        log_error "  or configure DEFAULT_WEBHOOK_URL in the script"
        return 1
    fi
    
    # 验证 webhook URL
    if [[ -z "$webhook_url" ]]; then
        log_error "SLACK_WEBHOOK_URL is not set!"
        log_error "Please set it via environment variable or configure DEFAULT_WEBHOOK_URL in the script"
        log_error ""
        log_error "Example:"
        log_error "  export SLACK_WEBHOOK_URL=\"https://hooks.slack.com/services/YOUR/WEBHOOK/URL\""
        log_error "  $0 \"Your message\""
        return 1
    fi
    
    # 发送消息
    send_message "$message_text" "$webhook_url"
}

# 如果脚本被直接执行
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
