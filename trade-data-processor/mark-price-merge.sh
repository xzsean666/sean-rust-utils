#!/bin/bash

#############################################################################
# Mark Price Merge Script
# 用途: 自动处理 mark-price 数据
# 功能:
#   1. 支持默认模式（处理昨天的数据）或通过 --date 指定日期
#   2. 支持默认配置文件或通过 --config 指定配置文件
#   3. 调用 trade-data-processor
#   4. 将所有输出记录到 logs 目录
#   5. 统计操作用时
#   6. 成功/失败都发送 Slack 消息
#
# 用法:
#   ./mark-price-merge.sh                    # 默认：处理昨天的数据
#   ./mark-price-merge.sh --date 2025-11-09  # 处理指定日期
#   ./mark-price-merge.sh --config config/custom.yaml  # 使用自定义配置
#   ./mark-price-merge.sh --help             # 显示帮助信息
#############################################################################

set -euo pipefail

# ==================== 配置 ====================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESSOR_PATH="${SCRIPT_DIR}/release/linux-x64/trade-data-processor"
DEFAULT_CONFIG_FILE="${SCRIPT_DIR}/config/mark-price-http.config.yaml"
LOGS_DIR="${SCRIPT_DIR}/logs"
SLACK_SCRIPT="${SCRIPT_DIR}/slack-message.sh"

# 全局变量，将在 parse_args 中设置
CONFIG_FILE=""
PROCESS_DATE=""

# ==================== 颜色定义 ====================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ==================== 日志函数 ====================
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

# ==================== 初始化 ====================
init() {
    # 创建 logs 目录
    mkdir -p "${LOGS_DIR}"
    
    # 验证配置文件存在
    if [[ ! -f "${CONFIG_FILE}" ]]; then
        log_error "Config file not found: ${CONFIG_FILE}"
        return 1
    fi
    
    # 验证处理器可执行文件存在
    if [[ ! -f "${PROCESSOR_PATH}" ]]; then
        log_error "Processor executable not found: ${PROCESSOR_PATH}"
        return 1
    fi
    
    # 验证 slack-message.sh 存在
    if [[ ! -f "${SLACK_SCRIPT}" ]]; then
        log_error "Slack message script not found: ${SLACK_SCRIPT}"
        return 1
    fi
    
    log_info "Initialization completed successfully"
    return 0
}

# ==================== 获取昨天的日期 ====================
get_yesterday_date() {
    date -d "yesterday" "+%Y-%m-%d"
}

# ==================== 显示使用说明 ====================
show_usage() {
    cat << EOF
用法: $0 [选项]

选项:
    --config FILE    指定配置文件路径 (默认: ${DEFAULT_CONFIG_FILE})
    --date DATE      指定处理日期，格式: YYYY-MM-DD (默认: 昨天的日期)
    --help           显示此帮助信息

示例:
    $0                                    # 使用默认配置处理昨天的数据
    $0 --date 2025-11-09                 # 处理指定日期的数据
    $0 --config config/custom.config.yaml # 使用自定义配置文件
    $0 --config config/test.yaml --date 2025-11-09  # 同时指定配置和日期
EOF
}

# ==================== 解析命令行参数 ====================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config)
                if [[ -z "${2:-}" ]]; then
                    log_error "--config 选项需要一个文件路径参数"
                    show_usage
                    return 1
                fi
                # 如果是相对路径，转换为绝对路径（相对于脚本目录）
                if [[ "$2" != /* ]]; then
                    CONFIG_FILE="${SCRIPT_DIR}/$2"
                else
                    CONFIG_FILE="$2"
                fi
                shift 2
                ;;
            --date)
                if [[ -z "${2:-}" ]]; then
                    log_error "--date 选项需要一个日期参数 (格式: YYYY-MM-DD)"
                    show_usage
                    return 1
                fi
                # 验证日期格式
                if ! date -d "$2" "+%Y-%m-%d" >/dev/null 2>&1; then
                    log_error "无效的日期格式: $2 (请使用 YYYY-MM-DD 格式)"
                    show_usage
                    return 1
                fi
                PROCESS_DATE="$2"
                shift 2
                ;;
            --help|-h)
                show_usage
                exit 0
                ;;
            *)
                log_error "未知选项: $1"
                show_usage
                return 1
                ;;
        esac
    done
    
    # 设置默认值
    if [[ -z "$CONFIG_FILE" ]]; then
        CONFIG_FILE="$DEFAULT_CONFIG_FILE"
    fi
    
    if [[ -z "$PROCESS_DATE" ]]; then
        PROCESS_DATE=$(get_yesterday_date)
    fi
    
    return 0
}

# ==================== 发送 Slack 消息 ====================
send_slack_message() {
    local message="$1"
    local webhook_url="${SLACK_WEBHOOK_URL:-}"
    
    # 如果环境变量未设置，尝试从配置文件读取
    if [[ -z "$webhook_url" ]]; then
        webhook_url=$(grep -A 10 "^slack:" "${CONFIG_FILE}" | grep "webhook_url:" | awk '{print $2}' | tr -d '"' 2>/dev/null || echo "")
    fi
    
    # 检查 webhook URL 是否为空
    if [[ -z "$webhook_url" ]]; then
        log_warn "SLACK_WEBHOOK_URL not configured, skipping Slack notification"
        return 0
    fi
    
    # 调用 slack-message.sh
    if SLACK_WEBHOOK_URL="$webhook_url" bash "${SLACK_SCRIPT}" "$message"; then
        log_info "Slack message sent successfully"
        return 0
    else
        log_error "Failed to send Slack message"
        return 1
    fi
}

# ==================== 主函数 ====================
main() {
    local start_time
    local end_time
    local duration
    local log_file
    local exit_code
    local status
    local message
    
    # 解析命令行参数
    if ! parse_args "$@"; then
        return 1
    fi
    
    # 初始化
    if ! init; then
        return 1
    fi
    
    log_info "Processing data for date: $PROCESS_DATE"
    
    # 生成日志文件名
    log_file="${LOGS_DIR}/mark-price-merge-${PROCESS_DATE}-$(date '+%s').log"
    
    # 记录开始时间
    start_time=$(date +%s)
    echo "===============================================" | tee "${log_file}"
    echo "Mark Price Merge - Started at $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "${log_file}"
    echo "Date: $PROCESS_DATE" | tee -a "${log_file}"
    echo "Config: ${CONFIG_FILE}" | tee -a "${log_file}"
    echo "===============================================" | tee -a "${log_file}"
    
    log_info "Executing: ${PROCESSOR_PATH}"
    log_info "  --config ${CONFIG_FILE}"
    log_info "  --date ${PROCESS_DATE}"
    log_info "  --data-type mark-price"
    
    # 执行处理器，并捕获所有输出到日志文件和控制台
    if "${PROCESSOR_PATH}" \
        --config "${CONFIG_FILE}" \
        --date "${PROCESS_DATE}" \
        --data-type mark-price 2>&1 | tee -a "${log_file}"; then
        exit_code=0
        status="SUCCESS ✓"
    else
        exit_code=$?
        status="FAILED ✗"
    fi
    
    # 记录结束时间
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    # 记录执行摘要
    echo "===============================================" | tee -a "${log_file}"
    echo "Status: $status" | tee -a "${log_file}"
    echo "Duration: ${duration}s" | tee -a "${log_file}"
    echo "Log file: ${log_file}" | tee -a "${log_file}"
    echo "Finished at $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "${log_file}"
    echo "===============================================" | tee -a "${log_file}"
    
    # 读取日志文件的最后几行用于错误消息
    local log_tail=""
    if [[ -f "${log_file}" ]]; then
        log_tail=$(tail -n 10 "${log_file}" 2>/dev/null || echo "Unable to read log file")
    fi
    
    # 构建 Slack 消息
    if [[ $exit_code -eq 0 ]]; then
        message="✅ *Mark Price Merge - SUCCESS*

*📊 任务信息:*
  • 处理日期: $PROCESS_DATE
  • 主机名: $(hostname)
  • 执行时间: ${duration}s
  • 状态: ✓ 处理完成
  
*📝 执行时间:*
  • 开始: $(date -d @${start_time} '+%Y-%m-%d %H:%M:%S')
  • 结束: $(date -d @${end_time} '+%Y-%m-%d %H:%M:%S')
  • 耗时: ${duration}s"
    else
        message="❌ *Mark Price Merge - FAILED*

*⚠️ 错误信息:*
  • 处理日期: $PROCESS_DATE
  • 主机名: $(hostname)
  • 退出代码: $exit_code
  • 执行时间: ${duration}s
  • 状态: ✗ 处理失败
  
*📝 执行时间:*
  • 开始: $(date -d @${start_time} '+%Y-%m-%d %H:%M:%S')
  • 结束: $(date -d @${end_time} '+%Y-%m-%d %H:%M:%S')
  • 耗时: ${duration}s

*📋 日志摘要 (最后10行):*
\`\`\`
$log_tail
\`\`\`"
    fi
    
    # 发送 Slack 消息
    send_slack_message "$message"
    
    # 输出最终消息到控制台
    if [[ $exit_code -eq 0 ]]; then
        log_info "Mark Price Merge completed successfully in ${duration}s"
        log_info "Log file: ${log_file}"
    else
        log_error "Mark Price Merge failed (exit code: $exit_code) in ${duration}s"
        log_error "Log file: ${log_file}"
    fi
    
    return $exit_code
}

# 执行主函数
main "$@"

