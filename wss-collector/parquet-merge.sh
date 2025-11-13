#!/usr/bin/env bash
# 🚀 通用 Parquet 合并脚本
# 自动检测 CPU 核心数以并行处理
# 自动分析文件前缀智能命名输出文件
# 用法: ./parquet-merge.sh --input <输入目录> [--output <输出文件>] [--order-column <列名>] [--order-dir <asc|desc>]

set -e

# 默认值
INPUT_DIR=""
OUTPUT_FILE=""
OUTPUT_SPECIFIED=false
ORDER_COLUMN=""
ORDER_DIR="asc"

# 显示帮助信息
show_help() {
  cat << EOF
用法: $0 --input <输入目录> [选项]

必需参数:
  --input <DIR>              输入目录路径

可选参数:
  --output <FILE>            输出文件路径或目录 (默认: 基于文件前缀自动生成)
                             如果以 .parquet 结尾则作为完整文件路径
                             否则作为目录路径，自动生成带前缀的文件名
  --order-column <COLUMN>    排序列名称
  --order-dir <asc|desc>     排序方向 (默认: asc)
  -h, --help                 显示帮助信息

例子:
  $0 --input data
  $0 --input data --output /custom/output
  $0 --input data --output /custom/path/custom_name.parquet
  $0 --input data --output /custom/output --order-column E --order-dir desc
EOF
  exit 0
}

# 提取公共前缀
# 用法: get_common_prefix <dir>
get_common_prefix() {
  local input_dir="$1"
  
  # 获取所有 .parquet 文件名(不含路径和扩展名)
  local files=()
  while IFS= read -r file; do
    files+=("$(basename "$file" .parquet)")
  done < <(find "$input_dir" -maxdepth 1 -name "*.parquet" | sort)
  
  if [ ${#files[@]} -eq 0 ]; then
    echo ""
    return
  fi
  
  if [ ${#files[@]} -eq 1 ]; then
    # 如果只有一个文件,使用该文件名作为前缀
    echo "${files[0]}"
    return
  fi
  
  # 找公共前缀
  local prefix=""
  local first_file="${files[0]}"
  
  for ((i = 0; i < ${#first_file}; i++)); do
    local char="${first_file:$i:1}"
    local is_common=true
    
    for file in "${files[@]:1}"; do
      if [ "${file:$i:1}" != "$char" ]; then
        is_common=false
        break
      fi
    done
    
    if [ "$is_common" = true ]; then
      prefix="${prefix}${char}"
    else
      break
    fi
  done
  
  # 清理末尾的非字母数字字符
  prefix="${prefix%[_-]*}"
  
  # 确保至少有一个有意义的前缀
  if [ -z "$prefix" ] || [ "$prefix" = "" ]; then
    prefix="data"
  fi
  
  echo "$prefix"
}

# 解析命令行参数
while [[ $# -gt 0 ]]; do
  case $1 in
    --input)
      INPUT_DIR="$2"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="$2"
      OUTPUT_SPECIFIED=true
      shift 2
      ;;
    --order-column)
      ORDER_COLUMN="$2"
      shift 2
      ;;
    --order-dir)
      ORDER_DIR="$2"
      shift 2
      ;;
    -h|--help)
      show_help
      ;;
    *)
      echo "❌ 未知参数: $1"
      show_help
      ;;
  esac
done

# 验证必需参数
if [ -z "$INPUT_DIR" ]; then
  echo "❌ 错误: --input 参数是必需的"
  show_help
fi

# 如果未指定 output，则在 input 目录下创建 merged 文件夹
if [ "$OUTPUT_SPECIFIED" = false ]; then
  OUTPUT_DIR="${INPUT_DIR}/merged"
  mkdir -p "$OUTPUT_DIR"
  
  # 检测公共前缀并生成智能命名的输出文件
  PREFIX=$(get_common_prefix "$INPUT_DIR")
  if [ -z "$PREFIX" ]; then
    echo "⚠️  未检测到有效的前缀,使用默认名称"
    OUTPUT_FILE="${OUTPUT_DIR}/merged.parquet"
  else
    OUTPUT_FILE="${OUTPUT_DIR}/${PREFIX}_merged.parquet"
    echo "🏷️  检测到前缀: ${PREFIX}"
  fi
else
  # 如果指定了 output，检查是否是目录或文件路径
  if [[ "$OUTPUT_FILE" == *.parquet ]]; then
    # 以 .parquet 结尾，认为是完整文件路径，直接使用
    echo "📄 使用指定的输出文件路径"
  else
    # 认为是目录路径，在该目录下使用智能命名
    OUTPUT_DIR="$OUTPUT_FILE"
    mkdir -p "$OUTPUT_DIR"
    
    # 检测公共前缀并生成智能命名的输出文件
    PREFIX=$(get_common_prefix "$INPUT_DIR")
    if [ -z "$PREFIX" ]; then
      echo "⚠️  未检测到有效的前缀,使用默认名称"
      OUTPUT_FILE="${OUTPUT_DIR}/merged.parquet"
    else
      OUTPUT_FILE="${OUTPUT_DIR}/${PREFIX}_merged.parquet"
      echo "🏷️  检测到前缀: ${PREFIX}"
    fi
  fi
fi

if ! command -v duckdb >/dev/null 2>&1; then
  echo "❌ 请先安装 duckdb (curl https://install.duckdb.org | sh)"
  exit 1
fi

# 自动检测 CPU 核心数
THREADS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
echo "🧠 检测到 CPU 核心数: ${THREADS}"

echo "📂 输入目录: $INPUT_DIR"
echo "📄 输出文件: $OUTPUT_FILE"

QUERY="SELECT * FROM read_parquet('${INPUT_DIR}/*.parquet')"

if [ -n "$ORDER_COLUMN" ]; then
  echo "🧭 排序列: $ORDER_COLUMN ($ORDER_DIR)"
  QUERY="${QUERY} ORDER BY \"${ORDER_COLUMN}\" ${ORDER_DIR}"
else
  echo "⚡ 未指定排序列 -> 跳过 ORDER BY (合并更快)"
fi

echo "⚙️  正在合并..."
duckdb -c "PRAGMA threads=${THREADS}; COPY (${QUERY}) TO '${OUTPUT_FILE}' (FORMAT PARQUET);"

echo "✅ 合并完成: ${OUTPUT_FILE}"
