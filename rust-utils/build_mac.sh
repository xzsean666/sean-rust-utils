#!/bin/bash

# macOS 平台构建脚本
# 编译Rust项目到macOS平台

# 不使用 set -e，手动处理错误

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 输出目录
OUTPUT_DIR="release"
BUILD_DIR="build_output" # 仍然创建，但在此脚本中可能不完全使用

# 清理函数
cleanup() {
    echo -e "${YELLOW}清理临时文件...${NC}"
    if [ -d "$BUILD_DIR" ]; then
        rm -rf "$BUILD_DIR"
    fi
}

# 设置清理陷阱
trap cleanup EXIT

# 创建输出目录
echo -e "${BLUE}创建输出目录...${NC}"
mkdir -p "$OUTPUT_DIR"
mkdir -p "$BUILD_DIR"

# 定义目标平台 (仅限macOS)
declare -a TARGETS=(
    "x86_64-apple-darwin"           # macOS x64
    "aarch64-apple-darwin"          # macOS ARM64 (M1/M2)
)

# 定义平台名称映射函数
get_platform_name() {
    case "$1" in
        "x86_64-apple-darwin") echo "macos-x64" ;;
        "aarch64-apple-darwin") echo "macos-arm64" ;;
        *) echo "unknown" ;;
    esac
}

# 获取项目名称
PROJECT_NAME=$(grep '^name = ' Cargo.toml | sed 's/name = "\(.*\)"/\1/')

echo -e "${GREEN}项目名称: $PROJECT_NAME${NC}"
echo -e "${GREEN}开始macOS平台构建...${NC}"

# 检查并安装rustup和cargo
if ! command -v rustup &> /dev/null; then
    echo -e "${RED}错误: 未找到rustup，请先安装Rust工具链${NC}"
    exit 1
fi

if ! command -v cargo &> /dev/null; then
    echo -e "${RED}错误: 未找到cargo，请先安装Rust工具链${NC}"
    exit 1
fi

# 安装目标平台
echo -e "${BLUE}安装交叉编译目标...${NC}"
for target in "${TARGETS[@]}"; do
    echo -e "${YELLOW}安装目标: $target${NC}"
    rustup target add "$target" || {
        echo -e "${RED}警告: 无法安装目标 $target，请确保您的Rust环境支持此目标${NC}"
        continue
    }
done

# 编译每个目标平台
success_count=0
total_count=${#TARGETS[@]}

echo -e "${GREEN}开始编译 $total_count 个macOS目标平台...${NC}"

for target in "${TARGETS[@]}"; do
    platform_name=$(get_platform_name "$target")
    echo -e "${BLUE}编译目标: $target ($platform_name)${NC}"
    
    # 编译
    if cargo build --release --target="$target"; then
        echo -e "${GREEN}✓ $target 编译成功${NC}"
        
        # 确定二进制文件名称 (macOS没有.exe后缀)
        binary_name="$PROJECT_NAME"
        
        # 复制二进制文件到输出目录
        source_path="target/$target/release/$binary_name"
        if [ -f "$source_path" ]; then
            # 创建平台子目录
            platform_dir="$OUTPUT_DIR/$platform_name"
            mkdir -p "$platform_dir"
            
            # 目标文件路径
            dest_path="$platform_dir/$binary_name"
            
            cp "$source_path" "$dest_path"
            echo -e "${GREEN}✓ 已复制到: $dest_path${NC}"
            ((success_count++))
        else
            echo -e "${RED}✗ 未找到编译输出: $source_path${NC}"
        fi
    else
        echo -e "${RED}✗ $target 编译失败${NC}"
    fi
    echo ""
done

# 清理target目录（可选，释放空间）
echo -e "${YELLOW}清理target目录...${NC}"
cargo clean

# 显示结果
echo -e "${GREEN}===========================================${NC}"
echo -e "${GREEN}构建完成！${NC}"
echo -e "${GREEN}成功编译: $success_count/$total_count 个macOS目标平台${NC}"
echo ""
echo -e "${BLUE}编译输出位于 '$OUTPUT_DIR' 目录:${NC}"
if [ -d "$OUTPUT_DIR" ]; then
    ls -la "$OUTPUT_DIR/"
else
    echo -e "${RED}输出目录不存在${NC}"
fi

echo ""
echo -e "${GREEN}===========================================${NC}"
echo -e "${GREEN}使用方法:${NC}"
echo -e "${YELLOW}macOS x64:${NC}    ./$OUTPUT_DIR/macos-x64/$PROJECT_NAME"
echo -e "${YELLOW}macOS ARM64:${NC}  ./$OUTPUT_DIR/macos-arm64/$PROJECT_NAME"
echo -e "${GREEN}===========================================${NC}"
