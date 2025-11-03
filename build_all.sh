#!/bin/bash

# Ensure cargo and rustup are in PATH for sudo execution
export PATH="$HOME/.cargo/bin:$PATH"

# 全平台构建脚本
# 编译Rust项目到多个目标平台

# 不使用 set -e，手动处理错误

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 输出目录
OUTPUT_DIR="release"
BUILD_DIR="build_output"

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

# 定义目标平台
declare -a TARGETS=(
    "x86_64-unknown-linux-gnu"      # Linux x64
    "aarch64-unknown-linux-gnu"     # Linux ARM64
    "x86_64-pc-windows-gnu"         # Windows x64
    "x86_64-apple-darwin"           # macOS x64
    "aarch64-apple-darwin"          # macOS ARM64 (M1/M2)
)

# 定义平台名称映射
declare -A PLATFORM_NAMES=(
    ["x86_64-unknown-linux-gnu"]="linux-x64"
    ["aarch64-unknown-linux-gnu"]="linux-arm64"
    ["x86_64-pc-windows-gnu"]="windows-x64"
    ["x86_64-apple-darwin"]="macos-x64"
    ["aarch64-apple-darwin"]="macos-arm64"
)

# 获取项目名称
PROJECT_NAME=$(grep '^name = ' Cargo.toml | sed 's/name = "\(.*\)"/\1/')

echo -e "${GREEN}项目名称: $PROJECT_NAME${NC}"
echo -e "${GREEN}开始全平台构建...${NC}"

# 检查并安装rustup和cargo
if ! command -v rustup &> /dev/null; then
    echo -e "${RED}错误: 未找到rustup，请先安装Rust工具链${NC}"
    exit 1
fi

if ! command -v cargo &> /dev/null; then
    echo -e "${RED}错误: 未找到cargo，请先安装Rust工具链${NC}"
    exit 1
fi

# 检查并安装交叉编译工具
check_and_install_cross_tools() {
    local need_update=false
    local missing_tools=()
    
    # 检查Windows交叉编译工具
    if ! command -v x86_64-w64-mingw32-gcc &> /dev/null; then
        echo -e "${YELLOW}未找到Windows交叉编译工具 gcc-mingw-w64-x86-64${NC}"
        missing_tools+=("gcc-mingw-w64-x86-64")
        need_update=true
    else
        echo -e "${GREEN}✓ Windows交叉编译工具已安装${NC}"
    fi
    
    # 检查Linux ARM64交叉编译工具
    if ! command -v aarch64-linux-gnu-gcc &> /dev/null; then
        echo -e "${YELLOW}未找到Linux ARM64交叉编译工具 gcc-aarch64-linux-gnu${NC}"
        missing_tools+=("gcc-aarch64-linux-gnu")
        need_update=true
    else
        echo -e "${GREEN}✓ Linux ARM64交叉编译工具已安装${NC}"
    fi
    
    # 如果缺少工具，提示用户手动安装
    if [ "$need_update" = true ]; then
        echo -e "${YELLOW}===========================================${NC}"
        echo -e "${YELLOW}检测到缺少交叉编译工具，需要手动安装：${NC}"
        for tool in "${missing_tools[@]}"; do
            echo -e "${BLUE}  sudo apt install -y $tool${NC}"
        done
        echo -e "${YELLOW}===========================================${NC}"
        echo -e "${YELLOW}您可以选择：${NC}"
        echo -e "${BLUE}1. 现在退出脚本，手动安装上述工具后重新运行${NC}"
        echo -e "${BLUE}2. 继续运行，但会跳过需要这些工具的目标平台${NC}"
        echo ""
        read -p "请选择 (1 退出 / 2 继续): " choice
        
        case "$choice" in
            1)
                echo -e "${YELLOW}脚本已退出，请安装所需工具后重新运行${NC}"
                exit 0
                ;;
            2)
                echo -e "${YELLOW}继续运行，将跳过需要交叉编译工具的目标平台${NC}"
                ;;
            *)
                echo -e "${YELLOW}无效选择，默认继续运行${NC}"
                ;;
        esac
    fi
}

# 检查并安装交叉编译工具
echo -e "${BLUE}检查交叉编译工具...${NC}"
check_and_install_cross_tools

# 安装目标平台
echo -e "${BLUE}安装交叉编译目标...${NC}"
for target in "${TARGETS[@]}"; do
    echo -e "${YELLOW}安装目标: $target${NC}"
    rustup target add "$target" || {
        echo -e "${RED}警告: 无法安装目标 $target，跳过${NC}"
        continue
    }
done

# 编译每个目标平台
success_count=0
# 计算实际要编译的目标数量（跳过macOS）
actual_targets=()
for t in "${TARGETS[@]}"; do
    if [[ "$t" != *"apple-darwin"* ]]; then
        actual_targets+=("$t")
    fi
done
total_count=${#actual_targets[@]}

echo -e "${GREEN}开始编译 $total_count 个目标平台（跳过macOS）...${NC}"

for target in "${TARGETS[@]}"; do
    platform_name=${PLATFORM_NAMES[$target]}
    echo -e "${BLUE}编译目标: $target ($platform_name)${NC}"
    
    # 设置交叉编译环境变量
    unset CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER
    unset CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER
    
    case "$target" in
        "x86_64-pc-windows-gnu")
            if ! command -v x86_64-w64-mingw32-gcc &> /dev/null; then
                echo -e "${YELLOW}跳过 $target: 缺少Windows交叉编译工具${NC}"
                continue
            fi
            export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc
            ;;
        "aarch64-unknown-linux-gnu")
            if ! command -v aarch64-linux-gnu-gcc &> /dev/null; then
                echo -e "${YELLOW}跳过 $target: 缺少Linux ARM64交叉编译工具${NC}"
                continue
            fi
            export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
            ;;
        "x86_64-apple-darwin"|"aarch64-apple-darwin")
            echo -e "${YELLOW}警告: macOS交叉编译在Linux上需要特殊的SDK，跳过此目标${NC}"
            continue
            ;;
    esac
    
    # 编译
    if cargo build --release --target="$target"; then
        echo -e "${GREEN}✓ $target 编译成功${NC}"
        
        # 确定二进制文件扩展名
        if [[ "$target" == *"windows"* ]]; then
            binary_name="${PROJECT_NAME}.exe"
        else
            binary_name="$PROJECT_NAME"
        fi
        
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
echo -e "${GREEN}成功编译: $success_count/$total_count 个目标平台${NC}"
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
echo -e "${YELLOW}Linux x64:${NC}    ./$OUTPUT_DIR/linux-x64/$PROJECT_NAME"
echo -e "${YELLOW}Linux ARM64:${NC}  ./$OUTPUT_DIR/linux-arm64/$PROJECT_NAME"
echo -e "${YELLOW}Windows x64:${NC}  $OUTPUT_DIR/windows-x64/$PROJECT_NAME.exe"
echo -e "${BLUE}注意：macOS目标被跳过，如需macOS版本请在macOS系统上运行此脚本${NC}"
echo -e "${GREEN}===========================================${NC}"
