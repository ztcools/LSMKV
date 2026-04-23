#!/bin/bash

# LSM-Tree KV 一键构建和测试脚本
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}   LSM-Tree KV 项目构建和测试${NC}"
echo -e "${GREEN}=========================================${NC}"

echo -e "${YELLOW}[1/5] 准备构建目录...${NC}"
mkdir -p build

echo -e "\n${YELLOW}[2/5] 运行 CMake 配置...${NC}"
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release

echo -e "\n${YELLOW}[3/5] 编译项目...${NC}"
make -j$(nproc)

echo -e "\n${YELLOW}[4/5] 运行功能测试...${NC}"
./minimal_test
./simple_test

echo -e "\n${YELLOW}[5/5] 运行多线程性能基准测试...${NC}"
./db_bench --num=1000000 --value_size=256 --threads=8

echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}   构建 & 测试 & 压测 全部完成！${NC}"
echo -e "${GREEN}   可执行程序位于：build/ 目录${NC}"
echo -e "${GREEN}=========================================${NC}"
