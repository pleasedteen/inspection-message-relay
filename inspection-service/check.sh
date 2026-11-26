#!/usr/bin/env bash
# 一条命令：缺依赖时先安装，然后构建并运行全部功能验证 + 10 万条基准。
# 实测在本机约 20~40 秒，满足 80 秒内输出可读结果。
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -x node_modules/.bin/tsc ]; then
  echo "[check] node_modules 不完整，先 npm install ..."
  npm install --no-audit --no-fund
fi

exec npm test
