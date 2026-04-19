#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
qs_config.py — 见龙在田精选系统 · 路径配置文件
==================================================
部署到新机器时，只需修改本文件中的路径配置即可，无需改动其他脚本。

配置项说明
----------
TDX_DIR     : 通达信安装目录下的 vipdoc 文件夹
              示例（Windows）：r"C:\TongDaXin\vipdoc"

TDX_CW_DIR  : 通达信财务数据目录（vipdoc\cw），含 gpcw*.zip 文件
              通常是 TDX_DIR + r"\cw"，保持同步即可

注意事项
--------
- 路径必须使用原始字符串 r"..." 或双反斜杠 "..."
- 目录末尾不要加反斜杠
- 通达信客户端需运行，且日线数据已更新，分析结果才准确
"""

import os

# ============================================================
# 通达信路径配置（⚠️ 首次使用必须修改此处）
# ============================================================

# 通达信 vipdoc 目录（包含 sh/sz/lday 等子目录）
TDX_DIR    = r"C:\TongDaXin\vipdoc"

# 通达信财务 zip 目录（含 gpcw*.zip，用于基本面数据）
TDX_CW_DIR = os.path.join(TDX_DIR, "cw")

# ============================================================
# 以下配置通常无需修改（使用相对路径自动定位）
# ============================================================

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 主数据库（与 tdx-analysis 项目共用）
DB_FILE = os.path.join(_SCRIPT_DIR, "..", "db", "concept_weekly.db")

# 自选股文件
EBK_FILE = os.path.join(_SCRIPT_DIR, "picks", "见龙在田.EBK")
