#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
qs_config.py — 见龙在田精选系统 · 项目专属配置
==================================================
通用路径（TDX_DIR, DB_FILE 等）从根目录 tdx_config.py 导入，
本文件只存放项目专属配置。

本文件在 .gitignore 中，不会被提交到 GitHub。
"""

import os
import sys

# 将根目录加入搜索路径，以便导入 tdx_config
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from tdx_config import TDX_DIR, TDX_CW_DIR, DB_FILE  # noqa: E402

# ============================================================
# 项目专属配置
# ============================================================

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 自选股文件
EBK_FILE = os.path.join(_SCRIPT_DIR, "picks", "见龙在田.EBK")
