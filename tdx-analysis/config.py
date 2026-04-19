#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 配置文件

import os

# 工作空间显示名称
WORKSPACE_NAME = "飞龙在天"

# 分析配置
class Config:
    # 板块数据文件（相对路径，位于 db/ 目录下）
    # 注意：实际运行时以 tool_config.py 中的 CONCEPT_FILE 为准
    SECTOR_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "db", "概念板块.txt")

    # 通达信日线数据目录（⚠️ 请修改为本机通达信 vipdoc 路径）
    # 优先使用 tool_config.py 中的 VIPDOC_DIR
    try:
        from tool_config import VIPDOC_DIR as _VIPDOC
        VIPDOC_DIR = _VIPDOC
    except ImportError:
        VIPDOC_DIR = r"C:\TongDaXin\vipdoc"  # 默认示例，请按实际修改

    # 分析板块数量（按成分股数量排序）
    TOP_N = 30

    # 最小K线数据要求
    MIN_BARS = 24

    # 选股公式说明
    FORMULA_DESC = "XG:妖线角度1>=63 AND C>=主升1 AND X>0"
    FORMULA_DETAIL = """
    • 妖线角度1 >= 63°：主升线角度达到上升趋势
    • 收盘价 >= 主升1：股价位于主升线上方
    • X > 0：过滤无效股票（仅主板、创业板、科创板）
    """
