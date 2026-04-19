# 飞龙在天 · A股板块妖线分析系统

> 每日收盘后扫描全市场 ~4900 只股票，用自研「妖线角度」指标计算各概念板块热度排名，生成可交互 HTML 报告。

---

## 功能概述

- **三周期分析**：同时计算日线、三日、周线三个周期的妖线满足数
- **268个概念板块**：全市场概念板块热度排名
- **可视化报告**：交互式 HTML 报告，含趋势图、板块涨跌统计、个股明细
- **虚拟板块**：支持自定义聚合主题板块（如医疗主题）

---

## 核心算法（妖线角度）

```
X1  = EMA(收盘价, 9)
主升1 = EMA(X1 × 1.14, 5)

选股条件：
  ① 妖线角度 ≥ 63°     （主升线斜率足够陡峭）
  ② 收盘价 ≥ 主升1      （股价在主升线上方）
  ③ 有效股票            （主板/创业板/科创板，非ST）
```

---

## 环境要求

- **Python** 3.10+
- **通达信**（本地行情客户端，需安装并运行）
- 依赖库：

```bash
pip install -r requirements.txt
```

---

## 配置说明

编辑 `tool_config.py`，修改通达信路径：

```python
# 通达信 vipdoc 目录（⚠️ 必须修改为本机路径）
VIPDOC_DIR = r"C:\TongDaXin\vipdoc"   # 示例，请按实际路径填写
```

---

## 快速上手

```bash
cd tdx-analysis

# 每日完整流程（分析 + 生成报告）
run.bat          # Windows 交互菜单

# 或手动运行：
python concept_tool.py backfill --days 1       # 每日分析
python gen_concept_html_V1.3.py                # 生成 HTML 报告
```

---

## 目录结构

```
tdx-analysis/
├── concept_tool.py                   # 每日数据引擎（核心）
├── gen_concept_html_V1.3.py          # HTML 报告生成器
├── update_concept_data_with_filter.py # 板块数据更新（低频）
├── virtual_sector_mgr.py             # 虚拟板块管理
├── extract_candidates.py             # 候选股提取
├── tool_config.py                    # 路径配置（⚠️ 首次使用必改）
├── config.py                         # 基础配置
├── run.bat                           # Windows 交互菜单
├── ConceptReport/                    # 生成的 HTML 报告目录
└── Archive/                          # 历史版本备份
```

---

## 与 Quan-Select 的协同

本项目为 `Quan-Select`（精选选股系统）提供板块热度数据。  
两个项目共用 `../db/concept_weekly.db` 数据库。

**每日标准流程：**

```bash
# Step 1：先跑板块分析（本项目）
python concept_tool.py backfill --days 1

# Step 2：再跑精选选股
cd ../Quan-Select
python main.py
```

---

## 数据库

- 路径：`../db/concept_weekly.db`（与 Quan-Select 共用）
- 类型：SQLite，无需安装数据库服务
- 首次使用：运行 `run.bat → 选项 8` 更新概念板块数据

---

## 初次部署清单

```
□ 安装通达信并更新行情
□ 修改 tool_config.py 中的 VIPDOC_DIR
□ 将 概念板块.txt 放置到 ../db/ 目录
□ pip install -r requirements.txt
□ 运行选项 8（更新概念板块数据）
□ 运行选项 1（每日完整流程）
```

---

*仅供学习研究，不构成投资建议*
