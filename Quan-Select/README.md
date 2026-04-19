# 见龙在田 · 精选选股系统

> 从自选股池（~325 只）中每日自动精选 TOP N 标的，多策略基本面评分 + 板块热度加成，输出 HTML / Excel / EBK 报告。

---

## 功能概述

- **四套基本面策略**：classic（稳健）/ growth（成长）/ surprise（超预期）/ single_line（短线）
- **板块热度加成**：与 tdx-analysis 协同，热门板块内股票额外加分
- **多格式输出**：HTML 精选报告、Excel 明细、EBK 文件（可直接导入通达信）
- **回测服务**：内置 HTTP 回测服务（端口 8765）

---

## 环境要求

- **Python** 3.10+
- **通达信**（本地行情客户端）
- 依赖库：

```bash
pip install -r requirements.txt
```

---

## 配置说明

编辑 `qs_config.py`，修改通达信路径（只需改这一处）：

```python
# 通达信 vipdoc 目录（⚠️ 必须修改为本机路径）
TDX_DIR    = r"C:\TongDaXin\vipdoc"     # 示例，请按实际路径填写
TDX_CW_DIR = os.path.join(TDX_DIR, "cw")  # 财务数据目录（自动跟随 TDX_DIR）
```

---

## 快速上手

```bash
cd Quan-Select

# 每日全流程（采集 + 评分 + 报告）
python main.py

# 或使用交互菜单
daily_analysis.bat
```

---

## 策略选择

| 策略 | 适用场景 | 命令示例 |
|------|---------|---------|
| `classic` | 普通行情 / 防御型 | `python main.py` |
| `growth` | 牛市 / 高速成长 | `python main.py --fund-strategy growth` |
| `classic,surprise` | 稳健 + 预期差 | `python main.py --fund-strategy classic,surprise` |
| `single_line` | 短线爆发 | `python main.py --fund-strategy single_line` |

---

## 目录结构

```
Quan-Select/
├── main.py              # 主入口，串联全流程
├── collect.py           # STEP 1：每日采集（行情 + 热门板块 + 财务）
├── score.py             # STEP 2：精选评分（技术 + 基本面）
├── report.py            # STEP 3：报告生成（HTML + Excel + EBK）
├── fund_strategies.py   # 四套策略算法库（核心）
├── backtest.py          # 回测 HTTP 服务（端口 8765）
├── build_sector_db.py   # 板块数据检查（一次性）
├── qs_config.py         # 路径配置（⚠️ 首次使用必改）
├── requirements.txt     # Python 依赖
├── daily_analysis.bat   # Windows 交互菜单
├── start_backtest.bat   # 启动回测服务
└── picks/               # 所有输入/输出文件
    ├── 见龙在田.EBK      # 输入：自选股池（从通达信导出）
    ├── 概念板块.txt      # 输入：板块关联数据
    └── report_*.html    # 输出：精选报告
```

---

## 数据流

```
见龙在田.EBK
    │
    ▼ collect.py
    ├─ 腾讯行情 API  → 每日行情
    ├─ 东财财务 API  → 基本面数据（缓存7天）
    └─ concept_weekly.db 热门板块（来自 tdx-analysis）
    │
    ▼ score.py
    ├─ 通达信 .day 文件 → K线技术打分
    └─ 基本面打分（四套策略可选）
    │
    ▼ report.py
    └─ HTML + Excel + EBK
```

---

## 与 tdx-analysis 协同

两个项目共用 `../db/concept_weekly.db`，需先运行 tdx-analysis 生成板块热度数据：

```bash
# 先跑板块分析
cd ../tdx-analysis
python concept_tool.py backfill --days 1

# 再跑精选
cd ../Quan-Select
python main.py
```

---

## 初次部署清单

```
□ 安装通达信并更新行情
□ 修改 qs_config.py 中的 TDX_DIR
□ 将 picks/见龙在田.EBK 替换为自己的自选股文件（从通达信导出）
□ 将 picks/概念板块.txt 放置到 picks/ 目录
□ pip install -r requirements.txt
□ python build_sector_db.py       # 初始化板块数据（首次）
□ python main.py                  # 运行全流程
```

---

## 回测服务

```bash
# 启动
python backtest.py --serve
# 或双击 start_backtest.bat

# 访问
http://localhost:8765/latest
```

---

*见龙在田精选系统 · 仅供学习研究，不构成投资建议*
