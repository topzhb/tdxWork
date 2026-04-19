# 见龙在田精选系统 · 工作流手册

> 最后更新：2026-04-09

---

## 目录结构

```
Quan-Select/
├── main.py              # 主入口，串联全流程
├── collect.py           # STEP 1：每日采集
├── score.py             # STEP 2：精选评分
├── report.py            # STEP 3：报告生成
├── build_sector_db.py   # STEP 0：板块基础数据（一次性）
├── picks.db             # SQLite 数据库
│
└── picks/               # ★ 所有输入 + 输出文件统一在此
    ├── 见龙在田.EBK                    # 输入：自选股池（325只）
    ├── 概念板块.txt                    # 输入：板块关联数据
    ├── report_YYYY-MM-DD.html         # 输出：精选报告（HTML）
    ├── 精选标的_YYYYMMDD_TOPN.xlsx    # 输出：精选报告（Excel）
    └── 精选标的_YYYYMMDD.EBK          # 输出：精选结果导入通达信
```

---

## 数据流

```
见龙在田.EBK
    │
    ▼
[STEP 1] collect.py
    ├── 解析 EBK → 腾讯行情 → qs_ebk_stocks（每日自选股候选池）
    ├── 双源交集算法 → qs_trend_sectors（热门板块）
    └── 批量获取财务 → qs_finance_cache（财务数据缓存）
    │
    ▼
[STEP 2] score.py
    ├── 板块筛选：qs_ebk_stocks ∩ qs_trend_sectors → 候选个股
    ├── K线评分：通达信本地 .day 文件（趋势/位置/动量/量能，满分100）
    ├── 财务评分：qs_finance_cache 优先，fallback 网络/本地
    └── 综合评分：final = total×0.7 + min(hot×2, 30) → qs_picks 表
    │
    ▼
[STEP 3] report.py
    ├── HTML 精选报告（TOP9卡片 + 汇总表）
    ├── Excel 精选报告
    └── EBK 自选股文件（可直接导入通达信）
```

---

## 快速上手

### 日常使用（每个交易日下午收盘后）

```bash
# 最常用：今日全流程，精选TOP30
python main.py

# 精选数量改为20
python main.py --top 20
```

> ⏱ 正常耗时约 1～3 分钟（取决于需要网络请求的股票数量）

---

### 常用命令速查

| 场景 | 命令 |
|------|------|
| 今日全流程 TOP30 | `python main.py` |
| 今日全流程 TOP20 | `python main.py --top 20` |
| 指定日期全流程 | `python main.py --date 20260326` |
| 仅重新生成报告 | `python main.py --only-report` |
| 仅重新生成报告（指定TOP） | `python main.py --only-report --top 20` |
| 跳过采集，只评分+报告 | `python main.py --skip-collect` |
| 跳过采集和评分，只出报告 | `python main.py --skip-score --skip-collect` |
| 强制刷新板块基础数据 | `python main.py --update-sector` |
| 不生成 EBK 文件 | `python main.py --no-ebk` |
| 只要 HTML，不要Excel/EBK | `python main.py --no-excel --no-ebk` |

---

### 分步单独运行

```bash
# 仅采集（写入 ebk_stocks + trend_sectors）
python collect.py

# 仅评分（读 ebk_stocks + trend_sectors，写入 picks）
python score.py

# 仅出报告（读 picks，生成 HTML/Excel/EBK）
python report.py --top 20

# 重建板块基础数据（概念板块.txt 有更新时）
python build_sector_db.py --update
```

---

## 四步详解

### STEP 0 — 板块基础数据（`build_sector_db.py`）

- **何时需要**：初次使用，或 `概念板块.txt` 有更新时
- **自动跳过**：`sectors` 表已有数据时自动跳过，无需手动干预
- **强制刷新**：`python main.py --update-sector`
- **数据源**：`picks/概念板块.txt`（GBK编码，45098行）
- **结果**：`sectors` 表（270个板块）+ `sector_stocks` 表（45098条关联）

### STEP 1 — 每日采集（`collect.py`）

- **输入**：`picks/见龙在田.EBK`（325只）+ 腾讯行情接口 + concept_weekly.db
- **输出**：
  - `qs_ebk_stocks`（每日候选池含行情）
  - `qs_trend_sectors`（热门板块，双源交集算法）
  - `qs_finance_cache`（财务数据缓存，7天有效期内复用）
- **幂等**：同日重复运行会先删再写，安全
- **财务缓存**：优先东财网络接口，失败时 fallback 本地通达信 zip，结果写入缓存表供后续复用

### STEP 2 — 精选评分（`score.py`）

| 维度 | 满分 | 指标 |
|------|------|------|
| 趋势结构 | 40 | 多头排列、MA20/60斜率 |
| 位置形态 | 15 | 52周位置、布林带 |
| 动量 | 25 | MACD、RSI、KDJ |
| 量能 | 20 | 近期均量对比、量比 |
| **技术总分** | **100** | |
| **热门板块加成** | **最多+30** | `hot_score = avg_ratio × (31-avg_rank)/30` |
| **综合得分** | — | `final = total×0.7 + min(hot×2, 30)` |

- **财务数据**：优先 `qs_finance_cache` 缓存，无缓存时 fallback 网络/本地
- **幂等**：同日重复运行会先删再写
- **重新打分**：`--skip-collect` 时直接从缓存读取财务，无需网络请求，大幅提速

### STEP 3 — 报告生成（`report.py`）

- **HTML 报告**：TOP9精选卡片（3×3宫格）+ 完整汇总表，板块标签黄色=命中热门，灰色=其他
- **Excel 报告**：完整评分明细，含所有指标列
- **EBK 文件**：直接导入通达信查看精选个股

---

## 数据库表说明

| 表名 | 说明 | 主要字段 |
|------|------|---------|
| `t_sector` | 板块定义（只读） | sector_code, sector_name |
| `t_sector_stock` | 板块-个股关联（只读） | sector_code, stock_code |
| `qs_ebk_stocks` | 每日自选股候选池 | date, code, name, close, chg_pct, pe_ttm, mcap |
| `qs_trend_sectors` | 每日热门板块 | date, sector_name, ratio, rank |
| `qs_finance_cache` | 财务数据缓存 | code, report_period, profit_yoy, revenue_yoy, roe, eps |
| `qs_picks` | 每日精选评分结果 | date, code, name, total_score, hot_score, final_score, matched_sectors |

---

## 初次部署清单

```
□ 准备文件
  □ picks/见龙在田.EBK          （从通达信导出自选股）
  □ picks/概念板块.txt           （板块关联数据）
  □ <通达信vipdoc目录>   （通达信数据目录，修改 qs_config.py 中的 TDX_DIR）

□ 初始化板块数据
  python build_sector_db.py

□ 运行全流程
  python main.py --top 30
```

---

## 注意事项

1. **EBK 更新**：通达信里调整了自选股后，需重新导出 `见龙在田.EBK` 到 `picks/` 目录
2. **概念板块更新**：概念板块.txt 更新后，运行 `python main.py --update-sector` 重建
3. **趋势报告**：`trend_tracking_YYYYMMDD.xlsx` 需当日提前生成（来自 tdx-analysis 项目），collect.py 自动取最新日期的文件
4. **财务数据**：本地通达信 gpcw*.zip 每季度更新一次，更新后自动生效
5. **历史回溯**：`python main.py --date 20260310` 可重新生成任意历史日期报告（需 picks 表有数据）

---

*见龙在田精选系统 · 仅供学习研究，不构成投资建议*
