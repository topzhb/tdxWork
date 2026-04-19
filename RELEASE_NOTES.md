# 发布版本整理说明

**整理日期**：2026-04-19  
**整理人**：工作区清理  
**版本**：v1.0-release  

---

## 变更摘要

本次整理将两个项目（`tdx-analysis` 和 `Quan-Select`）从个人开发环境迁移为可供第三方共享的发布版本，主要变更如下：

---

## 一、脱敏处理

### 已清理的个人信息

| 位置 | 原内容 | 修改后 |
|------|--------|--------|
| `tdx-analysis/config.py` | 旧版硬编码路径 | 改为 `os.path.join(...)` 相对路径 |
| `Quan-Select/.gitignore` 注释 | 旧版硬编码路径 | 改为通用说明 |
| `db/SCHEMA.md` | 旧版硬编码路径 | 改为 `<项目根目录>/db/...` |
| `Quan-Select/report.py` HTML | 旧版硬编码路径 | 改为 `<项目目录>\Quan-Select` |
| `Quan-Select/WORKFLOW.md` | 硬编码通达信路径 | 改为 `<通达信vipdoc目录>` |
| `tdx-analysis/最佳核心分析流程.md` | 旧版硬编码路径 | 改为 `<项目目录>/tdx-analysis` |

### 安全核查结论

- ✅ **无 API Key / Token / 密码**：全部外部接口均为公开免费接口
- ✅ **无数据库凭证**：仅使用本地 SQLite，无需账号密码
- ⚠️ **数据库文件不发布**：`concept_weekly.db`（484MB）已被 `.gitignore` 排除

---

## 二、配置文件改造

### Quan-Select：新增 `qs_config.py`

**问题**：原来通达信路径散落在 `collect.py`、`score.py`、`backtest.py`、`fund_strategies.py` 6处，第三方需要改动多个文件。

**解决方案**：新建 `qs_config.py` 作为统一配置入口，所有脚本改为从此文件导入路径。

```python
# qs_config.py —— 只需修改这一处
TDX_DIR    = r"C:\TongDaXin\vipdoc"      # 改为本机通达信路径
TDX_CW_DIR = os.path.join(TDX_DIR, "cw") # 财务目录自动跟随
```

受影响文件（已全部改造为从 qs_config.py 导入）：
- `collect.py`
- `score.py`
- `backtest.py`
- `fund_strategies.py`（原 6 处硬编码 → 统一引用模块变量）

### tdx-analysis：`tool_config.py` 已有集中配置

`tool_config.py` 已是集中配置文件，本次仅补充注释说明：

```python
# 通达信日线数据目录（⚠️ 首次使用请修改为本机通达信 vipdoc 路径）
VIPDOC_DIR = r"C:\TongDaXin\vipdoc"
```

---

## 三、新增文件

| 文件 | 说明 |
|------|------|
| `Quan-Select/README.md` | 项目说明、安装部署、使用文档 |
| `Quan-Select/qs_config.py` | 集中路径配置文件（核心新增） |
| `Quan-Select/requirements.txt` | Python 依赖声明 |
| `tdx-analysis/README.md` | 项目说明、安装部署、使用文档 |
| `tdx-analysis/requirements.txt` | Python 依赖声明 |

---

## 四、.gitignore 更新

| 项目 | 新增排除项 | 原因 |
|------|-----------|------|
| `tdx-analysis/.gitignore` | `Archive/` | 历史备份目录，含旧路径，不对外发布 |
| `Quan-Select/.gitignore` | `__temp/` | 临时调试脚本目录（原 `__temp__/` 规则未覆盖实际目录名） |

---

## 五、发布目录结构（排除后）

```
tdxWork/
├── db/
│   ├── concept_weekly.db          ← 不发布（体积484MB，使用方自行生成）
│   ├── picks.db                   ← 不发布（旧版数据库）
│   ├── SCHEMA.md                  ← 表结构文档 ✅
│   └── *.py                       ← 维护脚本 ✅
│
├── tdx-analysis/
│   ├── README.md                  ← 新增 ✅
│   ├── requirements.txt           ← 新增 ✅
│   ├── tool_config.py             ← 路径配置（用户必改）✅
│   ├── config.py                  ← 基础配置 ✅
│   ├── concept_tool.py            ← 核心脚本 ✅
│   ├── gen_concept_html_V1.3.py   ← 报告生成 ✅
│   ├── update_concept_data_with_filter.py ← 板块更新 ✅
│   ├── virtual_sector_mgr.py      ← 虚拟板块 ✅
│   ├── extract_candidates.py      ← 候选股提取 ✅
│   ├── run.bat                    ← 交互菜单 ✅
│   ├── ConceptReport/.gitkeep     ← 空目录占位 ✅
│   ├── Archive/                   ← 不发布（gitignore）
│   └── .gitignore                 ✅
│
└── Quan-Select/
    ├── README.md                  ← 新增 ✅
    ├── requirements.txt           ← 新增 ✅
    ├── qs_config.py               ← 集中路径配置（用户必改）新增 ✅
    ├── main.py                    ← 主入口 ✅
    ├── collect.py                 ← 采集 ✅
    ├── score.py                   ← 评分 ✅
    ├── report.py                  ← 报告 ✅
    ├── fund_strategies.py         ← 策略库 ✅
    ├── backtest.py                ← 回测服务 ✅
    ├── build_sector_db.py         ← 板块检查 ✅
    ├── batch_backtest_cli.py      ← 批量回测 ✅
    ├── daily_analysis.bat         ← 交互菜单 ✅
    ├── start_backtest.bat         ← 启动回测 ✅
    ├── WORKFLOW.md                ← 工作流手册 ✅
    ├── SCORING.md / SCORING_GROWTH.md / SURPRISE_SYSTEM.md ← 文档 ✅
    ├── picks/                     ← 输入输出目录 ✅
    ├── __temp/                    ← 不发布（gitignore）
    └── .gitignore                 ✅
```

---

## 六、第三方部署步骤

1. **克隆/复制** 整个 `tdxWork/` 目录（不含 `.db` 文件）
2. **修改通达信路径**：
   - `tdx-analysis/tool_config.py` → 修改 `VIPDOC_DIR`
   - `Quan-Select/qs_config.py` → 修改 `TDX_DIR`
3. **安装依赖**：
   ```bash
   pip install -r tdx-analysis/requirements.txt
   pip install -r Quan-Select/requirements.txt
   ```
4. **准备数据文件**：
   - `db/概念板块.txt`（GBK 编码）
   - `Quan-Select/picks/见龙在田.EBK`（从通达信导出自选股）
5. **初始化数据库**：
   ```bash
   cd tdx-analysis
   python update_concept_data_with_filter.py   # 初始化板块数据
   ```
6. **首次运行**：
   ```bash
   python concept_tool.py backfill --days 1
   cd ../Quan-Select
   python main.py
   ```

---

*整理完成于 2026-04-19*
