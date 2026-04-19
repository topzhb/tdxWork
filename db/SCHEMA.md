# concept_weekly.db 表结构文档

> 路径：`<项目根目录>/db/concept_weekly.db`  
> 更新：2026-03-28  
> 两个项目共用：`Quan-Select`（qs_ 前缀）+ `tdx-analysis`（t_ 前缀）

---

## 本项目表（qs_ 前缀）

### qs_ebk_stocks — 自选股行情快照
> 每日采集见龙在田.EBK 里325只股票的行情，1358行（约4天×325只）

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| date | TEXT NOT NULL | 日期 YYYY-MM-DD |
| code | TEXT NOT NULL | 6位股票代码 |
| name | TEXT | 股票名称 |
| market | TEXT | 市场（SH/SZ） |
| close | REAL | 收盘价 |
| chg_pct | REAL | 涨跌幅% |
| pe_ttm | REAL | 市盈率TTM |
| mcap | REAL | 总市值（亿元） |
| created_at | TEXT | 插入时间 |

唯一索引：`(date, code)`

---

### qs_picks — 精选个股评分结果
> 每日 score.py 输出，879行

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| date | TEXT NOT NULL | 日期 |
| rank_no | INTEGER | 排名 |
| code | TEXT | 6位股票代码 |
| name | TEXT | 股票名称 |
| industry | TEXT | 行业（东财接口） |
| report_period | TEXT | 财报期（如2025Q3） |
| close | REAL | 收盘价 |
| chg_pct | TEXT | 涨跌幅% |
| total_score | REAL | 综合评分（满分100） |
| tech_score | INTEGER | 技术面得分（满分100） |
| fund_score | INTEGER | 基本面得分（满分100） |
| pe_ttm | TEXT | 市盈率 |
| mcap | TEXT | 总市值 |
| profit_yoy | TEXT | 净利润同比% |
| revenue_yoy | TEXT | 营收同比% |
| roe | TEXT | ROE加权% |
| eps | REAL | 每股收益 |
| tech_sigs | TEXT | 技术信号描述（逗号分隔） |
| fund_sigs | TEXT | 基本面信号描述 |
| action | TEXT | 操作建议（买入/观察等） |
| buy_range | TEXT | 买入区间（如 4.28~4.47） |
| stop_loss | TEXT | 止损位 |
| target | TEXT | 目标价 |
| position_pct | TEXT | 建议仓位% |
| created_at | TEXT | 插入时间 |
| matched_sectors | TEXT | 命中热门板块列表（JSON） |
| sector_count | INTEGER | 命中热门板块数 |
| hot_score | REAL | 热门板块加成分 |
| final_score | REAL | 最终得分（含板块加成） |
| fin_source | TEXT | 财务数据来源（local/eastmoney） |

唯一索引：`(date, code)`

---

### qs_trend_sectors — 热门板块排行
> 每日 collect.py 计算，105行（约4天×25板块）

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| date | TEXT NOT NULL | 日期 |
| sector_rank | INTEGER | 排名（1=最热） |
| sector_name | TEXT | 板块名称 |
| stock_count | INTEGER | 板块总股票数 |
| ratio | REAL | 总比满足率（%，越高越热门） |
| created_at | TEXT | 插入时间 |

唯一索引：`(date, sector_name)`

---

## 对方项目表（t_ 前缀，只读引用）

### t_sector — 板块基础信息
> 265个板块（概念/行业/地区）

| 列名 | 类型 | 说明 |
|------|------|------|
| sector_code | TEXT PK | 板块代码 |
| sector_name | TEXT NOT NULL | 板块名称 |
| created_at | TEXT NOT NULL | 创建时间 |

---

### t_sector_stock — 板块-股票关联
> 40330条，全市场股票与板块的多对多关系

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| sector_code | TEXT NOT NULL | 板块代码（→t_sector） |
| stock_code | TEXT NOT NULL | 6位股票代码 |
| stock_name | TEXT | 股票名称 |

唯一索引：`(sector_code, stock_code)`

---

### t_daily_report — 每日板块满足数
> 63038条，热门板块算法的数据源

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| report_date | TEXT NOT NULL | 日期 |
| run_id | TEXT NOT NULL | 运行批次ID（→t_run_log） |
| sector_code | TEXT NOT NULL | 板块代码 |
| sector_name | TEXT NOT NULL | 板块名称 |
| rank_no | INTEGER | 该批次排名 |
| total_stocks | INTEGER | 板块总股票数 |
| analyzed_count | INTEGER | 已分析数 |
| satisfied_count | INTEGER | 满足条件数 |
| satisfied_rate | REAL | 满足率% |

唯一索引：`(report_date, sector_code)`

---

### t_run_log — 运行日志
> 239条，每次 tdx-analysis 运行记录

| 列名 | 类型 | 说明 |
|------|------|------|
| run_id | TEXT PK | 运行ID（格式：YYYYMMDD_HHMMSS 或历史回溯标记） |
| version | TEXT NOT NULL | 版本号 |
| total_sectors | INTEGER | 扫描板块总数 |
| total_stocks | INTEGER | 扫描个股总数 |
| unique_stocks | INTEGER | 去重后个股数 |
| analyzed_count | INTEGER | 已分析数 |
| satisfied_count | INTEGER | 满足条件总数（热门板块算法分母） |
| satisfied_rate | REAL | 总体满足率% |
| run_time | TEXT NOT NULL | 运行时间 |
| note | TEXT | 备注（历史回溯时为 `YYYY-MM-DD`） |

---

### t_stock — 股票基础信息
> 4901条全市场股票

| 列名 | 类型 | 说明 |
|------|------|------|
| stock_code | TEXT PK | 6位股票代码 |
| stock_name | TEXT | 股票名称 |
| market | TEXT | 市场 |
| updated_at | TEXT NOT NULL | 更新时间 |

---

### t_sector_stat — 板块统计（按批次）
> 63836条

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| run_id | TEXT NOT NULL | 运行批次ID |
| sector_code | TEXT NOT NULL | 板块代码 |
| rank_no | INTEGER | 排名 |
| total_stocks | INTEGER | 总股票数 |
| analyzed_count | INTEGER | 已分析数 |
| satisfied_count | INTEGER | 满足数 |
| satisfied_rate | REAL | 满足率% |
| calc_date | TEXT NOT NULL | 计算日期 |
| total_rate | REAL | 总比满足率（热门板块算法用） |

唯一索引：`(run_id, sector_code)`

---

### t_stock_calc — 个股技术计算结果
> 1,189,634条（最大表）

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| run_id | TEXT NOT NULL | 运行批次ID |
| stock_code | TEXT NOT NULL | 6位股票代码 |
| angle | REAL | 均线角度（主升浪指标） |
| zhusheng1 | REAL | 主升浪指标值1 |
| close_price | REAL | 收盘价 |
| x_val | INTEGER | X值（自定义技术指标） |
| x1_ema9 | REAL | EMA9值 |
| is_satisfied | INTEGER NOT NULL DEFAULT 0 | 是否满足条件（0/1） |
| calc_date | TEXT NOT NULL | 计算日期 |
| daily_change | REAL | 日涨跌幅% |

唯一索引：`(run_id, stock_code)`

---

## 热门板块算法 SQL 参考

```sql
-- 计算各板块总比满足率，取TOP 25热门板块
SELECT
    s.sector_name,
    SUM(dr.satisfied_count) AS total_satisfied,
    rl.satisfied_count       AS run_total_satisfied,
    ROUND(SUM(dr.satisfied_count) * 100.0 / rl.satisfied_count, 4) AS ratio
FROM t_daily_report dr
JOIN t_run_log rl ON dr.run_id = rl.run_id
JOIN t_sector s   ON dr.sector_code = s.sector_code
WHERE dr.report_date = '2026-03-28'
GROUP BY dr.sector_code
ORDER BY ratio DESC
LIMIT 25;
```
