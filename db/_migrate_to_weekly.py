"""
迁移脚本：将 picks.db 中本项目独有的三张表迁入 concept_weekly.db
- ebk_stocks   → qs_ebk_stocks
- picks        → qs_picks
- trend_sectors → qs_trend_sectors
- trend_picks  废弃，不迁移

策略：以 concept_weekly.db 为准，不影响对方已有数据。
"""
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))
SRC  = os.path.join(BASE, 'picks.db')
DST  = os.path.join(BASE, 'concept_weekly.db')

src = sqlite3.connect(SRC)
dst = sqlite3.connect(DST)

src.row_factory = sqlite3.Row

# ── 1. qs_ebk_stocks ──────────────────────────────────────────────
print("[1/3] 创建 qs_ebk_stocks ...")
dst.execute("""
CREATE TABLE IF NOT EXISTS qs_ebk_stocks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    date       TEXT    NOT NULL,
    code       TEXT    NOT NULL,
    name       TEXT,
    market     TEXT,
    close      REAL,
    chg_pct    REAL,
    pe_ttm     REAL,
    mcap       REAL,
    created_at TEXT    DEFAULT (datetime('now','localtime')),
    UNIQUE(date, code)
)
""")
rows = src.execute("SELECT date,code,name,market,close,chg_pct,pe_ttm,mcap,created_at FROM ebk_stocks").fetchall()
dst.executemany("""
    INSERT OR IGNORE INTO qs_ebk_stocks(date,code,name,market,close,chg_pct,pe_ttm,mcap,created_at)
    VALUES(?,?,?,?,?,?,?,?,?)
""", [tuple(r) for r in rows])
dst.commit()
print(f"    迁入 {len(rows)} 行（已有重复行忽略）")

# ── 2. qs_picks ───────────────────────────────────────────────────
print("[2/3] 创建 qs_picks ...")
dst.execute("""
CREATE TABLE IF NOT EXISTS qs_picks (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    date           TEXT    NOT NULL,
    rank_no        INTEGER,
    code           TEXT,
    name           TEXT,
    industry       TEXT,
    report_period  TEXT,
    close          REAL,
    chg_pct        TEXT,
    total_score    REAL,
    tech_score     INTEGER,
    fund_score     INTEGER,
    pe_ttm         TEXT,
    mcap           TEXT,
    profit_yoy     TEXT,
    revenue_yoy    TEXT,
    roe            TEXT,
    eps            REAL,
    tech_sigs      TEXT,
    fund_sigs      TEXT,
    action         TEXT,
    buy_range      TEXT,
    stop_loss      TEXT,
    target         TEXT,
    position_pct   TEXT,
    created_at     TEXT,
    matched_sectors TEXT,
    sector_count   INTEGER,
    hot_score      REAL,
    final_score    REAL,
    fin_source     TEXT,
    UNIQUE(date, code)
)
""")
rows = src.execute("""
    SELECT date,rank_no,code,name,industry,report_period,close,chg_pct,
           total_score,tech_score,fund_score,pe_ttm,mcap,profit_yoy,revenue_yoy,
           roe,eps,tech_sigs,fund_sigs,action,buy_range,stop_loss,target,
           position_pct,created_at,matched_sectors,sector_count,hot_score,
           final_score,fin_source FROM picks
""").fetchall()
dst.executemany("""
    INSERT OR IGNORE INTO qs_picks(
        date,rank_no,code,name,industry,report_period,close,chg_pct,
        total_score,tech_score,fund_score,pe_ttm,mcap,profit_yoy,revenue_yoy,
        roe,eps,tech_sigs,fund_sigs,action,buy_range,stop_loss,target,
        position_pct,created_at,matched_sectors,sector_count,hot_score,
        final_score,fin_source)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", [tuple(r) for r in rows])
dst.commit()
print(f"    迁入 {len(rows)} 行")

# ── 3. qs_trend_sectors ───────────────────────────────────────────
# 实际字段：id, date, sector_rank, sector_name, stock_count, ratio
print("[3/3] 创建 qs_trend_sectors ...")
dst.execute("""
CREATE TABLE IF NOT EXISTS qs_trend_sectors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT    NOT NULL,
    sector_rank  INTEGER,
    sector_name  TEXT,
    stock_count  INTEGER,
    ratio        REAL,
    created_at   TEXT    DEFAULT (datetime('now','localtime')),
    UNIQUE(date, sector_name)
)
""")
rows = src.execute(
    "SELECT date, sector_rank, sector_name, stock_count, ratio FROM trend_sectors"
).fetchall()
dst.executemany("""
    INSERT OR IGNORE INTO qs_trend_sectors(date, sector_rank, sector_name, stock_count, ratio)
    VALUES(?,?,?,?,?)
""", [tuple(r) for r in rows])
dst.commit()
print(f"    迁入 {len(rows)} 行")

# ── 验证 ──────────────────────────────────────────────────────────
print("\n=== 验证 concept_weekly.db qs_* 表 ===")
for t in ['qs_ebk_stocks','qs_picks','qs_trend_sectors']:
    cnt = dst.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t:30s} {cnt:>8} rows")

src.close()
dst.close()
print("\n迁移完成。")
