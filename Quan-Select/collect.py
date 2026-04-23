"""
collect.py  —— 每日数据采集
------------------------------------------------------------
功能：
  1. 解析 EBK 自选股文件 → 拉取当日行情 → 存入 qs_ebk_stocks 表
  2. 从 concept_weekly.db 计算热门板块 → 存入 qs_trend_sectors 表
     算法（三源交集，默认）：
       来源A：板块涨停统计TOP15（从 t_stock_calc 按当日涨停数量降序）
       来源B：日线分析满足数TOP15（t_daily_report WHERE period='daily'）
       来源C：三日分析满足数TOP15（t_daily_report WHERE period='3day'）
       热门板块 = A ∩ B ∩ C（无数量上限）
       退化策略：三源为空 → A ∩ B → 仅B（可通过 --no-fallback 禁止退化）
       --no-3day：退回双源模式（A ∩ B）

用法：
  python collect.py                          # 采集今日数据
  python collect.py --date 20260326          # 指定日期
  python collect.py --ebk 见龙在田.EBK       # 指定EBK文件路径
  python collect.py --top 20                 # 满足率板块取TOP N（默认20）

表结构（concept_weekly.db）：
  qs_ebk_stocks (date, code, name, market, close, chg_pct, pe_ttm, mcap, created_at)
  qs_trend_sectors (date, sector_rank, sector_name, stock_count, ratio, created_at)
------------------------------------------------------------
"""

import os, re, sys, sqlite3, time, argparse, glob
from datetime import date, datetime

import requests

# ── 路径配置 ─────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_FILE    = os.path.join(BASE_DIR, "..", "db", "concept_weekly.db")
EBK_FILE   = os.path.join(BASE_DIR, "picks", "见龙在田.EBK")
# 从集中配置文件读取通达信路径（修改 qs_config.py 即可适配不同环境）
try:
    from qs_config import TDX_CW_DIR
except ImportError:
    TDX_CW_DIR = r"C:\TongDaXin\vipdoc\cw"   # 财务 zip 目录（请修改 qs_config.py）
HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
               "Referer": "https://www.eastmoney.com/"}


# ══════════════════════════════════════════════════════════
# DDL
# ══════════════════════════════════════════════════════════
DDL = """
CREATE TABLE IF NOT EXISTS qs_ebk_stocks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    date       TEXT NOT NULL,
    code       TEXT NOT NULL,          -- 6位代码
    name       TEXT,
    market     TEXT,                   -- sh / sz
    close      REAL,
    chg_pct    REAL,
    pe_ttm     REAL,
    mcap       REAL,                   -- 总市值（亿元）
    created_at TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(date, code)
);
CREATE INDEX IF NOT EXISTS idx_qs_ebk_date ON qs_ebk_stocks(date);

CREATE TABLE IF NOT EXISTS qs_trend_sectors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT NOT NULL,
    sector_rank  INTEGER,
    sector_name  TEXT,
    stock_count  INTEGER,
    ratio        REAL,
    created_at   TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(date, sector_name)
);
CREATE INDEX IF NOT EXISTS idx_qs_tsec_date ON qs_trend_sectors(date);

CREATE TABLE IF NOT EXISTS qs_finance_cache (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    code          TEXT NOT NULL UNIQUE,
    -- 超预期数据（网络获取，72小时内有效）
    expect_yoy    REAL,               -- 一致预期净利润增速%
    ttm_yoy       REAL,               -- TTM净利润增速%
    org_num       INTEGER,            -- 机构覆盖数
    diff          REAL,               -- 预期差 = expect_yoy - ttm_yoy
    cached_at     TEXT DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_qs_fin_code ON qs_finance_cache(code);

-- 消息面评分缓存表（1天内有效）
CREATE TABLE IF NOT EXISTS qs_news_sentiment (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    code          TEXT NOT NULL UNIQUE,
    score         REAL,               -- 消息面评分 0-100
    level         TEXT,               -- 等级：重大利好/利好/偏利好/中性/偏利空/利空/重大利空
    event_count   INTEGER,            -- 公告数量
    bullish_count INTEGER,            -- 利好公告数
    bearish_count INTEGER,            -- 利空公告数
    summary       TEXT,               -- 摘要
    is_filtered   INTEGER DEFAULT 0,  -- 是否被过滤（1=利空/重大利空）
    fetch_status  TEXT DEFAULT 'success', -- 获取状态：success/failed
    cached_at     TEXT DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_qs_news_code ON qs_news_sentiment(code);
CREATE INDEX IF NOT EXISTS idx_qs_news_cached ON qs_news_sentiment(cached_at);
"""


# ══════════════════════════════════════════════════════════
# 1. 解析 EBK
# ══════════════════════════════════════════════════════════
def parse_ebk(filepath: str) -> list[dict]:
    """解析 EBK 文件，返回 [{'market': 'sz', 'code': '002616'}, ...]"""
    stocks = []
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            code = line.strip()
            if len(code) == 7 and code.isdigit():
                market = "sz" if code[0] == "0" else "sh"
                stocks.append({"market": market, "code": code[1:]})
    return stocks


# ══════════════════════════════════════════════════════════
# 2. 腾讯行情批量拉取
# ══════════════════════════════════════════════════════════
def fetch_tencent_batch(stocks: list[dict]) -> dict:
    """
    批量拉取腾讯行情，返回 {code6: {name, close, chg_pct, pe_ttm, mcap}}
    字段：[1]=名称 [3]=现价 [32]=涨跌幅 [39]=PE [45]=总市值(亿元)
    """
    result = {}
    batch_size = 80

    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i + batch_size]
        syms  = ",".join(s["market"] + s["code"] for s in batch)
        url   = f"https://qt.gtimg.cn/q={syms}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.encoding = "gbk"
            for line in r.text.strip().split("\n"):
                m = re.match(r'v_(\w+)="(.+)"', line)
                if not m:
                    continue
                sym    = m.group(1)       # sz002616
                fields = m.group(2).split("~")
                code   = sym[2:]          # 002616
                try:
                    result[code] = {
                        "name":    fields[1],
                        "close":   float(fields[3])  if fields[3]            else None,
                        "chg_pct": float(fields[32]) if len(fields) > 32 and fields[32] else None,
                        "pe_ttm":  float(fields[39]) if len(fields) > 39 and fields[39] else None,
                        "mcap":    float(fields[45]) if len(fields) > 45 and fields[45] else None,
                    }
                except Exception:
                    result[code] = {"name": "-", "close": None,
                                    "chg_pct": None, "pe_ttm": None, "mcap": None}
        except Exception as e:
            print(f"  [腾讯接口错误] {e}")
        time.sleep(0.1)

    return result


# ══════════════════════════════════════════════════════════
# 3. 从主库计算热门板块
# ══════════════════════════════════════════════════════════

def _is_limit_up(stock_code: str, daily_change: float) -> bool:
    """判断是否涨停：创业板/科创板≥19.9%，主板≥9.9%"""
    if stock_code.startswith(("30", "688")):
        return daily_change >= 19.9
    return daily_change >= 9.9


def compute_limit_up_sectors(conn: sqlite3.Connection, top_n: int = 30) -> set:
    """
    来源A：板块涨停统计。
    从 t_stock_calc（最新 run_id）统计每个板块当日涨停数量，降序取 TOP N，
    返回板块名称集合。

    算法与 gen_concept_html_V1.3.py 的 load_sector_change_stats() 一致。
    """
    # 获取最新 run_id
    row = conn.execute(
        "SELECT run_id FROM t_run_log ORDER BY run_time DESC LIMIT 1"
    ).fetchone()
    if not row:
        print("  [WARN] t_run_log 为空，无法计算涨停板块")
        return set()
    latest_run_id = row[0]

    # 获取所有板块
    sectors = conn.execute(
        "SELECT sector_code, sector_name FROM t_sector"
    ).fetchall()

    result = []
    for sector_code, sector_name in sectors:
        rows = conn.execute("""
            SELECT sc.stock_code, sc.daily_change
            FROM t_stock_calc sc
            JOIN t_sector_stock ss ON sc.stock_code = ss.stock_code
            WHERE ss.sector_code = ? AND sc.run_id = ?
        """, (sector_code, latest_run_id)).fetchall()

        if not rows:
            continue

        limit_up_count = sum(
            1 for stock_code, daily_change in rows
            if daily_change is not None and _is_limit_up(stock_code, daily_change)
        )
        result.append((sector_name, limit_up_count))

    # 按涨停数降序，取 TOP N
    result.sort(key=lambda x: x[1], reverse=True)
    top_names = {name for name, cnt in result[:top_n] if cnt > 0}

    print(f"  [来源A] 涨停板块TOP{top_n}（涨停数>0）: {len(top_names)} 个")
    if result[:5]:
        for name, cnt in result[:5]:
            print(f"          {name}: {cnt}只涨停")

    return top_names


def compute_hot_sectors(conn: sqlite3.Connection, date_str: str,
                        top_n: int = 15, limit_up_top_n: int = 15,
                        use_3day: bool = True, allow_fallback: bool = True) -> list[dict]:
    """
    热门板块 = 来源A ∩ 来源B ∩ 来源C（三源交集，无数量上限）

    来源A：板块涨停统计TOP15（按当日涨停数量降序）
    来源B：日线分析满足数TOP15（t_daily_report WHERE period='daily'，按 satisfied_count DESC）
    来源C：三日分析满足数TOP15（t_daily_report WHERE period='3day'，按 satisfied_count DESC）

    退化策略（仅当 use_3day=True 时适用）：
      - 三源交集为空 → 退化为 A ∩ B（双源）
      - 双源交集也为空 → 退化为仅来源B

    参数：
      date_str:         'YYYY-MM-DD'
      top_n:            来源B/C 取 TOP N
      limit_up_top_n:   来源A 取 TOP N
      use_3day:         是否启用三源交集（True=三源，False=双源）
      allow_fallback:   三源交集为空时是否退化为双源

    返回 [{"rank": 1, "name": "储能", "count": 78, "ratio": 32.64}, ...]
      rank/count/ratio 均来自来源B的数据，保持与下游 score.py 兼容
    """
    # ── 来源A：涨停统计TOP15 ──────────────────────────────
    limit_up_set = compute_limit_up_sectors(conn, top_n=limit_up_top_n)

    # ── 来源B：日线分析满足数TOP15 ───────────────────────
    # 找当日日线分析的 run_id（run_id 含 _HIST_DAILY，note 含 [日线]）
    row = conn.execute(
        "SELECT run_id, satisfied_count FROM t_run_log "
        "WHERE note LIKE ? AND note LIKE '%[日线]%' ORDER BY run_time DESC LIMIT 1",
        (f"%{date_str}%",)
    ).fetchone()

    if not row:
        # fallback：找最近一条日线历史记录
        row = conn.execute(
            "SELECT run_id, satisfied_count FROM t_run_log "
            "WHERE note LIKE '%历史回溯%' AND note LIKE '%[日线]%' ORDER BY run_time DESC LIMIT 1"
        ).fetchone()
        if row:
            print(f"  [WARN] t_run_log 无 {date_str} 日线数据，使用最近记录: {row[0]}")
        else:
            print(f"  [WARN] t_run_log 无日线数据，跳过热门板块计算")
            return []

    run_id, total_satisfied = row

    # 提取对应的 report_date
    report_date = conn.execute(
        "SELECT DISTINCT report_date FROM t_daily_report WHERE run_id=? LIMIT 1",
        (run_id,)
    ).fetchone()
    report_date = report_date[0] if report_date else date_str

    # 查询日线分析 TOP N 板块（按 satisfied_count DESC）
    # ratio 字段：为保持下游兼容，仍计算总比满足率（分母=日线总满足数）
    rows = conn.execute("""
        SELECT ds.sector_code,
               ds.sector_name,
               ds.satisfied_count,
               ROUND(CAST(ds.satisfied_count AS REAL) / ? * 100, 4) AS total_rate
        FROM t_daily_report ds
        WHERE ds.run_id = ? AND ds.period = 'daily'
        ORDER BY ds.satisfied_count DESC
        LIMIT ?
    """, (total_satisfied if total_satisfied else 1, run_id, top_n)).fetchall()

    satisfy_top = {r[1]: {"count": r[2], "ratio": round(r[3], 2)} for r in rows}
    print(f"  [来源B] 日线分析TOP{top_n}: {len(satisfy_top)} 个  (run_id={run_id})")

    # ── 来源C：三日分析满足数TOP15 ───────────────────────
    three_day_set = set()
    if use_3day:
        row_3d = conn.execute(
            "SELECT run_id, satisfied_count FROM t_run_log "
            "WHERE note LIKE ? AND note LIKE '%[三日]%' ORDER BY run_time DESC LIMIT 1",
            (f"%{date_str}%",)
        ).fetchone()

        if not row_3d:
            row_3d = conn.execute(
                "SELECT run_id, satisfied_count FROM t_run_log "
                "WHERE note LIKE '%历史回溯%' AND note LIKE '%[三日]%' ORDER BY run_time DESC LIMIT 1"
            ).fetchone()
            if row_3d:
                print(f"  [WARN] t_run_log 无 {date_str} 三日数据，使用最近记录: {row_3d[0]}")

        if row_3d:
            run_id_3d = row_3d[0]
            rows_3d = conn.execute("""
                SELECT ds.sector_name
                FROM t_daily_report ds
                WHERE ds.run_id = ? AND ds.period = '3day'
                ORDER BY ds.satisfied_count DESC
                LIMIT ?
            """, (run_id_3d, top_n)).fetchall()
            three_day_set = {r[0] for r in rows_3d}
            print(f"  [来源C] 三日分析TOP{top_n}: {len(three_day_set)} 个  (run_id={run_id_3d})")
        else:
            print(f"  [WARN] t_run_log 无三日数据，来源C为空")

    # ── 交集 ─────────────────────────────────────────────
    if use_3day and three_day_set:
        # 三源模式
        intersect_names = set(satisfy_top.keys()) & limit_up_set & three_day_set
        print(f"  [交集]  A ∩ B ∩ C = {len(intersect_names)} 个热门板块")

        # 退化策略
        if not intersect_names and allow_fallback:
            intersect_names = set(satisfy_top.keys()) & limit_up_set
            print(f"  [退化]  三源为空，退化为 A ∩ B = {len(intersect_names)} 个")

            if not intersect_names:
                print("  [退化]  双源也为空，退化为仅来源B")
                intersect_names = set(satisfy_top.keys())
        elif not intersect_names and not allow_fallback:
            # 不允许退化，直接返回空
            print(f"  [三源交集为空，不允许退化] 返回空结果")
            return []
    else:
        # 双源模式（use_3day=False 或来源C无数据）
        intersect_names = set(satisfy_top.keys()) & limit_up_set
        print(f"  [交集]  A ∩ B = {len(intersect_names)} 个热门板块")

        if not intersect_names:
            print("  [WARN] 交集为空，退化为仅使用来源B（日线分析TOP15）")
            intersect_names = set(satisfy_top.keys())

    # 按来源B的满足数排序，重新编号
    result = []
    rank = 1
    for r in rows:                       # rows 已按 satisfied_count DESC 排序
        name = r[1]
        if name in intersect_names:
            result.append({
                "rank":  rank,
                "name":  name,
                "count": satisfy_top[name]["count"],
                "ratio": satisfy_top[name]["ratio"],
            })
            rank += 1

    return result


# ══════════════════════════════════════════════════════════
# 4. 写入数据库
# ══════════════════════════════════════════════════════════
def init_db(conn: sqlite3.Connection):
    conn.executescript(DDL)
    conn.commit()
    _migrate_finance_cache(conn)


def _migrate_finance_cache(conn: sqlite3.Connection):
    """
    迁移 qs_finance_cache 到新结构（只保留超预期数据）：
    - 如果旧表存在多余列（report_period/profit_yoy等），重建表
    - 新表：code UNIQUE, expect_yoy, ttm_yoy, org_num, diff, cached_at
    """
    cur = conn.execute("PRAGMA table_info(qs_finance_cache)")
    cols = {row[1] for row in cur.fetchall()}
    # 如果有旧字段，说明是旧表结构，需要迁移
    if "report_period" in cols or "profit_yoy" in cols:
        print("  [DB迁移] qs_finance_cache 旧结构，重建为新版（仅超预期数据）...")
        conn.execute("DROP TABLE IF EXISTS qs_finance_cache_old")
        conn.execute("ALTER TABLE qs_finance_cache RENAME TO qs_finance_cache_old")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS qs_finance_cache (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                code          TEXT NOT NULL UNIQUE,
                expect_yoy    REAL,
                ttm_yoy       REAL,
                org_num       INTEGER,
                diff          REAL,
                cached_at     TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE INDEX IF NOT EXISTS idx_qs_fin_code ON qs_finance_cache(code);
        """)
        # 迁移超预期数据（如果旧表有的话）
        if "expect_yoy" in cols:
            conn.execute("""
                INSERT OR IGNORE INTO qs_finance_cache (code, expect_yoy, ttm_yoy, org_num, diff, cached_at)
                SELECT code, expect_yoy, ttm_yoy, org_num, diff, cached_at
                FROM qs_finance_cache_old
                WHERE expect_yoy IS NOT NULL
                  AND cached_at > datetime('now', '-3 days')
            """)
        conn.execute("DROP TABLE IF EXISTS qs_finance_cache_old")
        conn.commit()
        print("  [DB迁移] 完成")


def save_ebk_stocks(conn: sqlite3.Connection, date_str: str,
                    stocks: list[dict], quote_map: dict):
    """幂等写入 qs_ebk_stocks（先删当日，再插入）"""
    cur = conn.cursor()
    cur.execute("DELETE FROM qs_ebk_stocks WHERE date=?", (date_str,))

    rows = []
    for s in stocks:
        code = s["code"]
        q    = quote_map.get(code, {})
        rows.append((
            date_str, code, q.get("name", ""), s["market"],
            q.get("close"), q.get("chg_pct"),
            q.get("pe_ttm"), q.get("mcap"),
        ))

    cur.executemany(
        "INSERT INTO qs_ebk_stocks(date,code,name,market,close,chg_pct,pe_ttm,mcap) "
        "VALUES(?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    print(f"  [qs_ebk_stocks]    写入 {len(rows)} 条  (date={date_str})")


def save_trend_sectors(conn: sqlite3.Connection, date_str: str,
                       sectors: list[dict]):
    """幂等写入 qs_trend_sectors"""
    cur = conn.cursor()
    cur.execute("DELETE FROM qs_trend_sectors WHERE date=?", (date_str,))
    cur.executemany(
        "INSERT INTO qs_trend_sectors(date,sector_rank,sector_name,stock_count,ratio) "
        "VALUES(?,?,?,?,?)",
        [(date_str, s["rank"], s["name"], s["count"], s["ratio"]) for s in sectors]
    )
    conn.commit()
    print(f"  [qs_trend_sectors] 写入 {len(sectors)} 条  (date={date_str})")


# ══════════════════════════════════════════════════════════
# 5. 财务数据获取（本地TDX）与超预期缓存
# ══════════════════════════════════════════════════════════
# zip 内存缓存统一走 fund_strategies._load_fin_df，避免同进程重复解压
from fund_strategies import _load_fin_df as _load_fin_zip


def _safe_float(row, idx):
    try:
        v = row.iloc[idx] if hasattr(row, 'iloc') else row[idx]
        f = float(v)
        return f if not (f != f) else None   # NaN check
    except Exception:
        return None


def fetch_local_finance(code: str) -> dict:
    """
    从本地通达信 zip 读取基础财务数据，返回标准字段 dict；失败返回 None
    策略：优先最新 zip，若该股没收录则向前回溯最多 4 个季度

    列索引（已逐一对比东财接口确认，2026-03-27）：
      col[1]  = EPS基本
      col[281]= ROE加权季度(%)
      col[183]= 营收同比%（单季，与东财TOTALOPERATEREVETZ一致）
      col[184]= 净利同比%（单季，与东财PARENTNETPROFITTZ完全一致）
    """
    pattern = os.path.join(TDX_CW_DIR, "gpcw*.zip")
    all_files = sorted([f for f in glob.glob(pattern)
                        if os.path.getsize(f) >= 10 * 1024], reverse=True)
    if not all_files:
        return None

    found_zip = None
    for f in all_files[:4]:
        df_try = _load_fin_zip(f)
        if df_try is not None and code in df_try.index:
            found_zip = f
            break
    if found_zip is None:
        return None

    df  = _load_fin_zip(found_zip)
    row = df.loc[code]

    return {
        "eps":           _safe_float(row, 1),
        "roe":           _safe_float(row, 281),
        "revenue_yoy":   _safe_float(row, 183),
        "profit_yoy":    _safe_float(row, 184),
        "industry_type": "",
        "report_period": os.path.basename(found_zip).replace("gpcw", "").replace(".zip", ""),
        "fin_source":    "local",
    }


def fetch_surprise_data(code: str) -> dict:
    """
    获取超预期相关数据（网络接口）：一致预期EPS + TTM净利润增速
    返回: {expect_yoy, ttm_yoy, org_num, diff}
    """
    try:
        from fund_strategies import fetch_consensus_eps, calc_ttm_profit_growth
        cons = fetch_consensus_eps(code)
        time.sleep(0.15)
        ttm = calc_ttm_profit_growth(code)
        expect_yoy = cons.get("expect_yoy")
        org_num = cons.get("org_num")
        diff = expect_yoy - ttm if expect_yoy is not None and ttm is not None else None
        return {
            "expect_yoy": expect_yoy,
            "ttm_yoy": ttm,
            "org_num": org_num,
            "diff": diff,
        }
    except Exception:
        return {"expect_yoy": None, "ttm_yoy": None, "org_num": None, "diff": None}


def get_surprise_from_cache(conn: sqlite3.Connection, code: str) -> dict | None:
    """
    从缓存表读取超预期数据，72小时内有效才返回
    - 缓存存在且 expect_yoy 有效：返回数据 dict
    - 缓存存在但 expect_yoy 为 None：返回空 dict {}（表示已缓存但无有效数据）
    - 缓存不存在：返回 None
    """
    row = conn.execute("""
        SELECT expect_yoy, ttm_yoy, org_num, diff
        FROM qs_finance_cache
        WHERE code = ? AND cached_at > datetime('now', '-3 days')
        LIMIT 1
    """, (code,)).fetchone()
    if row is None:
        return None  # 缓存不存在
    if row[0] is not None:
        return {
            "expect_yoy": row[0],
            "ttm_yoy":    row[1],
            "org_num":    row[2],
            "diff":       row[3],
        }
    # 缓存存在但无有效数据（之前网络获取失败）
    return {"expect_yoy": None, "ttm_yoy": None, "org_num": None, "diff": None}


def save_surprise_cache(conn: sqlite3.Connection, code: str, data: dict):
    """写入/更新超预期缓存"""
    try:
        conn.execute("""
            INSERT INTO qs_finance_cache (code, expect_yoy, ttm_yoy, org_num, diff, cached_at)
            VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(code) DO UPDATE SET
                expect_yoy = excluded.expect_yoy,
                ttm_yoy    = excluded.ttm_yoy,
                org_num    = excluded.org_num,
                diff       = excluded.diff,
                cached_at  = excluded.cached_at
        """, (code, data.get("expect_yoy"), data.get("ttm_yoy"),
              data.get("org_num"), data.get("diff")))
        conn.commit()
    except Exception as e:
        print(f"  [超预期缓存写入失败] {code}: {e}")


# ══════════════════════════════════════════════════════════
# 快报/预告 TTM 数据缓存
# ══════════════════════════════════════════════════════════
_DDL_QUICK_REPORT = """
CREATE TABLE IF NOT EXISTS qs_quick_report (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    code          TEXT NOT NULL UNIQUE,
    report_type   TEXT,                 -- 'express' 或 'forecast'
    report_period TEXT,                 -- 报告期 YYYYMMDD
    ann_date      TEXT,                 -- 公告日期
    ttm_yoy       REAL,                 -- 估算的TTM净利同比增速%
    n_income      REAL,                 -- 快报归母净利润（express）
    net_profit_mid REAL,                -- 预告净利润中值（forecast）
    p_change_min  REAL,                 -- 预告增幅下限%（forecast）
    p_change_max  REAL,                 -- 预告增幅上限%（forecast）
    summary       TEXT,                 -- 预告摘要（forecast）
    cached_at     TEXT DEFAULT (datetime('now','localtime'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_qs_qr_code ON qs_quick_report(code);
"""


def _ensure_quick_report_table(conn: sqlite3.Connection):
    """确保快报/预告缓存表存在"""
    conn.executescript(_DDL_QUICK_REPORT)
    conn.commit()


def get_quick_report_from_cache(conn: sqlite3.Connection, code: str) -> dict | None:
    """
    从缓存读取快报/预告 TTM 数据，72小时内有效。
    返回 None = 无缓存；返回 dict = 有数据（ttm_yoy 可能为 None）
    """
    _ensure_quick_report_table(conn)
    row = conn.execute("""
        SELECT report_type, report_period, ann_date, ttm_yoy,
               n_income, net_profit_mid, p_change_min, p_change_max, summary
        FROM qs_quick_report
        WHERE code = ? AND cached_at > datetime('now', '-3 days')
        LIMIT 1
    """, (code,)).fetchone()
    if row is None:
        return None
    return {
        "report_type":    row[0],
        "report_period":  row[1],
        "ann_date":       row[2],
        "ttm_yoy":        row[3],
        "n_income":       row[4],
        "net_profit_mid": row[5],
        "p_change_min":   row[6],
        "p_change_max":   row[7],
        "summary":        row[8],
    }


def save_quick_report_cache(conn: sqlite3.Connection, code: str, data: dict):
    """写入/更新快报/预告缓存"""
    _ensure_quick_report_table(conn)
    try:
        conn.execute("""
            INSERT INTO qs_quick_report
            (code, report_type, report_period, ann_date, ttm_yoy,
             n_income, net_profit_mid, p_change_min, p_change_max, summary, cached_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(code) DO UPDATE SET
                report_type    = excluded.report_type,
                report_period  = excluded.report_period,
                ann_date       = excluded.ann_date,
                ttm_yoy        = excluded.ttm_yoy,
                n_income       = excluded.n_income,
                net_profit_mid = excluded.net_profit_mid,
                p_change_min   = excluded.p_change_min,
                p_change_max   = excluded.p_change_max,
                summary        = excluded.summary,
                cached_at      = excluded.cached_at
        """, (code,
              data.get("report_type"), data.get("report_period"), data.get("ann_date"),
              data.get("ttm_yoy"), data.get("n_income"), data.get("net_profit_mid"),
              data.get("p_change_min"), data.get("p_change_max"), data.get("summary")))
        conn.commit()
    except Exception as e:
        print(f"  [快报缓存写入失败] {code}: {e}")


# ══════════════════════════════════════════════════════════
# 季度一致预期缓存（fetch_quarterly_consensus 网络数据）
# ══════════════════════════════════════════════════════════
_DDL_QUARTERLY_CONSENSUS = """
CREATE TABLE IF NOT EXISTS qs_quarterly_consensus (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    code            TEXT NOT NULL UNIQUE,
    expected_np     REAL,               -- 预期当季净利润（万元，中位数）
    expected_eps    REAL,               -- 预期当季EPS
    predict_count   INTEGER,            -- 预测机构数
    latest_quarter  TEXT,               -- 最新预测季度如 "2026Q1"
    latest_report_date TEXT,            -- 最新报告日期
    source          TEXT,               -- "report_rc" / "none"
    cached_at       TEXT DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_qs_qc_code ON qs_quarterly_consensus(code);
"""


def _ensure_quarterly_consensus_table(conn: sqlite3.Connection):
    """确保季度一致预期缓存表存在"""
    conn.executescript(_DDL_QUARTERLY_CONSENSUS)
    conn.commit()


def get_quarterly_consensus_from_cache(conn: sqlite3.Connection, code: str) -> dict | None:
    """
    从缓存读取季度一致预期数据，72小时内有效。
    返回 None = 无缓存；返回 dict = 有数据
    """
    _ensure_quarterly_consensus_table(conn)
    row = conn.execute("""
        SELECT expected_np, expected_eps, predict_count,
               latest_quarter, latest_report_date, source
        FROM qs_quarterly_consensus
        WHERE code = ? AND cached_at > datetime('now', '-3 days')
        LIMIT 1
    """, (code,)).fetchone()
    if row is None:
        return None
    return {
        "expected_np":        row[0],
        "expected_eps":       row[1],
        "predict_count":      row[2],
        "latest_quarter":     row[3],
        "latest_report_date": row[4],
        "source":             row[5],
    }


def save_quarterly_consensus_cache(conn: sqlite3.Connection, code: str, data: dict):
    """写入/更新季度一致预期缓存"""
    _ensure_quarterly_consensus_table(conn)
    try:
        conn.execute("""
            INSERT INTO qs_quarterly_consensus
            (code, expected_np, expected_eps, predict_count,
             latest_quarter, latest_report_date, source, cached_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(code) DO UPDATE SET
                expected_np        = excluded.expected_np,
                expected_eps       = excluded.expected_eps,
                predict_count      = excluded.predict_count,
                latest_quarter     = excluded.latest_quarter,
                latest_report_date = excluded.latest_report_date,
                source             = excluded.source,
                cached_at          = excluded.cached_at
        """, (code,
              data.get("expected_np"), data.get("expected_eps"),
              data.get("predict_count"), data.get("latest_quarter"),
              data.get("latest_report_date"), data.get("source")))
        conn.commit()
    except Exception as e:
        print(f"  [季度预期缓存写入失败] {code}: {e}")


def fetch_quick_report_data(code: str) -> dict:
    """
    获取快报/预告 TTM 数据（网络接口），优先快报后预告。
    返回: {report_type, report_period, ann_date, ttm_yoy, n_income, net_profit_mid, p_change_min, p_change_max, summary}
    """
    from fund_strategies import fetch_express_ttm, fetch_forecast_ttm

    # 1. 快报
    express = fetch_express_ttm(code)
    if express.get("source") == "express":
        return {
            "report_type":    "express",
            "report_period":  express.get("report_period"),
            "ann_date":       express.get("ann_date"),
            "ttm_yoy":        express.get("ttm_yoy"),
            "n_income":       express.get("n_income"),
            "net_profit_mid": None,
            "p_change_min":   None,
            "p_change_max":   None,
            "summary":        None,
        }

    # 2. 预告
    forecast = fetch_forecast_ttm(code)
    if forecast.get("source") == "forecast":
        return {
            "report_type":    "forecast",
            "report_period":  forecast.get("report_period"),
            "ann_date":       forecast.get("ann_date"),
            "ttm_yoy":        forecast.get("ttm_yoy"),
            "n_income":       None,
            "net_profit_mid": forecast.get("net_profit_mid"),
            "p_change_min":   forecast.get("p_change_min"),
            "p_change_max":   forecast.get("p_change_max"),
            "summary":        forecast.get("summary"),
        }

    return {"report_type": None, "ttm_yoy": None}


def batch_fetch_finance(conn: sqlite3.Connection, stocks: list[dict],
                        force_refresh: bool = False, require_surprise: bool = False,
                        require_quarterly: bool = False) -> dict:
    """
    批量获取财务数据：
    - 基础财务（eps/roe/profit_yoy/revenue_yoy）：全部实时读本地 TDX zip
    - 超预期数据（expect_yoy/ttm_yoy/org_num/diff）：优先读缓存，缓存失效则网络获取并写入缓存
    - 季度一致预期（expected_np/expected_eps/predict_count）：仅 require_quarterly=True 时采集并缓存

    返回: {code: finance_dict}
    """
    print(f"\n[6] 批量获取财务数据（本地TDX + 超预期缓存"
          f"{' + 季度预期' if require_quarterly else ''}）...")
    result = {}
    local_ok = 0
    local_fail = 0
    surprise_cache_hit = 0
    surprise_fetch = 0
    qc_cache_hit = 0
    qc_fetch = 0

    for i, s in enumerate(stocks):
        code = s["code"]

        # ── 基础财务：始终读本地 TDX ────────────────────────
        fin = fetch_local_finance(code)
        if fin is None:
            fin = {"eps": None, "roe": None, "revenue_yoy": None,
                   "profit_yoy": None, "industry_type": "", "report_period": "", "fin_source": "none"}
            local_fail += 1
        else:
            local_ok += 1

        # ── 超预期数据：缓存优先，缺失或强刷则网络获取 ────────
        surprise = None
        if not force_refresh:
            surprise = get_surprise_from_cache(conn, code)
            if surprise:
                surprise_cache_hit += 1

        if surprise is None:
            surprise = fetch_surprise_data(code)
            # 无论成功与否都写入缓存，避免score阶段重复请求
            save_surprise_cache(conn, code, surprise)
            surprise_fetch += 1
            time.sleep(0.05)  # 网络限速

        # ── 季度一致预期：仅 require_quarterly 时采集并缓存 ────
        qc = {}
        if require_quarterly:
            qc = get_quarterly_consensus_from_cache(conn, code)
            if qc is not None:
                qc_cache_hit += 1
            else:
                # 无缓存，网络获取（含超时/失败也写缓存，3天内不再请求）
                from fund_strategies import fetch_quarterly_consensus
                qc = fetch_quarterly_consensus(code)
                save_quarterly_consensus_cache(conn, code, qc)
                qc_fetch += 1
                time.sleep(0.15)  # 网络限速

        result[code] = {**fin, **surprise, **qc, "_cache_hit": False}

        if (i + 1) % 5 == 0 or i == 0 or i == len(stocks) - 1:
            qpart = f" QC(缓存{qc_cache_hit}/网络{qc_fetch})" if require_quarterly else ""
            print(f"    [{i+1}/{len(stocks)}] 本地:{local_ok}"
                  f" 超预期(缓存{surprise_cache_hit}/网络{surprise_fetch}){qpart}", end='\r', flush=True)

    qpart = f"  季度预期(缓存:{qc_cache_hit} 网络:{qc_fetch})" if require_quarterly else ""
    print(f"    [{len(stocks)}/{len(stocks)}] 完成！本地TDX:{local_ok} 失败:{local_fail}"
          f" 超预期(缓存{surprise_cache_hit}/网络{surprise_fetch}){qpart}", flush=True)
    return result


# ══════════════════════════════════════════════════════════
# 7. 消息面评分采集与缓存
# ══════════════════════════════════════════════════════════
def get_news_from_cache(conn: sqlite3.Connection, code: str) -> Optional[dict]:
    """从缓存读取消息面评分（1天内有效）"""
    row = conn.execute(
        "SELECT score, level, event_count, bullish_count, bearish_count, summary, is_filtered, fetch_status "
        "FROM qs_news_sentiment WHERE code=? AND cached_at > datetime('now', '-1 day')",
        (code,)
    ).fetchone()
    if row:
        return {
            "score": row[0], "level": row[1], "event_count": row[2],
            "bullish_count": row[3], "bearish_count": row[4], "summary": row[5],
            "is_filtered": row[6], "fetch_status": row[7]
        }
    return None


def save_news_cache(conn: sqlite3.Connection, code: str, data: dict, fetch_status: str = "success"):
    """写入/更新消息面缓存"""
    try:
        conn.execute("""
            INSERT INTO qs_news_sentiment 
            (code, score, level, event_count, bullish_count, bearish_count, summary, is_filtered, fetch_status, cached_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(code) DO UPDATE SET
                score = excluded.score,
                level = excluded.level,
                event_count = excluded.event_count,
                bullish_count = excluded.bullish_count,
                bearish_count = excluded.bearish_count,
                summary = excluded.summary,
                is_filtered = excluded.is_filtered,
                fetch_status = excluded.fetch_status,
                cached_at = excluded.cached_at
        """, (code, data.get("score"), data.get("level"), data.get("event_count"),
              data.get("bullish_count"), data.get("bearish_count"), data.get("summary"),
              data.get("is_filtered", 0), fetch_status))
        conn.commit()
    except Exception as e:
        print(f"  [消息面缓存写入失败] {code}: {e}")


# ══════════════════════════════════════════════════════════
# 8. Surprise 窗口数据采集（财报公告日 + 研报发布日）
# ══════════════════════════════════════════════════════════
_DDL_SURPRISE_WINDOW = """
CREATE TABLE IF NOT EXISTS qs_surprise_window (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    code          TEXT NOT NULL UNIQUE,
    notice_date   TEXT,               -- 最新财报公告日（已发布的）
    notice_period TEXT,               -- 对应报告期（如20260331）
    rc_date       TEXT,               -- 最新研报发布日
    rc_count      INTEGER,            -- 研报数量
    auto_mode     TEXT,               -- 自动判定结果：forward / actual
    cached_at     TEXT DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_qs_sw_code ON qs_surprise_window(code);
"""


def _ensure_surprise_window_table(conn: sqlite3.Connection):
    """确保 surprise 窗口缓存表存在"""
    conn.executescript(_DDL_SURPRISE_WINDOW)
    conn.commit()


def get_surprise_window_from_cache(conn: sqlite3.Connection, code: str) -> dict | None:
    """
    从缓存读取 surprise 窗口数据，72小时内有效。
    返回 None = 无缓存；返回 dict = 有数据
    """
    _ensure_surprise_window_table(conn)
    row = conn.execute("""
        SELECT notice_date, notice_period, rc_date, rc_count, auto_mode
        FROM qs_surprise_window
        WHERE code = ? AND cached_at > datetime('now', '-3 days')
        LIMIT 1
    """, (code,)).fetchone()
    if row is None:
        return None
    return {
        "notice_date":   row[0],
        "notice_period": row[1],
        "rc_date":       row[2],
        "rc_count":      row[3],
        "auto_mode":     row[4],
    }


def save_surprise_window_cache(conn: sqlite3.Connection, code: str, data: dict):
    """写入/更新 surprise 窗口缓存"""
    _ensure_surprise_window_table(conn)
    try:
        conn.execute("""
            INSERT INTO qs_surprise_window
            (code, notice_date, notice_period, rc_date, rc_count, auto_mode, cached_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(code) DO UPDATE SET
                notice_date   = excluded.notice_date,
                notice_period = excluded.notice_period,
                rc_date       = excluded.rc_date,
                rc_count      = excluded.rc_count,
                auto_mode     = excluded.auto_mode,
                cached_at     = excluded.cached_at
        """, (code, data.get("notice_date"), data.get("notice_period"),
              data.get("rc_date"), data.get("rc_count"), data.get("auto_mode")))
        conn.commit()
    except Exception as e:
        print(f"  [窗口缓存写入失败] {code}: {e}")


def fetch_notice_date(code: str) -> dict:
    """
    从东财 RPT_DMSK_FN_INCOME 获取最新已公告财报的公告日。
    返回: {notice_date, notice_period}
    """
    result = {"notice_date": None, "notice_period": None}
    try:
        url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
        params = {
            "reportName": "RPT_DMSK_FN_INCOME",
            "columns": "REPORT_DATE,NOTICE_DATE",
            "filter": f'(SECURITY_CODE="{code}")',
            "pageSize": "10",
            "sortColumns": "NOTICE_DATE",
            "sortTypes": "-1",
            "source": "WEB",
            "client": "WEB",
        }
        r = requests.get(url, params=params, headers=HEADERS, timeout=5)
        data = r.json()
        if data.get("success") and data.get("result", {}).get("data"):
            today_str = date.today().strftime("%Y-%m-%d")
            for row in data["result"]["data"]:
                notice = row.get("NOTICE_DATE", "")[:10]  # 截取日期部分
                if notice and notice <= today_str:
                    result["notice_date"] = notice
                    result["notice_period"] = row.get("REPORT_DATE", "")[:10]
                    break
    except Exception:
        pass
    return result


def fetch_report_date(code: str) -> dict:
    """
    从东财研报列表获取最新研报发布日。
    返回: {rc_date, rc_count}
    """
    result = {"rc_date": None, "rc_count": 0}
    try:
        url = "https://reportapi.eastmoney.com/report/list"
        params = {
            "industryCode": "*",
            "pageSize": "10",
            "industry": "*",
            "rating": "*",
            "ratingChange": "*",
            "beginTime": "",
            "endTime": "",
            "pageNo": "1",
            "fields": "",
            "qType": "0",
            "orgCode": "",
            "code": code,         # 纯6位数字代码，不加.SH/.SZ
            "rcode": "",
            "p": "1",
            "pageNum": "1",
            "pageNumber": "1",
        }
        r = requests.get(url, params=params, headers=HEADERS, timeout=5)
        data = r.json()
        items = data.get("data") or []
        if items:
            # 取最新研报的 publishDate
            latest = items[0]
            publish_date = latest.get("publishDate", "")[:10]
            if publish_date:
                result["rc_date"] = publish_date
            result["rc_count"] = len(items)
    except Exception:
        pass
    return result


def batch_fetch_surprise_window(conn: sqlite3.Connection, stocks: list[dict],
                                 force_refresh: bool = False):
    """
    批量获取 surprise 窗口数据（财报公告日 + 研报发布日），缓存到DB。
    在 collect 阶段调用，score 阶段直接读缓存。
    """
    print(f"\n[8] 批量获取 Surprise 窗口数据（财报公告日 + 研报发布日）...")
    cache_hit = 0
    notice_ok = 0
    rc_ok = 0
    fetch_count = 0

    for i, s in enumerate(stocks):
        code = s["code"]

        # 先读缓存
        if not force_refresh:
            cached = get_surprise_window_from_cache(conn, code)
            if cached is not None:
                cache_hit += 1
                if (i + 1) % 5 == 0 or i == 0 or i == len(stocks) - 1:
                    print(f"    [{i+1}/{len(stocks)}] 缓存:{cache_hit} 采集:{fetch_count}", end='\r', flush=True)
                continue

        # 网络获取
        fetch_count += 1
        notice = fetch_notice_date(code)
        time.sleep(0.05)
        rc = fetch_report_date(code)
        time.sleep(0.05)

        # 自动判定模式
        auto_mode = "actual"  # 默认 actual（保守）
        nd = notice.get("notice_date")
        rd = rc.get("rc_date")
        if nd and rd:
            if rd > nd:
                auto_mode = "forward"
            else:
                auto_mode = "actual"  # 含同日
        elif nd:
            auto_mode = "actual"
        elif rd:
            auto_mode = "forward"  # 有研报但无公告日，默认 forward

        if notice.get("notice_date"):
            notice_ok += 1
        if rc.get("rc_date"):
            rc_ok += 1

        save_surprise_window_cache(conn, code, {
            "notice_date":   notice.get("notice_date"),
            "notice_period": notice.get("notice_period"),
            "rc_date":       rc.get("rc_date"),
            "rc_count":      rc.get("rc_count", 0),
            "auto_mode":     auto_mode,
        })

        if (i + 1) % 5 == 0 or i == 0 or i == len(stocks) - 1:
            print(f"    [{i+1}/{len(stocks)}] 缓存:{cache_hit} 采集:{fetch_count}"
                  f" 公告日:{notice_ok} 研报:{rc_ok}", end='\r', flush=True)

    print(f"    [{len(stocks)}/{len(stocks)}] 完成！缓存:{cache_hit} 采集:{fetch_count}"
          f" 公告日:{notice_ok} 研报:{rc_ok}", flush=True)


def batch_fetch_news_sentiment(conn: sqlite3.Connection, stocks: list[dict],
                                force_refresh: bool = False):
    """
    批量获取消息面评分（异步采集）
    - 优先读缓存，缓存失效则网络获取
    - 网络失败也保存记录（fetch_status='failed'，score=50中性）
    """
    print(f"\n[7] 批量获取消息面评分（缓存1天）...")
    
    # 导入消息面分析模块
    try:
        from news_sentiment import NewsSentimentAnalyzer
    except ImportError:
        print("  [WARN] news_sentiment.py 不存在，跳过消息面采集")
        return
    
    analyzer = NewsSentimentAnalyzer(delay=0.3)
    cache_hit = 0
    fetch_ok = 0
    fetch_fail = 0
    filtered = 0
    
    for i, s in enumerate(stocks):
        code = s["code"]
        name = s.get("name", "")
        
        # 检查缓存
        if not force_refresh:
            cached = get_news_from_cache(conn, code)
            if cached:
                cache_hit += 1
                continue
        
        # 网络获取
        try:
            result = analyzer.calculate_sentiment_score(code, name)
            
            # 判断是否过滤（利空/重大利空）
            is_filtered = 1 if result.level in ["利空", "重大利空"] else 0
            if is_filtered:
                filtered += 1
            
            data = {
                "score": result.total_score,
                "level": result.level,
                "event_count": result.event_count,
                "bullish_count": result.bullish_count,
                "bearish_count": result.bearish_count,
                "summary": result.summary,
                "is_filtered": is_filtered
            }
            save_news_cache(conn, code, data, fetch_status="success")
            fetch_ok += 1
            
        except Exception as e:
            # 网络失败，保存中性记录
            data = {
                "score": 50.0,
                "level": "中性",
                "event_count": 0,
                "bullish_count": 0,
                "bearish_count": 0,
                "summary": f"获取失败: {str(e)[:30]}",
                "is_filtered": 0
            }
            save_news_cache(conn, code, data, fetch_status="failed")
            fetch_fail += 1
        
        if (i + 1) % 5 == 0 or i == 0 or i == len(stocks) - 1:
            print(f"    [{i+1}/{len(stocks)}] 缓存:{cache_hit} 成功:{fetch_ok}"
                  f" 失败:{fetch_fail} 过滤:{filtered}", end='\r', flush=True)
    
    print(f"    [{len(stocks)}/{len(stocks)}] 完成！缓存:{cache_hit} 成功:{fetch_ok}"
          f" 失败:{fetch_fail} 过滤:{filtered}", flush=True)


# ══════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════
def run(date_str: str = None, ebk_path: str = None, top_n: int = None,
        use_3day: bool = True, allow_fallback: bool = True):
    """
    date_str:         'YYYY-MM-DD' 或 'YYYYMMDD'，默认今日
    ebk_path:         EBK文件路径，默认 见龙在田.EBK
    top_n:            热门板块取TOP N（默认取 compute_hot_sectors 的默认值）
    use_3day:         是否启用三源交集（True=涨停+日线+三日，False=涨停+日线）
    allow_fallback:   三源交集为空时是否退化为双源
    """
    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")

    # 统一为 YYYY-MM-DD（DB存储格式）
    compact  = date_str.replace("-", "")
    dash_str = f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"

    ebk_path = ebk_path or EBK_FILE

    print("=" * 56)
    print(f"  collect.py  采集日期: {dash_str}")
    print("=" * 56)

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)

    # ── Step 1: 解析 EBK ──────────────────────────────────
    print(f"\n[1] 解析 EBK: {os.path.basename(ebk_path)}")
    stocks = parse_ebk(ebk_path)
    print(f"    共 {len(stocks)} 只")

    # ── Step 2: 拉取行情 ───────────────────────────────────
    print(f"\n[2] 拉取腾讯行情...")
    quote_map = fetch_tencent_batch(stocks)
    ok = sum(1 for q in quote_map.values() if q.get("close"))
    print(f"    成功 {ok}/{len(stocks)} 只")

    # ── Step 3: 写入 qs_ebk_stocks ────────────────────────────
    print(f"\n[3] 写入 qs_ebk_stocks 表...")
    save_ebk_stocks(conn, dash_str, stocks, quote_map)

    # ── Step 4: 从主库计算热门板块 ─────────────────────────
    mode_label = "三源" if use_3day else "双源"
    fallback_label = "允许退化" if allow_fallback else "不允许退化"
    print(f"\n[4] 从主库计算热门板块（{mode_label}模式, {fallback_label}）...")

    # 构建 kwargs，top_n 为 None 时不传（使用函数默认值）
    _kw = dict(use_3day=use_3day, allow_fallback=allow_fallback)
    if top_n is not None:
        _kw["top_n"] = top_n
    hot_sectors = compute_hot_sectors(conn, dash_str, **_kw)
    if hot_sectors:
        print(f"    最终热门板块: {len(hot_sectors)} 个")
        for s in hot_sectors[:5]:
            print(f"      #{s['rank']:2d} {s['name']:20s} {s['count']:4d}只  {s['ratio']:.2f}%")
        if len(hot_sectors) > 5:
            print(f"      ... 共 {len(hot_sectors)} 个")

        # ── Step 5: 写入 qs_trend_sectors ──────────────────────
        print(f"\n[5] 写入 qs_trend_sectors 表...")
        save_trend_sectors(conn, dash_str, hot_sectors)
    else:
        print(f"  [WARN] 未获取到热门板块数据，跳过写入")

    # ── Step 6: 批量获取财务数据并缓存 ──────────────────────
    batch_fetch_finance(conn, stocks, require_surprise=True, require_quarterly=True)

    # ── Step 7: 批量获取消息面评分 ──────────────────────────
    batch_fetch_news_sentiment(conn, stocks)

    # ── Step 8: 批量获取 Surprise 窗口数据 ──────────────────
    batch_fetch_surprise_window(conn, stocks)

    conn.close()
    print(f"\n[OK] 采集完成  qs_ebk_stocks + qs_trend_sectors + qs_finance_cache + qs_news_sentiment + qs_surprise_window → concept_weekly.db")
    return dash_str


# ══════════════════════════════════════════════════════════
# 命令行入口
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="每日数据采集：EBK + 热门板块")
    parser.add_argument("--date", type=str, default=None,
                        help="指定日期，格式 YYYYMMDD 或 YYYY-MM-DD（默认今日）")
    parser.add_argument("--ebk",  type=str, default=None,
                        help="指定 EBK 文件路径（默认 见龙在田.EBK）")
    parser.add_argument("--top",  type=int, default=None,
                        help="热门板块取 TOP N（默认15）")
    parser.add_argument("--no-3day", action="store_true",
                        help="禁用三源交集，仅使用涨停+日线（双源模式）")
    parser.add_argument("--no-fallback", action="store_true",
                        help="三源交集为空时不退化为双源，直接返回空结果")
    args = parser.parse_args()

    run(date_str=args.date, ebk_path=args.ebk, top_n=args.top,
        use_3day=not args.no_3day, allow_fallback=not args.no_fallback)
