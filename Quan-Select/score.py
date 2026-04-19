"""
score.py  —— 精选分析与评分
------------------------------------------------------------
功能：
  1. 从 qs_ebk_stocks + qs_trend_sectors 关联，筛出命中热门板块的候选股
  2. 读取本地通达信 K 线 → 技术打分
  3. 财务数据：东方财富网络接口优先，失败时 fallback 本地通达信 zip
  4. 综合评分 → 存入 qs_picks 表（全量候选，按日幂等）

用法：
  python score.py                     # 分析今日数据
  python score.py --date 20260326     # 指定日期

依赖：
  concept_weekly.db 中已有当日 qs_ebk_stocks + qs_trend_sectors（先跑 collect.py）
------------------------------------------------------------
"""

import os, re, struct, time, glob, sys, sqlite3, argparse, warnings
from datetime import date, datetime
from pathlib import Path

# 强制行缓冲，保证管道/重定向下 print 立即可见
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
else:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, line_buffering=True)

import requests
import numpy as np
import pandas as pd

from fund_strategies import (fund_score_detail, surprise_score_detail,
                             fund_score_combo, parse_strategies, get_strategy_label,
                             STRATEGIES, DEFAULT_STRATEGY)

warnings.filterwarnings("ignore")

# ── 评分权重配置（修改此处即全局生效）────────────────────
# 旧模式：技术50% + 基本面50% 加权评分
# TECH_WEIGHT    = 0.5    # 技术面权重（技术评分 × 此值）
# FUND_WEIGHT    = 0.5    # 基本面权重（基本面评分 × 此值）
# 新模式：基本面100% + 技术过滤（高位不放量）
TECH_WEIGHT    = 0.0    # 技术面权重（新模式下设为0，技术面改为过滤条件）
FUND_WEIGHT    = 1.0    # 基本面权重（100%）
# 注意：新模式下 TECH_WEIGHT=0, FUND_WEIGHT=1.0，等价于 --skip-tech
HOT_WEIGHT     = 0.7    # 评分基础分权重（综合分 × 此值 + 热度加成）
HOT_BONUS_MAX  = 30     # 热度加成上限分
FUND_FILTER    = 30     # 基本面评分低于此分数直接剔除（仅精选流程）

# ── 技术过滤配置（回测验证：胜率34.5%→74.2%）────────────
TECH_FILTER_ENABLED   = True    # 是否启用技术过滤
TECH_FILTER_POS_MIN   = 0.70    # 52周位置下限（高位突破）
TECH_FILTER_VOL_MAX   = 1.3     # 5日/20日均量比上限（不放量）

# ── 消息面评分配置 ────────────────────────────────────────
NEWS_FILTER_LEVELS = ["利空", "重大利空"]  # 这些等级直接过滤
NEWS_SCORE_MAX = 10     # 消息面最高贡献10分热度分
NEWS_CACHE_HOURS = 24   # 消息面缓存有效期（小时）

# ── 路径配置 ─────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_FILE    = os.path.join(BASE_DIR, "..", "db", "concept_weekly.db")
# 从集中配置文件读取通达信路径（修改 qs_config.py 即可适配不同环境）
try:
    from qs_config import TDX_DIR, TDX_CW_DIR
except ImportError:
    TDX_DIR    = r"C:\TongDaXin\vipdoc"             # 默认示例，请修改 qs_config.py
    TDX_CW_DIR = r"C:\TongDaXin\vipdoc\cw"           # 财务 zip 目录
LOOKBACK   = 250
HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
               "Referer": "https://www.eastmoney.com/"}


def check_tdx_freshness() -> str | None:
    """
    检查本地 TDX 财务 zip 的报告期，返回最新有效 zip 的报告期字符串（如 '20260331'）。
    无有效文件则返回 None。
    """
    import glob as _glob
    pattern = os.path.join(TDX_CW_DIR, "gpcw*.zip")
    all_files = sorted([f for f in _glob.glob(pattern)
                        if os.path.getsize(f) >= 10 * 1024], reverse=True)
    if not all_files:
        return None
    # 取文件名中的报告期
    basename = os.path.basename(all_files[0])
    period = basename.replace("gpcw", "").replace(".zip", "")
    return period if period else None


# ══════════════════════════════════════════════════════════
# 1. 板块筛选候选池
# ══════════════════════════════════════════════════════════
def get_candidates(conn: sqlite3.Connection, date_str: str) -> list[dict]:
    """
    从 qs_ebk_stocks 与 qs_trend_sectors 关联，返回命中热门板块的个股列表
    每个元素：{code, name, market, close, chg_pct, pe_ttm, mcap, matched_sectors, hot_score}
    """
    # 取当日热门板块
    cur = conn.execute(
        "SELECT sector_name, sector_rank, ratio FROM qs_trend_sectors WHERE date=? ORDER BY sector_rank",
        (date_str,)
    )
    hot_sectors = {r[0]: {"rank": r[1], "ratio": r[2]} for r in cur.fetchall()}
    if not hot_sectors:
        print(f"  [WARN] qs_trend_sectors 无 {date_str} 数据，请先运行 collect.py")
        return []

    # 取当日 qs_ebk_stocks
    cur = conn.execute(
        "SELECT code, name, market, close, chg_pct, pe_ttm, mcap FROM qs_ebk_stocks WHERE date=?",
        (date_str,)
    )
    stocks = [{"code": r[0], "name": r[1], "market": r[2],
               "close": r[3], "chg_pct": r[4], "pe_ttm": r[5], "mcap": r[6]}
              for r in cur.fetchall()]
    if not stocks:
        print(f"  [WARN] qs_ebk_stocks 无 {date_str} 数据，请先运行 collect.py")
        return []

    # 关联板块
    candidates = []
    for s in stocks:
        cur2 = conn.execute(
            "SELECT sec.sector_name FROM t_sector_stock ss "
            "JOIN t_sector sec ON sec.sector_code=ss.sector_code "
            "WHERE ss.stock_code=?",
            (s["code"],)
        )
        all_secs = [r[0] for r in cur2.fetchall()]
        hit = [sec for sec in all_secs if sec in hot_sectors]
        if not hit:
            continue

        avg_ratio = sum(hot_sectors[sec]["ratio"] for sec in hit) / len(hit)
        avg_rank  = sum(hot_sectors[sec]["rank"]  for sec in hit) / len(hit)
        hot_score = avg_ratio * (31 - avg_rank) / 30

        candidates.append({
            **s,
            "matched_sectors": " / ".join(hit),
            "sector_count":    len(hit),
            "avg_ratio":       round(avg_ratio, 2),
            "avg_rank":        round(avg_rank, 1),
            "hot_score":       round(hot_score, 2),
        })

    return candidates


# ══════════════════════════════════════════════════════════
# 1.5 消息面评分读取（从缓存）
# ══════════════════════════════════════════════════════════
def get_news_sentiment(conn: sqlite3.Connection, code: str) -> dict:
    """
    从缓存读取消息面评分
    返回: {"score": float, "level": str, "summary": str, "is_filtered": bool}
    """
    row = conn.execute(
        "SELECT score, level, summary, is_filtered, fetch_status "
        "FROM qs_news_sentiment WHERE code=? AND cached_at > datetime('now', '-{} hours')".format(NEWS_CACHE_HOURS),
        (code,)
    ).fetchone()
    
    if row:
        return {
            "score": row[0] or 50.0,
            "level": row[1] or "中性",
            "summary": row[2] or "",
            "is_filtered": bool(row[3]),
            "fetch_status": row[4] or "unknown"
        }
    
    # 无缓存，返回中性
    return {
        "score": 50.0,
        "level": "中性",
        "summary": "无缓存数据",
        "is_filtered": False,
        "fetch_status": "missing"
    }


def calc_news_heat_score(news_score: float) -> float:
    """
    将消息面评分(0-100)映射到热度分(0-10)
    利空股票已在过滤阶段处理，此处只处理中性及以上
    """
    # 映射规则
    if news_score >= 80:      # 重大利好
        return 10.0
    elif news_score >= 65:    # 利好
        return 8.0
    elif news_score >= 55:    # 偏利好
        return 5.0
    elif news_score >= 45:    # 中性
        return 2.0
    else:                     # 偏利空及以下（理论上已被过滤）
        return 0.0


# ══════════════════════════════════════════════════════════
# 2. 本地 K 线读取
# ══════════════════════════════════════════════════════════
def read_tdx_day(market: str, code: str, lookback: int = LOOKBACK):
    path = Path(TDX_DIR) / market / "lday" / f"{market}{code}.day"
    if not path.exists():
        return None
    data = []
    file_size    = path.stat().st_size
    record_count = file_size // 32
    with open(path, "rb") as f:
        start = max(0, record_count - lookback)
        f.seek(start * 32)
        for _ in range(record_count - start):
            raw = f.read(32)
            if len(raw) < 32:
                break
            d, o, h, l, c, amt, vol, _ = struct.unpack("<IIIIIfII", raw)
            try:
                dt = datetime.strptime(str(d), "%Y%m%d")
            except Exception:
                continue
            data.append({"date": dt, "open": o/100, "high": h/100,
                          "low": l/100, "close": c/100,
                          "amount": amt, "volume": vol})
    if not data:
        return None
    return pd.DataFrame(data).set_index("date").sort_index()


# ══════════════════════════════════════════════════════════
# 3. 技术指标 & 打分（满分100）
# ══════════════════════════════════════════════════════════
def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    c, v = df["close"], df["volume"]
    df["ma5"]   = c.rolling(5).mean()
    df["ma10"]  = c.rolling(10).mean()
    df["ma20"]  = c.rolling(20).mean()
    df["ma60"]  = c.rolling(60).mean()
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    df["dif"]   = ema12 - ema26
    df["dea"]   = df["dif"].ewm(span=9, adjust=False).mean()
    df["macd"]  = 2 * (df["dif"] - df["dea"])
    delta = c.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi14"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    low_min  = df["low"].rolling(9).min()
    high_max = df["high"].rolling(9).max()
    rsv = (c - low_min) / (high_max - low_min + 1e-9) * 100
    df["k"] = rsv.ewm(com=2, adjust=False).mean()
    df["d"] = df["k"].ewm(com=2, adjust=False).mean()
    df["j"] = 3 * df["k"] - 2 * df["d"]
    mid = c.rolling(20).mean()
    std = c.rolling(20).std()
    df["boll_up"]  = mid + 2 * std
    df["boll_mid"] = mid
    df["vol_ratio"] = v / v.rolling(5).mean().shift(1)
    return df


def tech_score(df) -> tuple[int, list[str]]:
    if df is None or len(df) < 60:
        return 0, []
    df   = calc_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    score, sigs = 0, []

    # 趋势 40分
    if last["ma5"] > last["ma10"] > last["ma20"] > last["ma60"]:
        score += 20; sigs.append("多头排列")  # +5
    elif last["ma5"] > last["ma20"] > last["ma60"]:
        score += 12;  sigs.append("中期多头")  # +4
    if last["close"] > last["ma20"]:
        score += 10; sigs.append("站上MA20")  # +2
    slope = (last["ma60"] - df["ma60"].iloc[-10]) / (df["ma60"].iloc[-10] + 1e-9)
    if slope > 0.01:  score += 8; sigs.append("60线向上")  # +1
    elif slope > 0:   score += 4  # +1

    # 位置 15分
    high_52w = df["high"].tail(250).max()
    low_52w  = df["low"].tail(250).min()
    pos = (last["close"] - low_52w) / (high_52w - low_52w + 1e-9)
    if   0.3 < pos < 0.7:    score += 8; sigs.append(f"位置适中({pos:.0%})")   # -7
    elif 0.7 <= pos < 0.85:  score += 4;  sigs.append(f"相对高位({pos:.0%})") # -4
    elif pos >= 0.85:         score += 1;  sigs.append(f"高位注意({pos:.0%})")  # -1
    else:                     score += 3;  sigs.append(f"低位({pos:.0%})")     # -2
    if last["boll_mid"] < last["close"] < last["boll_up"] * 0.98:
        score += 7; sigs.append("布林中上轨")  # -3

    # 动量 25分
    if prev["dif"] < prev["dea"] and last["dif"] > last["dea"]:
        score += 15; sigs.append("MACD金叉")
    elif last["dif"] > 0 and last["dif"] > last["dea"]:
        score += 12; sigs.append("MACD多头")
    if   50 < last["rsi14"] < 70:  score += 8; sigs.append(f"RSI强势({last['rsi14']:.0f})")
    elif last["rsi14"] >= 70:       score += 3; sigs.append(f"RSI偏热({last['rsi14']:.0f})")
    elif 40 < last["rsi14"] <= 50:  score += 4
    if 20 < last["j"] < 80 and last["k"] > last["d"]:
        score += 5; sigs.append("KDJ多头")

    # 量能 20分
    avg5  = df["volume"].tail(5).mean()
    avg20 = df["volume"].tail(20).mean()
    if   avg5 > avg20 * 1.5:  score += 12; sigs.append("近期放量")
    elif avg5 > avg20 * 1.2:  score += 8;  sigs.append("量能扩大")
    elif avg5 > avg20:         score += 4
    vr = last["vol_ratio"]
    if   1.5 < vr < 5:  score += 8; sigs.append(f"量比{vr:.1f}x")
    elif vr >= 5:         score += 4; sigs.append(f"超量{vr:.1f}x")

    return min(score, 100), sigs


# ══════════════════════════════════════════════════════════
# 3.5 技术过滤（回测验证：高位+不放量，胜率34.5%→74.2%）
# ══════════════════════════════════════════════════════════
def tech_filter(df: pd.DataFrame, pos_min: float = TECH_FILTER_POS_MIN,
                vol_max: float = TECH_FILTER_VOL_MAX) -> tuple[bool, dict]:
    """
    技术过滤：检查K线数据是否满足"高位+不放量"条件（保留满足条件的）。
    返回 (passed, info_dict)

    过滤条件（回测验证，保留高位+缩量）：
      1. 52周位置 >= pos_min（默认70%）：确认已突破到高位
      2. 5日均量/20日均量 < vol_max（默认1.3）：不放量冲高

    info_dict 包含各指标值，供报告和调试使用。
    """
    info = {"passed": False, "pos_pct": None, "vol_ratio": None}

    if df is None or len(df) < 30:
        return False, info

    df = calc_indicators(df)
    last = df.iloc[-1]

    # 52周位置
    high_52w = df["high"].tail(250).max()
    low_52w  = df["low"].tail(250).min()
    pos_pct = (last["close"] - low_52w) / (high_52w - low_52w + 1e-9)
    info["pos_pct"] = round(pos_pct, 4)

    # 量比：5日均量 / 20日均量
    avg5  = df["volume"].tail(5).mean()
    avg20 = df["volume"].tail(20).mean()
    vol_ratio = avg5 / (avg20 + 1e-9)
    info["vol_ratio"] = round(vol_ratio, 4)

    passed = (pos_pct >= pos_min) and (vol_ratio < vol_max)
    info["passed"] = passed
    return passed, info


# ══════════════════════════════════════════════════════════
# 4. 财务数据（本地优先 + 超预期缓存）
# ══════════════════════════════════════════════════════════
# 统一从 collect 模块 import，避免重复维护
from collect import (
    fetch_local_finance,       # 本地TDX基础财务读取
    get_surprise_from_cache,   # 超预期缓存读取
    save_surprise_cache,       # 超预期缓存写入
    fetch_surprise_data,       # 网络获取超预期数据
    _load_fin_zip,             # zip加载（内存缓存）
    get_quick_report_from_cache,   # 快报/预告缓存读取
    save_quick_report_cache,       # 快报/预告缓存写入
    fetch_quick_report_data,       # 网络获取快报/预告数据
)


def _safe_float(row, idx):
    try:
        v = row.iloc[idx] if hasattr(row, "iloc") else row[idx]
        f = float(v)
        return f if not (f != f) else None   # NaN check
    except Exception:
        return None


def get_finance_batch(conn: sqlite3.Connection, stocks: list[dict],
                      require_surprise: bool = False, require_quick_report: bool = False) -> dict:
    """
    批量获取财务数据：
    - 基础财务：实时读本地 TDX zip（无缓存）
    - 超预期数据：优先读缓存，缓存缺失且 require_surprise=True 则网络获取并写缓存
    - 快报/预告：优先读缓存，缓存缺失且 require_quick_report=True 则网络获取并写缓存

    返回: {code: finance_dict}
    """
    result = {}
    need_surprise = []   # 需要网络补充超预期数据的股票
    need_quick = []      # 需要网络补充快报/预告数据的股票

    for s in stocks:
        code = s["code"]
        # 基础财务：实时本地TDX
        fin = fetch_local_finance(code)
        if fin is None:
            fin = {"eps": None, "roe": None, "revenue_yoy": None,
                   "profit_yoy": None, "industry_type": "", "report_period": "", "fin_source": "none"}

        # 超预期：先从缓存读
        surprise = get_surprise_from_cache(conn, code)
        if surprise is None:
            need_surprise.append(s)
            surprise = {"expect_yoy": None, "ttm_yoy": None, "org_num": None, "diff": None}
            cache_hit = False
        else:
            cache_hit = True

        # 快报/预告：先从缓存读
        qr = get_quick_report_from_cache(conn, code)
        if qr is None:
            need_quick.append(s)
            qr = {}
            qr_cache_hit = False
        else:
            qr_cache_hit = True

        result[code] = {**fin, **surprise, **qr, "_cache_hit": cache_hit}

    # 补充未命中超预期缓存的股票（score阶段：require_surprise 控制是否走网络）
    if need_surprise and require_surprise:
        print(f"    [超预期] {len(need_surprise)} 只缓存未命中，网络补充中...")
        for s in need_surprise:
            code = s["code"]
            sp = fetch_surprise_data(code)
            save_surprise_cache(conn, code, sp)
            result[code].update({k: sp.get(k) for k in ("expect_yoy", "ttm_yoy", "org_num", "diff")})
    elif need_surprise:
        print(f"    [超预期] {len(need_surprise)} 只缓存未命中（非 surprise 策略，跳过网络获取）")

    # 补充未命中快报/预告缓存的股票
    if need_quick and require_quick_report:
        print(f"    [快报/预告] {len(need_quick)} 只缓存未命中，网络补充中...")
        qr_ok = 0
        for s in need_quick:
            code = s["code"]
            qd = fetch_quick_report_data(code)
            time.sleep(0.15)
            if qd.get("report_type") is not None:
                save_quick_report_cache(conn, code, qd)
                qr_ok += 1
            result[code].update({k: qd.get(k) for k in
                ("report_type", "report_period", "ann_date", "ttm_yoy",
                 "n_income", "net_profit_mid", "p_change_min", "p_change_max", "summary")})
        print(f"    [快报/预告] 获取完成：{qr_ok}/{len(need_quick)} 只有数据")
    elif need_quick:
        print(f"    [快报/预告] {len(need_quick)} 只缓存未命中（未启用，跳过）")

    return result


# 保留兼容接口
def get_finance(code: str, market: str, conn: sqlite3.Connection = None) -> dict:
    """单只股票财务数据（兼容旧调用），内部走本地TDX"""
    fin = fetch_local_finance(code)
    if fin is not None:
        return fin
    return {"eps": None, "roe": None, "revenue_yoy": None,
            "profit_yoy": None, "industry_type": "", "report_period": "", "fin_source": "none"}





# ══════════════════════════════════════════════════════════
# 5. 基本面打分（已移至 fund_strategies.py，此处直接 import）
# ══════════════════════════════════════════════════════════
# fund_score(pe, mcap, profit_yoy, revenue_yoy, roe, strategy="classic")
# strategy 可选：classic（稳健价值型）/ growth（牛市成长型）


# ══════════════════════════════════════════════════════════
# 6. 操作建议生成
# ══════════════════════════════════════════════════════════
def gen_action(total_score: float, tech_s: int, rsi14=None, pos=None) -> dict:
    if total_score >= 75:
        action, pos_pct = "积极买入", "15-20%"
    elif total_score >= 65:
        action, pos_pct = "关注买入", "10-15%"
    elif total_score >= 55:
        action, pos_pct = "逢低关注", "5-10%"
    else:
        action, pos_pct = "观望", "0%"
    return {"action": action, "position_pct": pos_pct}


# ══════════════════════════════════════════════════════════
# 7. 写入 qs_picks 表
# ══════════════════════════════════════════════════════════
DDL_PICKS = """
CREATE TABLE IF NOT EXISTS qs_picks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT NOT NULL,
    strategy     TEXT NOT NULL DEFAULT 'classic',
    rank_no      INTEGER,
    code         TEXT,
    name         TEXT,
    industry     TEXT,
    report_period TEXT,
    close        REAL,
    chg_pct      TEXT,
    total_score  REAL,
    tech_score   INTEGER,
    fund_score   INTEGER,
    pe_ttm       TEXT,
    mcap         TEXT,
    profit_yoy   TEXT,
    revenue_yoy  TEXT,
    roe          TEXT,
    eps          REAL,
    tech_sigs    TEXT,
    fund_sigs    TEXT,
    matched_sectors TEXT,
    sector_count INTEGER,
    hot_score    REAL,
    final_score  REAL,
    news_score   REAL,        -- 消息面原始评分 0-100
    news_level   TEXT,        -- 消息面等级
    news_summary TEXT,        -- 消息面摘要
    sector_heat  REAL,        -- 板块热度分
    news_heat    REAL,        -- 消息面热度分
    action       TEXT,
    buy_range    TEXT,
    stop_loss    TEXT,
    target       TEXT,
    position_pct TEXT,
    fin_source   TEXT,
    created_at   TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(date, code, strategy)
);
CREATE INDEX IF NOT EXISTS idx_qs_picks_date ON qs_picks(date);
CREATE INDEX IF NOT EXISTS idx_qs_picks_date_strategy ON qs_picks(date, strategy);
"""


def save_picks(conn: sqlite3.Connection, date_str: str, results: list[dict],
                fund_strategy: str = "classic"):
    conn.executescript(DDL_PICKS)

    # 自动补齐旧表缺失的新字段（ALTER TABLE IF NOT EXISTS 不支持，逐一尝试）
    new_cols = [
        ("strategy",            "TEXT DEFAULT 'classic'"),
        ("matched_sectors", "TEXT"),
        ("sector_count",    "INTEGER"),
        ("hot_score",       "REAL"),
        ("final_score",     "REAL"),
        ("fin_source",      "TEXT"),
        ("news_score",      "REAL"),
        ("news_level",      "TEXT"),
        ("news_summary",    "TEXT"),
        ("sector_heat",     "REAL"),
        ("news_heat",       "REAL"),
        ("tech_filter_pos",      "REAL"),     # 52周位置百分比
        ("tech_filter_vol",      "REAL"),     # 5日/20日均量比
        ("tech_filter_passed",   "INTEGER"),  # 技术过滤是否通过（0/1）
    ]
    for col, typ in new_cols:
        try:
            conn.execute(f"ALTER TABLE qs_picks ADD COLUMN {col} {typ}")
            conn.commit()
        except Exception:
            pass   # 列已存在，忽略

    # 迁移旧数据：如果 strategy 列刚加且存在旧数据，回填默认值
    try:
        row = conn.execute("SELECT COUNT(*) FROM qs_picks WHERE strategy IS NULL OR strategy=''").fetchone()
        if row and row[0] > 0:
            conn.execute("UPDATE qs_picks SET strategy='classic' WHERE strategy IS NULL OR strategy=''")
            conn.commit()
    except Exception:
        pass

    cur = conn.cursor()
    cur.execute("DELETE FROM qs_picks WHERE date=? AND strategy=?", (date_str, fund_strategy))

    for i, r in enumerate(results, 1):
        adv = gen_action(r["total_score"], r["tech_score"])
        close = r.get("close") or 0
        buy_lo = round(close * 0.97, 2) if close else 0
        buy_hi = round(close * 1.02, 2) if close else 0
        stop   = round(close * 0.93, 2) if close else 0
        target = round(close * 1.15, 2) if close else 0

        def sf(v):
            if v is None: return "-"
            try: return f"{float(v):.2f}"
            except: return str(v)

        # surprise_meta JSON序列化
        import json
        surprise_meta = r.get("surprise_meta")
        surprise_meta_str = json.dumps(surprise_meta, ensure_ascii=False) if surprise_meta else None
        
        cur.execute("""
            INSERT INTO qs_picks
            (date,strategy,rank_no,code,name,industry,report_period,close,chg_pct,
             total_score,tech_score,fund_score,pe_ttm,mcap,profit_yoy,revenue_yoy,
             roe,eps,tech_sigs,fund_sigs,fund_reasons,matched_sectors,sector_count,hot_score,final_score,
             news_score,news_level,news_summary,sector_heat,news_heat,
             action,buy_range,stop_loss,target,position_pct,fin_source,surprise_meta,
             tech_filter_pos,tech_filter_vol,tech_filter_passed)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            date_str, fund_strategy, i,
            r.get("code",""), r.get("name",""),
            r.get("industry",""), r.get("report_period",""),
            r.get("close"), sf(r.get("chg_pct")),
            round(r["total_score"], 1), r["tech_score"], r["fund_score"],
            sf(r.get("pe_ttm")), sf(r.get("mcap")),
            sf(r.get("profit_yoy")), sf(r.get("revenue_yoy")),
            sf(r.get("roe")), r.get("eps"),
            " | ".join(r.get("tech_sigs", [])),
            " | ".join(r.get("fund_sigs", [])),
            " | ".join(r.get("fund_reasons", [])),
            r.get("matched_sectors",""), r.get("sector_count", 0),
            r.get("hot_score", 0), r.get("final_score", r["total_score"]),
            r.get("news_score"), r.get("news_level"), r.get("news_summary"),
            r.get("sector_heat"), r.get("news_heat"),
            adv["action"],
            f"{buy_lo}-{buy_hi}", sf(stop), sf(target),
            adv["position_pct"], r.get("fin_source",""),
            surprise_meta_str,
            r.get("tech_filter_pos"), r.get("tech_filter_vol"),
            1 if r.get("tech_filter_passed") else 0,
        ))

    conn.commit()
    print(f"  [qs_picks] 写入 {len(results)} 条  (date={date_str})")


# ══════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════
def run(date_str: str = None, fund_strategy: str = DEFAULT_STRATEGY,
        skip_tech: bool = False, surprise_only: bool = False,
        no_tech_filter: bool = False, surprise_mode: str = "auto",
        qdiff_mode: str = "quarter") -> list[dict]:
    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")
    compact  = date_str.replace("-", "")
    dash_str = f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"

    strat_label = get_strategy_label(fund_strategy)
    tech_filter_enabled = TECH_FILTER_ENABLED and not no_tech_filter

    if no_tech_filter:
        # 恢复旧技术权重模式
        _tech_w = 0.5
        _fund_w = 0.5
        mode_label = "技术+基本面模式"
    elif tech_filter_enabled:
        _tech_w = 0.0
        _fund_w = 1.0
        mode_label = f"基本面+技术过滤模式(pos>={int(TECH_FILTER_POS_MIN*100)}%+vol<{TECH_FILTER_VOL_MAX})"
    elif skip_tech:
        _tech_w = 0.0
        _fund_w = 1.0
        mode_label = "纯基本面模式"
    else:
        _tech_w = TECH_WEIGHT
        _fund_w = FUND_WEIGHT
        mode_label = "技术+基本面模式"
    
    # surprise_only 表示纯 surprise 策略（不限TOP数量）
    if surprise_only:
        mode_label += " | 超预期命中（不限TOP）"
    print("=" * 58)
    print(f"  score.py  分析日期: {dash_str}")
    print(f"  基本面策略: {strat_label}  |  {mode_label}")
    print("=" * 58)

    conn = sqlite3.connect(DB_FILE)

    # ── Step 1: 板块筛选候选池 ────────────────────────────
    print(f"\n[1] 板块筛选候选池...")
    candidates = get_candidates(conn, dash_str)
    if not candidates:
        conn.close()
        return []
    print(f"    命中 {len(candidates)} 只候选股")

    # ── Step 2: K线读取 ───────────────────────────────────
    # 需要K线的场景：技术过滤(tech_filter_enabled) 或 旧技术权重(!skip_tech && no_tech_filter)
    total_cand = len(candidates)
    need_kline = tech_filter_enabled or (not skip_tech and no_tech_filter) or (not skip_tech and not TECH_FILTER_ENABLED)

    if not need_kline:
        print(f"\n[2] 跳过K线读取（纯基本面模式）")
        # 不读K线，直接将候选池转为scored（tech_score=0）
        scored = []
        for s in candidates:
            scored.append({
                **s,
                "tech_score":  0,
                "tech_sigs":   [],
                "close_local": s.get("close", 0),
                "last_date":   dash_str,
                "df":          None,
            })
        print(f"    候选股: {len(scored)} 只（无K线过滤）", flush=True)
    else:
        print(f"\n[2] K线读取  (共 {total_cand} 只)...", flush=True)
        scored = []
        for idx, s in enumerate(candidates):
            df = read_tdx_day(s["market"], s["code"])
            if df is None or len(df) < 30:
                continue
            # 仍计算技术评分（供参考），但不参与综合评分
            ts, tsigs = tech_score(df)
            last_close = df["close"].iloc[-1]
            last_date  = df.index[-1].strftime("%Y-%m-%d")
            scored.append({
                **s,
                "tech_score":  ts,
                "tech_sigs":   tsigs,
                "close_local": last_close,
                "last_date":   last_date,
                "df":          df,
            })
            if (idx + 1) % 5 == 0 or idx == 0 or idx == total_cand - 1:
                print(f"    [{idx+1}/{total_cand}] K线 {s['market']}{s['code']}"
                      f" | 有效{len(scored)}只", end='\r', flush=True)
        print(f"    [{total_cand}/{total_cand}] K线完成 | 有效{len(scored)}/{total_cand}只", flush=True)

    # ── Step 3: 财务数据（本地TDX实时读取 + 超预期缓存）───
    total_fin = len(scored)
    print(f"\n[3] 财务数据获取（本地TDX + 超预期缓存）  (共 {total_fin} 只)...")
    
    # 判断是否需要超预期数据
    parsed_strats = parse_strategies(fund_strategy)
    require_surprise = "surprise" in parsed_strats

    # 判断是否需要快报/预告数据（暂默认不启用，后续按需打开）
    require_quick_report = False

    # 批量获取财务数据
    fin_data = get_finance_batch(conn, scored, require_surprise=require_surprise,
                                 require_quick_report=require_quick_report)
    
    # 统计来源
    local_count  = sum(1 for s in scored if fin_data.get(s["code"], {}).get("fin_source") == "local")
    failed_count = sum(1 for s in scored if fin_data.get(s["code"], {}).get("fin_source") in ("none", "", None))
    surprise_ok  = sum(1 for s in scored if fin_data.get(s["code"], {}).get("expect_yoy") is not None)
    qr_ok        = sum(1 for s in scored if fin_data.get(s["code"], {}).get("report_type") is not None)
    
    # 合并数据到 scored
    for s in scored:
        code = s["code"]
        fin = fin_data.get(code, {})
        s.update({
            "profit_yoy":    fin.get("profit_yoy"),
            "revenue_yoy":   fin.get("revenue_yoy"),
            "industry":      fin.get("industry", fin.get("industry_type", "")),
            "report_period": fin.get("report_period", ""),
            "eps":           fin.get("eps"),
            "roe":           fin.get("roe"),
            "fin_source":    fin.get("fin_source", fin.get("source", "")),
            "expect_yoy":    fin.get("expect_yoy"),
            "ttm_yoy":       fin.get("ttm_yoy"),
            "org_num":       fin.get("org_num"),
            # 快报/预告字段（独立于超预期缓存）
            "qr_type":       fin.get("report_type"),
            "qr_period":     fin.get("report_period"),
            "qr_ann_date":   fin.get("ann_date"),
            "qr_ttm_yoy":    fin.get("ttm_yoy") if fin.get("report_type") else None,
            "qr_n_income":   fin.get("n_income"),
            "qr_net_mid":    fin.get("net_profit_mid"),
            "qr_p_min":      fin.get("p_change_min"),
            "qr_p_max":      fin.get("p_change_max"),
            "qr_summary":    fin.get("summary"),
        })

    qr_label = f"  |  快报/预告: {qr_ok} 只" if qr_ok else ""
    print(f"    本地TDX: {local_count} 只  |  失败: {failed_count} 只  |  超预期数据: {surprise_ok} 只{qr_label}", flush=True)

    # ── Step 3.5: Surprise auto 模式解析 ───────────────────
    # surprise_mode="auto" 时，从 qs_surprise_window 缓存读取每只股票的自动判定结果
    # 其他模式（forward/actual）直接使用指定值
    sw_cache = {}   # code -> auto_mode
    is_auto = (surprise_mode == "auto")
    if is_auto:
        from collect import get_surprise_window_from_cache
        auto_forward = 0
        auto_actual = 0
        auto_unknown = 0
        latest_notice_period = None   # 用于 TDX 新鲜度检查
        for s in scored:
            code = s["code"]
            try:
                sw = get_surprise_window_from_cache(conn, code)
            except Exception:
                sw = None
            if sw and sw.get("auto_mode"):
                mode = sw["auto_mode"]
                sw_cache[code] = mode
                if mode == "forward":
                    auto_forward += 1
                else:
                    auto_actual += 1
                # 收集最新公告期
                if sw.get("notice_period"):
                    np_val = sw["notice_period"].replace("-", "")[:8]
                    if latest_notice_period is None or np_val > latest_notice_period:
                        latest_notice_period = np_val
            else:
                # 无缓存数据，默认 actual（保守）
                sw_cache[code] = "actual"
                auto_unknown += 1
        print(f"    [auto] Surprise窗口: forward={auto_forward} actual={auto_actual} 无数据={auto_unknown}", flush=True)

        # actual 模式下检查 TDX 本地数据新鲜度
        if (auto_actual > 0 or auto_unknown > 0) and latest_notice_period:
            tdx_period = check_tdx_freshness()
            if tdx_period and tdx_period < latest_notice_period:
                print(f"    [WARN] TDX本地财报={tdx_period} < 最新公告期={latest_notice_period}")
                print(f"           actual模式下本地数据可能滞后，建议更新通达信数据后重跑")

    # ── Step 4: 综合评分 ──────────────────────────────────
    if tech_filter_enabled:
        print(f"\n[4] 综合评分...  (策略: {strat_label})")
        print(f"    评分权重：基本面100%，基本面<{FUND_FILTER}分直接过滤")
        print(f"    技术过滤：52周位置>={int(TECH_FILTER_POS_MIN*100)}% + 量比<{TECH_FILTER_VOL_MAX}")
    elif skip_tech:
        print(f"\n[4] 综合评分...  (纯基本面模式 | 策略: {strat_label})")
        print(f"    评分权重：基本面100%，基本面<{FUND_FILTER}分直接过滤")
    else:
        print(f"\n[4] 综合评分...  (策略: {strat_label})")
        print(f"    评分权重：技术:{int(_tech_w*100)}% + 基本面:{int(_fund_w*100)}%，基本面<{FUND_FILTER}分直接过滤")
    print(f"    消息面：利空/重大利空直接过滤，利好加分（板块热度+消息面 ≤ {HOT_BONUS_MAX}分）")
    results = []
    filtered_count = 0
    news_filtered_count = 0

    # surprise 策略（含组合）：超预期数据已在 Step3 合并到 scored 中
    has_surprise_strat = "surprise" in parsed_strats
    if has_surprise_strat:
        has_surprise = sum(1 for s in scored if s.get("expect_yoy") is not None)
        sm_label = "auto(逐股判定)" if is_auto else surprise_mode
        print(f"    [surprise] 超预期数据已就绪: {has_surprise}/{len(scored)} 只 | 模式: {sm_label}")

    # single_line 策略：需要TTM环比 + 季度预期
    has_sl_strat = "single_line" in parsed_strats
    sl_ttm_pair_cache = {}  # {code: (ttm_yoy, prev_ttm_yoy)}
    sl_qc_cache = {}        # {code: quarterly_consensus}
    if has_sl_strat:
        print(f"    [single_line] 正在采集TTM环比+季度预期...", flush=True)
        from fund_strategies import calc_ttm_profit_growth_pair, fetch_quarterly_consensus
        from collect import get_quarterly_consensus_from_cache, save_quarterly_consensus_cache
        qc_cache_hit = 0
        qc_fetch = 0
        ttm_pair_ok = 0
        for s in scored:
            code = s["code"]
            # TTM环比：一次读取当期+上期
            try:
                ttm_cur, ttm_prev = calc_ttm_profit_growth_pair(code)
                sl_ttm_pair_cache[code] = (ttm_cur, ttm_prev)
                if ttm_cur is not None and ttm_prev is not None:
                    ttm_pair_ok += 1
            except Exception:
                sl_ttm_pair_cache[code] = (None, None)
            # 季度预期：缓存优先，未命中再网络获取
            qc = get_quarterly_consensus_from_cache(conn, code)
            if qc is not None:
                sl_qc_cache[code] = qc
                qc_cache_hit += 1
            else:
                try:
                    qc = fetch_quarterly_consensus(code)
                    sl_qc_cache[code] = qc
                    save_quarterly_consensus_cache(conn, code, qc)
                    qc_fetch += 1
                    time.sleep(0.15)
                except Exception:
                    sl_qc_cache[code] = {}
        sl_qc_ok = sum(1 for v in sl_qc_cache.values() if v.get("source") == "report_rc")
        print(f"    [single_line] TTM环比: {ttm_pair_ok}/{len(scored)} | 季度预期: {sl_qc_ok}/{len(scored)}"
              f" (缓存:{qc_cache_hit} 网络:{qc_fetch})", flush=True)

    for s in scored:
        pe   = s.get("pe_ttm")
        mcap = s.get("mcap")

        # 确定 per-stock 的 surprise_mode
        if is_auto:
            _sm = sw_cache.get(s["code"], "actual")
        else:
            _sm = surprise_mode

        # 构造 surprise 额外参数（从缓存读取）
        extra_kwargs = {}
        if has_surprise_strat:
            extra_kwargs = {
                "expect_yoy": s.get("expect_yoy"),
                "ttm_yoy":    s.get("ttm_yoy"),
                "org_num":    s.get("org_num"),
                # surprise_mode: forward(前瞻) / actual(验证) / auto(逐股判定)
                "surprise_mode": _sm,
            }
        if has_sl_strat:
            code = s["code"]
            _ttm_cur, _ttm_prev = sl_ttm_pair_cache.get(code, (None, None))
            sl_kwargs = {
                "quarterly_consensus":  sl_qc_cache.get(code, {}),
                "qdiff_mode":          qdiff_mode,
                "surprise_mode":       _sm,
                "ttm_yoy":             _ttm_cur,
                "prev_ttm_yoy":        _ttm_prev,
            }
            # 合并基础财务（TTM模式需要 expect_yoy/ttm_yoy）
            if qdiff_mode == "ttm":
                sl_kwargs["expect_yoy"] = s.get("expect_yoy")
                sl_kwargs["ttm_yoy"] = s.get("ttm_yoy")
                sl_kwargs["org_num"] = s.get("org_num")
            extra_kwargs.update(sl_kwargs)

        fund_detail = fund_score_combo(
            pe, mcap, s.get("profit_yoy"), s.get("revenue_yoy"), s.get("roe"),
            strategy_str=fund_strategy,
            **extra_kwargs
        )
        fs = fund_detail["total"]

        # 基本面评分过滤：低于阈值直接剔除
        if fs < FUND_FILTER:
            filtered_count += 1
            continue

        fsigs = fund_detail["sigs"]

        # ── 消息面评分过滤 ───────────────────────────────────
        news = get_news_sentiment(conn, s["code"])
        if news["level"] in NEWS_FILTER_LEVELS:
            news_filtered_count += 1
            continue  # 跳过利空/重大利空股票

        if skip_tech:
            # 纯基本面：综合分 = 基本面分（满100）
            total = float(fs)
        else:
            total = s["tech_score"] * _tech_w + fs * _fund_w

        # ── 热度分计算：板块热度 + 消息面 ────────────────────
        sector_heat = s["hot_score"] * 2  # 原板块热度映射
        news_heat = calc_news_heat_score(news["score"])  # 消息面映射到0-10
        total_heat = sector_heat + news_heat
        
        final = round(total * HOT_WEIGHT + min(total_heat, HOT_BONUS_MAX), 2)

        s["fund_score"]    = fs
        s["fund_sigs"]     = fsigs
        s["fund_reasons"]  = fund_detail.get("reasons", [])
        s["surprise_meta"] = fund_detail.get("surprise_meta")
        s["sub_scores"]    = fund_detail.get("sub_scores", {})  # 多策略各分项
        s["news_score"]    = news["score"]       # 消息面原始分
        s["news_level"]    = news["level"]       # 消息面等级
        s["news_summary"]  = news["summary"]     # 消息面摘要
        s["sector_heat"]   = round(sector_heat, 1)  # 板块热度分
        s["news_heat"]     = round(news_heat, 1)    # 消息面热度分
        s["total_score"]   = round(total, 1)
        s["final_score"]   = final
        results.append(s)

    if filtered_count > 0:
        print(f"    过滤基本面<{FUND_FILTER}分股票：{filtered_count}只")
    if news_filtered_count > 0:
        print(f"    过滤消息面利空股票：{news_filtered_count}只")

    results.sort(key=lambda x: x["final_score"], reverse=True)

    # ── 技术过滤标识计算（不淘汰，只记录）─────────────────────
    # 评分阶段计算技术过滤指标，写入DB供报告阶段使用
    if tech_filter_enabled:
        filter_pass_count = 0
        for r in results:
            df = r.get("df")
            passed, info = tech_filter(df)
            r["tech_filter_pos"] = info["pos_pct"]
            r["tech_filter_vol"] = info["vol_ratio"]
            r["tech_filter_passed"] = info["passed"]
            if passed:
                filter_pass_count += 1
        print(f"\n[TECH-FILTER] 技术过滤标识计算完成：{filter_pass_count}/{len(results)} 只通过"
              f"（条件：位置>={int(TECH_FILTER_POS_MIN*100)}% + 量比<{TECH_FILTER_VOL_MAX}）")
        print(f"    过滤将在报告阶段执行（report.py）")

    # ── surprise_only 过滤：只保留有超预期信号（diff > 0）的股票 ──
    if surprise_only:
        before = len(results)
        def _has_surprise_signal(r):
            meta = r.get("surprise_meta") or {}
            diff = meta.get("diff")
            return diff is not None and diff > 0
        results = [r for r in results if _has_surprise_signal(r)]
        print(f"\n[SURPRISE-ONLY] 过滤：{before} → {len(results)} 只（仅保留预期差>0的股票）")
        for r in results:
            meta = r.get("surprise_meta") or {}
            diff = meta.get("diff")
            expect = meta.get("expect_yoy")
            ttm    = meta.get("ttm_yoy")
            print(f"    {r['market']}{r['code']} {r.get('name','')[:6]:<6}  "
                  f"预期差={diff:+.1f}%  预期={expect:.0f}%  TTM={ttm:.0f}%"
                  if diff is not None and expect is not None and ttm is not None
                  else f"    {r['market']}{r['code']} {r.get('name','')[:6]}")

    # 打印 TOP10（surprise_only 时打印全部）
    print_n = len(results) if surprise_only else 10
    print(f"\n  {'#':>2}  {'代码':<10} {'名称':<8} {'综合':>5} {'技术':>4} {'基本面':>5} "
          f"{'热度':>5} {'消息':>6} {'板块'}")
    print("  " + "-" * 80)
    for i, r in enumerate(results[:print_n], 1):
        news_indicator = ""
        if r.get("news_level") == "重大利好":
            news_indicator = "++"
        elif r.get("news_level") == "利好":
            news_indicator = "+"
        elif r.get("news_level") == "偏利好":
            news_indicator = "~"
        print(f"  #{i:02d}  {r['market']}{r['code']:<8} {r.get('name','')[:6]:<8} "
              f"{r['final_score']:5.1f} {r['tech_score']:4d} {r['fund_score']:5d} "
              f"{r['hot_score']:5.1f} {news_indicator:>6} {r.get('matched_sectors','')[:30]}")

    # ── Step 5: 写入 qs_picks 表 ─────────────────────────────
    print(f"\n[5] 写入 qs_picks 表...")
    save_picks(conn, dash_str, results, fund_strategy=fund_strategy)

    conn.close()
    print(f"\n[OK] 评分完成，共 {len(results)} 只候选，已存入 qs_picks 表")
    return results


# ══════════════════════════════════════════════════════════
# 命令行入口
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="精选分析与评分")
    parser.add_argument("--date", type=str, default=None,
                        help="指定日期，格式 YYYYMMDD 或 YYYY-MM-DD（默认今日）")
    parser.add_argument("--fund-strategy", type=str, default=DEFAULT_STRATEGY,
                        help=("基本面评分策略，支持逗号组合，如 classic,surprise。"
                              f"可选值：{' / '.join(STRATEGIES.keys())}（默认: {DEFAULT_STRATEGY}）"))
    parser.add_argument("--skip-tech", action="store_true", default=False,
                        help="纯基本面模式：跳过K线读取和技术打分，仅用基本面评分排序")
    parser.add_argument("--no-tech-filter", action="store_true", default=False,
                        help="禁用技术过滤（高位+不放量），恢复为旧评分模式")
    parser.add_argument("--surprise-only", action="store_true", default=False,
                        help=("超预期命中模式：仅保留 surprise_meta.diff>0 的股票，且纯surprise策略时不限TOP数量。"
                              "自动强制使用 surprise 策略"))
    args = parser.parse_args()

    # surprise-only 时强制使用纯 surprise 策略（非组合）
    strat = args.fund_strategy
    if args.surprise_only:
        if strat != "surprise":
            strat = "surprise"
            print(f"[INFO] --surprise-only 已强制使用纯 surprise 策略")

    # 判断是否纯 surprise 策略（用于决定报告是否截断TOP）
    is_pure_surprise = (strat == "surprise")

    run(date_str=args.date, fund_strategy=strat,
        skip_tech=args.skip_tech, surprise_only=is_pure_surprise,
        no_tech_filter=args.no_tech_filter)
