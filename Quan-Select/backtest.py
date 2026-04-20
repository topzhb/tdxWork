"""
backtest.py  —— 个股回测诊断 + 批量回测 + 历史重跑
------------------------------------------------------------
功能：
  1. 单股回测：输入代码 + 基准日期，重现评分，验证后续走势
  2. 批量回测（mode A）：读 qs_picks 某日精选，批量验证胜率
  3. 历史重跑（mode B）：给定日期，从 qs_ebk_stocks 快照重跑完整筛选流程
  4. K线图可视化（前30 + 后30，含目标/止损线）
  5. HTTP 服务（端口 8765）

设计原则：
  - 独立模块：不依赖 score.py 主流程，可单独运行
  - 不主动发起网络请求：财务/超预期/消息面均只读本地+缓存
  - 超预期：读 qs_finance_cache，无缓存则忽略（该维度得0）
  - 消息面：读 qs_picks.news_level 历史记录，有利空则跳过，无加分
  - 热门板块：精确读 qs_trend_sectors 对应日期，无记录则取最近一条并标注

用法：
  python backtest.py --serve                                         # 启动服务
  python backtest.py --code 601991 --date 2026-03-13                 # 单股回测
  python backtest.py --code 601991 --date 2026-03-13 --target 20 --stop 5
  python backtest.py --batch-date 2026-03-25 --window 10 --top-n 20  # 批量回测
  python backtest.py --rerun-date 2026-03-25 --top-n 30              # 历史重跑

价格逻辑：
  买入区间：收盘价 × 0.97 ～ 1.02
  止损位：  收盘价 × (1 - stop_pct/100)    默认 -5%
  目标位：  收盘价 × (1 + target_pct/100)  默认 +15%
------------------------------------------------------------
"""

import os, re, struct, glob, sys, sqlite3, json, argparse, warnings
from datetime import datetime, date, timedelta
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import time

import numpy as np
import pandas as pd
import requests

from fund_strategies import fund_score_detail, STRATEGIES, DEFAULT_STRATEGY
from score import TECH_WEIGHT, FUND_WEIGHT, HOT_WEIGHT, HOT_BONUS_MAX

warnings.filterwarnings("ignore")

# ── 路径配置 ─────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_FILE    = os.path.join(BASE_DIR, "..", "db", "concept_weekly.db")
# 从集中配置文件读取通达信路径（修改 qs_config.py 即可适配不同环境）
try:
    from qs_config import TDX_DIR, TDX_CW_DIR
except ImportError:
    TDX_DIR    = r"C:\TongDaXin\vipdoc"
    TDX_CW_DIR = r"C:\TongDaXin\vipdoc\cw"
LOOKBACK   = 500      # 读取更多K线以支持历史截断
SERVE_PORT = 8765
HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
               "Referer": "https://www.eastmoney.com/"}


# ══════════════════════════════════════════════════════════
# 1. K线读取（全量）
# ══════════════════════════════════════════════════════════
def read_tdx_day_full(market: str, code: str) -> pd.DataFrame | None:
    """读取通达信 .day 文件，返回全量 DataFrame（index=date）"""
    path = Path(TDX_DIR) / market / "lday" / f"{market}{code}.day"
    if not path.exists():
        return None
    data = []
    file_size    = path.stat().st_size
    record_count = file_size // 32
    with open(path, "rb") as f:
        start = max(0, record_count - LOOKBACK)
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


def get_market(code: str) -> str:
    """根据6位代码推断市场（sh/sz）"""
    if code.startswith(("600", "601", "603", "605", "688", "900")):
        return "sh"
    return "sz"


# ══════════════════════════════════════════════════════════
# 2. 技术指标 & 打分（与 score.py 完全一致）
# ══════════════════════════════════════════════════════════
def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    c, v = df["close"], df["volume"]
    df = df.copy()
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


def tech_score_detail(df) -> dict:
    """
    打分，返回详细结果（各维度分数 + 信号）
    """
    if df is None or len(df) < 60:
        return {"total": 0, "trend": 0, "position": 0, "momentum": 0,
                "volume": 0, "sigs": [], "rsi14": None, "pos_pct": None}
    df   = calc_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]

    trend_s, pos_s, mom_s, vol_s = 0, 0, 0, 0
    sigs = []

    # 趋势结构 40分
    if last["ma5"] > last["ma10"] > last["ma20"] > last["ma60"]:
        trend_s += 20; sigs.append("多头排列")
    elif last["ma5"] > last["ma20"] > last["ma60"]:
        trend_s += 12;  sigs.append("中期多头")
    if last["close"] > last["ma20"]:
        trend_s += 10; sigs.append("站上MA20")
    slope = (last["ma60"] - df["ma60"].iloc[-10]) / (df["ma60"].iloc[-10] + 1e-9)
    if slope > 0.01:  trend_s += 8; sigs.append("60线向上")
    elif slope > 0:   trend_s += 4

    # 位置形态 15分
    high_52w = df["high"].tail(250).max()
    low_52w  = df["low"].tail(250).min()
    pos_pct  = (last["close"] - low_52w) / (high_52w - low_52w + 1e-9)
    if   0.3 < pos_pct < 0.7:    pos_s += 8; sigs.append(f"位置适中({pos_pct:.0%})")
    elif 0.7 <= pos_pct < 0.85:  pos_s += 4;  sigs.append(f"相对高位({pos_pct:.0%})")
    elif pos_pct >= 0.85:         pos_s += 1;  sigs.append(f"高位注意({pos_pct:.0%})")
    else:                         pos_s += 3;  sigs.append(f"低位({pos_pct:.0%})")
    if last["boll_mid"] < last["close"] < last["boll_up"] * 0.98:
        pos_s += 7; sigs.append("布林中上轨")

    # 动量指标 25分
    if prev["dif"] < prev["dea"] and last["dif"] > last["dea"]:
        mom_s += 15; sigs.append("MACD金叉")
    elif last["dif"] > 0 and last["dif"] > last["dea"]:
        mom_s += 12; sigs.append("MACD多头")
    rsi14 = last["rsi14"]
    if   50 < rsi14 < 70:  mom_s += 8; sigs.append(f"RSI强势({rsi14:.0f})")
    elif rsi14 >= 70:       mom_s += 3; sigs.append(f"RSI偏热({rsi14:.0f})")
    elif 40 < rsi14 <= 50:  mom_s += 4
    if 20 < last["j"] < 80 and last["k"] > last["d"]:
        mom_s += 5; sigs.append("KDJ多头")

    # 量能评估 20分
    avg5  = df["volume"].tail(5).mean()
    avg20 = df["volume"].tail(20).mean()
    if   avg5 > avg20 * 1.5:  vol_s += 12; sigs.append("近期放量")
    elif avg5 > avg20 * 1.2:  vol_s += 8;  sigs.append("量能扩大")
    elif avg5 > avg20:         vol_s += 4
    vr = last["vol_ratio"]
    if   1.5 < vr < 5:  vol_s += 8; sigs.append(f"量比{vr:.1f}x")
    elif vr >= 5:         vol_s += 4; sigs.append(f"超量{vr:.1f}x")

    total = min(trend_s + pos_s + mom_s + vol_s, 100)
    return {
        "total":    total,
        "trend":    min(trend_s, 40),
        "position": min(pos_s, 15),
        "momentum": min(mom_s, 25),
        "volume":   min(vol_s, 20),
        "sigs":     sigs,
        "rsi14":    round(float(rsi14), 1) if rsi14 == rsi14 else None,
        "pos_pct":  round(float(pos_pct) * 100, 1),
        "vol_ratio": round(float(vr), 2) if vr == vr else None,
    }


# ══════════════════════════════════════════════════════════
# 3. 财务数据（本地优先，与 score.py 一致）
# ══════════════════════════════════════════════════════════
# zip 内存缓存统一走 fund_strategies._load_fin_df，避免同进程重复解压
from fund_strategies import _load_fin_df as _load_fin_zip


def _safe_float(row, idx):
    try:
        v = row.iloc[idx] if hasattr(row, "iloc") else row[idx]
        f = float(v)
        return f if not (f != f) else None
    except Exception:
        return None


def fetch_local_finance(code: str, ref_date: str = None) -> dict | None:
    """
    从本地 zip 读财务。
    ref_date: 'YYYYMMDD'，只取不晚于此日期的 zip（回测时间隔离）
    """
    pattern   = os.path.join(TDX_CW_DIR, "gpcw*.zip")
    all_files = sorted([f for f in glob.glob(pattern)
                        if os.path.getsize(f) >= 10 * 1024], reverse=True)
    if not all_files:
        return None

    # 时间隔离：过滤掉晚于基准日的 zip
    if ref_date:
        compact = ref_date.replace("-", "")
        filtered = []
        for f in all_files:
            m = re.search(r"gpcw(\d{8})\.zip", os.path.basename(f))
            if m and m.group(1) <= compact:
                filtered.append(f)
        all_files = filtered if filtered else all_files[:1]

    for f in all_files[:4]:
        df_try = _load_fin_zip(f)
        if df_try is not None and code in df_try.index:
            df   = df_try
            row  = df.loc[code]
            rp   = os.path.basename(f).replace("gpcw","").replace(".zip","")
            return {
                "eps":           _safe_float(row, 1),
                "roe":           _safe_float(row, 281),
                "revenue_yoy":   _safe_float(row, 183),
                "profit_yoy":    _safe_float(row, 184),
                "industry_type": "",
                "report_period": os.path.basename(f).replace("gpcw","").replace(".zip",""),
                "source":        "local",
            }
    return None


def get_finance(code: str, market: str, ref_date: str = None) -> dict:
    """
    纯本地读取财务数据（不走网络）。
    从通达信本地 zip 文件读取，支持基准日时间隔离。
    """
    local = fetch_local_finance(code, ref_date)
    if local is not None:
        return local
    return {"revenue_yoy": None, "profit_yoy": None, "industry_type": "",
            "eps": None, "roe": None, "report_period": "", "source": "none"}


# ══════════════════════════════════════════════════════════
# 4. 基本面打分（已移至 fund_strategies.py，此处直接 import）
# ══════════════════════════════════════════════════════════
# fund_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe, strategy="classic")
# strategy 可选：classic（稳健价值型）/ growth（牛市成长型）


def gen_action(total_score: float) -> dict:
    if total_score >= 75:
        return {"action": "积极买入", "position_pct": "15-20%"}
    elif total_score >= 65:
        return {"action": "关注买入", "position_pct": "10-15%"}
    elif total_score >= 55:
        return {"action": "逢低关注", "position_pct": "5-10%"}
    else:
        return {"action": "观望",   "position_pct": "0%"}


# ══════════════════════════════════════════════════════════
# 4b. 回测记录持久化
# ══════════════════════════════════════════════════════════

_BT_TABLE = """
CREATE TABLE IF NOT EXISTS qs_batch_backtest (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        TEXT NOT NULL UNIQUE,
    test_date       TEXT NOT NULL,
    mode            TEXT NOT NULL DEFAULT 'batch',
    window          INTEGER NOT NULL,
    top_n           INTEGER,
    strategy        TEXT,
    use_tech        INTEGER DEFAULT 1,
    sector_mode     TEXT DEFAULT 'dual',
    target_pct      REAL DEFAULT 15.0,
    stop_pct        REAL DEFAULT 5.0,
    total           INTEGER DEFAULT 0,
    success         INTEGER DEFAULT 0,
    failure         INTEGER DEFAULT 0,
    pending         INTEGER DEFAULT 0,
    insufficient    INTEGER DEFAULT 0,
    win_rate        REAL,
    avg_return      REAL,
    avg_return_success REAL,
    avg_return_failure  REAL,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS qs_backtest_detail (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        TEXT NOT NULL,
    code            TEXT NOT NULL,
    name            TEXT,
    base_date       TEXT,
    base_close      REAL,
    buy_range       TEXT,
    target_price    REAL,
    stop_price      REAL,
    verdict         TEXT,
    actual_return   REAL,
    max_gain        REAL,
    max_loss        REAL,
    tech_score      REAL,
    fund_score      REAL,
    final_score     REAL,
    hot_score       REAL,
    sectors         TEXT,
    FOREIGN KEY (batch_id) REFERENCES qs_batch_backtest(batch_id)
);

CREATE INDEX IF NOT EXISTS idx_bt_batch ON qs_backtest_detail(batch_id);
CREATE INDEX IF NOT EXISTS idx_bt_detail_date ON qs_backtest_detail(base_date);
"""

# 确保表已创建（模块加载时执行一次）
_conn_init = sqlite3.connect(DB_FILE)
_conn_init.executescript(_BT_TABLE)
_conn_init.close()


def save_batch_result(result: dict, params: dict) -> str:
    """
    将批量回测 / 历史重跑结果写入数据库。

    result: batch_analyze() 或 rerun() 返回的完整 dict
    params: 回测参数
        mode        - 'batch' 或 'rerun'
        strategy    - 策略字符串
        use_tech    - bool
        sector_mode - 'triple'/'dual'/'triple_nofallback'
    返回 batch_id
    """
    summary = result.get("summary", {})
    items   = result.get("items", [])
    counts  = summary.get("counts", {})
    ts      = time.strftime("%Y%m%d_%H%M%S")

    batch_id = f"BT_{ts}"
    mode     = params.get("mode", "batch")

    conn = sqlite3.connect(DB_FILE)
    try:
        conn.execute(
            """INSERT OR REPLACE INTO qs_batch_backtest
               (batch_id, test_date, mode, window, top_n, strategy,
                use_tech, sector_mode, target_pct, stop_pct,
                total, success, failure, pending, insufficient,
                win_rate, avg_return, avg_return_success, avg_return_failure)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (batch_id,
             result.get("date", ""),
             mode,
             summary.get("window", 0),
             params.get("top_n"),
             params.get("strategy", ""),
             1 if params.get("use_tech", True) else 0,
             params.get("sector_mode", "dual"),
             result.get("target_pct", params.get("target_pct", 15.0)),
             result.get("stop_pct", params.get("stop_pct", 5.0)),
             counts.get("total", len(items)),
             counts.get("success", 0),
             counts.get("failure", 0),
             counts.get("pending", 0),
             counts.get("insufficient", 0),
             summary.get("win_rate"),
             summary.get("avg_return"),
             _calc_avg_return(items, "success"),
             _calc_avg_return(items, "failure"),
            )
        )

        # 写入明细
        detail_rows = []
        for it in items:
            v = it.get("verdict", {})
            detail_rows.append((
                batch_id,
                it.get("code", ""),
                it.get("name", ""),
                it.get("actual_base_date", it.get("base_date", "")),
                it.get("base_close"),
                it.get("buy_range", ""),
                it.get("target"),
                it.get("stop"),
                v.get("status", ""),
                v.get("actual_return"),
                v.get("max_gain_pct"),
                v.get("max_loss_pct"),
                it.get("tech_score"),
                it.get("fund_score"),
                it.get("final_score"),
                it.get("hot_score"),
                ",".join(it.get("hit_sectors", [])) if isinstance(it.get("hit_sectors"), list) else "",
            ))

        conn.executemany(
            """INSERT INTO qs_backtest_detail
               (batch_id, code, name, base_date, base_close, buy_range,
                target_price, stop_price, verdict, actual_return,
                max_gain, max_loss, tech_score, fund_score,
                final_score, hot_score, sectors)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            detail_rows,
        )
        conn.commit()
        print(f"  [DB] 回测记录已保存: {batch_id} ({len(detail_rows)} 条明细)", flush=True)
    finally:
        conn.close()

    return batch_id


def _calc_avg_return(items: list, status: str) -> float | None:
    """计算指定 verdict 状态的平均 actual_return"""
    vals = [it["verdict"]["actual_return"]
            for it in items
            if it.get("verdict", {}).get("status") == status
            and isinstance(it["verdict"].get("actual_return"), (int, float))]
    return round(sum(vals) / len(vals), 2) if vals else None


# ══════════════════════════════════════════════════════════
# 5. 超预期缓存读取（优先缓存，缺失时网络获取）
# ══════════════════════════════════════════════════════════
def get_surprise_cache(conn: sqlite3.Connection, code: str, allow_fetch: bool = True) -> dict:
    """
    从 qs_finance_cache 读超预期数据（72小时有效）。
    无缓存或过期 → 若 allow_fetch=True 则网络获取并写入缓存
    """
    row = conn.execute(
        "SELECT expect_yoy, ttm_yoy, org_num, diff FROM qs_finance_cache "
        "WHERE code = ? AND cached_at > datetime('now', '-3 days') LIMIT 1",
        (code,)
    ).fetchone()
    if row:
        return {"expect_yoy": row[0], "ttm_yoy": row[1],
                "org_num": row[2], "diff": row[3]}
    
    # 缓存缺失，网络获取
    if allow_fetch:
        from fund_strategies import fetch_consensus_eps, calc_ttm_profit_growth
        consensus = fetch_consensus_eps(code)
        ttm_yoy = calc_ttm_profit_growth(code)
        
        expect_yoy = consensus.get("expect_yoy")
        org_num = consensus.get("org_num")
        diff = (expect_yoy - ttm_yoy) if (expect_yoy is not None and ttm_yoy is not None) else None
        
        # 无论成功与否都写入缓存，避免反复重试（3天过期，与collect一致）
        try:
            conn.execute(
                """INSERT OR REPLACE INTO qs_finance_cache 
                    (code, expect_yoy, ttm_yoy, org_num, diff, cached_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                (code, expect_yoy, ttm_yoy, org_num, diff)
            )
            conn.commit()
        except Exception:
            pass  # 写入失败不影响返回
        
        return {"expect_yoy": expect_yoy, "ttm_yoy": ttm_yoy,
                "org_num": org_num, "diff": diff}
    
    return {"expect_yoy": None, "ttm_yoy": None, "org_num": None, "diff": None}


# ══════════════════════════════════════════════════════════
# 6. 热门板块（三源交集，精确读库，无记录才近似）
# ══════════════════════════════════════════════════════════
def get_hot_sectors_at(conn: sqlite3.Connection, date_str: str,
                       use_3day: bool = True, allow_fallback: bool = True) -> dict:
    """
    取基准日当天或最近的热门板块（三源交集）。

    优先从 qs_trend_sectors 读取已有结果。
    若无记录，实时计算：
      来源A：板块涨停统计TOP20
      来源B：日线分析满足数TOP20
      来源C：三日分析满足数TOP20（use_3day=True 时启用）
    交集为空时退化策略与 collect.py 一致。

    返回 {sector_name: {"rank": rank, "ratio": ratio}, ...} 及 exact/ref_date
    """
    # 先查当天已有结果
    rows = conn.execute(
        "SELECT sector_name, sector_rank, ratio FROM qs_trend_sectors WHERE date=? ORDER BY sector_rank",
        (date_str,)
    ).fetchall()
    if rows:
        return {"sectors": {r[0]: {"rank": r[1], "ratio": r[2]} for r in rows},
                "exact": True, "ref_date": date_str, "mode": "cached"}

    # fallback：最近一条已有结果
    rows = conn.execute(
        "SELECT date, sector_name, sector_rank, ratio FROM qs_trend_sectors "
        "WHERE date <= ? ORDER BY date DESC LIMIT 50",
        (date_str,)
    ).fetchall()
    if rows:
        ref_date = rows[0][0]
        secs = {r[1]: {"rank": r[2], "ratio": r[3]} for r in rows if r[0] == ref_date}
        return {"sectors": secs, "exact": False, "ref_date": ref_date, "mode": "cached_nearby"}

    # 完全没有热门板块数据，实时计算（与 collect.py 逻辑一致）
    # ── 来源B：日线分析满足数TOP20 ───────────────────────
    row = conn.execute(
        "SELECT run_id, satisfied_count FROM t_run_log "
        "WHERE note LIKE ? AND note LIKE '%[日线]%' ORDER BY run_time DESC LIMIT 1",
        (f"%{date_str}%",)
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT run_id, satisfied_count FROM t_run_log "
            "WHERE note LIKE '%历史回溯%' AND note LIKE '%[日线]%' AND note <= ? "
            "ORDER BY run_time DESC LIMIT 1",
            (date_str,)
        ).fetchone()

    if not row:
        return {"sectors": {}, "exact": False, "ref_date": "无数据", "mode": "none"}

    run_id, total = row
    sec_rows = conn.execute("""
        SELECT ds.sector_name,
               ROW_NUMBER() OVER (ORDER BY ds.satisfied_count DESC) AS rk,
               ROUND(ds.satisfied_count * 100.0 / ?, 1) AS ratio
        FROM t_daily_report ds
        WHERE ds.run_id = ? AND ds.period = 'daily'
        ORDER BY ds.satisfied_count DESC
        LIMIT 20
    """, (total if total else 1, run_id)).fetchall()

    if not sec_rows:
        return {"sectors": {}, "exact": False, "ref_date": "无数据", "mode": "none"}

    satisfy_b = {r[0]: {"rank": r[1], "ratio": r[2]} for r in sec_rows}

    # ── 来源C：三日分析满足数TOP20 ───────────────────────
    three_day_set = set()
    if use_3day:
        row_3d = conn.execute(
            "SELECT run_id FROM t_run_log "
            "WHERE note LIKE ? AND note LIKE '%[三日]%' ORDER BY run_time DESC LIMIT 1",
            (f"%{date_str}%",)
        ).fetchone()
        if not row_3d:
            row_3d = conn.execute(
                "SELECT run_id FROM t_run_log "
                "WHERE note LIKE '%历史回溯%' AND note LIKE '%[三日]%' AND note <= ? "
                "ORDER BY run_time DESC LIMIT 1",
                (date_str,)
            ).fetchone()
        if row_3d:
            rows_3d = conn.execute("""
                SELECT ds.sector_name FROM t_daily_report ds
                WHERE ds.run_id = ? AND ds.period = '3day'
                ORDER BY ds.satisfied_count DESC LIMIT 20
            """, (row_3d[0],)).fetchall()
            three_day_set = {r[0] for r in rows_3d}

    # ── 交集计算 ─────────────────────────────────────────
    # 来源A（涨停）无法在回测中精确计算（需要 t_stock_calc 数据），
    # 回测场景下退化：有来源C时用 B∩C，无来源C时用仅B
    if use_3day and three_day_set:
        intersect_names = set(satisfy_b.keys()) & three_day_set
        mode_label = "B∩C(实时)"
        if not intersect_names and allow_fallback:
            intersect_names = set(satisfy_b.keys())
            mode_label = "B(fallback)"
    else:
        intersect_names = set(satisfy_b.keys())
        mode_label = "B(实时)"

    # 只保留 intersect 中的板块数据
    result_sectors = {name: satisfy_b[name] for name in intersect_names if name in satisfy_b}
    # 重新排名
    ranked = sorted(result_sectors.items(), key=lambda x: x[1]["ratio"], reverse=True)
    result_sectors = {name: {**info, "rank": i+1} for i, (name, info) in enumerate(ranked)}

    return {"sectors": result_sectors, "exact": False, "ref_date": date_str, "mode": mode_label}


def get_stock_sectors(conn: sqlite3.Connection, code: str) -> list[str]:
    """取个股所有板块"""
    rows = conn.execute(
        "SELECT sec.sector_name FROM t_sector_stock ss "
        "JOIN t_sector sec ON sec.sector_code=ss.sector_code "
        "WHERE ss.stock_code=?",
        (code,)
    ).fetchall()
    return [r[0] for r in rows]


# ══════════════════════════════════════════════════════════
# 6. 拉取股票名称（腾讯行情接口）
# ══════════════════════════════════════════════════════════
def fetch_stock_info(code: str, market: str) -> dict:
    """从腾讯行情接口获取股票名称和当前行情"""
    prefix = "sh" if market == "sh" else "sz"
    url = f"https://qt.gtimg.cn/q={prefix}{code}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=3)
        r.encoding = "gbk"
        parts = r.text.split("~")
        if len(parts) > 45:
            return {
                "name":    parts[1],
                "close":   float(parts[3]) if parts[3] else None,
                "chg_pct": float(parts[32]) if parts[32] else None,
                "pe_ttm":  float(parts[39]) if parts[39] else None,
                "mcap":    float(parts[45]) if parts[45] else None,
            }
    except Exception:
        pass
    return {"name": code, "close": None, "chg_pct": None, "pe_ttm": None, "mcap": None}


# ══════════════════════════════════════════════════════════
# 7. 核心分析函数
# ══════════════════════════════════════════════════════════
def analyze(code: str, date_str: str, window: int = 5,
            strategy: str = DEFAULT_STRATEGY,
            target_pct: float = 15.0, stop_pct: float = 5.0,
            use_surprise: bool = True, use_news: bool = True,
            surprise_mode: str = "forward",
            qdiff_mode: str = "quarter") -> dict:
    """
    单股回测主入口。
    code:       6位股票代码
    date_str:   'YYYY-MM-DD' 或 'YYYYMMDD'
    window:     验证窗口（交易日，默认10）
    strategy:   基本面策略 classic/growth/surprise 或组合 classic,surprise
    target_pct: 目标涨幅%（默认15）
    stop_pct:   止损幅度%（默认5）
    use_surprise: 是否启用超预期分析（默认True）
    use_news:   是否启用消息面分析（默认True）
    """
    # 规范化输入
    compact = date_str.replace("-", "")
    dash_str = f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    base_dt  = datetime.strptime(compact, "%Y%m%d")

    market = get_market(code)
    result = {
        "code":     code,
        "market":   market,
        "date":     dash_str,
        "window":   window,
        "error":    None,
    }

    # ── 读取全量K线 ────────────────────────────────────────
    df_full = read_tdx_day_full(market, code)
    if df_full is None or len(df_full) == 0:
        result["error"] = f"未找到 {market}{code} 的K线数据，请确认代码正确"
        return result

    # ── 检查上市时间 ─────────────────────────────────────
    first_date = df_full.index[0]
    if base_dt < first_date:
        result["error"] = (f"{code} 上市日期为 {first_date.strftime('%Y-%m-%d')}，"
                           f"早于基准日 {dash_str}，无法回测")
        return result

    # ── 基准日截断（仅保留基准日及之前的K线）──────────────
    df_base = df_full[df_full.index <= base_dt]
    if len(df_base) == 0:
        result["error"] = f"基准日 {dash_str} 无K线数据（可能为非交易日）"
        return result
    if len(df_base) < 60:
        result["error"] = f"基准日 {dash_str} 前的K线数据不足（仅{len(df_base)}条），无法计算指标"
        return result

    # 基准日实际交易日（取截断后最后一条）
    actual_base_dt   = df_base.index[-1]
    actual_base_str  = actual_base_dt.strftime("%Y-%m-%d")
    base_close       = float(df_base["close"].iloc[-1])

    # ── 获取股票名称 ──────────────────────────────────────
    # 优先从 qs_ebk_stocks 查
    conn = sqlite3.connect(DB_FILE)
    row = conn.execute(
        "SELECT name, pe_ttm, mcap FROM qs_ebk_stocks WHERE code=? ORDER BY date DESC LIMIT 1",
        (code,)
    ).fetchone()
    if row:
        name   = row[0]
        pe_ttm = float(row[1]) if row[1] else None
        mcap   = float(row[2]) if row[2] else None
    else:
        info   = fetch_stock_info(code, market)
        name   = info["name"]
        pe_ttm = info.get("pe_ttm")
        mcap   = info.get("mcap")

    result["name"] = name

    # ── 技术评分（使用截断K线）────────────────────────────
    tech = tech_score_detail(df_base)

    # ── 财务数据（纯本地，无网络）────────────────────────
    fin  = get_finance(code, market, ref_date=compact)

    # ── 基本面评分（支持组合策略）────────────────────────
    strategies_list = [s.strip() for s in strategy.split(",") if s.strip()]
    # 验证策略名，无效的降级为 DEFAULT_STRATEGY
    valid_strategies = [s for s in strategies_list if s in STRATEGIES]
    if not valid_strategies:
        valid_strategies = [DEFAULT_STRATEGY]
    # single_line 内置了季度预期差，不需要额外加 surprise
    is_single_line_only = (len(valid_strategies) == 1 and valid_strategies[0] == "single_line")
    # 如果启用超预期，将 surprise 自动加入组合（若尚未包含）
    if use_surprise and "surprise" not in valid_strategies and not is_single_line_only:
        valid_strategies.append("surprise")
    # 如果禁用超预期，过滤掉 surprise 策略
    if not use_surprise:
        valid_strategies = [s for s in valid_strategies if s != "surprise"]
        if not valid_strategies:
            valid_strategies = [DEFAULT_STRATEGY]

    if len(valid_strategies) == 1:
        s0 = valid_strategies[0]
        if s0 == "surprise":
            from fund_strategies import surprise_score_detail
            surprise_cache = get_surprise_cache(conn, code)
            fs = surprise_score_detail(
                pe_ttm, mcap,
                fin.get("profit_yoy"), fin.get("revenue_yoy"), fin.get("roe"),
                expect_yoy=surprise_cache.get("expect_yoy"),
                ttm_yoy=surprise_cache.get("ttm_yoy"),
                org_num=surprise_cache.get("org_num"),
                surprise_mode=surprise_mode,
            )
        elif s0 == "single_line":
            from fund_strategies import single_line_score_detail, calc_ttm_profit_growth_pair, fetch_quarterly_consensus
            from collect import get_quarterly_consensus_from_cache, save_quarterly_consensus_cache
            # TTM环比：一次读取当期+上期
            try:
                ttm_cur, ttm_prev = calc_ttm_profit_growth_pair(code)
            except Exception:
                ttm_cur, ttm_prev = None, None
            # 季度预期：缓存优先，未命中再网络获取
            qc_data = get_quarterly_consensus_from_cache(conn, code)
            if qc_data is None:
                try:
                    qc_data = fetch_quarterly_consensus(code)
                except Exception:
                    qc_data = {"expected_np": None, "expected_eps": None,
                               "predict_count": 0, "latest_quarter": None,
                               "latest_report_date": None, "source": "none"}
                save_quarterly_consensus_cache(conn, code, qc_data)
            surprise_cache = get_surprise_cache(conn, code)
            # 使用函数参数传入的 qdiff_mode 和 surprise_mode
            fs = single_line_score_detail(
                pe_ttm, mcap,
                fin.get("profit_yoy"), fin.get("revenue_yoy"), fin.get("roe"),
                quarterly_consensus=qc_data,
                expect_yoy=surprise_cache.get("expect_yoy"),
                ttm_yoy=ttm_cur or surprise_cache.get("ttm_yoy"),
                prev_ttm_yoy=ttm_prev,
                org_num=surprise_cache.get("org_num"),
                qdiff_mode=qdiff_mode, surprise_mode=surprise_mode,
            )
        else:
            fs = fund_score_detail(pe_ttm, mcap,
                                   fin.get("profit_yoy"), fin.get("revenue_yoy"),
                                   fin.get("roe"), strategy=s0)
    else:
        # 组合策略：各策略独立评分后取平均
        from fund_strategies import surprise_score_detail
        scores = []
        last_fs = None
        surprise_cache = get_surprise_cache(conn, code)
        for s0 in valid_strategies:
            if s0 == "surprise":
                fs_i = surprise_score_detail(
                    pe_ttm, mcap,
                    fin.get("profit_yoy"), fin.get("revenue_yoy"), fin.get("roe"),
                    expect_yoy=surprise_cache.get("expect_yoy"),
                    ttm_yoy=surprise_cache.get("ttm_yoy"),
                    org_num=surprise_cache.get("org_num"),
                    surprise_mode=surprise_mode,
                )
            elif s0 == "single_line":
                from fund_strategies import single_line_score_detail, calc_ttm_profit_growth_pair, fetch_quarterly_consensus
                from collect import get_quarterly_consensus_from_cache, save_quarterly_consensus_cache
                # TTM环比：一次读取当期+上期
                try:
                    _ttm_cur, _ttm_prev = calc_ttm_profit_growth_pair(code)
                except Exception:
                    _ttm_cur, _ttm_prev = None, None
                # 季度预期：缓存优先，未命中再网络获取
                _qc = get_quarterly_consensus_from_cache(conn, code)
                if _qc is None:
                    try:
                        _qc = fetch_quarterly_consensus(code)
                    except Exception:
                        _qc = {"expected_np": None, "expected_eps": None,
                               "predict_count": 0, "latest_quarter": None,
                               "latest_report_date": None, "source": "none"}
                    save_quarterly_consensus_cache(conn, code, _qc)
                fs_i = single_line_score_detail(
                    pe_ttm, mcap,
                    fin.get("profit_yoy"), fin.get("revenue_yoy"), fin.get("roe"),
                    quarterly_consensus=_qc,
                    expect_yoy=surprise_cache.get("expect_yoy"),
                    ttm_yoy=_ttm_cur or surprise_cache.get("ttm_yoy"),
                    prev_ttm_yoy=_ttm_prev,
                    org_num=surprise_cache.get("org_num"),
                    qdiff_mode=qdiff_mode, surprise_mode=surprise_mode,
                )
            else:
                fs_i = fund_score_detail(pe_ttm, mcap,
                                         fin.get("profit_yoy"), fin.get("revenue_yoy"),
                                         fin.get("roe"), strategy=s0)
            scores.append(fs_i["total"])
            last_fs = fs_i
        avg_score = round(sum(scores) / len(scores), 1)
        fs = last_fs.copy()
        fs["total"] = avg_score
        fs["strategy"] = strategy

    # ── 消息面：只读历史记录，有利空则标记跳过，不加分 ──
    # 从 qs_picks 读历史当日该股的 news_level（如有记录）
    news_level = None
    news_summary = ""
    news_bearish = False
    news_heat = 0
    if use_news:
        news_row = conn.execute(
            "SELECT news_level, news_summary FROM qs_picks WHERE date=? AND code LIKE ? LIMIT 1",
            (dash_str, f"%{code}")
        ).fetchone()
        if news_row:
            news_level = news_row[0]
            news_summary = news_row[1] or ""
        # 利空/重大利空 → 过滤（返回特殊标记，调用方决定是否跳过）
        news_bearish = news_level in ("bearish", "strong_bearish") if news_level else False
        news_heat = 0  # 回测中消息面不加分

    # ── 热门板块 ─────────────────────────────────────────
    hot_info   = get_hot_sectors_at(conn, dash_str)
    all_secs   = get_stock_sectors(conn, code)
    hot_secs   = hot_info["sectors"]
    hit_secs   = [s for s in all_secs if s in hot_secs]
    hot_score  = 0.0
    if hit_secs:
        # 与 score.py 保持一致的 hot_score 计算公式
        avg_ratio = sum(hot_secs.get(s, {}).get("ratio", 0) for s in hit_secs) / len(hit_secs)
        avg_rank  = sum(hot_secs.get(s, {}).get("rank", 25) for s in hit_secs) / len(hit_secs)
        hot_score = avg_ratio * (31 - avg_rank) / 30

    # ── 综合评分 ─────────────────────────────────────────
    # 与精选流程保持一致：
    #   总热度分 = 板块热度(0-20) + 消息面(0-10)
    #   回测中消息面固定为0（无历史数据，只做利空过滤）
    sector_heat = hot_score  # 板块热度分（0-20）
    total_heat  = sector_heat + news_heat  # 回测时 news_heat=0
    total_score  = round(tech["total"] * TECH_WEIGHT + fs["total"] * FUND_WEIGHT, 1)
    final_score  = round(total_score * HOT_WEIGHT + min(total_heat * 2, HOT_BONUS_MAX), 2)

    # ── 操作建议 ─────────────────────────────────────────
    adv      = gen_action(final_score)
    buy_lo   = round(base_close * 0.97, 2)
    buy_hi   = round(base_close * 1.02, 2)
    stop     = round(base_close * (1 - stop_pct / 100), 2)
    target   = round(base_close * (1 + target_pct / 100), 2)

    # ── 后续走势验证 ──────────────────────────────────────
    df_after  = df_full[df_full.index > actual_base_dt]
    future_bars = df_after.head(window)

    verdict      = "insufficient"   # insufficient / success / failure / pending
    verdict_day  = None
    verdict_price= None
    max_gain_pct = None
    max_loss_pct = None
    actual_return= None

    # ── 后续走势验证 ──────────────────────────────────────
    # 用已有数据计算（即使不满window期）
    if len(future_bars) == 0:
        verdict = "insufficient"   # 完全没有后续数据
    else:
        highs  = future_bars["high"].values
        lows   = future_bars["low"].values
        closes = future_bars["close"].values
        dates  = [d.strftime("%Y-%m-%d") for d in future_bars.index]

        max_high = float(highs.max())
        min_low  = float(lows.min())
        final_close = float(closes[-1])

        max_gain_pct  = round((max_high  - base_close) / base_close * 100, 2)
        max_loss_pct  = round((min_low   - base_close) / base_close * 100, 2)
        actual_return = round((final_close - base_close) / base_close * 100, 2)

        # 逐日判断（先触目标=成功，先触止损=失败）
        triggered = False
        for i, (d, h, l) in enumerate(zip(dates, highs, lows)):
            if h >= target:
                verdict       = "success"
                verdict_day   = d
                verdict_price = round(float(h), 2)
                triggered = True
                break
            if l <= stop:
                verdict       = "failure"
                verdict_day   = d
                verdict_price = round(float(l), 2)
                triggered = True
                break
        
        if not triggered:
            # 未触发目标/止损
            if len(future_bars) < window:
                verdict = "pending"   # 时间不足，显示待定
            else:
                verdict = "pending"   # 时间够了但未触发，也是待定

    # ── 原因分析 ──────────────────────────────────────────
    analysis = _gen_analysis(
        verdict, tech, fs, fin, hit_secs, hot_info,
        base_close, target, stop, max_gain_pct, max_loss_pct, actual_return, window,
        use_surprise=use_surprise, use_news=use_news,
        news_bearish=news_bearish, news_summary=news_summary
    )

    # ── K线图数据（前60 + 基准日 + 后30，共约91根）─────────
    pre_bars  = df_base.tail(61)          # 基准日前60 + 基准日本身
    post_bars = df_full[df_full.index > actual_base_dt].head(30)
    chart_df  = pd.concat([pre_bars, post_bars])

    def bar_dict(row, dt):
        return {
            "date":   dt.strftime("%Y-%m-%d"),
            "open":   round(float(row["open"]),  2),
            "high":   round(float(row["high"]),  2),
            "low":    round(float(row["low"]),   2),
            "close":  round(float(row["close"]), 2),
            "volume": int(row["volume"]),
            "ma5":    round(float(row["ma5"]),   2) if not pd.isna(row.get("ma5", float("nan"))) else None,
            "ma20":   round(float(row["ma20"]),  2) if not pd.isna(row.get("ma20", float("nan"))) else None,
        }

    # 补充指标到chart_df
    chart_df_ind = calc_indicators(df_base.tail(30))
    ma5_tail  = {}
    ma20_tail = {}
    for d in chart_df_ind.index:
        d_str = d.strftime("%Y-%m-%d")
        v5 = chart_df_ind["ma5"].get(d)
        v20 = chart_df_ind["ma20"].get(d)
        ma5_tail[d_str] = round(float(v5), 2) if pd.notna(v5) else None
        ma20_tail[d_str] = round(float(v20), 2) if pd.notna(v20) else None

    candles = []
    for dt, row in chart_df.iterrows():
        d_str = dt.strftime("%Y-%m-%d")
        candles.append({
            "date":     d_str,
            "open":     round(float(row["open"]),  2),
            "high":     round(float(row["high"]),  2),
            "low":      round(float(row["low"]),   2),
            "close":    round(float(row["close"]), 2),
            "volume":   int(row["volume"]),
            "ma5":      ma5_tail.get(d_str),
            "ma20":     ma20_tail.get(d_str),
            "is_base":  d_str == actual_base_str,
        })

    conn.close()

    # ── 组装最终结果 ──────────────────────────────────────
    result.update({
        "actual_base_date":  actual_base_str,
        "base_close":        base_close,
        "buy_range":         f"{buy_lo}-{buy_hi}",
        "stop":              stop,
        "target":            target,
        "target_pct":        target_pct,
        "stop_pct":          stop_pct,
        "action":            adv["action"],
        "position_pct":      adv["position_pct"],
        "tech": {
            "total":    tech["total"],
            "trend":    tech["trend"],
            "position": tech["position"],
            "momentum": tech["momentum"],
            "volume":   tech["volume"],
        "sigs":     tech["sigs"],
        "rsi14":    tech["rsi14"],
        "pos_pct":  tech["pos_pct"],
        "vol_ratio": tech.get("vol_ratio"),
        },
        "fund": {
            "total":       fs["total"],
            "sigs":        fs["sigs"],
            "breakdown":   fs.get("breakdown", {}),
            "reasons":     fs.get("reasons", []),
            "strategy":    fs.get("strategy", strategy),
            "eps":         fin.get("eps"),
            "roe":         fin.get("roe"),
            "profit_yoy":  fin.get("profit_yoy"),
            "revenue_yoy": fin.get("revenue_yoy"),
            "report_period": fin.get("report_period", ""),
            "fin_source":  fin.get("source", ""),
            "pe_ttm":      pe_ttm,
            "mcap":        mcap,
            "surprise_meta": fs.get("surprise_meta"),
        },
        "sectors": {
            "all":        all_secs,
            "hit":        hit_secs,
            "hot_info":   hot_info,
            "hot_score":  hot_score,
            "sector_heat": sector_heat,
        },
        "news": {
            "level":    news_level,
            "summary":  news_summary,
            "bearish":  news_bearish,
            "heat":     news_heat,
            "note":     "消息面仅供参考（读自历史精选记录），不计入评分加成",
        },
        "total_score":   total_score,
        "final_score":   final_score,
        "fund_strategy": strategy,
        "fund_strategy_label": STRATEGIES.get(strategy, strategy),
        "verdict": {
            "status":        verdict,
            "verdict_day":   verdict_day,
            "verdict_price": verdict_price,
            "max_gain_pct":  max_gain_pct,
            "max_loss_pct":  max_loss_pct,
            "actual_return": actual_return,
            "window":        window,
            "bars_available": len(future_bars),
        },
        "analysis": analysis,
        "candles":  candles,
    })

    return result


def batch_analyze(date_str: str, window: int = 5,
                  strategy: str = DEFAULT_STRATEGY,
                  top_n: int = None,
                  target_pct: float = 15.0,
                  stop_pct: float = 5.0,
                  tech_filter: bool = False) -> dict:
    """
    批量回测（mode A）：读取 qs_picks 指定日期的精选结果，逐只调用 analyze()，
    汇总胜率/收益分布等统计信息。

    date_str:   'YYYY-MM-DD' 或 'YYYYMMDD'
    window:     验证窗口（交易日）
    strategy:   基本面策略（classic/growth/surprise 或组合）
    top_n:      只取排名前 N（默认读全部）
    target_pct: 目标涨幅%（默认15）
    stop_pct:   止损幅度%（默认5）
    tech_filter: 是否只回测技术过滤通过的股票（tech_filter_passed=1）
    返回 dict
    """
    compact  = date_str.replace("-", "")
    dash_str = f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"

    conn = sqlite3.connect(DB_FILE)
    try:
        # 读取 qs_picks 当日精选
        # 第一步：取 strategy 匹配的全部 ranked 记录（不含 tech_filter）
        sql = (
            "SELECT code, name, final_score, rank_no FROM qs_picks "
            "WHERE date=? AND strategy=? AND rank_no IS NOT NULL AND rank_no > 0 "
            "ORDER BY rank_no ASC"
        )
        rows = conn.execute(sql, (dash_str, strategy)).fetchall()
    finally:
        conn.close()

    if not rows:
        return {
            "date": dash_str, "window": window,
            "error": f"qs_picks 中无 {dash_str} 的精选数据，请先运行当日分析",
            "items": [], "summary": {}
        }

    # 第二步：top_n 截断
    effective_top_n = top_n if top_n else 20
    rows = rows[:effective_top_n]

    # 第三步：tech_filter 过滤（在代码层做，确保先截断后过滤）
    if tech_filter:
        conn2 = sqlite3.connect(DB_FILE)
        try:
            codes = [r[0] for r in rows]
            placeholders = ",".join("?" * len(codes))
            tf_rows = conn2.execute(
                f"SELECT code FROM qs_picks "
                f"WHERE date=? AND strategy=? AND code IN ({placeholders}) "
                f"  AND COALESCE(tech_filter_passed, 0) = 1",
                (dash_str, strategy, *codes)
            ).fetchall()
            tf_codes = {r[0] for r in tf_rows}
        finally:
            conn2.close()
        rows = [r for r in rows if r[0] in tf_codes]

    total = len(rows)
    items = []
    print(f"  [批量回测] 共 {total} 只，开始分析...", flush=True)
    for i, (raw_code, name, final_score, rank_no) in enumerate(rows):
        # qs_picks.code 格式可能是 'sz002616' 或 '002616'，统一剥离前缀
        code = raw_code.lstrip("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
        # 每5只 + 首尾打印进度
        if (i + 1) % 5 == 0 or i == 0 or i == total - 1:
            print(f"  [{i+1}/{total}] {code} ...", end='\r', flush=True)
        try:
            r = analyze(code, dash_str, window, strategy=strategy,
                        target_pct=target_pct, stop_pct=stop_pct)
        except Exception as e:
            print(f"  [{i+1}/{total}] {code} ERR: {e}", end='\r', flush=True)
            r = {"code": code, "name": name, "error": str(e),
                 "verdict": {"status": "error"}}
        r["pick_rank"]        = rank_no
        r["pick_final_score"] = final_score
        # 补充 hit_sectors 字段供前端展示（从 sectors.hit 提取）
        r["hit_sectors"] = r.get("sectors", {}).get("hit", [])
        items.append(r)

    # ── 统计汇总 ─────────────────────────────────────────
    verdicts   = [it.get("verdict", {}).get("status", "error") for it in items]
    cnt = {
        "success":     verdicts.count("success"),
        "failure":     verdicts.count("failure"),
        "pending":     verdicts.count("pending"),
        "insufficient":verdicts.count("insufficient"),
        "error":       verdicts.count("error"),
        "total":       total,
    }
    # 胜率（分母：已有结果的 success+failure）
    decided   = cnt["success"] + cnt["failure"]
    win_rate  = round(cnt["success"] / decided * 100, 1) if decided > 0 else None

    # 待定票中最终涨跌分布
    pending_items = [it for it in items 
                     if it.get("verdict", {}).get("status") in ("pending", "insufficient")]
    pending_returns = [it["verdict"]["actual_return"] for it in pending_items
                       if isinstance(it.get("verdict", {}).get("actual_return"), (int, float))]
    pending_up = sum(1 for r in pending_returns if r > 0)
    pending_down = sum(1 for r in pending_returns if r < 0)
    pending_flat = len(pending_returns) - pending_up - pending_down
    cnt["pending_up"] = pending_up
    cnt["pending_down"] = pending_down
    cnt["pending_flat"] = pending_flat

    # 平均收益（success+failure+pending 均纳入，取 actual_return）
    returns = [
        it["verdict"]["actual_return"]
        for it in items
        if isinstance(it.get("verdict", {}).get("actual_return"), (int, float))
    ]
    avg_return = round(sum(returns) / len(returns), 2) if returns else None

    # 最大收益 / 最大亏损
    max_return = round(max(returns), 2) if returns else None
    min_return = round(min(returns), 2) if returns else None

    summary = {
        "date":        dash_str,
        "window":      window,
        "total":       total,
        "counts":      cnt,
        "win_rate":    win_rate,
        "avg_return":  avg_return,
        "max_return":  max_return,
        "min_return":  min_return,
        "decided":     decided,
        "strategy":    strategy,
    }

    result = {
        "date":    dash_str,
        "window":  window,
        "target_pct": target_pct,
        "stop_pct":   stop_pct,
        "summary": summary,
        "items":   items,
    }

    # 保存回测记录到数据库
    try:
        save_batch_result(result, {
            "mode": "batch",
            "strategy": strategy,
            "use_tech": True,  # batch_analyze 固定使用技术面
            "sector_mode": "dual",
            "top_n": effective_top_n,
            "target_pct": target_pct,
            "stop_pct": stop_pct,
        })
    except Exception as e:
        print(f"  [WARN] 回测记录保存失败: {e}", flush=True)

    return result


# ══════════════════════════════════════════════════════════
# 9. 历史重跑（mode B）
# ══════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════
# 策略配置常量（与 score.py 保持一致）
# ══════════════════════════════════════════════════════════
FUND_FILTER = 30  # 基本面过滤阈值

# 预定义策略组合映射
STRATEGY_COMBOS = {
    "classic":               ["classic"],
    "growth":                ["growth"],
    "surprise":              ["surprise"],
    "classic_surprise":      ["classic", "surprise"],
    "growth_surprise":       ["growth", "surprise"],
    "classic_growth":        ["classic", "growth"],
}


def parse_strategy(strategy_param: str) -> tuple[list[str], str]:
    """
    解析策略参数，返回 (策略列表, 显示名称)
    支持：单策略名、逗号分隔、预定义组合名
    """
    if not strategy_param:
        return [DEFAULT_STRATEGY], DEFAULT_STRATEGY
    
    # 先检查是否是预定义组合
    if strategy_param in STRATEGY_COMBOS:
        return STRATEGY_COMBOS[strategy_param], strategy_param
    
    # 逗号分隔解析
    strategies = [s.strip() for s in strategy_param.split(",") if s.strip()]
    valid = [s for s in strategies if s in STRATEGIES]
    
    if not valid:
        return [DEFAULT_STRATEGY], DEFAULT_STRATEGY
    
    return valid, strategy_param


def rerun(date_str: str, window: int = 5, top_n: int = 20,
          strategy: str = DEFAULT_STRATEGY,
          use_tech: bool = True,
          use_3day: bool = True, allow_fallback: bool = True,
          target_pct: float = 15.0, stop_pct: float = 5.0,
          surprise_mode: str = "forward",
          qdiff_mode: str = "quarter") -> dict:
    """
    历史重跑：给定日期，从 qs_ebk_stocks 快照重走完整筛选流程。
    
    【优化后流程】
    1. 先筛选命中热门板块的股票（大幅减少计算量）
    2. 对剩余股票进行基本面评分
    3. 基本面过滤（< FUND_FILTER 剔除）
    4. 按需进行技术评分（use_tech=False 时跳过）
    5. 综合评分计算
    6. 后续走势验证

    date_str: 'YYYY-MM-DD' 或 'YYYYMMDD'
    use_tech: 是否使用技术评分（False=纯基本面模式）
    use_3day: 是否启用三源交集（False=双源模式）
    allow_fallback: 三源为空时是否退化为双源
    """
    compact  = date_str.replace("-", "")
    dash_str = f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    start_time = time.time()

    # 解析策略
    valid_strategies, strategy_label = parse_strategy(strategy)
    
    conn = sqlite3.connect(DB_FILE)
    try:
        # ── Step 0: 获取热门板块（先算，用于预筛选）────────────────
        hot_info = get_hot_sectors_at(conn, dash_str, use_3day=use_3day,
                                       allow_fallback=allow_fallback)
        hot_secs = hot_info["sectors"]
        mode_3day = "三源" if use_3day else "双源"
        fallback_str = "" if allow_fallback else "(禁止退化)"
        mode_desc = '(精确匹配)' if hot_info['exact'] else f"({hot_info.get('mode','?')})"
        print(f"  [重跑] {dash_str} 热门板块：{len(hot_secs)} 个 ({mode_3day}{fallback_str}) {mode_desc}", flush=True)
        
        # ── Step 1: 读取当日自选股快照 ───────────────────────────
        stock_rows = conn.execute(
            "SELECT code, name, market, close, pe_ttm, mcap FROM qs_ebk_stocks WHERE date=? ORDER BY code",
            (dash_str,)
        ).fetchall()
    finally:
        conn.close()

    if not stock_rows:
        return {
            "date": dash_str, "window": window, "mode": "rerun",
            "error": f"qs_ebk_stocks 中无 {dash_str} 的自选股快照，请先运行当日采集",
            "items": [], "summary": {}
        }

    total_candidates = len(stock_rows)
    print(f"  [重跑] 候选池：{total_candidates} 只，开始预筛选...", flush=True)

    # ── Step 2: 预筛选 - 只保留命中热门板块的股票 ───────────────
    # 大幅减少后续计算量
    prefiltered = []
    skipped_sector = 0
    
    conn = sqlite3.connect(DB_FILE)
    try:
        for raw_code, name, market, close, pe_ttm, mcap in stock_rows:
            code = raw_code.strip()
            all_secs = get_stock_sectors(conn, code)
            hit_secs = [s for s in all_secs if s in hot_secs]
            if hit_secs:
                prefiltered.append({
                    "code": code, "name": name, "market": market,
                    "close": close, "pe_ttm": pe_ttm, "mcap": mcap,
                    "hit_secs": hit_secs, "all_secs": all_secs
                })
            else:
                skipped_sector += 1
    finally:
        conn.close()
    
    sector_filtered = len(prefiltered)
    print(f"  [重跑] 板块预筛选：{total_candidates} → {sector_filtered} 只"
          f"（跳过 {skipped_sector} 只）", flush=True)

    # ── Step 3: 逐只评分（只处理预筛选后的股票）─────────────────
    items = []
    skipped_fund = 0
    skipped_tech = 0
    processed = 0
    
    for i, stock in enumerate(prefiltered):
        code = stock["code"]
        name = stock["name"]
        market = stock["market"]
        hit_secs = stock["hit_secs"]
        
        # 每5只 + 首尾打印进度
        if (i + 1) % 5 == 0 or i == 0 or i == sector_filtered - 1:
            elapsed = time.time() - start_time
            print(f"  [{i+1}/{sector_filtered}] {code} | 已处理{processed}只 "
                  f"| 耗时{elapsed:.1f}s", end='\r', flush=True)

        try:
            conn2 = sqlite3.connect(DB_FILE)
            
            # ── 基本面评分（必须先算，用于过滤）───────────────────
            fin = get_finance(code, market, ref_date=compact)
            pe = float(stock["pe_ttm"]) if stock["pe_ttm"] else None
            mc = float(stock["mcap"]) if stock["mcap"] else None

            # 计算基本面评分
            if len(valid_strategies) == 1:
                s0 = valid_strategies[0]
                if s0 == "surprise":
                    from fund_strategies import surprise_score_detail
                    sc = get_surprise_cache(conn2, code, allow_fetch=True)
                    fs = surprise_score_detail(pe, mc,
                        fin.get("profit_yoy"), fin.get("revenue_yoy"), fin.get("roe"),
                        expect_yoy=sc.get("expect_yoy"), ttm_yoy=sc.get("ttm_yoy"),
                        org_num=sc.get("org_num"),
                        surprise_mode=surprise_mode)
                elif s0 == "single_line":
                    from fund_strategies import single_line_score_detail, calc_ttm_profit_growth_pair, fetch_quarterly_consensus
                    from collect import get_quarterly_consensus_from_cache, save_quarterly_consensus_cache
                    # TTM环比：一次读取当期+上期
                    try:
                        ttm_cur, ttm_prev = calc_ttm_profit_growth_pair(code)
                    except Exception:
                        ttm_cur, ttm_prev = None, None
                    # 季度预期：缓存优先，未命中再网络获取
                    qc_data = get_quarterly_consensus_from_cache(conn2, code)
                    if qc_data is None:
                        try:
                            qc_data = fetch_quarterly_consensus(code)
                        except Exception:
                            qc_data = {"expected_np": None, "expected_eps": None,
                                       "predict_count": 0, "latest_quarter": None,
                                       "latest_report_date": None, "source": "none"}
                        save_quarterly_consensus_cache(conn2, code, qc_data)
                    sc = get_surprise_cache(conn2, code, allow_fetch=True)
                    fs = single_line_score_detail(
                        pe, mc,
                        fin.get("profit_yoy"), fin.get("revenue_yoy"), fin.get("roe"),
                        quarterly_consensus=qc_data,
                        expect_yoy=sc.get("expect_yoy"), ttm_yoy=ttm_cur or sc.get("ttm_yoy"),
                        prev_ttm_yoy=ttm_prev,
                        org_num=sc.get("org_num"),
                        qdiff_mode=qdiff_mode, surprise_mode=surprise_mode)
                else:
                    fs = fund_score_detail(pe, mc,
                        fin.get("profit_yoy"), fin.get("revenue_yoy"),
                        fin.get("roe"), strategy=s0)
            else:
                from fund_strategies import surprise_score_detail
                sc = get_surprise_cache(conn2, code, allow_fetch=True)
                scores = []
                last_fs = None
                for s0 in valid_strategies:
                    if s0 == "surprise":
                        fs_i = surprise_score_detail(pe, mc,
                            fin.get("profit_yoy"), fin.get("revenue_yoy"), fin.get("roe"),
                            expect_yoy=sc.get("expect_yoy"), ttm_yoy=sc.get("ttm_yoy"),
                            org_num=sc.get("org_num"),
                            surprise_mode=surprise_mode)
                    elif s0 == "single_line":
                        from fund_strategies import single_line_score_detail, calc_ttm_profit_growth_pair, fetch_quarterly_consensus
                        from collect import get_quarterly_consensus_from_cache, save_quarterly_consensus_cache
                        # TTM环比：一次读取当期+上期
                        try:
                            ttm_cur, ttm_prev = calc_ttm_profit_growth_pair(code)
                        except Exception:
                            ttm_cur, ttm_prev = None, None
                        # 季度预期：缓存优先，未命中再网络获取
                        qc_data = get_quarterly_consensus_from_cache(conn2, code)
                        if qc_data is None:
                            try:
                                qc_data = fetch_quarterly_consensus(code)
                            except Exception:
                                qc_data = {"expected_np": None, "expected_eps": None,
                                           "predict_count": 0, "latest_quarter": None,
                                           "latest_report_date": None, "source": "none"}
                            save_quarterly_consensus_cache(conn2, code, qc_data)
                        fs_i = single_line_score_detail(
                            pe, mc,
                            fin.get("profit_yoy"), fin.get("revenue_yoy"), fin.get("roe"),
                            quarterly_consensus=qc_data,
                            expect_yoy=sc.get("expect_yoy"), ttm_yoy=ttm_cur or sc.get("ttm_yoy"),
                            prev_ttm_yoy=ttm_prev,
                            org_num=sc.get("org_num"),
                            qdiff_mode=qdiff_mode, surprise_mode=surprise_mode)
                    else:
                        fs_i = fund_score_detail(pe, mc,
                            fin.get("profit_yoy"), fin.get("revenue_yoy"),
                            fin.get("roe"), strategy=s0)
                    scores.append(fs_i["total"])
                    last_fs = fs_i
                fs = last_fs.copy()
                fs["total"] = round(sum(scores) / len(scores), 1)
                fs["strategy"] = strategy_label

            # ── 基本面过滤 ────────────────────────────────────────
            if fs["total"] < FUND_FILTER:
                skipped_fund += 1
                conn2.close()
                continue

            # ── 技术评分 & tech_filter 标记（按需执行）────────────
            tech = None
            tf_info = {"passed": True, "pos_pct": None, "vol_ratio": None}
            if use_tech:
                df_full = read_tdx_day_full(market, code)
                if df_full is None or len(df_full) == 0:
                    # K线数据缺失，标记后跳过此股（无K线无法回测）
                    conn2.close()
                    continue
                base_dt = datetime.strptime(compact, "%Y%m%d")
                df_base = df_full[df_full.index <= base_dt]
                if len(df_base) < 60:
                    # K线不足60条，标记后跳过
                    conn2.close()
                    continue
                tech = tech_score_detail(df_base)
                # tech_filter 标记（先标记，截断后再统一过滤）
                from score import tech_filter as score_tech_filter
                _, tf_info = score_tech_filter(df_base)
            else:
                # 纯基本面模式：技术评分设为0
                tech = {"total": 0, "trend": 0, "position": 0, "momentum": 0,
                        "volume": 0, "sigs": [], "rsi14": None, "pos_pct": None}
                df_full = None
                df_base = None

            processed += 1

            # ── 热门板块热度分 ────────────────────────────────────
            hot_score = 0.0
            avg_ratio = sum(hot_secs.get(s, {}).get("ratio", 0) for s in hit_secs) / len(hit_secs)
            avg_rank  = sum(hot_secs.get(s, {}).get("rank", 25) for s in hit_secs) / len(hit_secs)
            hot_score = avg_ratio * (31 - avg_rank) / 30

            # ── 综合评分（纯基本面100%，与主流程一致）──────────
            total_score = fs["total"]
            
            final_score = round(total_score * HOT_WEIGHT + min(hot_score * 2, HOT_BONUS_MAX), 2)

            # ── 止盈止损 & 后续验证 ───────────────────────────────
            if use_tech:
                actual_base_dt  = df_base.index[-1]
                actual_base_str = actual_base_dt.strftime("%Y-%m-%d")
                base_close_val  = float(df_base["close"].iloc[-1])
            else:
                # 纯基本面模式：使用快照收盘价
                actual_base_str = dash_str
                base_close_val  = float(stock["close"]) if stock["close"] else 0
                if base_close_val == 0:
                    conn2.close()
                    continue
                df_after = None
                # 纯基本面模式需要构造 actual_base_dt 用于后续K线筛选
                actual_base_dt = datetime.strptime(compact, "%Y%m%d")
            
            stop_price   = round(base_close_val * (1 - stop_pct / 100), 2)
            target_price = round(base_close_val * (1 + target_pct / 100), 2)

            # 后续走势验证（无论技术/基本面模式，都计算走势数据）
            verdict = "insufficient"
            verdict_day = None
            verdict_price = None
            max_gain_pct = None
            max_loss_pct = None
            actual_return = None

            # 获取后续K线数据（技术评分时已有 df_full，纯基本面模式需要重新读取）
            if df_full is None:
                try:
                    df_full = read_tdx_day_full(market, code)
                    if df_full is None or df_full.empty:
                        df_full = None
                except Exception:
                    df_full = None

            if df_full is not None:
                df_after    = df_full[df_full.index > actual_base_dt]
                future_bars = df_after.head(window)

                if len(future_bars) == 0:
                    verdict = "insufficient"
                else:
                    highs  = future_bars["high"].values
                    lows   = future_bars["low"].values
                    closes = future_bars["close"].values
                    dates  = [d.strftime("%Y-%m-%d") for d in future_bars.index]
                    max_high = float(highs.max())
                    min_low  = float(lows.min())
                    final_close = float(closes[-1])
                    max_gain_pct  = round((max_high  - base_close_val) / base_close_val * 100, 2)
                    max_loss_pct  = round((min_low   - base_close_val) / base_close_val * 100, 2)
                    actual_return = round((final_close - base_close_val) / base_close_val * 100, 2)
                    triggered = False
                    for d, h, l in zip(dates, highs, lows):
                        if h >= target_price:
                            verdict = "success"; verdict_day = d; verdict_price = round(float(h), 2)
                            triggered = True; break
                        if l <= stop_price:
                            verdict = "failure"; verdict_day = d; verdict_price = round(float(l), 2)
                            triggered = True; break
                    if not triggered:
                        verdict = "pending"  # 窗口内未触发（含数据不足的情况）

            conn2.close()

            items.append({
                "code":           code,
                "name":           name,
                "market":         market,
                "base_close":     base_close_val,
                "actual_base_date": actual_base_str,
                "stop":           stop_price,
                "target":         target_price,
                "tech_score":     tech["total"] if use_tech else None,
                "fund_score":     fs["total"],
                "hot_score":      round(hot_score, 2),
                "total_score":    total_score,
                "final_score":    final_score,
                "hit_sectors":    hit_secs[:3],
                "pe_ttm":         pe,
                "mcap":           mc,
                "profit_yoy":     fin.get("profit_yoy"),
                "revenue_yoy":    fin.get("revenue_yoy"),
                "roe":            fin.get("roe"),
                "fund_sigs":      fs.get("sigs", []),
                "tech_sigs":      tech.get("sigs", []) if use_tech else [],
                "tech_filter_passed": tf_info.get("passed", True),
                "tech_filter_pos":    tf_info.get("pos_pct"),
                "tech_filter_vol":    tf_info.get("vol_ratio"),
                "verdict": {
                    "status":        verdict,
                    "verdict_day":   verdict_day,
                    "verdict_price": verdict_price,
                    "max_gain_pct":  max_gain_pct,
                    "max_loss_pct":  max_loss_pct,
                    "actual_return": actual_return,
                    "window":        window,
                },
            })
        except Exception as e:
            # 个股异常静默跳过
            pass

    # ── 按 final_score 排序，取前 top_n ─────────────────
    items.sort(key=lambda x: x["final_score"], reverse=True)
    if top_n:
        items = items[:top_n]

    # ── 技术过滤（先截断后过滤，与 batch_analyze 同逻辑）──────
    tech_filtered_count = 0
    if use_tech:
        before_tf = len(items)
        items = [it for it in items if it.get("tech_filter_passed", True)]
        tech_filtered_count = before_tf - len(items)
        if tech_filtered_count > 0:
            print(f"  [TECH-FILTER] TOP{top_n}中过滤掉 {tech_filtered_count} 只高位+缩量股"
                  f"，剩余 {len(items)} 只", flush=True)

    # ── 汇总统计 ─────────────────────────────────────────
    verdicts  = [it["verdict"]["status"] for it in items]
    cnt = {
        "success":      verdicts.count("success"),
        "failure":      verdicts.count("failure"),
        "pending":      verdicts.count("pending"),
        "insufficient": verdicts.count("insufficient"),
        "total":        len(items),
    }
    decided  = cnt["success"] + cnt["failure"]
    win_rate = round(cnt["success"] / decided * 100, 1) if decided > 0 else None
    returns  = [it["verdict"]["actual_return"] for it in items
                if isinstance(it["verdict"].get("actual_return"), (int, float))]
    avg_return = round(sum(returns) / len(returns), 2) if returns else None

    # 待定票涨跌分布（对齐 batch_analyze）
    pending_items = [it for it in items
                     if it["verdict"]["status"] in ("pending", "insufficient")]
    pending_returns = [it["verdict"]["actual_return"] for it in pending_items
                       if isinstance(it["verdict"].get("actual_return"), (int, float))]
    cnt["pending_up"]   = sum(1 for r in pending_returns if r > 0)
    cnt["pending_down"] = sum(1 for r in pending_returns if r < 0)
    cnt["pending_flat"] = len(pending_returns) - cnt["pending_up"] - cnt["pending_down"]
    
    elapsed_total = time.time() - start_time
    mode_label = "基本面+技术过滤" if use_tech else "纯基本面"

    print(f"\n  [重跑] 完成：候选{total_candidates}只 → 板块预筛{sector_filtered}只 "
          f"→ 基本面过滤{skipped_fund}只 → TOP{top_n or '全部'}截断 → "
          f"技术过滤{tech_filtered_count}只 → 精选{len(items)}只 "
          f"| 模式:{mode_label} | 策略:{strategy_label} | 耗时{elapsed_total:.1f}s", flush=True)

    return {
        "date":    dash_str,
        "window":  window,
        "mode":    "rerun",
        "use_tech": use_tech,
        "use_3day": use_3day,
        "strategy": strategy_label,
        "target_pct": target_pct,
        "stop_pct":   stop_pct,
        "filter_info": {
            "candidates":      total_candidates,
            "sector_filtered": sector_filtered,
            "skipped_fund":    skipped_fund,
            "skipped_tech":    tech_filtered_count,
            "selected":        len(items),
        },
        "summary": {
            "date":       dash_str,
            "window":     window,
            "total":      len(items),
            "counts":     cnt,           # 子字典，含 success/failure/pending/insufficient/pending_up/down/flat
            "win_rate":   win_rate,
            "avg_return": avg_return,
            "max_return": round(max(returns), 2) if returns else None,
            "min_return": round(min(returns), 2) if returns else None,
            "decided":    decided,
            "strategy":   strategy_label,
            "use_tech":   use_tech,
            "elapsed_sec": round(elapsed_total, 1),
        },
        "items": items,
    }

    # 保存回测记录到数据库
    try:
        sec_mode = "triple" if use_3day else "dual"
        if not allow_fallback and use_3day:
            sec_mode = "triple_nofallback"
        save_batch_result(result, {
            "mode": "rerun",
            "strategy": strategy_label,
            "use_tech": use_tech,
            "sector_mode": sec_mode,
            "target_pct": target_pct,
            "stop_pct": stop_pct,
        })
    except Exception as e:
        print(f"  [WARN] 回测记录保存失败: {e}", flush=True)

    return result


def _gen_analysis(verdict, tech, fs, fin, hit_secs, hot_info,
                  base_close, target, stop, max_gain, max_loss,
                  actual_return, window,
                  use_surprise: bool = True, use_news: bool = True,
                  news_bearish: bool = False, news_summary: str = "") -> list[str]:
    """生成文字分析结论"""
    lines = []

    # 分析选项说明
    opts = []
    if use_surprise:
        opts.append("超预期")
    if use_news:
        opts.append("消息面")
    if opts:
        lines.append(f"📋 分析选项：{' + '.join(opts)}已启用")
    else:
        lines.append("📋 分析选项：仅基础财务数据")

    # 技术评分结论
    if tech["total"] >= 75:
        lines.append(f"✅ 技术评分较高（{tech['total']}分），多项强势信号共振：{'、'.join(tech['sigs'][:3])}")
    elif tech["total"] >= 60:
        lines.append(f"📊 技术评分中等（{tech['total']}分），信号：{'、'.join(tech['sigs'][:3]) if tech['sigs'] else '无明显信号'}")
    else:
        lines.append(f"⚠️ 技术评分偏低（{tech['total']}分），技术面偏弱")

    # 基本面评分结论（使用详细理由）
    # surprise 策略：显示超预期信号详情
    surprise_meta = fs.get("surprise_meta")
    if surprise_meta and surprise_meta.get("diff") is not None:
        diff = surprise_meta["diff"]
        if diff > 20:
            lines.append(f"🚀 强超预期（预期差 +{diff:.1f}%）：一致预期增速{surprise_meta.get('expect_yoy',0):.1f}% 远高于 TTM 增速{surprise_meta.get('ttm_yoy',0):.1f}%，存在二次加速信号")
        elif diff > 10:
            lines.append(f"✅ 超预期（预期差 +{diff:.1f}%）：一致预期增速{surprise_meta.get('expect_yoy',0):.1f}% 高于 TTM 增速{surprise_meta.get('ttm_yoy',0):.1f}%")
        elif diff > 0:
            lines.append(f"📊 微超预期（预期差 +{diff:.1f}%）：一致预期略高于历史增速")
        elif diff > -10:
            lines.append(f"📊 增速持平（预期差 {diff:.1f}%）：一致预期与历史增速基本一致")
        else:
            lines.append(f"⚠️ 不及预期（预期差 {diff:.1f}%）：一致预期低于历史增速，需警惕增速放缓")
        org_num = surprise_meta.get("org_num")
        if org_num and diff > 0:
            lines.append(f"📊 {org_num}家机构覆盖，共识度较高")

    if fs.get("reasons"):
        # 如果有具体理由，显示更详细的说明
        if fs["total"] >= 60:
            lines.append(f"✅ 基本面较好（{fs['total']}分）：{'、'.join(fs['sigs'][:3])}")
        elif fs["total"] >= 40:
            lines.append(f"📊 基本面一般（{fs['total']}分）：{'、'.join(fs['sigs'][:2]) if fs['sigs'] else '数据有限'}")
        else:
            # 低分时显示具体原因
            reason_str = "；".join(fs["reasons"])
            lines.append(f"⚠️ 基本面偏弱（{fs['total']}分）：{reason_str}")
    else:
        # 兼容旧版本
        if fs["total"] >= 60:
            lines.append(f"✅ 基本面较好（{fs['total']}分）：{'、'.join(fs['sigs'][:3])}")
        elif fs["total"] >= 40:
            lines.append(f"📊 基本面一般（{fs['total']}分）：{'、'.join(fs['sigs'][:2]) if fs['sigs'] else '数据有限'}")
        else:
            lines.append(f"⚠️ 基本面偏弱（{fs['total']}分）")

    if hit_secs:
        exact_str = "（当日精确匹配）" if hot_info.get("exact") else f"（参考日期：{hot_info.get('ref_date')}）"
        lines.append(f"✅ 命中热门板块 {exact_str}：{'、'.join(hit_secs[:3])}")
    else:
        lines.append("⚠️ 未命中当日热门板块，缺乏板块催化")

    # 验证结论
    if verdict == "success":
        lines.append(f"✅ 验证结论：建议有效！{verdict_day_str()} 触及目标位 {target:.2f}，"
                     f"最大涨幅 +{max_gain:.1f}%")
    elif verdict == "failure":
        lines.append(f"❌ 验证结论：止损触发！触及止损位 {stop:.2f}，"
                     f"最大跌幅 {max_loss:.1f}%")
        # 分析失败原因
        if tech["total"] >= 70:
            lines.append("🔍 分析：技术面评分较高但未能兑现，可能受大盘环境或突发利空影响")
        if not hit_secs:
            lines.append("🔍 缺乏板块驱动，纯技术面行情持续性不足")
    elif verdict == "pending":
        lines.append(f"⏳ 验证结论：窗口内未触及目标/止损。"
                     f"期间最高+{max_gain:.1f}%，最低{max_loss:.1f}%，"
                     f"最终收益{actual_return:+.1f}%")
    elif verdict == "insufficient":
        lines.append(f"⏳ 数据不足：距基准日未满{window}个交易日，暂无完整验证结果")

    # 因子分析
    if verdict in ("success", "failure", "pending"):
        if tech["trend"] >= 20 and verdict == "success":
            lines.append("🔍 趋势结构贡献突出，多头排列格局在验证期持续有效")
        if tech["volume"] >= 15 and verdict == "success":
            lines.append("🔍 量能放大配合良好，是本次上涨的重要支撑")
        if tech["momentum"] >= 20 and verdict == "failure":
            lines.append("🔍 动量指标虽强，但未能持续，需结合大盘环境综合判断")

    return [l for l in lines if l]


def verdict_day_str():
    return ""


# ══════════════════════════════════════════════════════════
# 8. HTTP 服务
# ══════════════════════════════════════════════════════════
HTML_PAGE = None   # 延迟加载


def get_html_page():
    global HTML_PAGE
    if HTML_PAGE is None:
        html_path = os.path.join(BASE_DIR, "backtest_ui.html")
        if os.path.exists(html_path):
            with open(html_path, encoding="utf-8") as f:
                HTML_PAGE = f.read()
        else:
            HTML_PAGE = "<h1>前端页面未找到，请确认 backtest_ui.html 存在</h1>"
    return HTML_PAGE


class BacktestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass   # 静默日志

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self._send_html(get_html_page())
        elif parsed.path == "/health":
            self._send_json({"status": "ok"})
        elif parsed.path == "/reports":
            # 列出 picks/ 目录下所有 HTML 报告，按时间倒序，优先 report_
            picks_dir = os.path.join(BASE_DIR, "picks")
            try:
                all_files = [(f, os.path.getmtime(os.path.join(picks_dir, f)))
                             for f in os.listdir(picks_dir) if f.endswith(".html")]
                def sort_key(item):
                    name, mtime = item
                    is_report = 0 if name.startswith("report_") else 1
                    return (is_report, -mtime)
                files = [item[0] for item in sorted(all_files, key=sort_key)]
                result = [{"name": f, "url": f"/picks/{f}"} for f in files]
                self._send_json(result)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
        elif parsed.path == "/latest":
            # 重定向到最新报告（按修改时间倒序，优先 report_*.html）
            picks_dir = os.path.join(BASE_DIR, "picks")
            try:
                all_files = [(f, os.path.getmtime(os.path.join(picks_dir, f)))
                             for f in os.listdir(picks_dir) if f.endswith(".html")]
                # 优先 report_ 开头的，再按时间倒序
                def sort_key(item):
                    name, mtime = item
                    is_report = 0 if name.startswith("report_") else 1
                    return (is_report, -mtime)
                files = [item[0] for item in sorted(all_files, key=sort_key)]
                if files:
                    self.send_response(302)
                    self.send_header("Location", f"/picks/{files[0]}")
                    self._cors_headers()
                    self.end_headers()
                else:
                    self._send_json({"error": "暂无报告"}, 404)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
        elif parsed.path.startswith("/picks/"):
            # 静态文件托管：从 picks/ 目录提供 HTML/Excel/EBK 文件
            filename = parsed.path[len("/picks/"):]
            # 安全检查：不允许路径穿越
            filename = os.path.basename(filename)
            filepath = os.path.join(BASE_DIR, "picks", filename)
            if os.path.isfile(filepath):
                ext = os.path.splitext(filename)[1].lower()
                mime_map = {
                    ".html": "text/html; charset=utf-8",
                    ".htm":  "text/html; charset=utf-8",
                    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    ".ebk":  "application/octet-stream",
                    ".css":  "text/css; charset=utf-8",
                    ".js":   "application/javascript; charset=utf-8",
                }
                content_type = mime_map.get(ext, "application/octet-stream")
                with open(filepath, "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self._cors_headers()
                self.end_headers()
                self.wfile.write(data)
            else:
                self._send_404()
        elif parsed.path == "/search":
            # 股票名称/代码模糊搜索：先查自选股，不足8条再补查全市场 t_sector_stock
            qs = parse_qs(parsed.query)
            q  = qs.get("q", [""])[0].strip()
            if not q:
                self._send_json([])
                return
            try:
                _conn = sqlite3.connect(DB_FILE)
                seen = set()
                result = []

                def _add_rows(rows):
                    for r in rows:
                        if r[0] not in seen and len(result) < 8:
                            seen.add(r[0])
                            result.append({"code": r[0], "name": r[1]})

                if q.isdigit():
                    # 代码前缀匹配：先自选股，再全市场
                    _add_rows(_conn.execute(
                        "SELECT code, name FROM qs_ebk_stocks WHERE code LIKE ? ORDER BY date DESC LIMIT 8",
                        (f"{q}%",)
                    ).fetchall())
                    if len(result) < 8:
                        _add_rows(_conn.execute(
                            "SELECT DISTINCT stock_code, stock_name FROM t_sector_stock WHERE stock_code LIKE ? LIMIT ?",
                            (f"{q}%", 8 - len(result))
                        ).fetchall())
                else:
                    # 名称模糊匹配：先自选股，再全市场
                    _add_rows(_conn.execute(
                        "SELECT code, name FROM qs_ebk_stocks WHERE name LIKE ? ORDER BY date DESC LIMIT 8",
                        (f"%{q}%",)
                    ).fetchall())
                    if len(result) < 8:
                        _add_rows(_conn.execute(
                            "SELECT DISTINCT stock_code, stock_name FROM t_sector_stock WHERE stock_name LIKE ? LIMIT ?",
                            (f"%{q}%", 8 - len(result))
                        ).fetchall())

                _conn.close()
                self._send_json(result)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
        else:
            self._send_404()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/analyze":
            length  = int(self.headers.get("Content-Length", 0))
            body    = self.rfile.read(length)
            try:
                params = json.loads(body)
            except Exception:
                self._send_json({"error": "请求格式错误"}, 400)
                return

            code     = str(params.get("code", "")).strip().lstrip("szSZshSH")
            date_str = str(params.get("date", "")).strip()
            window   = int(params.get("window", 5))
            window   = max(3, min(window, 22))
            strategy = str(params.get("strategy", DEFAULT_STRATEGY)).strip()
            # 支持组合策略（逗号分隔），仅验证各子策略合法性
            strategy_parts = [s.strip() for s in strategy.split(",")]
            if not all(s in STRATEGIES for s in strategy_parts if s):
                strategy = DEFAULT_STRATEGY
            _tp = params.get("target_pct")
            _sp = params.get("stop_pct")
            target_pct = float(_tp if _tp is not None else params.get("target", 15.0))
            stop_pct   = float(_sp if _sp is not None else params.get("stop",   5.0))
            target_pct = max(5.0, min(target_pct, 50.0))
            stop_pct   = max(2.0, min(stop_pct,   30.0))

            if not code or not date_str:
                self._send_json({"error": "缺少 code 或 date 参数"}, 400)
                return

            # ── 名称→代码转换 ─────────────────────────────────
            # 若输入不是纯数字，则按名称模糊查询（先自选股，再全市场）
            if not code.isdigit():
                try:
                    _conn = sqlite3.connect(DB_FILE)
                    # 先查自选股
                    rows = _conn.execute(
                        "SELECT code, name FROM qs_ebk_stocks WHERE name LIKE ? ORDER BY date DESC LIMIT 5",
                        (f"%{code}%",)
                    ).fetchall()
                    # 去重后不足5条，补查全市场
                    seen_codes = {r[0] for r in rows}
                    if len(rows) < 5:
                        extra = _conn.execute(
                            "SELECT DISTINCT stock_code, stock_name FROM t_sector_stock WHERE stock_name LIKE ? LIMIT ?",
                            (f"%{code}%", 5 - len(rows))
                        ).fetchall()
                        for r in extra:
                            if r[0] not in seen_codes:
                                rows.append(r)
                                seen_codes.add(r[0])
                    _conn.close()
                    # 去重（qs_ebk_stocks 同一代码多日记录）
                    seen2, deduped = set(), []
                    for r in rows:
                        if r[0] not in seen2:
                            seen2.add(r[0])
                            deduped.append(r)
                    rows = deduped
                    if not rows:
                        self._send_json({"error": f"未找到名称包含「{code}」的股票，请尝试输入6位代码"}, 404)
                        return
                    if len(rows) > 1:
                        # 优先精确匹配
                        exact = [r for r in rows if r[1] == code]
                        if exact:
                            code = exact[0][0]
                        else:
                            # 返回候选列表让前端提示
                            candidates = [{"code": r[0], "name": r[1]} for r in rows]
                            self._send_json({"error": f"找到多只匹配股票，请选择", "candidates": candidates}, 200)
                            return
                    else:
                        code = rows[0][0]
                except Exception as ne:
                    self._send_json({"error": f"名称查询失败：{ne}"}, 500)
                    return
            # ─────────────────────────────────────────────────

            # 读取超预期和消息面选项
            use_surprise = params.get("use_surprise", True)
            use_news = params.get("use_news", True)
            surprise_mode = str(params.get("surprise_mode", "forward")).strip()
            qdiff_mode = str(params.get("qdiff_mode", "quarter")).strip()

            try:
                result = analyze(code, date_str, window, strategy=strategy,
                                 target_pct=target_pct, stop_pct=stop_pct,
                                 use_surprise=use_surprise, use_news=use_news,
                                 surprise_mode=surprise_mode,
                                 qdiff_mode=qdiff_mode)
            except Exception as e:
                import traceback
                result = {"error": str(e), "detail": traceback.format_exc()}

            self._send_json(result)
        elif parsed.path == "/batch":
            length   = int(self.headers.get("Content-Length", 0))
            body     = self.rfile.read(length)
            try:
                params = json.loads(body)
            except Exception:
                self._send_json({"error": "请求格式错误"}, 400)
                return

            date_str = str(params.get("date", "")).strip()
            window   = int(params.get("window", 5))
            window   = max(3, min(window, 22))
            strategy = str(params.get("strategy", DEFAULT_STRATEGY)).strip()
            top_n    = params.get("top_n")
            target_pct = float(params.get("target_pct", 15.0))
            stop_pct   = float(params.get("stop_pct", 5.0))
            tech_filter = bool(params.get("tech_filter", False))
            if top_n is not None:
                try:
                    top_n = int(top_n)
                except Exception:
                    top_n = None
            strategy_parts = [s.strip() for s in strategy.split(",")]
            if not all(s in STRATEGIES for s in strategy_parts if s):
                strategy = DEFAULT_STRATEGY
            if not date_str:
                self._send_json({"error": "缺少 date 参数"}, 400)
                return

            # 查可用日期 + 每日期对应策略列表
            if date_str == "list":
                try:
                    _conn = sqlite3.connect(DB_FILE)
                    dates = [r[0] for r in _conn.execute(
                        "SELECT DISTINCT date FROM qs_picks ORDER BY date DESC LIMIT 60"
                    ).fetchall()]
                    # 单次查询：每个日期的策略 + 数量，按数量降序
                    rows = _conn.execute(
                        "SELECT date, strategy, COUNT(*) as cnt "
                        "FROM qs_picks WHERE strategy IS NOT NULL AND strategy!='' "
                        "  AND rank_no IS NOT NULL AND rank_no > 0 "
                        "GROUP BY date, strategy ORDER BY date DESC, cnt DESC"
                    ).fetchall()
                    sbd = {}
                    for d, s, cnt in rows:
                        sbd.setdefault(d, []).append(s)
                    _conn.close()
                    self._send_json({"dates": dates, "strategies_by_date": sbd})
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)
                return

            try:
                result = batch_analyze(date_str, window, strategy=strategy, top_n=top_n,
                                       target_pct=target_pct, stop_pct=stop_pct,
                                       tech_filter=tech_filter)
            except Exception as e:
                import traceback
                result = {"error": str(e), "detail": traceback.format_exc()}
            self._send_json(result)

        elif parsed.path == "/rerun":
            length   = int(self.headers.get("Content-Length", 0))
            body     = self.rfile.read(length)
            try:
                params = json.loads(body)
            except Exception:
                self._send_json({"error": "请求格式错误"}, 400)
                return

            date_str = str(params.get("date", "")).strip()
            window   = int(params.get("window", 5))
            window   = max(3, min(window, 22))
            strategy = str(params.get("strategy", DEFAULT_STRATEGY)).strip()
            top_n    = int(params.get("top_n", 20))
            target_pct = float(params.get("target_pct", 15.0))
            stop_pct   = float(params.get("stop_pct", 5.0))
            use_tech   = bool(params.get("use_tech", True))  # 新增：是否使用技术评分
            use_3day   = bool(params.get("use_3day", True))  # 新增：是否启用三源交集
            allow_fallback = bool(params.get("allow_fallback", True))  # 新增：是否允许退化
            surprise_mode = str(params.get("surprise_mode", "forward")).strip()
            qdiff_mode = str(params.get("qdiff_mode", "quarter")).strip()
            
            # 验证策略（支持预定义组合名）
            if strategy not in STRATEGY_COMBOS:
                strategy_parts = [s.strip() for s in strategy.split(",")]
                if not all(s in STRATEGIES for s in strategy_parts if s):
                    strategy = DEFAULT_STRATEGY

            if not date_str:
                self._send_json({"error": "缺少 date 参数"}, 400)
                return

            # 查询可用日期（qs_ebk_stocks 有记录的日期）
            if date_str == "list":
                try:
                    _conn = sqlite3.connect(DB_FILE)
                    dates = [r[0] for r in _conn.execute(
                        "SELECT DISTINCT date FROM qs_ebk_stocks ORDER BY date DESC LIMIT 60"
                    ).fetchall()]
                    _conn.close()
                    self._send_json({"dates": dates})
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)
                return

            try:
                result = rerun(date_str, window, top_n=top_n, strategy=strategy,
                               use_tech=use_tech, use_3day=use_3day,
                               allow_fallback=allow_fallback,
                               target_pct=target_pct, stop_pct=stop_pct,
                               surprise_mode=surprise_mode,
                               qdiff_mode=qdiff_mode)
            except Exception as e:
                import traceback
                result = {"error": str(e), "detail": traceback.format_exc()}
            self._send_json(result)

        else:
            self._send_404()

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_html(self, html: str):
        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, data: dict, code: int = 200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_404(self):
        self._send_json({"error": "Not Found"}, 404)


def serve(port: int = SERVE_PORT):
    server = HTTPServer(("127.0.0.1", port), BacktestHandler)
    bt_url = f"http://localhost:{port}/picks/backtest_ui.html"
    print(f"\n  ╔══════════════════════════════════════════════╗")
    print(f"  ║  回测诊断 + 报告服务  已启动                 ║")
    print(f"  ╠══════════════════════════════════════════════╣")
    print(f"  ║  回测工具：{bt_url}")
    print(f"  ║  最新报告：http://localhost:{port}/latest     ║")
    print(f"  ║  报告列表：http://localhost:{port}/reports    ║")
    print(f"  ╚══════════════════════════════════════════════╝")
    print(f"  按 Ctrl+C 停止服务\n")
    # 自动打开回测工具
    try:
        import webbrowser
        print(f"  自动打开回测工具...\n")
        webbrowser.open(bt_url)
    except Exception:
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  [停止] 服务已关闭")
        server.server_close()


# ══════════════════════════════════════════════════════════
# 命令行入口
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="个股回测诊断 + 历史重跑")
    parser.add_argument("--serve",       action="store_true", help="启动本地HTTP服务")
    parser.add_argument("--port",        type=int, default=SERVE_PORT)
    parser.add_argument("--code",        type=str, help="股票代码（6位）")
    parser.add_argument("--date",        type=str, help="基准日期 YYYY-MM-DD 或 YYYYMMDD")
    parser.add_argument("--window",      type=int, default=5, help="验证窗口（交易日，默认5）")
    parser.add_argument("--target",      type=float, default=15.0, help="止盈目标%%（默认15）")
    parser.add_argument("--stop",        type=float, default=5.0,  help="止损幅度%%（默认5）")
    parser.add_argument("--batch-date",  type=str, dest="batch_date",
                        help="批量回测（mode A）：指定日期读 qs_picks 精选全部分析")
    parser.add_argument("--rerun-date",  type=str, dest="rerun_date",
                        help="历史重跑（mode B）：指定日期重走完整筛选流程")
    parser.add_argument("--top-n",       type=int, dest="top_n", default=None,
                        help="批量/重跑时只取前 N 只（默认全部）")
    parser.add_argument("--fund-strategy", type=str, dest="fund_strategy", default=None,
                        help=f"基本面策略（default: {DEFAULT_STRATEGY}）。支持: classic/growth/surprise/classic_surprise/growth_surprise")
    parser.add_argument("--skip-tech",   action="store_true", dest="skip_tech",
                        help="纯基本面模式：跳过技术评分（提速）")
    args = parser.parse_args()

    strategy = args.fund_strategy or DEFAULT_STRATEGY
    use_tech = not args.skip_tech

    if args.serve:
        serve(args.port)
    elif args.rerun_date:
        top_n = args.top_n or 30
        mode_label = "纯基本面" if not use_tech else "技术+基本面"
        print(f"\n[历史重跑] 日期: {args.rerun_date}  窗口: {args.window} 交易日  "
              f"策略: {strategy}  模式: {mode_label}  目标: +{args.target}%  止损: -{args.stop}%  TOP{top_n}", flush=True)
        result = rerun(args.rerun_date, args.window, top_n=top_n, strategy=strategy,
                       use_tech=use_tech,
                       target_pct=args.target, stop_pct=args.stop)
        summary = result.get("summary", {})
        items   = result.get("items", [])
        fi      = result.get("filter_info", {})
        if result.get("error"):
            print(f"错误：{result['error']}")
        else:
            print(f"\n候选: {fi.get('candidates')}只 → 板块预筛{fi.get('sector_filtered')}只 "
                  f"→ 基本面过滤{fi.get('skipped_fund')}只 → 技术过滤{fi.get('skipped_tech',0)}只 → 精选{fi.get('selected')}只")
            cnt = summary.get("counts", {})
            print(f"胜率: {summary.get('win_rate')}%  "
                  f"成功: {cnt.get('success')}  失败: {cnt.get('failure')}  "
                  f"待定: {cnt.get('pending')}  数据不足: {cnt.get('insufficient')}")
            print(f"平均收益: {summary.get('avg_return')}%  "
                  f"最大: {summary.get('max_return')}%  最小: {summary.get('min_return')}%")
            print()
            for it in items[:20]:
                v = it.get("verdict", {})
                tech_str = f"技{it['tech_score']:3d} " if it.get('tech_score') is not None else "技-- "
                print(f"  [{it['final_score']:5.1f}] {it['code']} {it['name'][:8]:8s} "
                      f"{tech_str}基{it['fund_score']:4.0f} "
                      f"板块:{','.join(it['hit_sectors'][:2])}  "
                      f"→ {v.get('status','?')} ({v.get('actual_return','-')}%)")
    elif args.batch_date:
        print(f"\n[批量回测] 日期: {args.batch_date}  窗口: {args.window} 交易日  "
              f"策略: {STRATEGIES.get(strategy, strategy)}  "
              f"目标: +{args.target}%  止损: -{args.stop}%", flush=True)
        result = batch_analyze(args.batch_date, args.window, strategy=strategy,
                                top_n=args.top_n, target_pct=args.target, stop_pct=args.stop)
        summary = result.get("summary", {})
        items   = result.get("items", [])
        if result.get("error"):
            print(f"错误：{result['error']}")
        else:
            cnt = summary.get("counts", {})
            print(f"\n─── 汇总 ───────────────────────────────────────────")
            print(f"  共分析   : {summary['total']} 只")
            print(f"  成功     : {cnt.get('success',0)} 只（达目标 +15%）")
            print(f"  失败     : {cnt.get('failure',0)} 只（触止损 -7%）")
            print(f"  待定     : {cnt.get('pending',0)} 只（窗口内未触发）")
            print(f"  数据不足 : {cnt.get('insufficient',0)} 只")
            if summary.get("win_rate") is not None:
                print(f"  胜率     : {summary['win_rate']}%  "
                      f"（{cnt.get('success',0)}/{summary['decided']} 已有结果）")
            if summary.get("avg_return") is not None:
                print(f"  平均收益 : {summary['avg_return']:+.2f}%")
                print(f"  最大收益 : {summary['max_return']:+.2f}%")
                print(f"  最大亏损 : {summary['min_return']:+.2f}%")
            print(f"────────────────────────────────────────────────────")
            status_map = {
                "success":      "[+] 达目标",
                "failure":      "[-] 触止损",
                "pending":      "[?] 待定  ",
                "insufficient": "[.] 数据不足",
                "error":        "[!] 分析错误",
            }
            print(f"\n{'排名':>4} {'代码':>7} {'名称':<8} {'得分':>6} {'结果':<12} {'收益':>7}")
            print(f"{'─'*52}")
            for it in items:
                v = it.get("verdict", {})
                status_str = status_map.get(v.get("status",""), v.get("status",""))
                ret_str = f"{v['actual_return']:+.2f}%" if isinstance(v.get("actual_return"), (int,float)) else "N/A"
                print(f"  {it.get('pick_rank','?'):>3} {it.get('code',''):>7} "
                      f"{(it.get('name','')[:6]):<8} "
                      f"{it.get('pick_final_score',0):>6.1f} "
                      f"{status_str:<12} {ret_str:>7}")
    elif args.code and args.date:
        result = analyze(args.code, args.date, args.window, strategy=strategy,
                         target_pct=args.target, stop_pct=args.stop)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        parser.print_help()
