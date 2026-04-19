"""
report.py  —— 精选报告生成
------------------------------------------------------------
功能：
  从 qs_picks 表取当日评分数据，生成：
  1. 亮色系 HTML 报告（TOP10 卡片 + 完整汇总表）
  2. Excel 报告（含操作建议，所有候选/精选均可）
  3. 通达信 EBK 自选股文件

用法：
  python report.py                       # 今日 TOP30 全部输出
  python report.py --date 20260326       # 指定日期
  python report.py --top 20              # 指定输出 TOP N
  python report.py --no-html             # 跳过 HTML
  python report.py --no-excel            # 跳过 Excel
  python report.py --no-ebk             # 跳过 EBK
------------------------------------------------------------
"""

import os, re, sys, sqlite3, argparse, html as _html
from datetime import date, datetime

import pandas as pd

# ── 路径配置 ─────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR    = os.path.join(_SCRIPT_DIR, "picks")   # 所有生成文件统一到 ./picks/ 子目录
DB_FILE     = os.path.join(_SCRIPT_DIR, "..", "db", "concept_weekly.db")
os.makedirs(BASE_DIR, exist_ok=True)

# ── 技术过滤配置（与 score.py 一致，回测验证：胜率34.5%→74.2%）────────
TECH_FILTER_ENABLED = True
TECH_FILTER_POS_MIN = 0.70    # 52周位置下限
TECH_FILTER_VOL_MAX = 1.3     # 5日/20日均量比上限


# ══════════════════════════════════════════════════════════
# 辅助函数
# ══════════════════════════════════════════════════════════
def fmt(v, decimals=2, suffix="", default="-"):
    try:
        f = float(v)
        if f != f: return default   # NaN
        return f"{f:.{decimals}f}{suffix}"
    except Exception:
        return str(v) if v not in (None, "", "nan", "None") else default


def chg_color(v):
    try:
        return "red" if float(v) >= 0 else "green"
    except Exception:
        return ""


def format_fund_tags(fund_sigs: str, surprise_meta, mcap) -> str:
    """
    格式化基本面信号标签
    按类别分组：估值类、成长类、超预期类（更醒目）
    """
    if not fund_sigs or fund_sigs == "nan":
        return ""
    
    # 分类存储
    valuation_tags = []   # 估值类
    growth_tags = []      # 成长类
    surprise_tags = []    # 超预期类
    other_tags = []       # 其他
    
    # 按分隔符分割信号
    sigs = re.split(r"[|/，、]", fund_sigs)
    for sg in sigs:
        sg = sg.strip()
        if not sg:
            continue
        
        # 判断类别
        if "预期" in sg or "surprise" in sg.lower() or "预期差" in sg:
            surprise_tags.append(sg)
        elif any(k in sg for k in ["估值", "PE", "PEG", "PB", "便宜", "合理"]):
            valuation_tags.append(sg)
        elif any(k in sg for k in ["增速", "增长", "同比", "净利", "营收", "成长"]):
            growth_tags.append(sg)
        elif "ROE" in sg or "盈利" in sg or "利润" in sg:
            growth_tags.append(sg)
        elif "市值" in sg:
            valuation_tags.append(sg)
        else:
            other_tags.append(sg)
    
    # 构建HTML，按类别分组
    html_parts = []
    
    # 估值类（蓝色）
    if valuation_tags:
        for sg in valuation_tags:
            html_parts.append(f'<span class="tag" style="background:#dbeafe;color:#1e40af;border:1px solid #93c5fd">{_html.escape(sg)}</span>')
    
    # 成长类（绿色）
    if growth_tags:
        for sg in growth_tags:
            html_parts.append(f'<span class="tag" style="background:#dcfce7;color:#166534;border:1px solid #86efac">{_html.escape(sg)}</span>')
    
    # 其他（灰色）
    if other_tags:
        for sg in other_tags:
            html_parts.append(f'<span class="tag" style="background:#f3f4f6;color:#374151;border:1px solid #d1d5db">{_html.escape(sg)}</span>')
    
    # 超预期类（橙色，加粗，更醒目 - 放最后突出显示）
    if surprise_tags:
        for sg in surprise_tags:
            html_parts.append(f'<span class="tag" style="background:#ff7d00;color:#fff;border:2px solid #ff9a3c;font-weight:bold;padding:3px 10px;box-shadow:0 2px 4px rgba(255,125,0,0.3)">{_html.escape(sg)}</span>')
    
    return "".join(html_parts)


def code_to_ebk(code: str) -> str:
    """
    将股票代码转为通达信 EBK 格式（7位：市场前缀 + 6位代码）
    支持输入：纯6位(603558)、sz/sh前缀(sz000001)、已含7位前缀(0000001)
    市场判断规则：
      深圳(0)：000/001/002/003/300/301/399 开头
      上海(1)：600/601/603/605/688/900 开头
    """
    code = str(code).strip().lower()
    # 已带 sz/sh 前缀
    if code.startswith("sz"):
        return "0" + code[2:]
    if code.startswith("sh"):
        return "1" + code[2:]
    # 已是7位
    if len(code) == 7 and code[0] in ("0", "1"):
        return code
    # 纯6位，按规则推断市场
    if len(code) == 6:
        pfx = code[:3]
        if pfx in ("000", "001", "002", "003", "300", "301") or code[:1] == "3" or pfx == "399":
            return "0" + code   # 深圳
        if pfx in ("600", "601", "603", "605", "688", "900"):
            return "1" + code   # 上海
        # 其余默认深圳
        return "0" + code
    return ""


# ══════════════════════════════════════════════════════════
# 1. 从数据库读取数据
# ══════════════════════════════════════════════════════════
def load_picks(conn: sqlite3.Connection, date_str: str, top_n: int,
               strategy: str = "classic",
               surprise_only: bool = False, no_top_limit: bool = False) -> pd.DataFrame:
    # 兼容旧表：先查实际列名，再决定排序字段
    cur = conn.execute("PRAGMA table_info(qs_picks)")
    cols = {r[1] for r in cur.fetchall()}

    order_col = "final_score" if "final_score" in cols else "total_score"

    # 按 strategy 过滤；如果表中没有 strategy 列（极旧数据），则不过滤
    if "strategy" in cols and strategy:
        df = pd.read_sql(
            f"SELECT * FROM qs_picks WHERE date=? AND strategy=? ORDER BY {order_col} DESC, total_score DESC",
            conn, params=(date_str, strategy)
        )
    else:
        df = pd.read_sql(
            f"SELECT * FROM qs_picks WHERE date=? ORDER BY {order_col} DESC, total_score DESC",
            conn, params=(date_str,)
        )
    if df.empty:
        return df
    # 补全新脚本期望但旧表可能缺失的列
    for c in ("final_score", "matched_sectors", "hot_score", "sector_count",
              "fin_source", "fund_sigs", "fund_reasons", "surprise_meta",
              "tech_filter_pos", "tech_filter_vol", "tech_filter_passed"):
        if c not in df.columns:
            df[c] = None
    if "final_score" in df.columns and df["final_score"].isna().all():
        df["final_score"] = df["total_score"]

    if surprise_only or no_top_limit:
        # 不按 TOP 截断——全部已保存的 surprise 命中结果都展示
        pass
    else:
        df = df.head(top_n).copy()

    df["rank_no"] = range(1, len(df) + 1)
    return df


def load_sectors(conn: sqlite3.Connection, date_str: str) -> list[dict]:
    cur = conn.execute(
        "SELECT sector_rank, sector_name, stock_count, ratio "
        "FROM qs_trend_sectors WHERE date=? ORDER BY sector_rank",
        (date_str,)
    )
    return [{"rank": r[0], "name": r[1], "count": r[2], "ratio": r[3]}
            for r in cur.fetchall()]


# ══════════════════════════════════════════════════════════
# 2. HTML 报告
# ══════════════════════════════════════════════════════════
STYLE = """
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Microsoft YaHei',Arial,sans-serif;background:#f0f4f8;color:#2d3748;font-size:14px}
.header{background:linear-gradient(135deg,#1a56db 0%,#0e9f6e 100%);color:#fff;padding:28px 40px}
.header h1{font-size:26px;font-weight:700;letter-spacing:1px}
.header .sub{margin-top:6px;font-size:13px;opacity:.85}
.wrap{max-width:1320px;margin:0 auto;padding:20px 24px}

.sector-bar{display:flex;flex-wrap:wrap;gap:8px;margin:16px 0 24px}
.sec-tag{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:20px;
  font-size:12px;font-weight:600;cursor:default}
.sec-tag.rank1{background:#fee2e2;color:#b91c1c}
.sec-tag.rank2{background:#fef3c7;color:#92400e}
.sec-tag.rank3{background:#d1fae5;color:#065f46}
.sec-tag.rank-other{background:#e0e7ff;color:#3730a3}
.sec-badge{background:rgba(0,0,0,.12);border-radius:10px;padding:1px 6px;font-size:11px}

.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px}
.stat-card{background:#fff;border-radius:12px;padding:16px 20px;
  box-shadow:0 1px 4px rgba(0,0,0,.08)}
.stat-card .num{font-size:28px;font-weight:700;color:#1a56db}
.stat-card .label{font-size:12px;color:#6b7280;margin-top:4px}

.section-title{font-size:17px;font-weight:700;color:#1a56db;
  border-left:4px solid #1a56db;padding-left:10px;margin:28px 0 14px}
.cards-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
.card{background:#fff;border-radius:14px;padding:18px 20px;
  box-shadow:0 2px 8px rgba(0,0,0,.07);border-top:4px solid #1a56db;transition:box-shadow .2s}
.card:hover{box-shadow:0 6px 20px rgba(0,0,0,.12)}
.card-head{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px}
.card-name{font-size:16px;font-weight:700}
.card-code{font-size:12px;color:#9ca3af;margin-top:2px}
.score-badge{text-align:center;background:linear-gradient(135deg,#1a56db,#3b82f6);
  color:#fff;border-radius:10px;padding:4px 12px;min-width:58px}
.score-badge .score{font-size:20px;font-weight:700}
.score-badge .slabel{font-size:10px;opacity:.8}
.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:10px 0}
.m-item{background:#f8fafc;border-radius:8px;padding:6px 8px;text-align:center}
.m-val{font-size:14px;font-weight:600;color:#1e40af}
.m-key{font-size:10px;color:#9ca3af;margin-top:1px}
.red{color:#dc2626!important} .green{color:#059669!important}
.tag-row{display:flex;flex-wrap:wrap;gap:4px;margin:8px 0}
.tag{font-size:11px;padding:2px 8px;border-radius:10px;background:#eff6ff;
  color:#1d4ed8;font-weight:500}
.tag.hot{background:#fef9c3;color:#854d0e}
.tag.gray{background:#f3f4f6;color:#9ca3af}
.adv-box{background:#f0fdf4;border-radius:8px;padding:10px 12px;
  margin-top:10px;font-size:12px}
.adv-title{color:#065f46;font-weight:700;margin-bottom:4px}
.adv-row{display:flex;flex-wrap:wrap;gap:10px;margin-top:4px}
.adv-item{display:flex;flex-direction:column;align-items:center}
.av{font-weight:700;color:#0f766e;font-size:13px}
.ak{font-size:10px;color:#6b7280}

.table-wrap{overflow-x:auto;margin-top:8px}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:12px;
  overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)}
th{background:#1a56db;color:#fff;padding:10px 12px;text-align:center;
  font-size:13px;font-weight:600;white-space:nowrap}
td{padding:9px 12px;text-align:center;font-size:13px;border-bottom:1px solid #f1f5f9}
tr:last-child td{border-bottom:none}
tr:hover td{background:#eff6ff}
tr:nth-child(even) td{background:#f8fafc}
tr:nth-child(even):hover td{background:#eff6ff}
.rank-badge{display:inline-block;background:#1a56db;color:#fff;border-radius:50%;
  width:22px;height:22px;line-height:22px;font-size:12px;font-weight:700}
.fin-src{font-size:10px;color:#9ca3af;margin-left:4px}

footer{text-align:center;padding:24px;color:#9ca3af;font-size:12px}
</style>
"""

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


def get_stock_sectors(conn: sqlite3.Connection, code: str, hot_set: set) -> tuple[list, list]:
    """
    从 sector_stocks 查该股所有板块，返回 (命中热门板块列表, 其他板块列表)
    code 支持 6 位或带市场前缀
    """
    code6 = str(code).strip()
    # 去掉市场前缀（sz/sh）如有
    if code6.lower().startswith(("sz", "sh")):
        code6 = code6[2:]

    cur = conn.execute(
        "SELECT s.sector_name FROM t_sector_stock ss "
        "JOIN t_sector s ON s.sector_code = ss.sector_code "
        "WHERE ss.stock_code = ?",
        (code6,)
    )
    all_secs = [r[0] for r in cur.fetchall()]
    hit   = [s for s in all_secs if s in hot_set]
    other = [s for s in all_secs if s not in hot_set]
    return hit, other


def gen_html(date_str: str, df: pd.DataFrame,
             hot_sectors: list[dict], top_n: int, out_path: str,
             conn: sqlite3.Connection = None,
             strategy_label: str = "",
             skip_tech: bool = False,
             surprise_only: bool = False):
    now     = datetime.now().strftime("%Y-%m-%d %H:%M")
    n_hot   = len(hot_sectors)
    hot_set = {s["name"] for s in hot_sectors}   # 当日热门板块名称集合

    # ── 板块标签云 ─────────────────────────────────────────
    tags_html = []
    for s in hot_sectors:
        r   = s["rank"]
        cls = "rank1" if r <= 3 else ("rank2" if r <= 7 else ("rank3" if r <= 12 else "rank-other"))
        tags_html.append(
            f'<span class="sec-tag {cls}">{_html.escape(s["name"])}'
            f'<span class="sec-badge">{s["count"]}只 {s["ratio"]:.1f}%</span></span>'
        )
    sectors_bar = '<div class="sector-bar">' + "".join(tags_html) + "</div>"

    # ── 统计卡片 ────────────────────────────────────────────
    top1_score = fmt(df.iloc[0]["final_score"], 1) if len(df) else "-"
    n_buy = len(df[df["action"].str.contains("买入", na=False)]) if len(df) else 0
    stats_html = f"""
<div class="stats">
  <div class="stat-card"><div class="num">{n_hot}</div><div class="label">今日热门板块数</div></div>
  <div class="stat-card"><div class="num">{len(df)}</div><div class="label">精选标的数</div></div>
  <div class="stat-card"><div class="num">{top1_score}</div><div class="label">第一名综合分</div></div>
  <div class="stat-card"><div class="num">{n_buy}</div><div class="label">买入信号数</div></div>
</div>"""

    # ── TOP9 详情卡片（3×3 九宫格） ─────────────────────────
    cards = []
    for _, row in df.head(9).iterrows():
        rk    = int(row.get("rank_no", 0))
        medal = MEDALS.get(rk, f"#{rk}")
        chg   = fmt(row.get("chg_pct", ""), 2, "%")
        chg_c = chg_color(row.get("chg_pct", ""))
        close_v = fmt(row.get("close", ""), 2)
        close_disp = f"¥{close_v}" if close_v != "-" else "-"

        raw_secs = row.get("matched_sectors", "")
        raw_secs = "" if str(raw_secs).strip() in ("", "None", "nan") else str(raw_secs)

        if conn is not None:
            # 从 DB 查全量板块，hot_set 决定哪些命中热门
            hit_secs, other_secs = get_stock_sectors(conn, row.get("code", ""), hot_set)
        elif raw_secs:
            # 无 DB 连接，只能用 matched_sectors 里已有的命中板块
            hit_secs   = [s for s in raw_secs.split(" / ") if s.strip()]
            other_secs = []
        else:
            hit_secs, other_secs = [], []

        # 命中热门板块 → 黄色标签，其余板块 → 浅灰色标签（最多8个）
        sec_tags = ""
        if hit_secs:
            sec_tags += "".join(
                f'<span class="tag hot">{_html.escape(s)}</span>' for s in hit_secs
            )
        if other_secs:
            show_other = other_secs[:max(0, 8 - len(hit_secs))]
            sec_tags += "".join(
                f'<span class="tag gray">{_html.escape(s)}</span>'
                for s in show_other
            )
        if not sec_tags:
            sec_tags = '<span style="font-size:11px;color:#9ca3af">暂无板块数据</span>'

        tech_sigs = str(row.get("tech_sigs", ""))
        sig_tags  = "".join(
            f'<span class="tag">{_html.escape(sg.strip())}</span>'
            for sg in re.split(r"[|/，、]", tech_sigs) if sg.strip()
        )
        
        # 基本面信号标签（按维度分类整理）
        fund_sigs = str(row.get("fund_sigs", ""))
        fund_tags = format_fund_tags(fund_sigs, row.get("surprise_meta"), row.get("mcap"))
        
        # 基本面评分理由（评分不高时显示）
        fund_reasons = str(row.get("fund_reasons", ""))
        reason_tags = ""
        if fund_reasons and fund_reasons != "nan":
            reasons_list = re.split(r"[|/，、]", fund_reasons)
            reason_tags = "".join(
                f'<span class="tag" style="background:#fee2e2;color:#b91c1c">{_html.escape(sg.strip())}</span>'
                for sg in reasons_list if sg.strip()
            )

        pe_s   = fmt(row.get("pe_ttm", ""), 1)
        mcap_s = fmt(row.get("mcap", ""), 1) + "亿" if row.get("mcap") not in (None, "", "-", "nan") else "-"
        pyoy_s = fmt(row.get("profit_yoy", ""), 1, "%")
        hot_s  = fmt(row.get("hot_score", ""), 1)
        fin_src = str(row.get("fin_source", ""))
        fin_badge = '<span class="fin-src">[本地]</span>' if "local" in fin_src else \
                    '<span class="fin-src">[网络]</span>' if "network" in fin_src else ""

        adv_html = ""
        if row.get("buy_range") and str(row.get("buy_range")) not in ("-", "", "nan"):
            adv_html = f"""
<div class="adv-box">
  <div class="adv-title">操作策略</div>
  <div style="margin-top:4px;color:#1e40af">{_html.escape(str(row.get('action','')))}
    &nbsp;|&nbsp; 仓位 {_html.escape(str(row.get('position_pct','')))}
  </div>
  <div class="adv-row">
    <div class="adv-item"><div class="av">{_html.escape(str(row.get('buy_range','-')))}</div><div class="ak">买点区间</div></div>
    <div class="adv-item"><div class="av">{_html.escape(str(row.get('stop_loss','-')))}</div><div class="ak">止损位</div></div>
    <div class="adv-item"><div class="av">{_html.escape(str(row.get('target','-')))}</div><div class="ak">目标位</div></div>
  </div>
</div>"""

        # 净利同比颜色
        try:
            pyoy_float = float(str(pyoy_s).replace("%", ""))
            pyoy_color = "red" if pyoy_float > 0 else "green"
        except Exception:
            pyoy_color = ""

        # 消息面标签
        news_level = str(row.get("news_level", "") or "")
        news_summary = str(row.get("news_summary", "") or "")
        news_heat = row.get("news_heat", 0) or 0
        
        news_tag = ""
        if news_level == "重大利好":
            news_tag = f'<span class="tag" style="background:#dcfce7;color:#166534;border:1px solid #86efac">重大利好</span>'
        elif news_level == "利好":
            news_tag = f'<span class="tag" style="background:#dbeafe;color:#1e40af;border:1px solid #93c5fd">利好</span>'
        elif news_level == "偏利好":
            news_tag = f'<span class="tag" style="background:#fef9c3;color:#854d0e;border:1px solid #fde047">偏利好</span>'
        elif news_level in ["偏利空", "利空", "重大利空"]:
            news_tag = f'<span class="tag" style="background:#fee2e2;color:#b91c1c;border:1px solid #fca5a5">{news_level}</span>'
        
        news_html = ""
        if news_tag:
            news_summary_short = news_summary[:25] + "..." if len(news_summary) > 25 else news_summary
            news_html = f'''
        <div style="margin:8px 0 4px;font-size:11px;color:#6b7280;font-weight:600">消息面 (+{fmt(news_heat,1)}分)</div>
        <div class="tag-row">{news_tag}<span style="font-size:11px;color:#6b7280;margin-left:8px">{_html.escape(news_summary_short)}</span></div>'''

        cards.append(f"""
<div class="card">
  <div class="card-head">
    <div>
      <div class="card-name">{medal} {_html.escape(str(row.get('name','')))}
        <span style="font-size:13px;color:#6b7280;font-weight:400">
          ({_html.escape(str(row.get('code','')))})
        </span>{fin_badge}
      </div>
      <div class="card-code">{_html.escape(str(row.get('industry','')))}
        &nbsp;·&nbsp; 报告期 {_html.escape(str(row.get('report_period','') or ''))}
      </div>
    </div>
    <div class="score-badge">
      <div class="score">{fmt(row.get('final_score',''),1)}</div>
      <div class="slabel">综合分</div>
    </div>
  </div>
  <div class="metrics">
    <div class="m-item">
      <div class="m-val {chg_c}" style="font-size:13px">{close_disp}</div>
      <div class="m-val {chg_c}" style="font-size:12px;margin-top:1px">{chg}</div>
    </div>
    <div class="m-item"><div class="m-val">{fmt(row.get('total_score',''),1)}</div><div class="m-key">原始评分</div></div>
    <div class="m-item"><div class="m-val" style="color:#0891b2">{hot_s}</div><div class="m-key">板块热度分</div></div>
    <div class="m-item"><div class="m-val">{pe_s}</div><div class="m-key">PE(TTM)</div></div>
    <div class="m-item"><div class="m-val">{mcap_s}</div><div class="m-key">总市值</div></div>
    <div class="m-item"><div class="m-val {pyoy_color}">{pyoy_s}</div><div class="m-key">净利同比</div></div>
  </div>
        <div style="margin:8px 0 4px;font-size:11px;color:#6b7280;font-weight:600">所属板块</div>
        <div class="tag-row">{sec_tags}</div>
        <div style="margin:8px 0 4px;font-size:11px;color:#6b7280;font-weight:600">技术信号</div>
        <div class="tag-row">{sig_tags}</div>
        <div style="margin:8px 0 4px;font-size:11px;color:#6b7280;font-weight:600">基本面信号</div>
        <div class="tag-row">{fund_tags}</div>
        <div style="margin:8px 0 4px;font-size:11px;color:#6b7280;font-weight:600">基本面评分理由</div>
        <div class="tag-row">{reason_tags}</div>
  {news_html}
  {adv_html}
</div>""")

    cards_section = f"""
<div class="section-title">精选 TOP9 详情（完整榜单见下表）</div>
<div class="cards-grid">{"".join(cards)}</div>"""

    # ── 完整汇总表 ───────────────────────────────────────────
    rows_html = []
    for _, row in df.iterrows():
        rk     = int(row.get("rank_no", 0))
        medal  = MEDALS.get(rk, f'<span class="rank-badge">{rk}</span>')
        chg_v  = fmt(row.get("chg_pct", ""), 2, "%")
        chg_c  = chg_color(row.get("chg_pct", ""))
        pyoy   = fmt(row.get("profit_yoy", ""), 1, "%")
        raw_s  = row.get("matched_sectors", "")
        raw_s  = "" if str(raw_s).strip() in ("", "None", "nan") else str(raw_s)

        # 板块：有存储时直接用，否则实时查
        if raw_s:
            hit_secs_t  = [s for s in raw_s.split(" / ") if s.strip()]
            secs_s = " / ".join(hit_secs_t[:3]) + ("…" if len(hit_secs_t) > 3 else "")
        elif conn is not None:
            hit_t, _ = get_stock_sectors(conn, row.get("code", ""), hot_set)
            secs_s = (" / ".join(hit_t[:3]) + ("…" if len(hit_t) > 3 else "")) if hit_t else "-"
        else:
            secs_s = "-"

        action = str(row.get("action", ""))[:8]

        try:
            pyoy_float2 = float(str(pyoy).replace("%", ""))
            pyoy_c2 = "red" if pyoy_float2 > 0 else "green"
        except Exception:
            pyoy_c2 = ""

        # 消息面结论
        news_level = str(row.get("news_level", "") or "")
        if news_level == "重大利好":
            news_conclusion = "重大利好"
            news_style = "background:#dcfce7;color:#166534;font-weight:bold;padding:2px 8px;border-radius:4px;"
        elif news_level == "利好":
            news_conclusion = "利好"
            news_style = "background:#dbeafe;color:#1e40af;font-weight:600;padding:2px 8px;border-radius:4px;"
        elif news_level == "偏利好":
            news_conclusion = "偏利好"
            news_style = "background:#fef9c3;color:#854d0e;font-weight:600;padding:2px 8px;border-radius:4px;"
        elif news_level:
            news_conclusion = news_level
            news_style = "color:#6c757d;"
        else:
            news_conclusion = "-"
            news_style = "color:#adb5bd;"

        # 超预期结论
        surprise_meta = _parse_surprise_meta(row.get("surprise_meta"))
        if surprise_meta.get("expect_yoy") is not None and surprise_meta.get("ttm_yoy") is not None:
            diff = surprise_meta.get("diff", 0)
            if diff > 20:
                surprise_conclusion = "强超预期"
                surprise_style = "background:#ff6b6b;color:#fff;font-weight:bold;padding:2px 8px;border-radius:4px;"
            elif diff > 10:
                surprise_conclusion = "超预期"
                surprise_style = "background:#ff9f43;color:#fff;font-weight:bold;padding:2px 8px;border-radius:4px;"
            elif diff > 0:
                surprise_conclusion = "微超预期"
                surprise_style = "background:#feca57;color:#333;font-weight:600;padding:2px 8px;border-radius:4px;"
            elif diff > -10:
                surprise_conclusion = "持平"
                surprise_style = "color:#6c757d;"
            else:
                surprise_conclusion = "不及预期"
                surprise_style = "color:#adb5bd;"
        else:
            surprise_conclusion = "无数据"
            surprise_style = "color:#adb5bd;"

        rows_html.append(f"""<tr>
  <td>{medal}</td>
  <td style="text-align:left;font-weight:600">{_html.escape(str(row.get('name','')))}
    <br><span style="font-size:11px;color:#9ca3af">{_html.escape(str(row.get('code','')))}</span></td>
  <td style="text-align:left;font-size:12px;color:#4b5563">{_html.escape(secs_s)}</td>
  <td class="{chg_c}" style="font-weight:600">{chg_v}</td>
  <td style="font-weight:700;color:#1a56db">{fmt(row.get('final_score',''),1)}</td>
  <td>{fmt(row.get('total_score',''),1)}</td>
  <td style="color:#0891b2">{fmt(row.get('hot_score',''),1)}</td>
  <td>{fmt(row.get('tech_score',''),0)}</td>
  <td>{fmt(row.get('fund_score',''),0)}</td>
  <td class="{pyoy_c2}">{pyoy}</td>
  <td>{fmt(row.get('pe_ttm',''),1)}</td>
  <td style="font-size:12px"><span style="{surprise_style}">{_html.escape(surprise_conclusion)}</span></td>
  <td style="font-size:12px"><span style="{news_style}">{_html.escape(news_conclusion)}</span></td>
  <td style="font-size:12px">{_html.escape(action)}</td>
</tr>""")

    # 分页逻辑：超过50只时启用分页
    total_rows = len(rows_html)
    page_size = 50
    if total_rows > page_size:
        pages_html = []
        num_pages = (total_rows + page_size - 1) // page_size
        for page_idx in range(num_pages):
            start = page_idx * page_size
            end = min(start + page_size, total_rows)
            page_rows = rows_html[start:end]
            display_style = "block" if page_idx == 0 else "none"
            pages_html.append(f'<div id="page-{page_idx}" class="page-content" style="display:{display_style}"><table><thead><tr><th>排名</th><th>标的</th><th>命中板块</th><th>涨幅</th><th>综合分</th><th>原始分</th><th>热度分</th><th>技术</th><th>基本面</th><th>净利同比</th><th>PE</th><th>超预期</th><th>消息面</th><th>操作建议</th></tr></thead><tbody>{"".join(page_rows)}</tbody></table></div>')
        
        # 分页控件
        page_buttons = " ".join([f'<button class="page-btn" onclick="showPage({i})" id="btn-page-{i}">{i+1}</button>' for i in range(num_pages)])
        pagination_html = f'<div class="pagination" style="margin:16px 0;text-align:center"><span style="color:#666;font-size:13px">共 {total_rows} 条，每页 {page_size} 条</span><div style="margin-top:8px">{page_buttons}</div></div><script>function showPage(n){{document.querySelectorAll(".page-content").forEach(function(el){{el.style.display="none";}});document.querySelectorAll(".page-btn").forEach(function(btn){{btn.classList.remove("active");}});var p=document.getElementById("page-"+n);if(p)p.style.display="block";var b=document.getElementById("btn-page-"+n);if(b)b.classList.add("active");}};document.getElementById("btn-page-0").classList.add("active");</script><style>.page-btn{{background:#fff;border:1px solid #d0d7e3;border-radius:6px;padding:6px 14px;margin:0 4px;cursor:pointer;font-size:13px;color:#555}}.page-btn:hover{{background:#f0f4f8}}.page-btn.active{{background:#1a56db;color:#fff;border-color:#1a56db}}</style>'
        
        table_section = f'<div class="section-title">精选 {len(df)} 完整汇总表（分页）</div><div class="table-wrap">{"".join(pages_html)}</div>{pagination_html}'
    else:
        table_section = f"""
<div class="section-title">精选 TOP{len(df)} 完整汇总表</div>
<div class="table-wrap">
<table>
  <thead><tr>
    <th>排名</th><th>标的</th><th>命中板块</th><th>涨幅</th>
    <th>综合分</th><th>原始分</th><th>热度分</th><th>技术</th><th>基本面</th>
    <th>净利同比</th><th>PE</th><th>超预期</th><th>消息面</th><th>操作建议</th>
  </tr></thead>
  <tbody>{"".join(rows_html)}</tbody>
</table>
</div>"""

    note = """
<div class="section-title">评分说明</div>
<div style="background:#fff;border-radius:12px;padding:16px 20px;font-size:13px;
  line-height:2;color:#374151">
  <b>综合分 = 原始评分×70% + 热度分加权（板块热度+消息面，最高+30）</b><br>
  原始评分 = 技术面×50% + 基本面×50%（基本面<30分直接过滤）<br>
  热度分 = 板块热度分(0-20) + 消息面热度分(0-10)，利空股票直接过滤<br>
  技术面（满分100）：趋势结构40 + 位置形态15 + 动量25 + 量能20<br>
  基本面（满分100）：PE估值25 + 净利同比35 + 营收同比15 + ROE15 + 市值弹性10<br>
  消息面：重大利好+10分、利好+8分、偏利好+5分、中性+2分，利空/重大利空过滤<br>
  本报告仅展示命中当日热门板块且非利空的候选标的，其余个股已过滤。
</div>"""

    body = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>见龙在田·精选报告 {date_str}</title>
{STYLE}
</head>
<body>
<div class="header">
  <h1>见龙在田 · 精选报告</h1>
  <div class="sub">分析日期：{date_str} &nbsp;|&nbsp; 热门板块：{n_hot} 个 &nbsp;|&nbsp;
    精选标的：{len(df)} 只 &nbsp;|&nbsp; 生成时间：{now}{"&nbsp;|&nbsp; 策略：" + _html.escape(strategy_label) if strategy_label else ""}{"&nbsp;|&nbsp; <span style='color:#e67e22;font-weight:600'>纯基本面模式</span>" if skip_tech else ""}{"&nbsp;|&nbsp; <span style='color:#8e44ad;font-weight:600'>⚡ 超预期命中</span>" if surprise_only else ""}</div>
</div>
<div class="wrap">
  <div class="section-title">当日热门板块（按满足率排序）</div>
  {sectors_bar}
  {stats_html}
  {cards_section}
  {table_section}
  {note}
</div>
<footer>见龙在田自选股分析系统 · 仅供参考，不构成投资建议</footer>

<!-- ══ 回测诊断浮动入口（内联 UI，fetch 调后端）══ -->
<style>
.bt-fab{{position:fixed;bottom:28px;right:28px;z-index:9999;background:#f5a623;color:#000;border:none;
  width:52px;height:52px;border-radius:50%;font-size:22px;cursor:pointer;
  box-shadow:0 4px 18px rgba(0,0,0,.35);display:flex;align-items:center;justify-content:center;
  transition:transform .2s,box-shadow .2s;}}
.bt-fab:hover{{transform:scale(1.1);box-shadow:0 6px 24px rgba(0,0,0,.45);}}
/* 弹窗直接 fixed 定位，不用遮罩层 */
.bt-modal{{
  display:none;position:fixed;z-index:10000;
  top:60px;left:50%;transform:translateX(-50%);
  background:#fff;border:1px solid #d0d7e3;border-radius:14px;
  width:min(960px,96vw);
  min-height:440px;
  max-height:92vh;
  box-shadow:0 8px 40px rgba(0,0,0,.18);
  flex-direction:column;overflow:hidden;
  transition:max-height .3s ease;
}}
.bt-modal.active{{display:flex;}}
/* 回测结果渲染后切换紧凑模式 */
.bt-modal.compact{{min-height:0;max-height:90vh;}}
/* 标题栏：拖拽手柄 */
.bt-bar{{background:#f8f9fb;padding:10px 16px;display:flex;align-items:center;
  justify-content:space-between;border-bottom:1px solid #e4e8f0;flex-shrink:0;cursor:move;user-select:none;}}
.bt-bar-title{{color:#e07b00;font-weight:700;font-size:15px;}}
.bt-close{{background:none;border:1px solid #ccc;border-radius:6px;color:#555;font-size:14px;
  cursor:pointer;line-height:1;padding:3px 10px;font-weight:700;}}
.bt-close:hover{{background:#fee;border-color:#f99;color:#c00;}}
.bt-body{{padding:16px 18px;overflow-y:auto;flex:1;color:#333;font-family:system-ui,monospace;font-size:13px;}}
.bt-form{{display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap;margin-bottom:14px;}}
.bt-form label{{color:#666;font-size:12px;display:flex;flex-direction:column;gap:4px;}}
.bt-input{{background:#fff;border:1px solid #c8d0de;border-radius:6px;color:#333;
  padding:7px 10px;font-size:13px;width:130px;outline:none;}}
.bt-input:focus{{border-color:#f5a623;box-shadow:0 0 0 2px rgba(245,166,35,.15);}}
.bt-input-sm{{width:70px;}}
.bt-btn{{background:#f5a623;color:#000;border:none;border-radius:6px;padding:8px 20px;
  font-weight:700;cursor:pointer;font-size:13px;height:34px;}}
.bt-btn:hover{{background:#ffc04a;}}
.bt-btn:disabled{{background:#ddd;color:#999;cursor:not-allowed;}}
.bt-offline{{background:#fffbf0;border:1px solid #f5a623;border-radius:8px;padding:16px 18px;color:#b37000;text-align:center;}}
.bt-offline p{{margin:5px 0;color:#666;font-size:12px;}}
.bt-offline code{{background:#f5f5f5;padding:3px 7px;border-radius:4px;color:#0066cc;font-size:12px;}}
.bt-result{{background:#f8f9fb;border:1px solid #e4e8f0;border-radius:10px;padding:14px 16px;margin-top:4px;}}
.bt-score-box{{display:flex;gap:16px;align-items:center;flex-wrap:wrap;margin-bottom:10px;}}
.bt-score-num{{font-size:34px;font-weight:900;color:#e07b00;line-height:1;}}
.bt-score-label{{color:#888;font-size:12px;margin-top:2px;}}
.bt-badge{{display:inline-block;padding:3px 10px;border-radius:20px;font-size:12px;font-weight:700;}}
.bt-badge-buy{{background:#ffeded;color:#c00;border:1px solid #ffb3b3;}}
.bt-badge-watch{{background:#fff5e0;color:#805000;border:1px solid #f5c96a;}}
.bt-badge-avoid{{background:#f0f0f0;color:#888;border:1px solid #ccc;}}
.bt-badge-success{{background:#edfff3;color:#1a7a38;border:1px solid #8fd4a9;}}
.bt-badge-failure{{background:#ffeded;color:#c00;border:1px solid #ffb3b3;}}
.bt-badge-pending{{background:#e8f3ff;color:#1a5a99;border:1px solid #99c5f5;}}
.bt-badge-insufficient{{background:#f5f5f5;color:#888;border:1px solid #ccc;}}
.bt-dim-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:10px 0;}}
.bt-dim{{background:#fff;border:1px solid #e4e8f0;border-radius:8px;padding:8px 10px;text-align:center;}}
.bt-dim-name{{color:#888;font-size:11px;margin-bottom:3px;}}
.bt-dim-score{{font-size:18px;font-weight:700;color:#e07b00;}}
.bt-dim-max{{font-size:11px;color:#bbb;}}
.bt-price-row{{display:flex;gap:12px;flex-wrap:wrap;margin:10px 0;font-size:12px;}}
.bt-price-item{{background:#fff;border:1px solid #e4e8f0;border-radius:6px;padding:6px 11px;}}
.bt-price-item span{{color:#888;}}
.bt-price-item b{{color:#333;}}
.bt-analysis{{margin-top:8px;padding:10px 13px;background:#fff;border:1px solid #e4e8f0;border-radius:8px;
  font-size:12px;line-height:1.8;color:#444;}}
.bt-analysis b{{color:#e07b00;}}
#bt-chart{{width:100%;height:300px;margin-top:10px;}}
.bt-error{{color:#c00;background:#fff5f5;border:1px solid #ffd0d0;border-radius:8px;padding:10px 14px;font-size:12px;}}
.bt-loading{{color:#888;text-align:center;padding:24px;font-size:13px;}}
</style>

<button class="bt-fab" title="回测诊断" onclick="window.open('start_backtest.html', '_blank')">📊</button>

<div class="bt-modal" id="bt-modal">
  <div class="bt-bar" id="bt-bar">
    <span class="bt-bar-title">📊 个股回测诊断</span>
    <button class="bt-close" onclick="btClose()">✕ 关闭</button>
  </div>
  <div class="bt-body">
    <div class="bt-form">
      <label>代码 / 名称
        <div style="position:relative">
          <input class="bt-input" id="bt-code" placeholder="如 601991 或 大唐发电" autocomplete="off" style="width:160px">
          <div id="bt-suggest" style="display:none;position:absolute;top:100%;left:0;z-index:999;
            background:#fff;border:1px solid #d0d7e3;border-radius:6px;min-width:200px;
            box-shadow:0 4px 16px rgba(0,0,0,.12);font-size:12px;max-height:180px;overflow-y:auto"></div>
        </div>
      </label>
      <label>基准日期
        <input class="bt-input" id="bt-date" type="date">
      </label>
      <label>验证窗口
        <select class="bt-input bt-input-sm" id="bt-window" style="width:80px;cursor:pointer">
          <option value="5">5 日</option>
          <option value="10" selected>10 日</option>
          <option value="30">30 日</option>
        </select>
      </label>
      <label>基本面策略
        <select class="bt-input" id="bt-strategy" style="width:120px;cursor:pointer" title="稳健价值型：适合普通行情，静态PE+ROE绝对值&#10;牛市成长型：适合政策驱动牛市，PEG+增速加速度">
          <option value="classic">稳健价值型</option>
          <option value="growth">牛市成长型</option>
        </select>
      </label>
      <button class="bt-btn" id="bt-submit" onclick="btSubmit()">开始回测</button>
    </div>
    <div id="bt-output"></div>
  </div>
</div>

<script>
(function(){{
  // 初始化日期为今天
  var d = new Date();
  var ds = d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
  document.getElementById('bt-date').value = ds;

  var BT_API = 'http://localhost:8765';

  window.btOpen = function(){{
    var modal = document.getElementById('bt-modal');
    modal.classList.add('active');
    modal.classList.remove('compact');
    if(!document.getElementById('bt-output').innerHTML){{
      btCheckService();
    }}
  }};
  window.btClose = function(){{
    document.getElementById('bt-modal').classList.remove('active');
  }};

  // ── 拖拽移动逻辑 ──────────────────────────────────────
  (function(){{
    var bar = document.getElementById('bt-bar');
    var modal = document.getElementById('bt-modal');
    var dragging = false, ox = 0, oy = 0;
    bar.addEventListener('mousedown', function(e){{
      // 排除按钮点击
      if(e.target.classList.contains('bt-close')) return;
      dragging = true;
      var rect = modal.getBoundingClientRect();
      // 切换到绝对定位（脱离 transform 居中）
      modal.style.left = rect.left + 'px';
      modal.style.top  = rect.top  + 'px';
      modal.style.transform = 'none';
      ox = e.clientX - rect.left;
      oy = e.clientY - rect.top;
      e.preventDefault();
    }});
    document.addEventListener('mousemove', function(e){{
      if(!dragging) return;
      modal.style.left = (e.clientX - ox) + 'px';
      modal.style.top  = (e.clientY - oy) + 'px';
    }});
    document.addEventListener('mouseup', function(){{ dragging = false; }});
  }})();

  function btCheckService(){{
    var out = document.getElementById('bt-output');
    fetch(BT_API+'/health',{{signal:AbortSignal.timeout(2000)}})
      .then(function(r){{return r.json();}})
      .then(function(){{ out.innerHTML='<div style="color:#6f9;font-size:12px">✅ 回测服务已就绪，请输入股票代码和日期</div>'; }})
      .catch(function(){{ out.innerHTML = btOfflineHtml(); }});
  }}

  function btOfflineHtml(){{
    return '<div class="bt-offline">⚠️ 回测服务未运行或报告未通过 HTTP 打开'+
      '<p>请先启动回测服务（双击 <b>启动回测服务.bat</b> 或运行命令）：</p>'+
      '<p><code>cd &lt;项目目录&gt;\\Quan-Select</code></p>'+
      '<p><code>python backtest.py --serve</code></p>'+
      '<p style="color:#e07000;background:#fff8ec;padding:8px 10px;border-radius:6px;font-size:12px">'+
      '⚠️ 注意：报告必须通过 <b>http://localhost:8765/latest</b> 打开，'+
      '直接双击 .html 文件因浏览器跨域限制无法使用回测功能。</p>'+
      '<p style="margin-top:12px"><button class="bt-btn" onclick="btCheckService()" style="height:30px;padding:0 16px">🔄 重新连接</button>'+
      ' <a href="http://localhost:8765/latest" target="_blank" style="margin-left:8px"><button class="bt-btn" style="height:30px;padding:0 16px;background:#2a82e4">🌐 通过HTTP打开报告</button></a></p></div>';
  }}

  window.btCheckService = btCheckService;

  // ── 实时候选搜索 ──────────────────────────────────────
  var btSuggestTimer = null;
  var codeInput = document.getElementById('bt-code');
  var suggestBox = document.getElementById('bt-suggest');

  codeInput.addEventListener('input', function(){{
    clearTimeout(btSuggestTimer);
    var q = codeInput.value.trim();
    if(q.length < 1){{ suggestBox.style.display='none'; return; }}
    btSuggestTimer = setTimeout(function(){{
      fetch(BT_API+'/search?q='+encodeURIComponent(q))
        .then(function(r){{ return r.json(); }})
        .then(function(list){{
          if(!Array.isArray(list)||list.length===0){{ suggestBox.style.display='none'; return; }}
          suggestBox.innerHTML = list.map(function(item){{
            return '<div class="bt-sug-item" data-code="'+item.code+'" data-name="'+item.name+'"'+
              ' style="padding:7px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0">'+
              '<b style="color:#333">'+item.name+'</b> <span style="color:#999;font-size:11px">'+item.code+'</span></div>';
          }}).join('');
          suggestBox.style.display='block';
        }})
        .catch(function(){{ suggestBox.style.display='none'; }});
    }}, 200);
  }});
  // 候选列表：事件委托（避免内联 onclick 引号嵌套问题）
  suggestBox.addEventListener('mouseover', function(e){{
    var item = e.target.closest('.bt-sug-item');
    if(item) item.style.background='#fff8ec';
  }});
  suggestBox.addEventListener('mouseout', function(e){{
    var item = e.target.closest('.bt-sug-item');
    if(item) item.style.background='';
  }});
  suggestBox.addEventListener('mousedown', function(e){{
    var item = e.target.closest('.bt-sug-item');
    if(item){{
      e.preventDefault();
      btPickSuggest(item.dataset.code, item.dataset.name);
    }}
  }});
  codeInput.addEventListener('blur', function(){{
    setTimeout(function(){{ suggestBox.style.display='none'; }}, 200);
  }});
  window.btPickSuggest = function(code, name){{
    codeInput.value = code;
    codeInput.dataset.name = name;
    suggestBox.style.display='none';
  }};

  window.btSubmit = function(){{
    var code     = document.getElementById('bt-code').value.trim();
    var date     = document.getElementById('bt-date').value.trim();
    var win      = parseInt(document.getElementById('bt-window').value)||10;
    var strategy = document.getElementById('bt-strategy').value||'classic';
    var stratLabel = {{'classic':'稳健价值型','growth':'牛市成长型'}}[strategy]||strategy;
    var out  = document.getElementById('bt-output');
    var btn  = document.getElementById('bt-submit');

    if(!code){{ alert('请输入股票代码或名称'); return; }}
    if(!date){{ alert('请选择基准日期'); return; }}

    btn.disabled=true; btn.textContent='分析中...';
    out.innerHTML='<div class="bt-loading">⏳ 正在分析 '+code+' @ '+date+' ['+stratLabel+'] ...</div>';

    fetch(BT_API+'/analyze',{{
      method:'POST',
      headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{code:code,date:date,window:win,strategy:strategy}}),
      signal:AbortSignal.timeout(30000)
    }})
    .then(function(r){{return r.json();}})
    .then(function(d){{
      btn.disabled=false; btn.textContent='开始回测';
      if(d.error && d.candidates){{
        // 多候选：展示选择列表（用 data-code 避免内联 onclick 引号嵌套）
        var html='<div class="bt-offline" style="text-align:left"><b>🔍 找到多只匹配股票，请点击选择：</b><div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:8px">';
        d.candidates.forEach(function(c){{
          html+='<button class="bt-btn bt-cand-btn" data-code="'+c.code+'" style="height:32px;padding:0 14px;font-size:12px;font-weight:normal">'+
            c.name+' <span style="opacity:.7">'+c.code+'</span></button>';
        }});
        html+='</div></div>';
        out.innerHTML=html;
        // 绑定点击事件
        out.querySelectorAll('.bt-cand-btn').forEach(function(btn2){{
          btn2.addEventListener('click', function(){{
            document.getElementById('bt-code').value = this.dataset.code;
            btSubmit();
          }});
        }});
        return;
      }}
      if(d.error){{ out.innerHTML='<div class="bt-error">❌ '+d.error+'</div>'; return; }}
      out.innerHTML = btRenderResult(d);
      btDrawChart(d);
      document.getElementById('bt-modal').classList.add('compact');
    }})
    .catch(function(err){{
      btn.disabled=false; btn.textContent='开始回测';
      if(err.name==='TimeoutError'||err.message.includes('Failed to fetch')){{
        out.innerHTML = btOfflineHtml();
      }} else {{
        out.innerHTML='<div class="bt-error">❌ 请求失败：'+err.message+'</div>';
      }}
    }});
  }};

  function btRenderResult(d){{
    // ── 字段归一化（后端真实字段 → 前端统一变量）──
    var score      = d.final_score || d.total_score || 0;
    var close_p    = d.base_close || 0;
    var stop_p     = d.stop || 0;
    var target_p   = d.target || 0;
    var buy_range  = d.buy_range || '';          // "4.6-4.83"
    var buy_parts  = buy_range.split('-');
    var buy_low    = parseFloat(buy_parts[0]) || close_p * 0.97;
    var buy_high   = parseFloat(buy_parts[1]) || close_p * 1.02;
    var tech       = d.tech || {{}};
    var verdict    = d.verdict || {{}};
    var vst        = verdict.status || 'insufficient';
    var sectors    = d.sectors || {{}};
    var hot_info   = sectors.hot_info || {{}};
    var sector_note= hot_info.exact === false ? ('参考日期：' + (hot_info.ref_date||'')) : '';
    var stratLabel = d.fund_strategy_label || ({{'classic':'稳健价值型','growth':'牛市成长型'}}[d.fund_strategy]||'');

    var actionClass = {{'积极关注':'bt-badge-buy','逢低关注':'bt-badge-watch','暂时回避':'bt-badge-avoid'}}[d.action]||'bt-badge-avoid';
    var verifyClass = {{'success':'bt-badge-success','failure':'bt-badge-failure','pending':'bt-badge-pending','insufficient':'bt-badge-insufficient'}}[vst]||'bt-badge-pending';
    var verifyLabel = {{'success':'✅ 达到目标','failure':'❌ 触及止损','pending':'⏳ 窗口内未决','insufficient':'⚠️ 数据不足'}}[vst]||vst;

    var html = '<div class="bt-result">';
    // 头部：名称+评分+建议+策略标签
    html += '<div class="bt-score-box">';
    html += '<div><div class="bt-score-num">'+score.toFixed(1)+'</div><div class="bt-score-label">综合评分</div></div>';
    html += '<div><div style="font-size:16px;font-weight:700;color:#333">'+(d.name||d.code)+'</div>';
    html += '<div style="margin-top:4px"><span class="bt-badge '+actionClass+'">'+(d.action||'—')+'</span>&nbsp;&nbsp;<span class="bt-badge '+verifyClass+'">'+verifyLabel+'</span>';
    if(stratLabel) html += '&nbsp;&nbsp;<span style="background:#e8f3ff;color:#1a5a99;border:1px solid #99c5f5;padding:2px 8px;border-radius:12px;font-size:11px">📋 '+stratLabel+'</span>';
    html += '</div>';
    if(sector_note) html += '<div style="color:#f5a623;font-size:11px;margin-top:4px">'+sector_note+'</div>';
    html += '</div></div>';

    // 四维评分（来自 tech 子对象）
    var dims = [
      ['趋势结构', tech.trend||0, 40],
      ['位置形态', tech.position||0, 15],
      ['动量',     tech.momentum||0, 25],
      ['量能',     tech.volume||0, 20]
    ];
    html += '<div class="bt-dim-grid">';
    dims.forEach(function(dm){{
      html += '<div class="bt-dim"><div class="bt-dim-name">'+dm[0]+'</div>';
      html += '<div class="bt-dim-score">'+dm[1].toFixed(1)+'</div>';
      html += '<div class="bt-dim-max">/'+dm[2]+'</div></div>';
    }});
    html += '</div>';

    // 价格区间
    html += '<div class="bt-price-row">';
    html += '<div class="bt-price-item"><span>基准收盘</span><br><b>¥'+close_p.toFixed(2)+'</b></div>';
    html += '<div class="bt-price-item"><span>买入区间</span><br><b>¥'+buy_low.toFixed(2)+' ~ '+buy_high.toFixed(2)+'</b></div>';
    html += '<div class="bt-price-item"><span>止损位</span><br><b style="color:#f87171">¥'+stop_p.toFixed(2)+'</b></div>';
    html += '<div class="bt-price-item"><span>目标位</span><br><b style="color:#6f9">¥'+target_p.toFixed(2)+'</b></div>';
    if(vst !== 'insufficient' && verdict.max_gain_pct !== undefined){{
      var vprice = verdict.verdict_price || 0;
      var vday   = verdict.verdict_day || '';
      html += '<div class="bt-price-item"><span>触发价@日期</span><br><b>¥'+vprice.toFixed(2)+' '+vday+'</b></div>';
      html += '<div class="bt-price-item"><span>最大涨/跌幅</span><br><b>'+
        '<span style="color:#6f9">'+verdict.max_gain_pct.toFixed(1)+'%</span> / '+
        '<span style="color:#f87171">'+verdict.max_loss_pct.toFixed(1)+'%</span></b></div>';
    }}
    html += '</div>';

    // 原因分析
    if(d.analysis && d.analysis.length){{
      html += '<div class="bt-analysis"><b>分析：</b><br>';
      d.analysis.forEach(function(a){{ html += '• '+a+'<br>'; }});
      html += '</div>';
    }}

    // 技术信号标签
    if(tech.sigs && tech.sigs.length){{
      html += '<div style="margin-top:8px;font-size:11px;color:#aab">技术信号：';
      tech.sigs.forEach(function(s){{ html += '<span style="background:#1e2130;border-radius:4px;padding:2px 7px;margin:0 3px 3px 0;display:inline-block">'+s+'</span>'; }});
      html += '</div>';
    }}

    // K线图占位
    html += '<div id="bt-chart"></div>';
    html += '</div>';

    // 把归一化后的值挂回 d，供 btDrawChartNow 使用
    d._stop  = stop_p;
    d._target= target_p;
    d._base_date = d.actual_base_date || d.date;
    return html;
  }}

  function btDrawChart(d){{
    if(!d.candles||!d.candles.length) return;
    var el = document.getElementById('bt-chart');
    if(!el) return;
    if(!window.echarts){{
      var s=document.createElement('script');
      s.src='https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js';
      s.onload=function(){{ btDrawChartNow(d); }};
      document.head.appendChild(s);
    }} else {{
      btDrawChartNow(d);
    }}
  }}

  function btDrawChartNow(d){{
    var el = document.getElementById('bt-chart');
    if(!el||!window.echarts) return;
    var chart = echarts.init(el);   // 浅色主题
    var dates=[],opens=[],closes=[],lows=[],highs=[],vols=[],ma5=[],ma20=[];
    d.candles.forEach(function(c){{
      dates.push(c.date);opens.push(c.open);closes.push(c.close);
      lows.push(c.low);highs.push(c.high);vols.push(c.volume);
      ma5.push(c.ma5!==null?c.ma5:'-');ma20.push(c.ma20!==null?c.ma20:'-');
    }});
    var baseDate = d._base_date || d.actual_base_date || d.date;
    var stop_p   = d._stop   || d.stop   || 0;
    var target_p = d._target || d.target || 0;
    var markLines=[{{xAxis:baseDate,lineStyle:{{color:'#e07b00',width:2,type:'dashed'}},label:{{formatter:'基准日',color:'#e07b00',fontSize:10}}}}];
    var option={{
      backgroundColor:'#f8f9fb',
      tooltip:{{trigger:'axis',axisPointer:{{type:'cross'}}}},
      legend:{{data:['K线','MA5','MA20'],textStyle:{{color:'#666'}},top:2,itemHeight:10,textStyle:{{fontSize:11}}}},
      grid:{{left:48,right:14,top:30,bottom:50}},
      xAxis:{{type:'category',data:dates,axisLabel:{{color:'#888',fontSize:10}},axisLine:{{lineStyle:{{color:'#ccc'}}}}}},
      yAxis:{{type:'value',scale:true,splitLine:{{lineStyle:{{color:'#eee'}}}},axisLabel:{{color:'#888',fontSize:10}}}},
      dataZoom:[{{type:'inside',start:0,end:100}},{{start:0,end:100,height:18,bottom:6,textStyle:{{color:'#999',fontSize:10}}}}],
      series:[
        {{name:'K线',type:'candlestick',data:dates.map(function(_,i){{return[opens[i],closes[i],lows[i],highs[i]];}}) ,
          itemStyle:{{color:'#e84040',color0:'#26a69a',borderColor:'#e84040',borderColor0:'#26a69a'}},
          markLine:{{silent:true,symbol:'none',data:markLines.concat([
            {{yAxis:target_p,lineStyle:{{color:'#1a9a5a',type:'dashed',width:1}},label:{{formatter:'目标 ¥'+target_p.toFixed(2),color:'#1a9a5a',fontSize:10,position:'insideEndTop'}}}},
            {{yAxis:stop_p,lineStyle:{{color:'#cc3333',type:'dashed',width:1}},label:{{formatter:'止损 ¥'+stop_p.toFixed(2),color:'#cc3333',fontSize:10,position:'insideEndBottom'}}}}
          ])}}
        }},
        {{name:'MA5',type:'line',data:ma5,smooth:true,showSymbol:false,lineStyle:{{color:'#e07b00',width:1.5}}}},
        {{name:'MA20',type:'line',data:ma20,smooth:true,showSymbol:false,lineStyle:{{color:'#3388cc',width:1.5}}}}
      ]
    }};
    chart.setOption(option);
    window.addEventListener('resize',function(){{chart.resize();}});
  }}
}})();
</script>

</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(body)
    print(f"  [HTML]  {out_path}")


# ══════════════════════════════════════════════════════════
# 3. Excel 报告
# ══════════════════════════════════════════════════════════
def _parse_surprise_meta(val):
    """解析 surprise_meta JSON 字符串，返回字典"""
    if not val or val in ("None", "nan", "null", ""):
        return {}
    try:
        import json
        return json.loads(val) if isinstance(val, str) else val
    except Exception:
        return {}


def _get_surprise_conclusion(surprise_meta, with_badge: bool = False) -> str:
    """根据 surprise_meta 返回超预期结论
    
    Args:
        surprise_meta: JSON字符串或字典
        with_badge: 是否为Excel添加标记前缀
    """
    meta = _parse_surprise_meta(surprise_meta)
    if meta.get("expect_yoy") is not None and meta.get("ttm_yoy") is not None:
        diff = meta.get("diff", 0)
        if diff > 20:
            return "【强超预期】" if with_badge else "强超预期"
        elif diff > 10:
            return "【超预期】" if with_badge else "超预期"
        elif diff > 0:
            return "微超预期"
        elif diff > -10:
            return "持平"
        else:
            return "不及预期"
    return "无数据"


def gen_excel(date_str: str, df: pd.DataFrame, out_path: str):
    # 预处理 surprise_meta，生成超预期结论（带标记前缀）
    df["surprise_conclusion"] = df["surprise_meta"].apply(lambda x: _get_surprise_conclusion(x, with_badge=True))

    cols = {
        "rank_no":             "排名",
        "code":                "代码",
        "name":                "名称",
        "industry":            "行业",
        "report_period":       "财报期",
        "close":               "收盘价",
        "chg_pct":             "今日涨幅",
        "final_score":         "综合分",
        "total_score":         "原始分",
        "tech_score":          "技术分",
        "fund_score":          "基本面分",
        "hot_score":           "热度分",
        "sector_heat":         "板块热度分",
        "news_heat":           "消息面热度分",
        "news_score":          "消息面评分",
        "news_level":          "消息面等级",
        "news_summary":        "消息面摘要",
        "pe_ttm":              "PE(TTM)",
        "mcap":                "总市值(亿)",
        "profit_yoy":          "净利同比%",
        "revenue_yoy":         "营收同比%",
        "roe":                 "ROE%",
        "eps":                 "EPS",
        "surprise_conclusion": "超预期",
        "matched_sectors":     "命中板块",
        "sector_count":        "命中板块数",
        "action":              "操作建议",
        "buy_range":           "买点区间",
        "stop_loss":           "止损位",
        "target":              "目标位",
        "position_pct":        "仓位建议",
        "tech_sigs":           "技术信号",
        "fund_sigs":           "基本面信号",
        "fund_reasons":        "基本面评分理由",
        "fin_source":          "财务来源",
    }

    export_cols = [c for c in cols.keys() if c in df.columns]
    out_df = df[export_cols].rename(columns=cols)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        out_df.to_excel(writer, index=False, sheet_name=f"精选_{date_str}")
        ws = writer.sheets[f"精选_{date_str}"]
        # 列宽自适应
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)

    print(f"  [Excel] {out_path}")


# ══════════════════════════════════════════════════════════
# 4. EBK 文件
# ══════════════════════════════════════════════════════════
def gen_ebk(date_str: str, df: pd.DataFrame, out_path: str):
    lines = [""]   # 第一行空行
    for _, row in df.iterrows():
        ebk = code_to_ebk(str(row.get("code", "")))
        if ebk:
            lines.append(ebk)
    content = "\r\n".join(lines) + "\r\n"
    with open(out_path, "wb") as f:
        f.write(content.encode("ascii"))
    print(f"  [EBK]   {out_path}  ({len(df)} 只)")


# ══════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════
def run(date_str: str = None, top_n: int = 30,
        gen_html_flag: bool = True,
        gen_excel_flag: bool = True,
        gen_ebk_flag: bool = True,
        strategy_label: str = "",
        strategy: str = "classic",
        skip_tech: bool = False,
        surprise_only: bool = False,
        no_top_limit: bool = False):

    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")
    compact  = date_str.replace("-", "")
    dash_str = f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"

    print("=" * 56)
    mode_hint = " [超预期命中模式]" if surprise_only else ""
    strat_hint = f" [{strategy}]" if strategy and strategy != "classic" else ""
    top_label = f"TOP{top_n}" if not no_top_limit else "无截断"
    print(f"  report.py  日期: {dash_str}  策略:{strategy}{strat_hint}  {top_label}{mode_hint}")
    print("=" * 56)

    conn = sqlite3.connect(DB_FILE)

    df = load_picks(conn, dash_str, top_n, strategy=strategy,
                    surprise_only=surprise_only, no_top_limit=no_top_limit)
    if df.empty:
        print(f"  [WARN] qs_picks 表无 {dash_str} 数据，请先运行 score.py")
        conn.close()
        return

    # ── 技术过滤：保留高位+缩量（从DB标识位筛选）─────────────────
    if TECH_FILTER_ENABLED and not surprise_only and not no_top_limit and "tech_filter_passed" in df.columns:
        before_filter = len(df)
        # 优先用DB中已计算的标识位；缺失时回退为不过滤
        mask_valid = df["tech_filter_passed"].notna()
        mask_pass = df["tech_filter_passed"] == 1
        # 无标识位的保留（兼容旧数据或纯基本面模式）
        df_filtered = df[mask_pass | ~mask_valid].copy()
        after_filter = len(df_filtered)
        if before_filter > after_filter:
            print(f"\n  [TECH-FILTER] 技术过滤：{before_filter} → {after_filter} 只"
                  f"（淘汰{before_filter - after_filter}只：位置<{int(TECH_FILTER_POS_MIN*100)}% 或 量比>{TECH_FILTER_VOL_MAX}）")
        else:
            print(f"\n  [TECH-FILTER] 技术过滤：全部{before_filter}只通过")
        df = df_filtered
        df["rank_no"] = range(1, len(df) + 1)

    hot_sectors = load_sectors(conn, dash_str)
    # 注意：conn 在此处不关闭，gen_html 需要用它实时查板块

    print(f"\n  报告精选 {len(df)} 只，热门板块 {len(hot_sectors)} 个")

    # ── 输出（文件名带策略标识）──────────────────────────────
    print("\n[生成报告]")
    s_tag = f"_{strategy}" if strategy and strategy != "classic" else ""

    if gen_html_flag:
        html_path = os.path.join(BASE_DIR, f"report_{dash_str}{s_tag}.html")
        gen_html(dash_str, df, hot_sectors, top_n, html_path, conn=conn,
                 strategy_label=strategy_label or strategy_label, skip_tech=skip_tech,
                 surprise_only=surprise_only)

    if gen_excel_flag:
        xlsx_path = os.path.join(BASE_DIR, f"精选标的_{compact}{s_tag}_TOP{top_n}.xlsx")
        gen_excel(dash_str, df, xlsx_path)

    if gen_ebk_flag:
        ebk_path = os.path.join(BASE_DIR, f"精选标的_{compact}{s_tag}.EBK")
        gen_ebk(dash_str, df, ebk_path)

    conn.close()
    print(f"\n[OK] 报告生成完成")
    if gen_html_flag:
        html_filename = f"report_{dash_str}{s_tag}.html"
        print(f"  ┌─────────────────────────────────────────────────────┐")
        print(f"  │  报告文件：picks/{html_filename}")
        print(f"  │  Excel：picks/精选标的_{compact}{s_tag}_TOP{top_n}.xlsx")
        print(f"  │  EBK：picks/精选标的_{compact}{s_tag}.EBK")
        print(f"  │  回测诊断：http://localhost:8765/backtest_ui.html")
        print(f"  │  （需先运行 start_backtest.bat）")
        print(f"  └─────────────────────────────────────────────────────┘")
    return df


# ══════════════════════════════════════════════════════════
# 命令行入口
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="精选报告生成：HTML + Excel + EBK")
    parser.add_argument("--date",     type=str, default=None,
                        help="指定日期，格式 YYYYMMDD 或 YYYY-MM-DD（默认今日）")
    parser.add_argument("--top",      type=int, default=20,
                        help="输出 TOP N（默认20）")
    parser.add_argument("--no-html",  action="store_true", help="跳过 HTML 生成")
    parser.add_argument("--no-excel", action="store_true", help="跳过 Excel 生成")
    parser.add_argument("--no-ebk",   action="store_true", help="跳过 EBK 生成")
    args = parser.parse_args()

    run(date_str=args.date, top_n=args.top,
        gen_html_flag=not args.no_html,
        gen_excel_flag=not args.no_excel,
        gen_ebk_flag=not args.no_ebk)
