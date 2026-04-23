"""
fund_strategies.py  —— 基本面打分策略集
------------------------------------------------------------
提供四套可互换的基本面评分算法：

  classic  「稳健价值型」（默认）
      适用：普通行情、防御性持仓、蓝筹价值股
      逻辑：静态 PE 估值 + 净利增速 + 营收增速 + ROE 绝对值 + 市值弹性
      满分：100 分

  growth   「牛市成长型」
      适用：牛市行情 + 国家政策扶持 + 行业景气 + 高速成长企业
      逻辑：PEG 估值（抛弃静态 PE）+ 净利增速加速度 + 收入/利润结构
            + ROE 趋势意识 + 市值弹性（牛市加成）
      满分：100 分

  surprise 「超预期成长型」
      适用：寻找"二次加速"成长股——市场一致预期增速 > 历史 TTM 增速
      逻辑：超预期信号（预期差）+ TTM加速(ttm1 vs ttm2) + PEG 估值
            + 净利增速 + 收入/利润结构 + ROE 成长 + 机构关注度
      维度：超预期信号20 + TTM加速20 + PEG15 + 净利20 + 结构15 + ROE10 = 100
      满分：100 分
      数据需求：需额外获取一致预期 EPS（东财 RPT_WEB_RESPREDICT）+ TTM pair

  single_line 「短线策略」
      适用：短线选股，找催化因素而非长期价值
      逻辑：利润边际变化（单季同比+季节调整后环比+季度预期差）
            + 营收质量 + 估值安全垫（放宽）+ 盈利质量（ROE低不惩罚）+ 市值弹性
      满分：100 分
      数据需求：
        - calc_qoq_growth() 单季环比（通达信多期zip，精细方案带季节调整）
        - fetch_quarterly_consensus() 季度级净利润预期（report_rc接口）

使用方式：
  from fund_strategies import fund_score, STRATEGIES

  # 默认 classic
  score, sigs = fund_score(pe, mcap, profit_yoy, revenue_yoy, roe)

  # 指定策略
  score, sigs = fund_score(pe, mcap, profit_yoy, revenue_yoy, roe,
                           strategy="growth")

  # 超预期策略（需要额外数据）
  detail = surprise_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                                  expect_yoy, ttm_yoy, org_num)
  # expect_yoy = 一致预期净利润增速(%)
  # ttm_yoy = TTM净利润同比增速(%)
  # org_num = 覆盖机构家数

  # 短线策略（需要环比数据+季度预期）
  detail = single_line_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                                     qoq_data=qoq_dict, quarterly_consensus=qc_dict,
                                     expect_yoy, ttm_yoy, org_num)

  # 获取策略详情（供 backtest 前端展示）
  detail = fund_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                             strategy="growth")
  # detail = {"total": int, "sigs": [...], "breakdown": {...}, "strategy": str}

可用策略名称：
  STRATEGIES = {
    "classic": "稳健价值型", "growth": "牛市成长型",
    "surprise": "超预期成长型", "single_line": "短线策略",
  }
------------------------------------------------------------
"""

STRATEGIES = {
    "classic":     "稳健价值型",
    "growth":      "牛市成长型",
    "surprise":    "超预期成长型",
    "single_line": "短线策略",
}

# 预定义组合策略（用于展示友好名称）
COMBO_LABELS = {
    "classic,growth":    "稳健+成长型",
    "classic,surprise":  "稳健+超预期型",
    "growth,surprise":   "成长+超预期型",
    "classic,growth,surprise": "全策略均衡型",
}

DEFAULT_STRATEGY = "classic"

import re
import os
import glob
import time

# 从集中配置文件读取通达信路径（修改 qs_config.py 即可适配不同环境）
try:
    from qs_config import TDX_CW_DIR as _TDX_CW_DIR
except ImportError:
    _TDX_CW_DIR = r"C:\TongDaXin\vipdoc\cw"


def parse_strategies(strategy_str: str) -> list[str]:
    """
    解析策略字符串，支持逗号分隔的多策略。
    返回有效策略名列表（去重、保序）。
    例如：'classic,surprise' -> ['classic', 'surprise']
    """
    parts = [s.strip() for s in strategy_str.split(",")]
    valid = [p for p in parts if p in STRATEGIES]
    # 去重保序
    seen = set()
    result = []
    for p in valid:
        if p not in seen:
            seen.add(p)
            result.append(p)
    return result if result else [DEFAULT_STRATEGY]


def get_strategy_label(strategy_str: str) -> str:
    """获取策略的友好显示名称（支持组合）"""
    parts = parse_strategies(strategy_str)
    if len(parts) == 1:
        return STRATEGIES.get(parts[0], parts[0])
    key = ",".join(parts)
    if key in COMBO_LABELS:
        return COMBO_LABELS[key]
    return "+".join(STRATEGIES.get(p, p) for p in parts)


# ══════════════════════════════════════════════════════════
# 策略 A：稳健价值型（classic）
# ══════════════════════════════════════════════════════════
def _classic(pe, mcap, profit_yoy, revenue_yoy, roe) -> dict:
    """
    经典基本面打分，满分 100：
      PE估值 25 + 净利同比 35 + 营收同比 15 + ROE绝对值 15 + 市值弹性 10
    """
    score, sigs = 0, []

    # ── PE 估值（25 分）────────────────────────────────────
    pe_s = 0
    if pe is not None:
        if   0 < pe < 20:    pe_s = 25; sigs.append(f"PE{pe:.1f}低估")
        elif 0 < pe < 35:    pe_s = 18; sigs.append(f"PE{pe:.1f}")
        elif 35 <= pe < 60:  pe_s = 10; sigs.append(f"PE{pe:.1f}偏高")
        elif pe >= 60:       pe_s =  3; sigs.append(f"PE{pe:.1f}高估")
        elif pe < 0:         pe_s = -5; sigs.append("市盈率为负")
    score += pe_s

    # ── 净利润同比（35 分）─────────────────────────────────
    profit_s = 0
    if profit_yoy is not None:
        if   profit_yoy > 50:   profit_s = 35; sigs.append(f"净利+{profit_yoy:.1f}%爆发")
        elif profit_yoy > 30:   profit_s = 28; sigs.append(f"净利+{profit_yoy:.1f}%高增")
        elif profit_yoy > 15:   profit_s = 20; sigs.append(f"净利+{profit_yoy:.1f}%")
        elif profit_yoy > 0:    profit_s = 12; sigs.append(f"净利+{profit_yoy:.1f}%")
        elif profit_yoy > -10:  profit_s = -5; sigs.append(f"净利{profit_yoy:.1f}%小降")
        else:                   profit_s =-15; sigs.append(f"净利{profit_yoy:.1f}%大降")
    score += profit_s

    # ── 营收同比（15 分）───────────────────────────────────
    rev_s = 0
    if revenue_yoy is not None:
        if   revenue_yoy > 30:  rev_s = 15; sigs.append(f"营收+{revenue_yoy:.1f}%")
        elif revenue_yoy > 15:  rev_s = 10; sigs.append(f"营收+{revenue_yoy:.1f}%")
        elif revenue_yoy > 0:   rev_s =  5; sigs.append(f"营收+{revenue_yoy:.1f}%")
        else:                   rev_s = -3; sigs.append(f"营收{revenue_yoy:.1f}%")
    score += rev_s

    # ── ROE 绝对值（15 分）─────────────────────────────────
    roe_s = 0
    if roe is not None:
        if   roe > 20:  roe_s = 15; sigs.append(f"ROE{roe:.1f}%优")
        elif roe > 15:  roe_s = 10; sigs.append(f"ROE{roe:.1f}%")
        elif roe > 10:  roe_s =  5; sigs.append(f"ROE{roe:.1f}%")
        elif roe < 5:   roe_s = -5; sigs.append(f"ROE{roe:.1f}%低")
    score += roe_s

    # ── 市值弹性（10 分）───────────────────────────────────
    mcap_s = 0
    if mcap:
        if   20 < mcap < 100:    mcap_s = 10; sigs.append(f"小盘{mcap:.0f}亿")
        elif 100 <= mcap < 300:  mcap_s =  8; sigs.append(f"中盘{mcap:.0f}亿")
        elif 300 <= mcap < 1000: mcap_s =  5; sigs.append(f"大盘{mcap:.0f}亿")
        else:                    mcap_s =  3; sigs.append(f"超大盘{mcap:.0f}亿")
    score += mcap_s

    return {
        "total":     max(score, 0),
        "strategy":  "classic",
        "sigs":      sigs,
        "breakdown": {
            "pe":      pe_s,
            "profit":  profit_s,
            "revenue": rev_s,
            "roe":     roe_s,
            "mcap":    mcap_s,
        },
    }


# ══════════════════════════════════════════════════════════
# 策略 B：牛市成长型（growth）
# ══════════════════════════════════════════════════════════
def _growth(pe, mcap, profit_yoy, revenue_yoy, roe) -> dict:
    """
    牛市成长股打分，满分 100：
      PEG/PS估值 20 + 净利增速加速度感知 40 + 收入/利润结构 20
      + ROE成长潜力 10 + 市值弹性（牛市加成）10

    核心差异：
    - 用 PEG 替代静态 PE（成长股高PE不惩罚）
    - 净利增速分档更细，重奖高增速
    - 利润增速 > 营收增速（利润弹性）额外加分
    - ROE 阈值放宽（扩张期 ROE 被摊薄是正常的）
    - 市值中小盘在牛市弹性更大，加成更高
    """
    score, sigs = 0, []

    # ── PEG 估值（20 分）───────────────────────────────────
    # PEG = PE / 净利增速；无法计算时退化为宽松 PE 判断
    peg_s = 0
    if pe is not None and profit_yoy and profit_yoy > 0:
        peg = pe / profit_yoy
        if   peg < 0.5:             peg_s = 20; sigs.append(f"PEG{peg:.2f}严重低估")
        elif peg < 1.0:             peg_s = 16; sigs.append(f"PEG{peg:.2f}合理偏低")
        elif peg < 1.5:             peg_s = 10; sigs.append(f"PEG{peg:.2f}合理")
        elif peg < 2.0:             peg_s =  5; sigs.append(f"PEG{peg:.2f}偏贵")
        else:                       peg_s = -5; sigs.append(f"PEG{peg:.2f}高估")
    elif pe is not None:
        # 亏损或增速为负时，退化为宽松 PE 判断（不重惩高 PE）
        if   pe < 0:                peg_s = -8; sigs.append("亏损")
        elif pe < 50:               peg_s = 10; sigs.append(f"PE{pe:.1f}")
        elif pe < 100:              peg_s =  5; sigs.append(f"PE{pe:.1f}偏高")
        else:                       peg_s =  0; sigs.append(f"PE{pe:.1f}高")
    score += peg_s

    # ── 净利润同比（40 分，核心维度）──────────────────────
    # 成长股核心：增速越高得分越高，大降惩罚更重
    profit_s = 0
    if profit_yoy is not None:
        if   profit_yoy > 100:  profit_s = 40; sigs.append(f"净利+{profit_yoy:.1f}%超高增")
        elif profit_yoy > 50:   profit_s = 35; sigs.append(f"净利+{profit_yoy:.1f}%爆发")
        elif profit_yoy > 30:   profit_s = 28; sigs.append(f"净利+{profit_yoy:.1f}%高增")
        elif profit_yoy > 15:   profit_s = 18; sigs.append(f"净利+{profit_yoy:.1f}%")
        elif profit_yoy > 0:    profit_s =  8; sigs.append(f"净利+{profit_yoy:.1f}%低增")
        elif profit_yoy > -20:  profit_s =-10; sigs.append(f"净利{profit_yoy:.1f}%下滑")
        else:                   profit_s =-20; sigs.append(f"净利{profit_yoy:.1f}%大降")
    score += profit_s

    # ── 收入/利润结构（20 分）──────────────────────────────
    # 利润增速 > 收入增速 = 规模效应释放 / 利润弹性，最优
    # 收入高增但利润被压 = 扩张期，正常但要观察
    # 收入负增但利润正增 = 一次性/降本，可持续性存疑
    struct_s = 0
    if profit_yoy is not None and revenue_yoy is not None:
        if revenue_yoy > 0 and profit_yoy > revenue_yoy:
            struct_s = 20; sigs.append(f"利润弹性(营收+{revenue_yoy:.1f}%→净利+{profit_yoy:.1f}%)")
        elif revenue_yoy > 20 and profit_yoy > 0:
            struct_s = 14; sigs.append(f"营收高增+{revenue_yoy:.1f}%扩张期")
        elif revenue_yoy > 0 and profit_yoy > 0:
            struct_s = 10; sigs.append(f"营收+{revenue_yoy:.1f}%稳健")
        elif revenue_yoy > 0 and profit_yoy <= 0:
            struct_s =  3; sigs.append(f"营收增但利润承压")
        elif revenue_yoy <= 0 and profit_yoy > 20:
            struct_s =  2; sigs.append(f"收入负增/利润增，一次性因素?")
        else:
            struct_s = -5; sigs.append(f"营收{revenue_yoy:.1f}%利润{profit_yoy:.1f}%双降")
    elif revenue_yoy is not None:
        # 仅有营收数据
        if   revenue_yoy > 30:  struct_s = 12; sigs.append(f"营收+{revenue_yoy:.1f}%")
        elif revenue_yoy > 15:  struct_s =  8; sigs.append(f"营收+{revenue_yoy:.1f}%")
        elif revenue_yoy > 0:   struct_s =  4; sigs.append(f"营收+{revenue_yoy:.1f}%")
        else:                   struct_s = -3; sigs.append(f"营收{revenue_yoy:.1f}%")
    score += struct_s

    # ── ROE 成长潜力（10 分，放宽阈值）────────────────────
    # 成长股扩张期 ROE 被摊薄是正常的，阈值放宽，不大幅扣分
    roe_s = 0
    if roe is not None:
        if   roe > 20:  roe_s = 10; sigs.append(f"ROE{roe:.1f}%优")
        elif roe > 15:  roe_s =  8; sigs.append(f"ROE{roe:.1f}%")
        elif roe > 10:  roe_s =  5; sigs.append(f"ROE{roe:.1f}%")
        elif roe > 5:   roe_s =  2; sigs.append(f"ROE{roe:.1f}%扩张期")
        else:           roe_s = -3; sigs.append(f"ROE{roe:.1f}%偏低")
    score += roe_s

    # ── 市值弹性（10 分，牛市加成）─────────────────────────
    # 牛市中小盘弹性更大，加分更激进
    mcap_s = 0
    if mcap:
        if   10 < mcap < 50:     mcap_s = 10; sigs.append(f"微盘{mcap:.0f}亿弹性强")
        elif 50 <= mcap < 150:   mcap_s = 10; sigs.append(f"小盘{mcap:.0f}亿")
        elif 150 <= mcap < 400:  mcap_s =  8; sigs.append(f"中盘{mcap:.0f}亿")
        elif 400 <= mcap < 1200: mcap_s =  5; sigs.append(f"大盘{mcap:.0f}亿")
        else:                    mcap_s =  2; sigs.append(f"超大盘{mcap:.0f}亿")
    score += mcap_s

    return {
        "total":     max(score, 0),
        "strategy":  "growth",
        "sigs":      sigs,
        "breakdown": {
            "peg":    peg_s,
            "profit": profit_s,
            "struct": struct_s,
            "roe":    roe_s,
            "mcap":   mcap_s,
        },
    }


# ══════════════════════════════════════════════════════════
# 一致预期数据获取
# ══════════════════════════════════════════════════════════
_CONSENSUS_CACHE = {}  # {code: {expire_time, data}}

def fetch_consensus_eps(code: str) -> dict:
    """
    从东财获取一致预期 EPS 数据
    返回：{
        "eps1": float,       # 最近年度EPS（A=实际/E=预估）
        "eps2": float,       # 下一年EPS（E=预估）
        "year1": int,        # 对应年份
        "year2": int,        # 对应年份
        "year_mark1": str,   # A=实际, E=预估
        "year_mark2": str,
        "expect_yoy": float, # 预期净利润增速% = (eps2-eps1)/|eps1|
        "org_num": int,      # 覆盖机构家数
        "buy_num": int,      # 买入评级家数
        "source": str,       # "eastmoney" / "none"
    }
    """
    import time as _time
    now = _time.time()
    if code in _CONSENSUS_CACHE and _CONSENSUS_CACHE[code]["expire_time"] > now:
        return _CONSENSUS_CACHE[code]["data"]

    result = {"eps1": None, "eps2": None, "year1": None, "year2": None,
              "year_mark1": None, "year_mark2": None,
              "expect_yoy": None, "org_num": None, "buy_num": None,
              "source": "none"}

    try:
        import requests as _req
        url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
        params = {
            "reportName": "RPT_WEB_RESPREDICT",
            "columns": "ALL",
            "filter": f'(SECURITY_CODE="{code}")',
            "pageSize": "1",
            "source": "WEB",
            "client": "WEB",
        }
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                   "Referer": "https://data.eastmoney.com/"}
        r = _req.get(url, params=params, headers=headers, timeout=5)
        data = r.json()
        if data.get("success") and data.get("result", {}).get("data"):
            row = data["result"]["data"][0]
            result["eps1"] = _to_f(row.get("EPS1"))
            result["eps2"] = _to_f(row.get("EPS2"))
            result["year1"] = row.get("YEAR1")
            result["year2"] = row.get("YEAR2")
            result["year_mark1"] = row.get("YEAR_MARK1")
            result["year_mark2"] = row.get("YEAR_MARK2")
            result["org_num"] = _to_f(row.get("RATING_ORG_NUM"))
            result["buy_num"] = _to_f(row.get("RATING_BUY_NUM"))
            result["source"] = "eastmoney"
            # 计算预期增速
            if result["eps1"] and result["eps2"] and result["eps1"] != 0:
                result["expect_yoy"] = (result["eps2"] - result["eps1"]) / abs(result["eps1"]) * 100
    except Exception:
        pass

    _CONSENSUS_CACHE[code] = {"data": result, "expire_time": now + 3600}
    return result


# ── report_rc 季度预测缓存 ──
_QRC_CACHE = {}  # {code: {data, expire_time}}


def fetch_quarterly_consensus(code: str) -> dict:
    """
    通过 report_rc 接口获取券商对最近季度的净利润预测（中位数）。
    
    数据源：金融数据接口 report_rc（券商盈利预测数据）
    用途：替代年度EPS预期，提供季度级别的"实际vs预期"匹配
    
    算法：
      1. 取该股最新的所有券商预测报告
      2. 识别最新报告对应的预测季度 quarter (如 2026Q1)
      3. 取该季度的 np(预测净利润) 中位数
      4. 返回预期值和覆盖机构数
    
    返回：
      {
        "expected_np": float|None,     # 预期当季净利润（万元，中位数）
        "expected_eps": float|None,    # 预期当季EPS
        "predict_count": int,          # 预测机构数
        "latest_quarter": str|None,    # 最新预测季度如 "2026Q1"
        "latest_report_date": str|None,# 最新报告日期
        "source": str,                 # "report_rc" / "none"
      }
    """
    now = time.time()
    if code in _QRC_CACHE and _QRC_CACHE[code]["expire_time"] > now:
        return _QRC_CACHE[code]["data"]

    result = {"expected_np": None, "expected_eps": None, "predict_count": 0,
              "latest_quarter": None, "latest_report_date": None, "source": "none"}

    try:
        import requests as _req

        # 转换代码格式：300xxx → 300xxx.SZ, 600xxx → 600xxx.SH
        if code.startswith("6"):
            ts_code = f"{code}.SH"
        elif code.startswith(("0", "3")):
            ts_code = f"{code}.SZ"
        elif code.startswith("4") or code.startswith("8"):
            ts_code = f"{code}.BJ"
        else:
            ts_code = code

        url = "https://www.codebuddy.cn/v2/tool/financedata"
        payload = {
            "api_name": "report_rc",
            "params": {"ts_code": ts_code},
            "fields": "ts_code,name,report_date,quarter,np,eps,org_name",
        }
        r = _req.post(url, json=payload, timeout=8)
        data = r.json()

        if data.get("code") == 0 and data.get("data", {}).get("items"):
            items = data["data"]["items"]
            
            # 按报告日期排序，取最新的
            items.sort(key=lambda x: x[2] if len(x) > 2 else "", reverse=True)
            
            # 找最新预测季度（quarter字段格式: 2025Q4 / 2026Q1 等）
            latest_q = None
            for item in items:
                q = item[3] if len(item) > 3 else None  # quarter 列
                if q and "Q" in q:
                    latest_q = q
                    break
            
            if latest_q:
                # 过滤出该季度的所有预测，取np中位数
                q_nps = []
                q_eps_list = []
                for item in items:
                    q_item = item[3] if len(item) > 3 else None
                    if q_item == latest_q:
                        np_val = item[5] if len(item) > 5 else None   # np列
                        eps_val = item[6] if len(item) > 6 else None  # eps列
                        if np_val is not None and isinstance(np_val, (int, float)) and np_val > 0:
                            q_nps.append(np_val)
                        if eps_val is not None and isinstance(eps_val, (int, float)) and eps_val > 0:
                            q_eps_list.append(eps_val)
                
                if q_nps:
                    q_nps.sort()
                    mid = len(q_nps) // 2
                    result["expected_np"] = q_nps[mid]  # 中位数
                if q_eps_list:
                    q_eps_list.sort()
                    mid_e = len(q_eps_list) // 2
                    result["expected_eps"] = q_eps_list[mid_e]
                
                result["predict_count"] = len(q_nps)
                result["latest_quarter"] = latest_q
                result["latest_report_date"] = items[0][2] if len(items[0]) > 2 else None
                result["source"] = "report_rc"

    except Exception as e:
        pass  # 静默失败，返回默认空结果

    _QRC_CACHE[code] = {"data": result, "expire_time": now + 3600}
    return result


def _to_f(v) -> float | None:
    """安全转 float"""
    try:
        f = float(v)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None


# ══════════════════════════════════════════════════════════
# TTM 净利润增速计算（从通达信本地财务 zip）
# ══════════════════════════════════════════════════════════
_TTM_CACHE = {}  # {code: {expire_time, ttm_yoy}}
_ZIP_DF_CACHE = {}  # {zip_path: DataFrame} — zip解压缓存，避免反复解析


def _load_fin_df(zip_path: str):
    """加载zip财务DataFrame（带内存缓存）"""
    if zip_path in _ZIP_DF_CACHE:
        return _ZIP_DF_CACHE[zip_path]
    try:
        from pytdx.reader import HistoryFinancialReader
        df = HistoryFinancialReader().get_df(zip_path)
        _ZIP_DF_CACHE[zip_path] = df
        return df
    except Exception:
        _ZIP_DF_CACHE[zip_path] = None
        return None


def _read_col25(zip_path: str, code: str):
    """从zip中读取指定股票的累计归母净利润(col[96])"""
    try:
        df = _load_fin_df(zip_path)
        if df is None or code not in df.index:
            return None
        val = df.loc[[code]].iloc[0].iloc[96]
        return float(val) if val == val else None
    except Exception:
        return None


def _calc_ttm(cur_cum, prev_cum, prev_year_full):
    """
    [已废弃] 旧版TTM计算，仅保留兼容。
    新版请用 _build_quarterly_series + _rolling_ttm_yoy。
    """
    if cur_cum is None or prev_cum is None or prev_year_full is None:
        return None
    if prev_year_full == 0:
        return None
    return cur_cum - prev_cum + prev_year_full


# ── 滚动季度利润序列 ──
_QUARTERLY_CACHE = {}  # {code: {expire_time, quarters}}


def _build_quarterly_series(code: str):
    """
    从所有有效zip中提取单季归母净利润的时间序列。

    返回: [(year, quarter, profit), ...] 按时间正序（最旧→最新）
      year=int, quarter=1~4, profit=float（单季利润，元）
    
    算法：
      遍历所有有效zip（按年份正序），对每个zip用 _get_single_quarter_profit
      拆出单季利润，记录 (年份, 季度, 利润)。
    """
    now = time.time()
    if code in _QUARTERLY_CACHE and _QUARTERLY_CACHE[code]["expire_time"] > now:
        return _QUARTERLY_CACHE[code]["quarters"]

    cw_dir = _TDX_CW_DIR
    all_files = sorted([f for f in glob.glob(os.path.join(cw_dir, "gpcw*.zip"))
                        if os.path.getsize(f) >= 10 * 1024])  # 正序：最旧在前

    mmdd_to_q = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}
    quarters = []
    seen = set()  # 去重：(year, quarter)

    for z in all_files:
        mmdd = _extract_mmdd(z)
        if mmdd not in mmdd_to_q:
            continue
        m = re.search(r"gpcw(\d{4})", os.path.basename(z))
        if not m:
            continue
        year = int(m.group(1))
        q = mmdd_to_q[mmdd]
        key = (year, q)
        if key in seen:
            continue

        profit, _ = _get_single_quarter_profit(z, code, all_files)
        if profit is not None:
            quarters.append((year, q, profit))
            seen.add(key)

    _QUARTERLY_CACHE[code] = {"quarters": quarters, "expire_time": now + 3600}
    return quarters


def _rolling_ttm_yoy(quarters: list) -> float | None:
    """
    从单季利润序列计算滚动TTM同比增速。

    算法（按季度滚动，基期往前挪1季）：
      本期TTM = 最近4个单季之和 (quarters[-4:])
      基期TTM = 往前挪1季的4个单季之和 (quarters[-5:-1])
      TTM_YoY = (本期TTM - 基期TTM) / |基期TTM| × 100%

    要求至少5个季度数据。
    返回 None 表示数据不足或分母为0。
    """
    if len(quarters) < 5:
        return None

    cur_ttm = sum(q[2] for q in quarters[-4:])
    base_ttm = sum(q[2] for q in quarters[-5:-1])

    if base_ttm == 0:
        return None

    return (cur_ttm - base_ttm) / abs(base_ttm) * 100


def _rolling_ttm_yoy_pair(quarters: list) -> tuple[float | None, float | None]:
    """
    同时返回 (当期TTM增速, 上期TTM增速)。

    当期 = 最近4季 vs 往前1季的4季
    上期 = 再往前1季的4季 vs 再往前2季的4季（整体再挪1季）

    要求至少6个季度数据。
    """
    if len(quarters) < 6:
        return None, None

    # 当期：[-4:] vs [-5:-1]
    cur_ttm = sum(q[2] for q in quarters[-4:])
    base_ttm = sum(q[2] for q in quarters[-5:-1])
    cur_yoy = (cur_ttm - base_ttm) / abs(base_ttm) * 100 if base_ttm != 0 else None

    # 上期：[-5:-1] vs [-6:-2]
    prev_ttm = sum(q[2] for q in quarters[-5:-1])
    prev_base = sum(q[2] for q in quarters[-6:-2])
    prev_yoy = (prev_ttm - prev_base) / abs(prev_base) * 100 if prev_base != 0 else None

    return cur_yoy, prev_yoy


# ── 单季环比计算缓存 ──
_QOQ_CACHE = {}  # {code: {data, expire_time}}


def _get_single_quarter_profit(zip_path: str, code: str, all_files: list = None):
    """
    从报告期zip中提取单季度归母净利润。
    
    算法：本期累计(col[96]) - 上期累计(col[96])
    需要找到上一期的zip文件来读取上期累计值。
    
    返回：(single_quarter_profit, report_mmdd) 或 (None, None)
    """
    try:
        cw_dir = _TDX_CW_DIR
        cur_cum = _read_col25(zip_path, code)
        if cur_cum is None:
            return None, None
        
        mmdd = _extract_mmdd(zip_path)
        if not mmdd:
            return None, None
        
        year_str = re.search(r"gpcw(\d{4})", os.path.basename(zip_path)).group(1)
        
        # 根据当前报告期确定上一期
        quarter_map = {"0331": None, "0630": "0331", "0930": "0630", "1231": "0930"}
        prev_mmdd = quarter_map.get(mmdd)
        
        if prev_mmdd is None:
            # Q1没有上期（去年Q4），用年报代替
            prev_zip_path = os.path.join(cw_dir, f"gpcw{int(year_str)-1}1231.zip")
            if not os.path.exists(prev_zip_path) or os.path.getsize(prev_zip_path) < 10 * 1024:
                # 找最近的年报
                if all_files is None:
                    all_files = sorted([f for f in glob.glob(os.path.join(cw_dir, "gpcw*.zip"))
                                        if os.path.getsize(f) >= 10 * 1024], reverse=True)
                for f in all_files:
                    bn = os.path.basename(f)
                    if f == zip_path:
                        continue
                    if "1231" in bn and int(re.search(r"gpcw(\d{4})", bn).group(1)) < int(year_str):
                        prev_zip_path = f
                        break
            
            # Q1累计就是Q1单季，直接返回
            return cur_cum, mmdd
        else:
            # 其他季度：本期累计 - 同年上期累计
            prev_zip_path = os.path.join(cw_dir, f"gpcw{year_str}{prev_mmdd}.zip")
            if not os.path.exists(prev_zip_path) or os.path.getsize(prev_zip_path) < 10 * 1024:
                if all_files is None:
                    all_files = sorted([f for f in glob.glob(os.path.join(cw_dir, "gpcw*.zip"))
                                        if os.path.getsize(f) >= 10 * 1024], reverse=True)
                for f in all_files:
                    bn = os.path.basename(f)
                    if f == zip_path:
                        continue
                    if prev_mmdd in bn:
                        prev_zip_path = f
                        break
            
            prev_cum = _read_col25(prev_zip_path, code) if prev_zip_path else None
            if prev_cum is not None:
                return cur_cum - prev_cum, mmdd
            return None, None
    except Exception:
        return None, None


def calc_qoq_growth(code: str) -> dict:
    """
    计算单季净利润/营收环比增速（精细方案：季节调整）。
    
    精细方案说明：
      直接环比受季节性影响大（如Q1通常弱于Q4），
      因此用"去年同期环比"做基准进行季节调整：
      
      调整后环比 = 本期实际环比 - 去年同期环比
      
      > 0 表示本季环比改善幅度超过去年同期（边际加速）
      < 0 表示本季环比恶化或改善不足
    
    数据来源：通达信财务zip（col[96]累计归母净利润、col[74]营业总收入）
    
    返回：
      {
        "profit_qoq_sa": float|None,   # 季节调整后净利环比% (核心指标)
        "profit_qoq_raw": float|None,  # 原始净利环比%
        "revenue_qoq_sa": float|None,  # 季节调整后营收环比%
        "revenue_qoq_raw": float|None, # 原始营收环比%
      }
      若任何值为None表示数据不可用
    """
    now = time.time()
    if code in _QOQ_CACHE and _QOQ_CACHE[code]["expire_time"] > now:
        return _QOQ_CACHE[code]["data"]

    result = {
        "profit_qoq_sa": None, "profit_qoq_raw": None,
        "revenue_qoq_sa": None, "revenue_qoq_raw": None,
        "profit_single_q": None,   # 单季净利润绝对值(万元)，用于季度预期差对比
    }

    try:
        cw_dir = _TDX_CW_DIR
        all_files = sorted([f for f in glob.glob(os.path.join(cw_dir, "gpcw*.zip"))
                            if os.path.getsize(f) >= 10 * 1024], reverse=True)
        if len(all_files) < 2:
            return result

        latest_zip = all_files[0]

        # ── 本期单季利润 ──
        cur_profit, cur_mmdd = _get_single_quarter_profit(latest_zip, code, all_files)
        if cur_profit is None or cur_profit == 0:
            _QOQ_CACHE[code] = {"data": result, "expire_time": now + 3600}
            return result

        # ── 上期单季利润 ──
        prev_profit, prev_mmdd = None, None
        quarter_prev = {"0331": ("1231", -1), "0630": ("0331", 0), "0930": ("0630", 0), "1231": ("0930", 0)}
        target_mmdd, year_offset = quarter_prev.get(cur_mmdd, (None, 0))
        year_str = re.search(r"gpcw(\d{4})", os.path.basename(latest_zip)).group(1)
        prev_year = int(year_str) + year_offset

        if target_mmdd:
            prev_zip_for_q = os.path.join(cw_dir, f"gpcw{prev_year}{target_mmdd}.zip")
            if not os.path.exists(prev_zip_for_q) or os.path.getsize(prev_zip_for_q) < 10 * 1024:
                for f in all_files:
                    bn = os.path.basename(f)
                    if target_mmdd in bn:
                        m2 = re.search(r"gpcw(\d{4})", bn)
                        if m2 and int(m2.group(1)) <= prev_year:
                            prev_zip_for_q = f
                            break

            if os.path.exists(prev_zip_for_q) and os.path.getsize(prev_zip_for_q) >= 10 * 1024:
                prev_profit, _ = _get_single_quarter_profit(prev_zip_for_q, code, all_files)

        if prev_profit is not None and prev_profit != 0:
            profit_qoq_raw = (cur_profit - prev_profit) / abs(prev_profit) * 100
        else:
            _QOQ_CACHE[code] = {"data": result, "expire_time": now + 3600}
            return result

        # ── 去年同期环比（季节调整基准）──
        last_year = str(int(year_str) - 1)
        # 去年同期的"本期"zip
        ly_cur_zip = os.path.join(cw_dir, f"gpcw{last_year}{cur_mmdd}.zip")
        ly_cur_exists = os.path.exists(ly_cur_zip) and os.path.getsize(ly_cur_zip) >= 10 * 1024
        if not ly_cur_exists:
            for f in all_files:
                bn = os.path.basename(f)
                if cur_mmdd in bn:
                    m2 = re.search(r"gpcw(\d{4})", bn)
                    if m2 and m2.group(1) == last_year:
                        ly_cur_zip = f
                        ly_cur_exists = True
                        break

        if ly_cur_exists:
            ly_cur_profit, _ = _get_single_quarter_profit(ly_cur_zip, code, all_files)

            # 去年同期的"上期"zip
            ly_target_mmdd = target_mmdd
            ly_prev_year = int(last_year) + year_offset
            ly_prev_zip = os.path.join(cw_dir, f"gpcw{ly_prev_year}{ly_target_mmdd}.zip") if ly_target_mmdd else None
            ly_prev_exists = False
            if ly_prev_zip:
                ly_prev_exists = os.path.exists(ly_prev_zip) and os.path.getsize(ly_prev_zip) >= 10 * 1024
                if not ly_prev_exists:
                    for f in all_files:
                        bn = os.path.basename(f)
                        if ly_target_mmdd in bn:
                            m2 = re.search(r"gpcw(\d{4})", bn)
                            if m2 and int(m2.group(1)) <= ly_prev_year:
                                ly_prev_zip = f
                                ly_prev_exists = True
                                break

            if ly_cur_profit is not None:
                ly_prev_profit = None
                if ly_prev_exists:
                    ly_prev_profit, _ = _get_single_quarter_profit(ly_prev_zip, code, all_files)

                if ly_prev_profit is not None and ly_prev_profit != 0:
                    ly_qoq = (ly_cur_profit - ly_prev_profit) / abs(ly_prev_profit) * 100
                    # 季节调整后环比 = 本期实际环比 - 去年同期环比
                    profit_qoq_sa = profit_qoq_raw - ly_qoq
                elif ly_prev_profit == 0:
                    # 去年上期为0（亏损转盈利），无法算基准，用原始值
                    profit_qoq_sa = profit_qoq_raw
                else:
                    # 去年同期无上期数据
                    profit_qoq_sa = profit_qoq_raw
            else:
                profit_qoq_sa = profit_qoq_raw
        else:
            profit_qoq_sa = profit_qoq_raw

        result["profit_qoq_sa"] = round(profit_qoq_sa, 2) if profit_qoq_sa is not None else None
        result["profit_qoq_raw"] = round(profit_qoq_raw, 2)
        result["profit_single_q"] = cur_profit / 10000  # 单季净利润(万元)，col[96]原始单位为元

        # ── 营收环比（同样逻辑，用 col[74] 营业总收入）──
        rev_result = _calc_revenue_qoq(code, latest_zip, cur_mmdd, year_str,
                                         target_mmdd, year_offset, all_files)
        result.update(rev_result)

    except Exception:
        pass

    _QOQ_CACHE[code] = {"data": result, "expire_time": now + 3600}
    return result


def _read_col74(zip_path: str, code: str):
    """从zip中读取营业总收入(col[74])"""
    try:
        df = _load_fin_df(zip_path)
        if df is None or code not in df.index:
            return None
        val = df.loc[[code]].iloc[0].iloc[74]
        return float(val) if val == val else None
    except Exception:
        return None


def _calc_revenue_qoq(code: str, latest_zip: str, cur_mmdd: str, year_str: str,
                       target_mmdd: str, year_offset: int, all_files: list) -> dict:
    """
    计算营收的环比增速（与净利润环比相同的逻辑）。
    返回 {"revenue_qoq_sa": float|None, "revenue_qoq_raw": float|None}
    """
    result = {"revenue_qoq_sa": None, "revenue_qoq_raw": None}
    try:
        cw_dir = _TDX_CW_DIR

        def get_cum_rev(zp):
            rv = _read_col74(zp, code)
            if rv is None:
                return None
            mm = _extract_mmdd(zp)
            if not mm:
                return rv  # 年报就是全年营收
            qprev = {"0331": None, "0630": "0330", "0930": "0630", "1231": "0930"}.get(mm)
            if qprev is None:
                # Q1营收 = Q1累计
                return rv
            prev_zp = os.path.join(cw_dir, f"gpcw{re.search(r'gpcw(\\d{4})', zp).group(1)}{qprev}.zip")
            prev_rv = None
            if os.path.exists(prev_zp) and os.path.getsize(prev_zp) >= 10 * 1024:
                prev_rv = _read_col74(prev_zp, code)
            if prev_rv is not None:
                return rv - prev_rv
            return rv

        cur_rev = get_cum_rev(latest_zip)
        if cur_rev is None or cur_rev == 0:
            return result

        # 上期单季营收
        prev_year_int = int(year_str) + year_offset
        if target_mmdd:
            prev_rev_zp = os.path.join(cw_dir, f"gpcw{prev_year_int}{target_mmdd}.zip")
            if not os.path.exists(prev_rev_zp) or os.path.getsize(prev_rev_zp) < 10 * 1024:
                for f in all_files:
                    bn = os.path.basename(f)
                    if target_mmdd in bn:
                        m2 = re.search(r"gpcw(\d{4})", bn)
                        if m2 and int(m2.group(1)) <= prev_year_int:
                            prev_rev_zp = f
                            break
            prev_rev = get_cum_rev(prev_rev_zp) if os.path.exists(prev_rev_zp) and os.path.getsize(prev_rev_zp) >= 10 * 1024 else None
        else:
            prev_rev = None

        if prev_rev is not None and prev_rev != 0:
            rev_qoq_raw = (cur_rev - prev_rev) / abs(prev_rev) * 100
        else:
            return result

        # 去年同期环比
        last_yr = str(int(year_str) - 1)
        ly_cur_zp = os.path.join(cw_dir, f"gpcw{last_yr}{cur_mmdd}.zip")
        ly_cur_ok = False
        if os.path.exists(ly_cur_zp) and os.path.getsize(ly_cur_zp) >= 10 * 1024:
            ly_cur_ok = True
        else:
            for f in all_files:
                if cur_mmdd in os.path.basename(f):
                    m2 = re.search(r"gpcw(\d{4})", f)
                    if m2 and m2.group(1) == last_yr:
                        ly_cur_zp = f
                        ly_cur_ok = True
                        break

        if ly_cur_ok:
            ly_cur_rev = get_cum_rev(ly_cur_zp)
            ly_prev_rev = None
            if target_mmdd:
                ly_prev_yr = int(last_yr) + year_offset
                ly_prev_zp = os.path.join(cw_dir, f"gpcw{ly_prev_yr}{target_mmdd}.zip")
                if os.path.exists(ly_prev_zp) and os.path.getsize(ly_prev_zp) >= 10 * 1024:
                    ly_prev_rev = get_cum_rev(ly_prev_zp)

            if ly_cur_rev is not None and ly_prev_rev is not None and ly_prev_rev != 0:
                ly_rev_qoq = (ly_cur_rev - ly_prev_rev) / abs(ly_prev_rev) * 100
                rev_qoq_sa = rev_qoq_raw - ly_rev_qoq
            else:
                rev_qoq_sa = rev_qoq_raw
        else:
            rev_qoq_sa = rev_qoq_raw

        result["revenue_qoq_sa"] = round(rev_qoq_sa, 2) if rev_qoq_sa is not None else None
        result["revenue_qoq_raw"] = round(rev_qoq_raw, 2)
    except Exception:
        pass
    return result


def _extract_mmdd(zip_path: str):
    """从zip文件名提取报告期mmdd，如 gpcw20250930.zip → '0930'"""
    m = re.search(r"gpcw\d{4}(\d{4})\.zip", os.path.basename(zip_path))
    return m.group(1) if m else None


def calc_ttm_profit_growth(code: str) -> float | None:
    """
    计算滚动TTM净利润同比增速（按季度滚动）。

    算法：
      1. 从所有有效zip中提取单季利润时间序列
      2. 本期TTM = 最近4个单季之和
      3. 基期TTM = 往前挪1季的4个单季之和
      4. TTM_YoY = (本期TTM - 基期TTM) / |基期TTM| × 100%

    例：最新报告是2025年报
      本期TTM = 2025Q1+Q2+Q3+Q4 = 2025全年
      基期TTM = 2024Q2+Q3+Q4 + 2025Q1（往前挪1季）

    回退：若季度数据不足5个，用最新zip的col[184]净利同比%替代。

    返回：TTM同比增速百分比，如 15.3 表示 15.3%；失败返回 None
    """
    now = time.time()
    if code in _TTM_CACHE and _TTM_CACHE[code]["expire_time"] > now:
        return _TTM_CACHE[code]["ttm_yoy"]

    try:
        quarters = _build_quarterly_series(code)
        if len(quarters) >= 5:
            ttm_yoy = _rolling_ttm_yoy(quarters)
            if ttm_yoy is not None:
                _TTM_CACHE[code] = {"ttm_yoy": ttm_yoy, "expire_time": now + 3600}
                return ttm_yoy

        # 回退：季度数据不足，用col[184]净利同比%
        cw_dir = _TDX_CW_DIR
        all_files = sorted([f for f in glob.glob(os.path.join(cw_dir, "gpcw*.zip"))
                            if os.path.getsize(f) >= 10 * 1024], reverse=True)
        for f in all_files[:4]:
            val = _read_col184(f, code)
            if val is not None:
                _TTM_CACHE[code] = {"ttm_yoy": val, "expire_time": now + 3600}
                return val
        return None

    except Exception:
        return None


_PREV_TTM_CACHE = {}  # {code: {expire_time, ttm_yoy_prev}}


def calc_ttm_profit_growth_pair(code: str) -> tuple[float | None, float | None]:
    """
    一次读取同时返回 (当期TTM增速, 上期TTM增速)。

    按季度滚动，窗口各往前挪1季：
      当期 = 最近4季 vs 前一组4季
      上期 = 倒数第2组4季 vs 再前一组4季（整体再往前挪1季）

    例：有2023Q1~2025Q4共12个季度
      当期: [2025Q1~Q4] vs [2024Q2~Q4+2025Q1]
      上期: [2024Q2~Q4+2025Q1] vs [2023Q2~Q4+2024Q1]

    用于 single_line 的 TTM环比维度：衡量业绩边际加速。
    """
    now = time.time()
    cache_key = code

    # 先算当期TTM（复用已有函数+缓存）
    cur_ttm = calc_ttm_profit_growth(code)

    # 上期TTM缓存
    if cache_key in _PREV_TTM_CACHE and _PREV_TTM_CACHE[cache_key]["expire_time"] > now:
        prev_ttm = _PREV_TTM_CACHE[cache_key]["ttm_yoy_prev"]
        return cur_ttm, prev_ttm

    try:
        quarters = _build_quarterly_series(code)
        if len(quarters) >= 6:
            _, prev_ttm = _rolling_ttm_yoy_pair(quarters)
        else:
            prev_ttm = None

        _PREV_TTM_CACHE[cache_key] = {"ttm_yoy_prev": prev_ttm, "expire_time": now + 3600}
        return cur_ttm, prev_ttm

    except Exception:
        return cur_ttm, None


def _read_col184(zip_path: str, code: str):
    """从zip中读取净利同比%(col[184])，用于TTM回退"""
    try:
        df = _load_fin_df(zip_path)
        if df is None or code not in df.index:
            return None
        val = df.loc[[code]].iloc[0].iloc[184]
        return float(val) if val == val else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════
# 快报/预告 TTM 估算
# ══════════════════════════════════════════════════════════
_EXPRESS_CACHE = {}   # {code: {data, expire_time}}
_FORECAST_CACHE = {}  # {code: {data, expire_time}}


def fetch_express_ttm(code: str) -> dict:
    """
    从业绩快报估算 TTM 净利润同比增速。
    
    算法：
      1. 获取最新业绩快报的净利润 n_income
      2. 从本地 zip 读取去年同期累计利润 prev_cum 和去年全年 prev_full
      3. 快报TTM = n_income - prev_cum + prev_full
      4. TTM_YoY = (快报TTM - 去年TTM) / |去年TTM| × 100%
    
    返回:
      {"ttm_yoy": float|None, "source": "express|none", "report_period": str|None,
       "n_income": float|None, "ann_date": str|None}
    """
    import time as _time
    now = _time.time()
    if code in _EXPRESS_CACHE and _EXPRESS_CACHE[code]["expire_time"] > now:
        return _EXPRESS_CACHE[code]["data"]

    result = {"ttm_yoy": None, "source": "none", "report_period": None,
              "n_income": None, "ann_date": None}

    try:
        import requests as _req
        # 查询业绩快报（按报告期倒序）
        r = _req.post("https://www.codebuddy.cn/v2/tool/financedata",
                      json={"api_name": "express",
                            "params": {"ts_code": f"{code}.SZ" if code.startswith("0") or code.startswith("3") else f"{code}.SH"},
                            "fields": "ts_code,ann_date,end_date,n_income,revenue,diluted_eps"},
                      timeout=5)
        items = r.json().get("data", {}).get("items", [])
        if not items:
            _EXPRESS_CACHE[code] = {"data": result, "expire_time": now + 3600}
            return result

        # 取最新一条（公告日期倒序，items 已排序）
        latest = items[0]
        end_date = latest[2]       # 报告期 YYYYMMDD
        ann_date = latest[1]       # 公告日期
        n_income = latest[3]       # 归母净利润（元）

        if not n_income or float(n_income) == 0:
            _EXPRESS_CACHE[code] = {"data": result, "expire_time": now + 3600}
            return result

        n_income = float(n_income)
        result["report_period"] = end_date
        result["ann_date"] = ann_date
        result["n_income"] = n_income

        # 从本地 zip 估算 TTM
        ttm_yoy = _estimate_ttm_from_external(code, end_date, n_income)
        if ttm_yoy is not None:
            result["ttm_yoy"] = ttm_yoy
            result["source"] = "express"

    except Exception:
        pass

    _EXPRESS_CACHE[code] = {"data": result, "expire_time": now + 3600}
    return result


def fetch_forecast_ttm(code: str) -> dict:
    """
    从业绩预告估算 TTM 净利润同比增速。
    
    算法：
      1. 获取最新业绩预告的净利润区间 (min, max)，取中值
      2. 从本地 zip 读取去年同期累计利润和去年全年利润
      3. 用中值估算 TTM 和 TTM_YoY
    
    返回:
      {"ttm_yoy": float|None, "source": "forecast|none", "report_period": str|None,
       "net_profit_mid": float|None, "p_change_min": float|None, "p_change_max": float|None,
       "ann_date": str|None, "summary": str|None}
    """
    import time as _time
    now = _time.time()
    if code in _FORECAST_CACHE and _FORECAST_CACHE[code]["expire_time"] > now:
        return _FORECAST_CACHE[code]["data"]

    result = {"ttm_yoy": None, "source": "none", "report_period": None,
              "net_profit_mid": None, "p_change_min": None, "p_change_max": None,
              "ann_date": None, "summary": None}

    try:
        import requests as _req
        ts_code = f"{code}.SZ" if code.startswith("0") or code.startswith("3") else f"{code}.SH"
        r = _req.post("https://www.codebuddy.cn/v2/tool/financedata",
                      json={"api_name": "forecast",
                            "params": {"ts_code": ts_code},
                            "fields": "ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,summary"},
                      timeout=5)
        items = r.json().get("data", {}).get("items", [])
        if not items:
            _FORECAST_CACHE[code] = {"data": result, "expire_time": now + 3600}
            return result

        # 取最新一条
        latest = items[0]
        end_date = latest[2]        # 报告期
        ann_date = latest[1]        # 公告日期
        net_min = _to_f(latest[6])  # 净利润下限
        net_max = _to_f(latest[7])  # 净利润上限
        p_min = _to_f(latest[4])    # 增幅下限%
        p_max = _to_f(latest[5])    # 增幅上限%
        summary = latest[8]         # 摘要

        result["report_period"] = end_date
        result["ann_date"] = ann_date
        result["p_change_min"] = p_min
        result["p_change_max"] = p_max
        result["summary"] = summary

        # 净利润中值
        if net_min is not None and net_max is not None and (net_min + net_max) != 0:
            net_mid = (net_min + net_max) / 2
            result["net_profit_mid"] = net_mid

            # 估算 TTM
            ttm_yoy = _estimate_ttm_from_external(code, end_date, net_mid)
            if ttm_yoy is not None:
                result["ttm_yoy"] = ttm_yoy
                result["source"] = "forecast"
        elif p_min is not None and p_max is not None:
            # 只有增幅区间没有绝对值，取中值作为 TTM_YoY 近似
            result["ttm_yoy"] = (p_min + p_max) / 2
            result["source"] = "forecast"

    except Exception:
        pass

    _FORECAST_CACHE[code] = {"data": result, "expire_time": now + 3600}
    return result


def _estimate_ttm_from_external(code: str, report_period: str, current_profit: float) -> float | None:
    """
    根据外部数据（快报/预告的净利润）+ 本地 zip，估算滚动TTM同比增速。

    current_profit: 快报/预告的累计归母净利润（最新报告期）

    算法：将快报/预告的累计值拆为单季，追加到季度序列中，
    然后用与 calc_ttm_profit_growth 相同的滚动窗口计算。
    """
    try:
        quarters = _build_quarterly_series(code)
        if len(quarters) < 4:
            return None

        # 从报告期提取年份和季度
        if len(report_period) != 8:
            return None
        rp_year = int(report_period[:4])
        rp_mmdd = report_period[4:]
        mmdd_to_q = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}
        if rp_mmdd not in mmdd_to_q:
            return None
        rp_q = mmdd_to_q[rp_mmdd]

        # 如果该季已存在于序列中（zip已出），不重复添加
        if (rp_year, rp_q) in {(q[0], q[1]) for q in quarters}:
            # 已有数据，直接用标准计算
            if len(quarters) >= 8:
                return _rolling_ttm_yoy(quarters)
            return None

        # 拆出快报的单季利润：current_profit - 上期累计
        cw_dir = _TDX_CW_DIR
        all_files = sorted([f for f in glob.glob(os.path.join(cw_dir, "gpcw*.zip"))
                            if os.path.getsize(f) >= 10 * 1024], reverse=True)

        # Q1累计就是Q1单季，直接用
        if rp_q == 1:
            single_q = current_profit
        else:
            q_map = {2: "0331", 3: "0630", 4: "0930"}
            prev_zip = os.path.join(cw_dir, f"gpcw{rp_year}{q_map[rp_q]}.zip")
            if os.path.exists(prev_zip) and os.path.getsize(prev_zip) >= 10 * 1024:
                prev_cum = _read_col25(prev_zip, code)
                if prev_cum is not None:
                    single_q = current_profit - prev_cum
                else:
                    return None
            else:
                return None

        # 追加到序列
        extended = quarters + [(rp_year, rp_q, single_q)]
        if len(extended) >= 5:
            return _rolling_ttm_yoy(extended)

        return None

    except Exception:
        return None


def fetch_best_ttm(code: str) -> dict:
    """
    获取最佳可用 TTM 净利润增速，优先级：
      1. 正式财报（calc_ttm_profit_growth，本地zip）
      2. 业绩快报（express，网络+本地zip估算）
      3. 业绩预告（forecast，网络+本地zip估算）
    
    返回:
      {"ttm_yoy": float|None, "source": "report|express|forecast|none",
       "report_period": str|None, ...}
    """
    # 1. 正式财报
    ttm_report = calc_ttm_profit_growth(code)
    if ttm_report is not None:
        return {"ttm_yoy": ttm_report, "source": "report"}

    # 2. 业绩快报
    express = fetch_express_ttm(code)
    if express.get("ttm_yoy") is not None:
        return {"ttm_yoy": express["ttm_yoy"], "source": "express",
                "report_period": express.get("report_period"),
                "ann_date": express.get("ann_date")}

    # 3. 业绩预告
    forecast = fetch_forecast_ttm(code)
    if forecast.get("ttm_yoy") is not None:
        return {"ttm_yoy": forecast["ttm_yoy"], "source": "forecast",
                "report_period": forecast.get("report_period"),
                "ann_date": forecast.get("ann_date"),
                "summary": forecast.get("summary")}

    return {"ttm_yoy": None, "source": "none"}


# ══════════════════════════════════════════════════════════
# 策略 C：超预期成长型（surprise）
# ══════════════════════════════════════════════════════════

# 超预期信号模式
SURPRISE_MODE_FORWARD = "forward"   # 前瞻模式：一致预期 > TTM实际（市场预期未来加速）
SURPRISE_MODE_ACTUAL  = "actual"    # 验证模式：TTM实际 > 一致预期（财报超市场预期）


def _calc_surprise_diff(expect_yoy, ttm_yoy, mode="forward"):
    """
    计算超预期 diff 和方向标签。
    
    forward 模式：diff = expect_yoy - ttm_yoy（预期>实际=超预期）
      适用：判断市场是否预期未来好于过去，寻找"二次加速"
    actual 模式：diff = ttm_yoy - expect_yoy（实际>预期=超预期）
      适用：财报发布后验证，实际跑赢预期=超预期
    """
    if expect_yoy is None or ttm_yoy is None:
        return None, "无数据"
    
    if mode == "actual":
        diff = ttm_yoy - expect_yoy
        if diff > 0:
            label = f"超预期(实际{ttm_yoy:.0f}%>预期{expect_yoy:.0f}%)"
        elif diff > -10:
            label = f"基本符合(实际{ttm_yoy:.0f}%vs预期{expect_yoy:.0f}%)"
        else:
            label = f"不及预期(实际{ttm_yoy:.0f}%<预期{expect_yoy:.0f}%)"
    else:  # forward
        diff = expect_yoy - ttm_yoy
        if diff > 0:
            label = f"预期加速(预期{expect_yoy:.0f}%>TTM{ttm_yoy:.0f}%)"
        elif diff > -10:
            label = f"预期持平(预期{expect_yoy:.0f}%vs TTM{ttm_yoy:.0f}%)"
        else:
            label = f"预期减速(预期{expect_yoy:.0f}%<TTM{ttm_yoy:.0f}%)"
    
    return diff, label


def _surprise(pe, mcap, profit_yoy, revenue_yoy, roe,
              expect_yoy=None, ttm_yoy=None, org_num=None,
              surprise_mode="forward", prev_ttm_yoy=None) -> dict:
    """
    超预期成长股打分，满分 100。

    维度：超预期信号20 + TTM加速20 + PEG15 + 净利20 + 结构15 + ROE10 = 100

    surprise_mode 控制超预期信号方向：
      "forward"（默认）：diff = 预期 - 实际，正值=市场预期加速
      "actual"：diff = 实际 - 预期，正值=财报超预期

    prev_ttm_yoy: 上期TTM增速，用于计算TTM加速(ttm_yoy - prev_ttm_yoy)
    """
    score, sigs = 0, []

    # ── 超预期信号（20 分，核心维度）────────────────────
    surprise_s = 0
    diff, label = _calc_surprise_diff(expect_yoy, ttm_yoy, surprise_mode)
    
    if diff is not None:
        # 分档（diff 正值越大越好，两种模式统一分档逻辑）
        if diff > 20:
            surprise_s = 20
        elif diff > 10:
            surprise_s = 16
        elif diff > 0:
            surprise_s = 10
        elif diff > -10:
            surprise_s = 3
        else:
            surprise_s = -3
        sigs.append(label)

        # 机构关注度加权
        if org_num is not None and org_num >= 10 and diff > 0:
            bonus = min(int(org_num / 10), 5)
            surprise_s = min(surprise_s + bonus, 20)
            if bonus > 0:
                sigs.append(f"{int(org_num)}家机构覆盖")
    else:
        sigs.append("无一致预期数据")

    score += surprise_s

    # ── TTM 加速（20 分，ttm1 vs ttm2）─────────────────
    ttm_accel_s = 0
    if ttm_yoy is not None and prev_ttm_yoy is not None:
        accel = ttm_yoy - prev_ttm_yoy  # 正值=加速，负值=减速
        if   accel > 20:  ttm_accel_s = 20; sigs.append(f"TTM加速+{accel:.1f}%(ttm1:{ttm_yoy:.0f}%>ttm2:{prev_ttm_yoy:.0f}%)")
        elif accel > 10:  ttm_accel_s = 16; sigs.append(f"TTM加速+{accel:.1f}%")
        elif accel > 0:   ttm_accel_s = 10; sigs.append(f"TTM微加速+{accel:.1f}%")
        elif accel > -10: ttm_accel_s =  3; sigs.append(f"TTM微减速{accel:.1f}%")
        else:             ttm_accel_s = -3; sigs.append(f"TTM减速{accel:.1f}%")
    elif ttm_yoy is not None:
        sigs.append(f"TTM{ttm_yoy:.0f}%(缺上期)")
    else:
        sigs.append("缺TTM数据")
    score += ttm_accel_s

    # ── PEG 估值（15 分）────────────────────────────────
    peg_s = 0
    # 优先用预期增速算 PEG（更前瞻），fallback 用 profit_yoy
    peg_growth = expect_yoy if expect_yoy and expect_yoy > 0 else profit_yoy
    if pe is not None and peg_growth and peg_growth > 0:
        peg = pe / peg_growth
        if   peg < 0.5:  peg_s = 15; sigs.append(f"PEG{peg:.2f}严重低估")
        elif peg < 1.0:  peg_s = 12; sigs.append(f"PEG{peg:.2f}合理偏低")
        elif peg < 1.5:  peg_s = 8;  sigs.append(f"PEG{peg:.2f}合理")
        elif peg < 2.0:  peg_s = 4;  sigs.append(f"PEG{peg:.2f}偏贵")
        else:            peg_s = -3; sigs.append(f"PEG{peg:.2f}高估")
    elif pe is not None:
        if   pe < 0:     peg_s = -5; sigs.append("亏损")
        elif pe < 50:    peg_s = 8;  sigs.append(f"PE{pe:.1f}")
        elif pe < 100:   peg_s = 4;  sigs.append(f"PE{pe:.1f}偏高")
        else:             peg_s = 0;  sigs.append(f"PE{pe:.1f}高")
    score += peg_s

    # ── 净利润同比（20 分）──────────────────────────────
    profit_s = 0
    if profit_yoy is not None:
        if   profit_yoy > 50:  profit_s = 20; sigs.append(f"净利+{profit_yoy:.1f}%爆发")
        elif profit_yoy > 30:  profit_s = 16; sigs.append(f"净利+{profit_yoy:.1f}%高增")
        elif profit_yoy > 15:  profit_s = 10; sigs.append(f"净利+{profit_yoy:.1f}%")
        elif profit_yoy > 0:   profit_s =  5; sigs.append(f"净利+{profit_yoy:.1f}%低增")
        elif profit_yoy > -15: profit_s = -5; sigs.append(f"净利{profit_yoy:.1f}%下滑")
        else:                  profit_s =-10; sigs.append(f"净利{profit_yoy:.1f}%大降")
    score += profit_s

    # ── 收入/利润结构（15 分）───────────────────────────
    struct_s = 0
    if profit_yoy is not None and revenue_yoy is not None:
        if revenue_yoy > 0 and profit_yoy > revenue_yoy:
            struct_s = 15; sigs.append(f"利润弹性(营收+{revenue_yoy:.1f}%→净利+{profit_yoy:.1f}%)")
        elif revenue_yoy > 20 and profit_yoy > 0:
            struct_s = 10; sigs.append(f"营收高增+{revenue_yoy:.1f}%")
        elif revenue_yoy > 0 and profit_yoy > 0:
            struct_s =  8; sigs.append(f"营收+{revenue_yoy:.1f}%稳健")
        elif revenue_yoy > 0 and profit_yoy <= 0:
            struct_s =  2; sigs.append("营收增但利润承压")
        else:
            struct_s = -5; sigs.append("收入利润双降")
    elif revenue_yoy is not None:
        if   revenue_yoy > 30: struct_s = 10; sigs.append(f"营收+{revenue_yoy:.1f}%")
        elif revenue_yoy > 15: struct_s =  6; sigs.append(f"营收+{revenue_yoy:.1f}%")
        elif revenue_yoy > 0:  struct_s =  3; sigs.append(f"营收+{revenue_yoy:.1f}%")
        else:                  struct_s = -3; sigs.append(f"营收{revenue_yoy:.1f}%")
    score += struct_s

    # ── ROE 成长潜力（10 分，放宽阈值）─────────────────
    roe_s = 0
    if roe is not None:
        if   roe > 20:  roe_s = 10; sigs.append(f"ROE{roe:.1f}%优")
        elif roe > 15:  roe_s =  8; sigs.append(f"ROE{roe:.1f}%")
        elif roe > 10:  roe_s =  5; sigs.append(f"ROE{roe:.1f}%")
        elif roe > 5:   roe_s =  2; sigs.append(f"ROE{roe:.1f}%扩张期")
        else:           roe_s = -3; sigs.append(f"ROE{roe:.1f}%偏低")
    score += roe_s




    return {
        "total":     max(score, 0),
        "strategy":  "surprise",
        "sigs":      sigs,
        "breakdown": {
            "surprise":  surprise_s,
            "ttm_accel": ttm_accel_s,
            "peg":       peg_s,
            "profit":    profit_s,
            "struct":    struct_s,
            "roe":       roe_s,
        },
        "surprise_meta": {
            "expect_yoy":    expect_yoy,
            "ttm_yoy":       ttm_yoy,
            "prev_ttm_yoy":  prev_ttm_yoy,
            "diff":          diff,
            "surprise_mode": surprise_mode,
            "org_num":       org_num,
        },
    }


def surprise_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                          expect_yoy=None, ttm_yoy=None, org_num=None,
                          surprise_mode="forward", prev_ttm_yoy=None) -> dict:
    """
    超预期策略评分（带额外数据）。
    surprise_mode: "forward"(前瞻) 或 "actual"(验证)
    prev_ttm_yoy: 上期TTM增速，用于TTM加速判断
    """
    result = _surprise(pe, mcap, profit_yoy, revenue_yoy, roe,
                       expect_yoy=expect_yoy, ttm_yoy=ttm_yoy, org_num=org_num,
                       surprise_mode=surprise_mode, prev_ttm_yoy=prev_ttm_yoy)
    result["reasons"] = _generate_reasons(result, "surprise")
    return result


# ══════════════════════════════════════════════════════════
# 策略 D：短线策略（single_line）
# ══════════════════════════════════════════════════════════
def _calc_qdiff_points(qdiff: float) -> int:
    """根据预期差值计算得分(0~8)。"""
    if   qdiff > 20: return 8
    elif qdiff > 10: return 7
    elif qdiff > 5:  return 6
    elif qdiff > 2:  return 5
    elif qdiff > 0:  return 4
    elif qdiff > -5: return 1
    else:            return 0


def _apply_qdiff_score(qdiff: float, qdiff_mode: str, s_mode: str, sigs: list):
    """生成预期差信号标签（副作用：往sigs追加文字）。"""
    if qdiff_mode == "ttm":
        label = "TTM" if s_mode == "actual" else "TTM预期差"
    else:
        label = "Q超预期" if s_mode == "actual" else "Q预期差"

    if   qdiff > 20: sigs.append(f"⚡{label}+{qdiff:.0f}%强催化")
    elif qdiff > 10: sigs.append(f"{label}+{qdiff:.0f}%显著")
    elif qdiff > 5:  sigs.append(f"{label}+{qdiff:.0f}%催化")
    elif qdiff > 2:  sigs.append(f"{label}+{qdiff:.0f}%")
    elif qdiff > 0:  sigs.append(f"微{label.replace('TTM','').replace('Q','')}{qdiff:.0f}%" if qdiff_mode == "ttm" else f"微{label}+{qdiff:.0f}%")
    elif qdiff > -10:sigs.append(f"{label}略负{qdiff:.0f}%")  # 不加分但记录
    # <= -10 不输出，静默


def _single_line(pe, mcap, profit_yoy, revenue_yoy, roe,
                 qoq_data=None, quarterly_consensus=None,
                 **kwargs) -> dict:
    """
    短线选股策略评分，满分100。
    
    核心理念：基本面不是"给公司打分"，而是"找催化因素"。
    短线核心关注边际动能：TTM环比加速 + 拐点信号。
    
    额外参数（kwargs）：
      ttm_yoy: 当期TTM净利润同比增速%
      prev_ttm_yoy: 上期TTM净利润同比增速%（用于环比计算）
      expect_yoy: 券商一致预期净利润增速%
      org_num: 覆盖机构数
      qdiff_mode: "quarter" / "ttm"
      surprise_mode: "forward" / "actual"
    
    评分结构（v3，短线边际动能优先）：
      利润边际变化 (40分)
        同比方向 (12分) + TTM环比加速 (15分) + TTM拐点 (5分) + 预期差 (8分)
      营收/利润结构 (20分)
        营收同比 (8分) + 利润弹性/结构 (12分)
      估值安全垫 (20分)
        PEG主导 (15分) + PE辅助 (5分)
      盈利质量 (14分)
        ROE放宽 (10分) + 反转加成 (0~4分)
      市值弹性 (6分)
    """
    score, sigs = 0, []

    # ── 1. 利润边际变化 (40 分) ─────────────────────────────
    
    # 1a. 同比方向 (12分，短线降权：绝对增速不如边际变化重要)
    yoy_s = 0
    if profit_yoy is not None:
        if   profit_yoy > 100: yoy_s = 12; sigs.append(f"净利同比+{profit_yoy:.0f}%超高增")
        elif profit_yoy > 50:  yoy_s = 10; sigs.append(f"净利同比+{profit_yoy:.0f}%爆发")
        elif profit_yoy > 30:  yoy_s = 8;  sigs.append(f"净利同比+{profit_yoy:.0f}%高增")
        elif profit_yoy > 15:  yoy_s = 6;  sigs.append(f"净利同比+{profit_yoy:.0f}%")
        elif profit_yoy > 5:   yoy_s = 4;  sigs.append(f"净利同比+{profit_yoy:.0f}%低增")
        elif profit_yoy > 0:   yoy_s = 2;  sigs.append(f"微增+{profit_yoy:.0f}%")
        elif profit_yoy > -20: yoy_s = -3; sigs.append(f"净利{profit_yoy:.0f}%下滑")
        else:                  yoy_s = -8; sigs.append(f"净利大降{profit_yoy:.0f}%")
    score += yoy_s

    # 1b. TTM环比加速 (15分，短线核心！)
    #   本期TTM增速 vs 上期TTM增速，衡量业绩边际改善
    ttm_qoq_s = 0
    ttm_yoy = kwargs.get("ttm_yoy")
    prev_ttm_yoy = kwargs.get("prev_ttm_yoy")
    
    if ttm_yoy is not None and prev_ttm_yoy is not None:
        ttm_accel = ttm_yoy - prev_ttm_yoy  # 正值=加速，负值=减速
        if   ttm_accel > 30:  ttm_qoq_s = 15; sigs.append(f"TTM环比+{ttm_accel:.0f}pp大幅加速")
        elif ttm_accel > 20:  ttm_qoq_s = 13; sigs.append(f"TTM环比+{ttm_accel:.0f}pp显著加速")
        elif ttm_accel > 10:  ttm_qoq_s = 11; sigs.append(f"TTM环比+{ttm_accel:.0f}pp加速")
        elif ttm_accel > 5:   ttm_qoq_s = 8;  sigs.append(f"TTM环比+{ttm_accel:.0f}pp微加速")
        elif ttm_accel > 0:   ttm_qoq_s = 5;  sigs.append(f"TTM环比+{ttm_accel:.0f}pp")
        elif ttm_accel > -5:  ttm_qoq_s = 2;  sigs.append("TTM环比持平")
        elif ttm_accel > -15: ttm_qoq_s = 0;  sigs.append(f"TTM环比{ttm_accel:.0f}pp减速")
        else:                 ttm_qoq_s = 0;  sigs.append(f"TTM环比{ttm_accel:.0f}pp显著减速")
    elif ttm_yoy is not None:
        sigs.append(f"TTM{ttm_yoy:.0f}%(缺上期数据)")
    # 无TTM数据时：不加分也不扣分，静默跳过
    score += ttm_qoq_s

    # 1c. TTM拐点 (5分，独立加分)
    #   上期TTM增速为负，本期TTM增速转正 → 业绩实质性改善拐点
    ttm_turn_s = 0
    if prev_ttm_yoy is not None and ttm_yoy is not None:
        if prev_ttm_yoy < 0 and ttm_yoy > 0:
            ttm_turn_s = 5
            sigs.append(f"⚡TTM拐点转正({prev_ttm_yoy:.0f}%→{ttm_yoy:.0f}%)")
    score += ttm_turn_s

    # 1d. 季度预期差 (8分)
    #   支持两种对比模式(qdiff_mode) × 两种方向(surprise_mode):
    #
    #   qdiff_mode="quarter"(默认): 用 expect_yoy(年度预期增速%) vs profit_yoy(累计实际增速%)
    #     forward: expect_yoy > profit_yoy → 市场预期加速 (找"预期差")
    #     actual:  profit_yoy > expect_yoy → 实际超预期 (找"惊喜")
    #
    #   qdiff_mode="ttm": 用 expect_yoy(年度EPS预期) vs ttm_yoy(TTM实际)
    #     forward: expect_yoy > ttm_yoy → 市场预期加速
    #     actual:  ttm_yoy > expect_yoy → 实际超预期
    #
    qdiff_s = 0
    qdiff_mode = kwargs.get("qdiff_mode", "quarter")      # quarter / ttm
    s_mode = kwargs.get("surprise_mode", "forward")         # forward / actual

    if qdiff_mode == "ttm":
        # ── TTM 模式：复用 surprise 的年度预期 vs TTM ──
        expect_ref = kwargs.get("expect_yoy")
        ttm_ref = kwargs.get("ttm_yoy")
        if expect_ref is not None and ttm_ref is not None:
            if s_mode == "forward":
                qdiff = expect_ref - ttm_ref       # 预期 > 实际 → 正=催化
            else:
                qdiff = ttm_ref - expect_ref       # 实际 > 预期 → 正=超预期
            _apply_qdiff_score(qdiff, qdiff_mode, s_mode, sigs)
            qdiff_s = _calc_qdiff_points(qdiff)
        elif expect_ref is not None:
            sigs.append(f"TTM预期{expect_ref:.0f}%(缺TTM实际)")
        elif ttm_ref is not None:
            sigs.append(f"TTM实际{ttm_ref:.0f}%(缺预期)")
        else:
            sigs.append("无TTM预期数据")

    else:
        # ── quarter 模式：年度预期增速% vs 累计实际增速% ──
        expect_ref = kwargs.get("expect_yoy")
        org_num_val = kwargs.get("org_num")

        if expect_ref is not None and profit_yoy is not None:
            if s_mode == "forward":
                qdiff = expect_ref - profit_yoy   # 预期 > 实际 → 正=催化
            else:
                qdiff = profit_yoy - expect_ref   # 实际 > 预期 → 正=超预期
            _apply_qdiff_score(qdiff, qdiff_mode, s_mode, sigs)
            qdiff_s = _calc_qdiff_points(qdiff)

            # 机构覆盖加成
            if org_num_val is not None and org_num_val >= 5 and qdiff_s > 0:
                qdiff_s = min(qdiff_s + 1, 8)
                sigs.append(f"{org_num_val}家机构覆盖")

        elif expect_ref is not None:
            # 有预期但无累计实际增速
            if org_num_val is not None and org_num_val >= 3:
                qdiff_s = 2
                sigs.append(f"{org_num_val}家覆盖(缺实际增速)")
        else:
            sigs.append("无预期数据")

    score += qdiff_s
    margin_total = yoy_s + ttm_qoq_s + ttm_turn_s + qdiff_s

    # ── 2. 营收/利润结构 (20 分，growth 风格) ───────────────
    rev_s = 0
    
    # 2a. 营收同比 (8分)
    if revenue_yoy is not None:
        if   revenue_yoy > 30: rev_s = 8; sigs.append(f"营收+{revenue_yoy:.0f}%高增")
        elif revenue_yoy > 15: rev_s = 6; sigs.append(f"营收+{revenue_yoy:.0f}%")
        elif revenue_yoy > 5:  rev_s = 4; sigs.append(f"营收+{revenue_yoy:.0f}%")
        elif revenue_yoy > 0:  rev_s = 2; sigs.append(f"微增+{revenue_yoy:.0f}%")
        elif revenue_yoy > -15: rev_s = 0
        else:                  rev_s = -3; sigs.append(f"营收大降{revenue_yoy:.0f}%")
    
    # 2b. 利润弹性/收入利润结构 (12分，growth 风格升级)
    struct_s = 0
    if profit_yoy is not None and revenue_yoy is not None:
        # 核心逻辑：利润增速 > 收入增速 = 规模效应/利润弹性释放（最优）
        if revenue_yoy > 0 and profit_yoy > revenue_yoy * 1.2:
            struct_s = 12; sigs.append(f"⚡高弹性行(营+{revenue_yoy:.0f}%→利+{profit_yoy:.0f}%)")
        elif revenue_yoy > 20 and profit_yoy > 0:
            struct_s = 9;  sigs.append(f"营收高增扩张+{revenue_yoy:.0f}%")
        elif revenue_yoy > 0 and profit_yoy > revenue_yoy:
            struct_s = 8;  sigs.append(f"利润弹性好(营+{revenue_yoy:.0f}%→利+{profit_yoy:.0f}%)")
        elif revenue_yoy > 0 and profit_yoy > 0:
            struct_s = 5;  sigs.append("营收利润双增")
        elif revenue_yoy > 0 and profit_yoy <= 0:
            struct_s = 1;  sigs.append("增收不增利⚠️")
        elif revenue_yoy <= 0 and profit_yoy > 20:
            struct_s = 2;  sigs.append("降收增利(控费?)")
        else:
            struct_s = -4; sigs.append("收入利润双降❌")
    elif revenue_yoy is not None:
        if   revenue_yoy > 30: struct_s = 7; sigs.append(f"营收+{revenue_yoy:.0f}%")
        elif revenue_yoy > 15: struct_s = 5; sigs.append(f"营收+{revenue_yoy:.0f}%")
        elif revenue_yoy > 0:  struct_s = 3; sigs.append(f"营收+{revenue_yoy:.0f}%")
        else:                  struct_s = -2; sigs.append(f"营收{revenue_yoy:.0f}%")
    score += rev_s + struct_s

    # ── 估值安全垫 (20 分，PEG主导 + PE辅助) ─────────────
    val_s = 0
    
    # 3a. PEG 估值 (15分，主维度，growth 同款逻辑)
    peg_s_val = 0
    growth_for_peg = profit_yoy if profit_yoy and profit_yoy > 0 else kwargs.get("ttm_yoy")
    if pe is not None and growth_for_peg and growth_for_peg > 0:
        peg = pe / growth_for_peg
        if   peg < 0.5: peg_s_val = 15; sigs.append(f"PEG{peg:.2f}严重低估")
        elif peg < 1.0: peg_s_val = 13; sigs.append(f"PEG{peg:.2f}合理偏低")
        elif peg < 1.5: peg_s_val = 10; sigs.append(f"PEG{peg:.2f}合理")
        elif peg < 2.0: peg_s_val = 6;  sigs.append(f"PEG{peg:.2f}偏贵")
        else:            peg_s_val = -2; sigs.append(f"PEG{peg:.2f}高估")
    elif pe is not None:
        # 亏损或增速为负时，退化为宽松 PE 判断（不重惩高PE成长股）
        if   pe < 0:     peg_s_val = -6; sigs.append("亏损")
        elif pe < 50:    peg_s_val = 8;  sigs.append(f"PE{pe:.0f}")
        elif pe < 100:   peg_s_val = 4;  sigs.append(f"PE{pe:.0f}偏高")
        else:            peg_s_val = 0;  sigs.append(f"PE{pe:.0f}高")
    score += peg_s_val
    
    # 3b. PE 辅助修正 (5分，对极端PE做微调)
    pe_adj = 0
    if pe is not None and pe > 0:
        if   pe < 10:   pass  # PEG已经给了高分，不再重复奖励
        elif pe < 25:   pass  # 合理区间
        elif pe < 50:   pe_adj = 0
        elif pe < 100:  pe_adj = -1
        elif pe < 200:  pe_adj = -2
        elif pe < 500:  pe_adj = -4
        else:           pe_adj = -5; sigs.append(f"PE{pe:.0f}泡沫⛔")
    score += pe_adj

    # ── 盈利质量 (14 分，ROE放宽 + 反转加成) ─────────────────
    quality_s = 0
    turnaround = 0  # 独立反转加分
    
    if roe is not None:
        # growth 风格：ROE阈值放宽，扩张期不重罚
        if   roe > 20:  quality_s = 10; sigs.append(f"ROE{roe:.1f}%优")
        elif roe > 15:  quality_s = 8;  sigs.append(f"ROE{roe:.1f}%")
        elif roe > 10:  quality_s = 5;  sigs.append(f"ROE{roe:.1f}%")
        elif roe > 5:   quality_s = 2;  sigs.append(f"ROE{roe:.1f}%扩张期")
        else:           quality_s = -2; sigs.append(f"ROE{roe:.1f}%偏低")
        
        # 周期反转特殊处理：ROE极低但利润暴发 → 独立反转加分
        if profit_yoy and profit_yoy > 50:
            if roe < 3:
                turnaround = 4; sigs.append(f"(⚡周期反转!利+{profit_yoy:.0f}%)")
            elif roe < 5:
                turnaround = 3; sigs.append(f"(利润暴发+{profit_yoy:.0f}%)")
            elif roe < 8 and quality_s < 4:
                turnaround = 2; sigs.append(f"(高增低ROE+{profit_yoy:.0f}%)")
    score += quality_s + turnaround

    # ── 市值弹性 (6 分，短线主力区间：100~1000亿) ─────
    mcap_s = 0
    if mcap:
        if   mcap <= 20:          mcap_s = 0;  sigs.append(f"微盘{mcap:.0f}亿")
        elif 20 < mcap < 50:      mcap_s = 2;  sigs.append(f"小盘{mcap:.0f}亿")
        elif 50 <= mcap < 100:    mcap_s = 4;  sigs.append(f"中小盘{mcap:.0f}亿")
        elif 100 <= mcap < 1000:  mcap_s = 6;  sigs.append(f"中盘{mcap:.0f}亿")
        elif 1000 <= mcap < 3000: mcap_s = 4;  sigs.append(f"大盘{mcap:.0f}亿")
        else:                     mcap_s = 2;  sigs.append(f"超大盘{mcap:.0f}亿")
    score += mcap_s

    return {
        "total": max(score, 0),
        "strategy": "single_line",
        "sigs": sigs,
        "breakdown": {
            "yoy":        yoy_s,
            "ttm_qoq":    ttm_qoq_s,
            "ttm_turn":   ttm_turn_s,
            "qdiff":      qdiff_s,
            "revenue":    rev_s,
            "struct":     struct_s,
            "peg":        peg_s_val,
            "pe_adj":     pe_adj,
            "roe":        quality_s,
            "turnaround": turnaround,
            "mcap":       mcap_s,
        },
        "single_line_meta": {
            "ttm_yoy":        ttm_yoy,
            "prev_ttm_yoy":   prev_ttm_yoy,
            "ttm_accel":      (ttm_yoy - prev_ttm_yoy) if (ttm_yoy is not None and prev_ttm_yoy is not None) else None,
            "q_expected_np":  q_expected_np if qdiff_mode != "ttm" else None,
            "q_predict_count": q_predict_count if qdiff_mode != "ttm" else None,
            "margin_total":   margin_total,
        },
    }


def single_line_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                              qoq_data=None, quarterly_consensus=None,
                              expect_yoy=None, ttm_yoy=None, org_num=None,
                              prev_ttm_yoy=None,
                              **kwargs) -> dict:
    """短线策略评分（带额外数据）"""
    result = _single_line(pe, mcap, profit_yoy, revenue_yoy, roe,
                           qoq_data=qoq_data, quarterly_consensus=quarterly_consensus,
                           expect_yoy=expect_yoy, ttm_yoy=ttm_yoy, org_num=org_num,
                           prev_ttm_yoy=prev_ttm_yoy,
                           **kwargs)
    result["reasons"] = _generate_reasons(result, "single_line")
    return result
# ══════════════════════════════════════════════════════════
def fund_score_combo(pe, mcap, profit_yoy, revenue_yoy, roe,
                     strategy_str: str = DEFAULT_STRATEGY,
                     **kwargs) -> dict:
    """
    多策略组合评分（核心接口）。
    strategy_str 支持逗号分隔，如 'classic,surprise'。
    多策略时取各策略分数的简单平均，sigs 合并展示。

    返回：
      {
        "total": int,          # 最终基本面分（0-100）
        "sigs": [...],         # 合并信号
        "breakdown": {...},    # 各策略细节
        "strategy": str,       # 实际使用的策略字符串
        "strategy_label": str, # 友好名称
        "reasons": [...],
        "surprise_meta": dict | None,  # 仅 surprise 策略时有值
      }
    """
    parts = parse_strategies(strategy_str)

    if len(parts) == 1:
        # 单策略，直接走原路径
        s = parts[0]
        if s == "surprise":
            result = surprise_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                                           expect_yoy=kwargs.get("expect_yoy"),
                                           ttm_yoy=kwargs.get("ttm_yoy"),
                                           org_num=kwargs.get("org_num"),
                                           surprise_mode=kwargs.get("surprise_mode", "forward"),
                                           prev_ttm_yoy=kwargs.get("prev_ttm_yoy"))
        elif s == "single_line":
            result = single_line_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                                              qoq_data=kwargs.get("qoq_data"),
                                              quarterly_consensus=kwargs.get("quarterly_consensus"),
                                              expect_yoy=kwargs.get("expect_yoy"),
                                              ttm_yoy=kwargs.get("ttm_yoy"),
                                              org_num=kwargs.get("org_num"),
                                              qdiff_mode=kwargs.get("qdiff_mode", "quarter"),
                                              surprise_mode=kwargs.get("surprise_mode", "forward"))
        else:
            result = fund_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                                       strategy=s, **kwargs)
        result["strategy"] = strategy_str
        result["strategy_label"] = get_strategy_label(strategy_str)
        return result

    # ── 多策略：逐一计算后取平均 ────────────────────────
    sub_results = {}
    s_mode = kwargs.get("surprise_mode", "forward")
    for s in parts:
        if s == "surprise":
            r = _surprise(pe, mcap, profit_yoy, revenue_yoy, roe,
                          expect_yoy=kwargs.get("expect_yoy"),
                          ttm_yoy=kwargs.get("ttm_yoy"),
                          org_num=kwargs.get("org_num"),
                          surprise_mode=s_mode,
                          prev_ttm_yoy=kwargs.get("prev_ttm_yoy"))
        elif s == "single_line":
            r = _single_line(pe, mcap, profit_yoy, revenue_yoy, roe,
                             qoq_data=kwargs.get("qoq_data"),
                             quarterly_consensus=kwargs.get("quarterly_consensus"),
                             expect_yoy=kwargs.get("expect_yoy"),
                             ttm_yoy=kwargs.get("ttm_yoy"),
                             org_num=kwargs.get("org_num"),
                             qdiff_mode=kwargs.get("qdiff_mode", "quarter"),
                             surprise_mode=kwargs.get("surprise_mode", "forward"))
        elif s == "growth":
            r = _growth(pe, mcap, profit_yoy, revenue_yoy, roe)
        else:
            r = _classic(pe, mcap, profit_yoy, revenue_yoy, roe)
        sub_results[s] = r

    # 平均分（四舍五入取整）
    avg_total = round(sum(r["total"] for r in sub_results.values()) / len(sub_results))

    # 合并 sigs（去重保序）
    seen_sigs = set()
    merged_sigs = []
    for s in parts:
        for sig in sub_results[s]["sigs"]:
            if sig not in seen_sigs:
                seen_sigs.add(sig)
                merged_sigs.append(sig)

    # 合并 breakdown（各策略独立保留）
    merged_breakdown = {s: sub_results[s]["breakdown"] for s in parts}

    # surprise_meta（若含 surprise 策略则携带）
    surprise_meta = sub_results["surprise"]["surprise_meta"] if "surprise" in sub_results else None

    result = {
        "total":          max(avg_total, 0),
        "strategy":       strategy_str,
        "strategy_label": get_strategy_label(strategy_str),
        "sigs":           merged_sigs,
        "breakdown":      merged_breakdown,
        "surprise_meta":  surprise_meta,
        # 各子策略分数（供报告展示）
        "sub_scores":     {s: sub_results[s]["total"] for s in parts},
    }
    result["reasons"] = _generate_combo_reasons(result, parts)
    return result


def _generate_combo_reasons(result: dict, parts: list[str]) -> list[str]:
    """为多策略组合生成理由说明"""
    reasons = []
    total = result.get("total", 0)
    sub_scores = result.get("sub_scores", {})

    if total < 60:
        reasons.append(f"组合基本面评分{total}分偏低")

    for s, score in sub_scores.items():
        label = STRATEGIES.get(s, s)
        if score < 50:
            reasons.append(f"{label}维度({score}分)较弱")

    if not reasons:
        if total >= 80:
            reasons.append("多维基本面综合优秀")
        elif total >= 60:
            reasons.append("多维基本面综合良好")
        else:
            reasons.append("基本面有待改善")

    return reasons


def fund_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe,
                      strategy: str = DEFAULT_STRATEGY, **kwargs) -> dict:
    """
    返回完整评分 dict，包含 total / sigs / breakdown / strategy / reasons。
    供 backtest.py 使用（需要各分项细节）。

    surprise 策略需要额外 kwargs：
      expect_yoy, ttm_yoy, org_num, surprise_mode("forward"/"actual")
    """
    s = strategy if strategy in STRATEGIES else DEFAULT_STRATEGY
    if s == "growth":
        result = _growth(pe, mcap, profit_yoy, revenue_yoy, roe)
    elif s == "surprise":
        result = _surprise(pe, mcap, profit_yoy, revenue_yoy, roe,
                           expect_yoy=kwargs.get("expect_yoy"),
                           ttm_yoy=kwargs.get("ttm_yoy"),
                           org_num=kwargs.get("org_num"),
                           surprise_mode=kwargs.get("surprise_mode", "forward"),
                           prev_ttm_yoy=kwargs.get("prev_ttm_yoy"))
    elif s == "single_line":
        result = _single_line(pe, mcap, profit_yoy, revenue_yoy, roe,
                              qoq_data=kwargs.get("qoq_data"),
                              quarterly_consensus=kwargs.get("quarterly_consensus"),
                              expect_yoy=kwargs.get("expect_yoy"),
                              ttm_yoy=kwargs.get("ttm_yoy"),
                              org_num=kwargs.get("org_num"),
                              qdiff_mode=kwargs.get("qdiff_mode", "quarter"),
                              surprise_mode=kwargs.get("surprise_mode", "forward"))
    else:
        result = _classic(pe, mcap, profit_yoy, revenue_yoy, roe)
    
    # 添加理由说明（当评分不高时）
    result["reasons"] = _generate_reasons(result, strategy)
    return result


def _generate_reasons(result: dict, strategy: str) -> list[str]:
    """生成基本面评分不高的理由说明"""
    reasons = []
    total = result.get("total", 0)
    breakdown = result.get("breakdown", {})
    
    # 总体评分不高
    if total < 60:
        reasons.append(f"基本面综合评分{total}分偏低")
    
    # 检查各分项扣分情况
    if strategy == "classic":
        # PE 估值
        pe_score = breakdown.get("pe", 0)
        if pe_score < 10:
            reasons.append("估值偏高")
        elif pe_score < 0:
            reasons.append("估值不佳")
        
        # 净利润
        profit_score = breakdown.get("profit", 0)
        if profit_score < 15:
            reasons.append("利润增速偏低")
        elif profit_score < 0:
            reasons.append("利润下滑")
        
        # 营收
        revenue_score = breakdown.get("revenue", 0)
        if revenue_score < 5:
            reasons.append("收入增长偏弱")
        elif revenue_score < 0:
            reasons.append("收入下降")
        
        # ROE
        roe_score = breakdown.get("roe", 0)
        if roe_score < 5:
            reasons.append("盈利能力一般")
        elif roe_score < 0:
            reasons.append("盈利能力较弱")
        
        # 市值
        mcap_score = breakdown.get("mcap", 0)
        if mcap_score < 5:
            reasons.append("市值弹性不足")
    
    elif strategy == "growth":
        # PEG/PE
        peg_score = breakdown.get("peg", 0)
        if peg_score < 8:
            reasons.append("成长估值偏高")
        elif peg_score < 0:
            reasons.append("估值不具吸引力")
        
        # 净利润
        profit_score = breakdown.get("profit", 0)
        if profit_score < 20:
            reasons.append("成长性不足")
        elif profit_score < 0:
            reasons.append("利润负增长")
        
        # 收入/利润结构
        struct_score = breakdown.get("struct", 0)
        if struct_score < 10:
            reasons.append("收入利润结构不佳")
        elif struct_score < 0:
            reasons.append("收入利润双降")
        
        # ROE
        roe_score = breakdown.get("roe", 0)
        if roe_score < 5:
            reasons.append("股东回报率偏低")
        elif roe_score < 0:
            reasons.append("盈利能力不足")
        
        # 市值
        mcap_score = breakdown.get("mcap", 0)
        if mcap_score < 8:
            reasons.append("市值弹性偏弱")

    elif strategy == "surprise":
        # 超预期信号
        surprise_score = breakdown.get("surprise", 0)
        if surprise_score < 15:
            reasons.append("无明显超预期信号")
        elif surprise_score < 0:
            reasons.append("预期不及历史增速")

        # PEG/PE
        peg_score = breakdown.get("peg", 0)
        if peg_score < 8:
            reasons.append("成长估值偏高")
        elif peg_score < 0:
            reasons.append("估值不具吸引力")

        # 净利润
        profit_score = breakdown.get("profit", 0)
        if profit_score < 10:
            reasons.append("利润增速偏低")
        elif profit_score < 0:
            reasons.append("利润下滑")

        # 收入/利润结构
        struct_score = breakdown.get("struct", 0)
        if struct_score < 8:
            reasons.append("收入利润结构一般")
        elif struct_score < 0:
            reasons.append("收入利润双降")

        # ROE
        roe_score = breakdown.get("roe", 0)
        if roe_score < 5:
            reasons.append("股东回报率偏低")

        # 市值
        mcap_score = breakdown.get("mcap", 0)
        if mcap_score < 8:
            reasons.append("市值弹性偏弱")
    
    elif strategy == "single_line":
        # PEG估值
        peg_score = breakdown.get("peg", 0)
        if peg_score < 8:
            reasons.append("成长估值偏高")
        elif peg_score < 0:
            reasons.append("估值不具吸引力")
        
        # 净利润同比
        yoy_score = breakdown.get("yoy", 0)
        if yoy_score < 12:
            reasons.append("增速不足")
        elif yoy_score < 0:
            reasons.append("利润下滑")
        
        # 收入/利润结构
        struct_score = breakdown.get("struct", 0)
        if struct_score < 6:
            reasons.append("收入利润结构一般")
        elif struct_score < 0:
            reasons.append("收入利润双降")
        
        # ROE
        roe_score = breakdown.get("roe", 0)
        if roe_score < 4:
            reasons.append("股东回报率偏低")
        
        # 市值
        mcap_score = breakdown.get("mcap", 0)
        if mcap_score < 8:
            reasons.append("市值弹性偏弱")
    
    # 如果没有具体理由，提供一个通用说明
    if not reasons:
        if total >= 80:
            reasons.append("基本面优秀")
        elif total >= 60:
            reasons.append("基本面良好")
        else:
            reasons.append("基本面有待改善")
    
    return reasons


def fund_score(pe, mcap, profit_yoy, revenue_yoy, roe,
               strategy: str = DEFAULT_STRATEGY, **kwargs) -> tuple[int, list[str]]:
    """
    返回 (score, sigs) 二元组，接口与原 fund_score() 完全兼容。
    供 score.py 使用。

    surprise 策略需要额外 kwargs：
      expect_yoy, ttm_yoy, org_num
    """
    d = fund_score_detail(pe, mcap, profit_yoy, revenue_yoy, roe, strategy, **kwargs)
    return d["total"], d["sigs"]


# ── 快速自测 ─────────────────────────────────────────────
if __name__ == "__main__":
    cases = [
        {"label": "高增成长股(PE=60,净利+80%,营收+50%,ROE=12%,市值80亿)",
         "args": (60, 80, 80, 50, 12)},
        {"label": "传统蓝筹(PE=12,净利+8%,营收+5%,ROE=18%,市值500亿)",
         "args": (12, 500, 8, 5, 18)},
        {"label": "亏损成长(PE=-1,净利-30%,营收+40%,ROE=3%,市值40亿)",
         "args": (-1, 40, -30, 40, 3)},
        {"label": "瑞丰高材(PE=95,净利+283%,营收+10%,ROE=1.75,市值38亿)",
         "args": (95, 38, 283, 10, 1.75)},
    ]
    for c in cases:
        pe, mcap, py, ry, roe = c["args"]
        cs, csigs = fund_score(pe, mcap, py, ry, roe, strategy="classic")
        gs, gsigs = fund_score(pe, mcap, py, ry, roe, strategy="growth")
        ss, ssigs = fund_score(pe, mcap, py, ry, roe, strategy="surprise",
                               expect_yoy=100, ttm_yoy=60, org_num=15)
        sl, slsigs = fund_score(pe, mcap, py, ry, roe, strategy="single_line")
        print(f"\n【{c['label']}】")
        print(f"  classic:    {cs:3d}分  {' | '.join(csigs)}")
        print(f"  growth:     {gs:3d}分  {' | '.join(gsigs)}")
        print(f"  surprise:   {ss:3d}分  {' | '.join(ssigs)}")
        print(f"  single_line:{sl:3d}分  {' | '.join(slsigs)}")

    # 测试一致预期接口
    print("\n\n=== 一致预期接口测试 ===")
    import time
    t = time.time()
    data = fetch_consensus_eps("300750")
    print(f"300750 宁德时代 ({time.time()-t:.1f}s):")
    print(f"  EPS1={data['eps1']}, EPS2={data['eps2']}, 预期增速={data['expect_yoy']}%")
    print(f"  机构数={data['org_num']}, 买入评级={data['buy_num']}")
    print(f"  source={data['source']}")

    # 测试 TTM 计算
    print("\n=== TTM 净利润增速测试 ===")
    t = time.time()
    ttm = calc_ttm_profit_growth("300750")
    print(f"300750 TTM增速={ttm}% ({time.time()-t:.1f}s)")

    ttm2 = calc_ttm_profit_growth("000001")
    print(f"000001 TTM增速={ttm2}%")
