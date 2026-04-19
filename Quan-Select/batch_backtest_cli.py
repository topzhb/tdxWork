#!/usr/bin/env python3
"""
batch_backtest_cli.py - 批量回测脚本

功能：对指定日期范围内的每日精选进行批量回测，汇总胜率和收益。
支持扩展：可自定义日期范围、窗口、策略、TOP数、止盈止损、技术过滤等参数。

用法：
  python batch_backtest_cli.py                                    # 默认参数
  python batch_backtest_cli.py --start 20260324 --end 20260410    # 指定日期范围
  python batch_backtest_cli.py --window 10 --top-n 20             # 窗口10日，TOP20
  python batch_backtest_cli.py --strategy classic,surprise         # 指定策略
  python batch_backtest_cli.py --filter vol<1.3,pos>=70           # 技术过滤
  python batch_backtest_cli.py --list                             # 列出可用日期

参数：
  --start       起始日期 YYYYMMDD（默认最早有数据的日期）
  --end         结束日期 YYYYMMDD（默认最新有数据的日期）
  --window      回测窗口/交易日数（默认 10）
  --top-n       每日取前N只（默认 20）
  --strategy    基本面策略（默认 classic）
  --target-pct  目标涨幅%（默认 15）
  --stop-pct    止损幅度%（默认 7）
  --filter      技术过滤条件，逗号分隔（默认无过滤）
                支持条件：
                  vol<N      量比(5日/20日均量)小于N，默认1.3
                  pos>=N     52周位置百分比>=N，默认70
                  rsi<N      RSI14小于N，默认75
                  ret>-N     当日跌幅大于-N%，默认1
  --list        仅列出 qs_picks 中可用的日期
"""

import argparse
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta

# ── 项目路径 ──────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

DB_FILE = os.path.join(os.path.dirname(PROJECT_DIR), "db", "concept_weekly.db")
if not os.path.exists(DB_FILE):
    raise FileNotFoundError(
        f"数据库文件不存在：{DB_FILE}\n"
        "请确认项目目录结构正确，db/concept_weekly.db 应位于项目根目录的 db/ 子目录下。"
    )

# 延迟导入 backtest 模块（避免加载时副作用）
def _import_batch_analyze():
    from backtest import batch_analyze
    return batch_analyze


# ── 技术过滤 ──────────────────────────────────────────────
def calc_filter_indicators(code, base_date):
    """计算技术过滤指标，返回字典或None"""
    import pandas as pd
    import numpy as np
    # 复用backtest.py的K线读取
    market = 'sh' if code.startswith('6') else 'sz'
    from backtest import read_tdx_day_full
    df = read_tdx_day_full(market, code)
    if df is None:
        return None
    bd = pd.Timestamp(base_date)
    df = df.loc[df.index <= bd]
    if len(df) < 60:
        return None
    c = df['close']
    v = df['volume']
    h = df['high']
    l = df['low']

    hi52 = h.rolling(252).max()
    lo52 = l.rolling(252).min()
    pos_pct = (c - lo52) / (hi52 - lo52 + 1e-9) * 100

    vol_5 = v.rolling(5).mean()
    vol_20 = v.rolling(20).mean()
    vol_520 = vol_5 / vol_20.replace(0, np.nan)

    delta = c.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi14 = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    ret_today = (c.iloc[-1] - c.iloc[-2]) / c.iloc[-2] * 100 if len(c) >= 2 else 0

    return {
        'pos_pct': pos_pct.iloc[-1],
        'vol_520': vol_520.iloc[-1],
        'rsi14': rsi14.iloc[-1],
        'ret_today': ret_today,
    }


def parse_filter_rules(filter_str):
    """解析过滤条件字符串，返回规则列表"""
    if not filter_str:
        return []
    rules = []
    for cond in filter_str.split(','):
        cond = cond.strip()
        if not cond:
            continue
        # vol<N
        m = re.match(r'vol\s*<\s*([\d.]+)', cond, re.I)
        if m:
            rules.append(('vol', float(m.group(1)), '<'))
            continue
        # pos>=N
        m = re.match(r'pos\s*>=?\s*([\d.]+)', cond, re.I)
        if m:
            rules.append(('pos', float(m.group(1)), '>='))
            continue
        # rsi<N
        m = re.match(r'rsi\s*<\s*([\d.]+)', cond, re.I)
        if m:
            rules.append(('rsi', float(m.group(1)), '<'))
            continue
        # ret>-N
        m = re.match(r'ret\s*>\s*-?([\d.]+)', cond, re.I)
        if m:
            rules.append(('ret', -float(m.group(1)), '>='))
            continue
        print(f"  [WARN] 无法解析过滤条件: {cond}")
    return rules


def apply_filter_rules(ind, rules):
    """检查指标是否满足所有过滤规则，返回True=通过"""
    if ind is None:
        return False
    for name, threshold, op in rules:
        if name == 'vol':
            val = ind.get('vol_520')
        elif name == 'pos':
            val = ind.get('pos_pct')
        elif name == 'rsi':
            val = ind.get('rsi14')
        elif name == 'ret':
            val = ind.get('ret_today')
        else:
            continue
        if val is None or val != val:  # None or NaN
            return False
        if op == '<' and not (val < threshold):
            return False
        elif op == '>=' and not (val >= threshold):
            return False
        elif op == '>' and not (val > threshold):
            return False
    return True


def filter_items_by_tech(items, base_date, filter_rules):
    """对回测结果做技术过滤，返回过滤后的items和统计"""
    passed = []
    filtered_out = 0
    no_data = 0
    for item in items:
        code = item.get('code')
        ind = calc_filter_indicators(code, base_date)
        if apply_filter_rules(ind, filter_rules):
            passed.append(item)
        else:
            if ind is None:
                no_data += 1
            filtered_out += 1
    return passed, filtered_out, no_data


def get_available_dates(conn) -> list[str]:
    """获取 qs_picks 中所有有精选数据的日期（YYYY-MM-DD）"""
    rows = conn.execute(
        "SELECT DISTINCT date FROM qs_picks WHERE rank_no IS NOT NULL AND rank_no > 0 "
        "ORDER BY date ASC"
    ).fetchall()
    return [r[0] for r in rows]


def parse_date(s: str) -> str:
    """统一日期格式为 YYYY-MM-DD"""
    s = s.replace("-", "")
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def filter_dates(all_dates: list[str], start: str, end: str) -> list[str]:
    """按日期范围过滤"""
    return [d for d in all_dates if start <= d <= end]


def run_batch(dates: list[str], window: int, top_n: int, strategy: str,
              target_pct: float, stop_pct: float, filter_rules: list = None) -> list[dict]:
    """对每个日期执行批量回测，可选技术过滤"""
    batch_analyze = _import_batch_analyze()
    results = []

    total_dates = len(dates)
    has_filter = bool(filter_rules)
    total_filtered = 0
    total_no_data = 0

    for i, dt in enumerate(dates, 1):
        print(f"\n{'='*60}")
        print(f"  [{i}/{total_dates}] {dt}  (TOP{top_n}, window={window}, 策略={strategy})")
        if has_filter:
            rule_desc = ', '.join(f"{n}{op}{t}" for n, t, op in filter_rules)
            print(f"  过滤条件: {rule_desc}")
        print(f"{'='*60}")

        try:
            result = batch_analyze(
                dt, window=window, strategy=strategy, top_n=top_n,
                target_pct=target_pct, stop_pct=stop_pct
            )

            if "error" in result:
                print(f"  [SKIP] {result['error']}")
                continue

            # 技术过滤
            original_items = result.get("items", [])
            original_count = len(original_items)

            if has_filter and original_items:
                filtered_items, filt_cnt, no_data_cnt = filter_items_by_tech(
                    original_items, dt, filter_rules
                )
                total_filtered += filt_cnt
                total_no_data += no_data_cnt

                # 重建 summary
                result['items'] = filtered_items
                result['_filtered_out'] = filt_cnt
                result['_no_data'] = no_data_cnt
                result['_original_count'] = original_count

                # 重新计算 summary
                counts = {'success': 0, 'failure': 0, 'pending': 0, 'insufficient': 0, 'total': len(filtered_items)}
                returns = []
                success_ret = []
                failure_ret = []
                for item in filtered_items:
                    v = item.get('verdict', {})
                    st = v.get('status', 'pending')
                    counts[st] = counts.get(st, 0) + 1
                    ret = v.get('actual_return')
                    if isinstance(ret, (int, float)):
                        returns.append(ret)
                        if st == 'success':
                            success_ret.append(ret)
                        elif st == 'failure':
                            failure_ret.append(ret)
                decided = counts['success'] + counts['failure']
                result['summary'] = {
                    'counts': counts,
                    'win_rate': round(counts['success'] / decided * 100, 1) if decided > 0 else None,
                    'avg_return': round(sum(returns) / len(returns), 2) if returns else None,
                    'avg_success': round(sum(success_ret) / len(success_ret), 2) if success_ret else None,
                    'avg_failure': round(sum(failure_ret) / len(failure_ret), 2) if failure_ret else None,
                }

                print(f"  [过滤] {original_count}只 → {len(filtered_items)}只 (淘汰{filt_cnt}, 无数据{no_data_cnt})")

            results.append(result)

            # 打印当日简要
            s = result.get("summary", {})
            cnt = s.get("counts", {})
            wr = s.get("win_rate")
            avg_r = s.get("avg_return")
            print(f"  结果：{cnt.get('success',0)}胜 {cnt.get('failure',0)}负 "
                  f"{cnt.get('pending',0)}待定 | "
                  f"胜率={'--' if wr is None else str(wr)+'%'} | "
                  f"平均收益={'--' if avg_r is None else str(avg_r)+'%'}")

        except Exception as e:
            print(f"  [ERR] {e}")
            import traceback; traceback.print_exc()

        if i < total_dates:
            time.sleep(0.5)

    if has_filter:
        print(f"\n  总过滤淘汰: {total_filtered}只, 无数据跳过: {total_no_data}只")

    return results


def print_summary(results: list[dict]):
    """打印汇总统计"""
    if not results:
        print("\n没有有效的回测结果。")
        return

    print(f"\n{'='*70}")
    print(f"  批量回测汇总报告")
    print(f"{'='*70}")

    total_stocks = 0
    total_success = 0
    total_failure = 0
    total_pending = 0
    total_insufficient = 0
    total_error = 0
    all_returns = []
    success_returns = []
    failure_returns = []

    for r in results:
        s = r.get("summary", {})
        cnt = s.get("counts", {})
        n = cnt.get("total", 0)
        total_stocks += n
        total_success += cnt.get("success", 0)
        total_failure += cnt.get("failure", 0)
        total_pending += cnt.get("pending", 0)
        total_insufficient += cnt.get("insufficient", 0)
        total_error += cnt.get("error", 0)

        # 收集所有 actual_return
        for item in r.get("items", []):
            v = item.get("verdict", {})
            ret = v.get("actual_return")
            if isinstance(ret, (int, float)):
                all_returns.append(ret)
                if v.get("status") == "success":
                    success_returns.append(ret)
                elif v.get("status") == "failure":
                    failure_returns.append(ret)

    decided = total_success + total_failure
    win_rate = round(total_success / decided * 100, 1) if decided > 0 else 0
    avg_return = round(sum(all_returns) / len(all_returns), 2) if all_returns else 0
    avg_success = round(sum(success_returns) / len(success_returns), 2) if success_returns else 0
    avg_failure = round(sum(failure_returns) / len(failure_returns), 2) if failure_returns else 0

    print(f"\n  回测天数：  {len(results)} 天")
    print(f"  总测试数：  {total_stocks} 只")
    if total_stocks == 0:
        print(f"\n  过滤后无有效样本。")
        return
    print(f"  ──────────────────────────────────")
    print(f"  目标达成：  {total_success} 只 ({round(total_success/total_stocks*100,1)}%)")
    print(f"  止损触发：  {total_failure} 只 ({round(total_failure/total_stocks*100,1)}%)")
    print(f"  结果待定：  {total_pending} 只 ({round(total_pending/total_stocks*100,1)}%)")
    print(f"  数据不足：  {total_insufficient} 只")
    print(f"  ──────────────────────────────────")
    print(f"  胜率（已决）：{win_rate}%  ({decided}/{total_stocks})")
    print(f"  平均收益：  {avg_return}%")
    print(f"  成功平均收益：{avg_success}%")
    print(f"  失败平均收益：{avg_failure}%")

    if all_returns:
        print(f"  最大收益：  {max(all_returns)}%")
        print(f"  最大亏损：  {min(all_returns)}%")

    # 按日期明细
    print(f"\n{'─'*70}")
    print(f"  {'日期':<12} {'总数':>4} {'胜':>3} {'负':>3} {'待定':>4} {'胜率':>6} {'均收益':>7}")
    print(f"  {'─'*12} {'─'*4} {'─'*3} {'─'*3} {'─'*4} {'─'*6} {'─'*7}")
    for r in results:
        s = r.get("summary", {})
        cnt = s.get("counts", {})
        wr = s.get("win_rate")
        avg_r = s.get("avg_return")
        d_decided = cnt.get("success", 0) + cnt.get("failure", 0)
        d_wr = f"{wr}%" if wr is not None else "--"
        d_avg = f"{avg_r}%" if avg_r is not None else "--"
        print(f"  {r['date']:<12} {cnt.get('total',0):>4} "
              f"{cnt.get('success',0):>3} {cnt.get('failure',0):>3} "
              f"{cnt.get('pending',0):>4} {d_wr:>6} {d_avg:>7}")
    print(f"{'─'*70}\n")


def main():
    parser = argparse.ArgumentParser(description="批量回测脚本")
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--window", type=int, default=10, help="回测窗口（交易日）")
    parser.add_argument("--top-n", type=int, default=20, help="每日取前N只")
    parser.add_argument("--strategy", type=str, default="classic", help="基本面策略")
    parser.add_argument("--target-pct", type=float, default=15.0, help="目标涨幅%%")
    parser.add_argument("--stop-pct", type=float, default=7.0, help="止损幅度%%")
    parser.add_argument("--filter", type=str, default=None,
                        help="技术过滤条件，逗号分隔 (vol<1.3,pos>=70,rsi<75,ret>-1)")
    parser.add_argument("--list", action="store_true", help="列出可用日期")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_FILE)
    try:
        all_dates = get_available_dates(conn)

        if args.list:
            print(f"qs_picks 可用日期（共 {len(all_dates)} 天）：")
            for d in all_dates:
                print(f"  {d}")
            return

        if not all_dates:
            print("qs_picks 中无可用数据。")
            return

        start = parse_date(args.start) if args.start else all_dates[0]
        end = parse_date(args.end) if args.end else all_dates[-1]
        dates = filter_dates(all_dates, start, end)

        if not dates:
            print(f"{start} ~ {end} 范围内无可用数据。")
            print(f"可用范围：{all_dates[0]} ~ {all_dates[-1]}")
            return

        print(f"批量回测参数：")
        print(f"  日期范围：{dates[0]} ~ {dates[-1]}（共 {len(dates)} 天）")
        print(f"  回测窗口：{args.window} 个交易日")
        print(f"  每日TOP： {args.top_n}")
        print(f"  策略：    {args.strategy}")
        print(f"  目标：    +{args.target_pct}%")
        print(f"  止损：    -{args.stop_pct}%")

        filter_rules = parse_filter_rules(args.filter)
        if filter_rules:
            rule_desc = ', '.join(f"{n}{op}{t}" for n, t, op in filter_rules)
            print(f"  过滤条件：{rule_desc}")

        results = run_batch(
            dates, window=args.window, top_n=args.top_n,
            strategy=args.strategy, target_pct=args.target_pct,
            stop_pct=args.stop_pct, filter_rules=filter_rules
        )
        print_summary(results)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
