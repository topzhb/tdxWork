#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
提取命中热门板块的候选股

热门板块定义：板块涨跌统计（涨停数前30）和每日TOP25板块同时命中的板块
候选股来源：热门板块中满足条件（is_satisfied=1）的个股
"""

import sqlite3
import json
import os
import sys
import struct
from datetime import datetime


# ============================================================
# 路径配置
# ============================================================
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "db", "concept_weekly.db")
from tool_config import VIPDOC_DIR as TDX_VIPDOC_DIR
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "db", "candidates.json")


# ============================================================
# 工具函数（从 gen_concept_html_V1.3.py 复制）
# ============================================================
def is_limit_up(stock_code, daily_change):
    """判断是否为涨停"""
    if stock_code.startswith(('30', '688')):
        return daily_change >= 19.9
    else:
        return daily_change >= 9.9


def read_sector_index_change(sector_code):
    """从通达信板块指数读取当日涨幅"""
    index_file = f"sh{sector_code}"
    day_file = os.path.join(TDX_VIPDOC_DIR, "sh", "lday", f"{index_file}.day")
    if not os.path.exists(day_file):
        return None
    try:
        with open(day_file, 'rb') as f:
            data = f.read()
        if len(data) < 64:
            return None
        record_size = 32
        factor = 100.0
        last_record = data[-record_size:]
        last_values = struct.unpack('<IIIIIIII', last_record)
        last_close = last_values[4] / factor
        prev_record = data[-record_size * 2:-record_size]
        prev_values = struct.unpack('<IIIIIIII', prev_record)
        prev_close = prev_values[4] / factor
        if prev_close > 0:
            return (last_close - prev_close) / prev_close * 100
        return None
    except Exception:
        return None


def get_market(stock_code):
    """判断市场"""
    if stock_code.startswith('6'):
        return 'SH'
    elif stock_code.startswith(('0', '3')):
        return 'SZ'
    return 'OTHER'


def get_stock_daily_change(stock_code, market):
    """读取个股当日涨幅"""
    day_file = os.path.join(TDX_VIPDOC_DIR, market, 'lday', f'{market}{stock_code}.day')
    if not os.path.exists(day_file):
        return None
    try:
        with open(day_file, 'rb') as f:
            data = f.read()
        if len(data) < 64:
            return None
        record_size = 32
        factor = 100.0
        last_record = data[-record_size:]
        last_values = struct.unpack('<IIIIIIII', last_record)
        last_close = last_values[4] / factor
        prev_record = data[-record_size * 2:-record_size]
        prev_values = struct.unpack('<IIIIIIII', prev_record)
        prev_close = prev_values[4] / factor
        if prev_close > 0:
            return round((last_close - prev_close) / prev_close * 100, 2)
        return None
    except Exception:
        return None


# ============================================================
# 主流程
# ============================================================
def main():
    conn = sqlite3.connect(DB_FILE)

    # ── 1. 获取最新 run_id ──
    run_row = conn.execute(
        "SELECT run_id, note FROM t_run_log ORDER BY run_time DESC LIMIT 1"
    ).fetchone()
    if not run_row:
        print("[ERROR] 数据库无运行记录")
        return
    latest_run_id = run_row[0]
    # 从 note 字段提取日期 (格式: "历史回溯：2026-03-31")
    note = run_row[1] or ''
    latest_date = note.split('：')[-1].strip() if '：' in note else latest_run_id[:8]
    print(f"最新 run_id: {latest_run_id}  日期: {latest_date}")

    # ── 2. 获取每日TOP25板块代码 ──
    # 从 t_daily_report 获取最新日期按满足数排名前25的板块
    top25_rows = conn.execute("""
        SELECT ds.sector_code, s.sector_name, ds.satisfied_count,
               (SELECT satisfied_count FROM t_run_log
                WHERE run_id = (SELECT run_id FROM t_daily_report WHERE report_date = ? LIMIT 1)
               ) as total_satisfied
        FROM t_daily_report ds
        JOIN t_sector s ON ds.sector_code = s.sector_code
        WHERE ds.report_date = ?
        ORDER BY ds.satisfied_count DESC
        LIMIT 25
    """, (latest_date, latest_date)).fetchall()

    top25_codes = set()
    top25_sectors = []
    for code, name, sat_count, total_sat in top25_rows:
        top25_codes.add(code)
        total_rate = (sat_count / total_sat * 100) if total_sat and total_sat > 0 else 0
        top25_sectors.append({
            'code': code, 'name': name, 'satisfied_count': sat_count,
            'total_rate': round(total_rate, 2)
        })

    print(f"\n=== 每日TOP25板块 ({len(top25_codes)}个) ===")
    print(f"{'板块名称':<15s} {'满足数':>6s} {'总比满足率':>10s}")
    print("-" * 40)
    for s in top25_sectors:
        print(f"{s['name']:<14s} {s['satisfied_count']:>6d} {s['total_rate']:>9.2f}%")

    # ── 3. 获取板块涨跌统计（涨停数前30） ──
    all_sectors = conn.execute("SELECT sector_code, sector_name FROM t_sector").fetchall()

    change_stats = []
    for sector_code, sector_name in all_sectors:
        rows = conn.execute("""
            SELECT sc.stock_code, sc.daily_change, sc.is_satisfied
            FROM t_stock_calc sc
            JOIN t_sector_stock ss ON sc.stock_code = ss.stock_code
            WHERE ss.sector_code = ? AND sc.run_id = ?
        """, (sector_code, latest_run_id)).fetchall()

        if not rows:
            continue

        limit_up_count = 0
        satisfied_changes = []
        for stock_code, daily_change, is_satisfied in rows:
            if daily_change is None:
                daily_change = 0
            if is_limit_up(stock_code, daily_change):
                limit_up_count += 1
            if is_satisfied:
                satisfied_changes.append(daily_change)

        analyzed_count = len(rows)
        avg_satisfied_change = sum(satisfied_changes) / len(satisfied_changes) if satisfied_changes else 0
        sector_change = read_sector_index_change(sector_code)
        if sector_change is None:
            sector_change = sum(r[1] for r in rows if r[1] is not None) / analyzed_count if analyzed_count > 0 else 0

        change_stats.append({
            'sector_code': sector_code,
            'sector_name': sector_name,
            'limit_up_count': limit_up_count,
            'analyzed_count': analyzed_count,
            'avg_satisfied_change': round(avg_satisfied_change, 2),
            'sector_change': round(sector_change, 2),
        })

    # 按涨停数降序取前30
    change_stats.sort(key=lambda x: x['limit_up_count'], reverse=True)
    change_top30 = change_stats[:30]
    change_top30_codes = set(s['sector_code'] for s in change_top30)

    print(f"\n=== 板块涨跌统计 TOP30 ({len(change_top30)}个) ===")
    print(f"{'板块名称':<15s} {'涨停':>5s} {'分析数':>6s} {'满足涨幅':>8s} {'板块涨幅':>8s}")
    print("-" * 55)
    for s in change_top30:
        print(f"{s['sector_name']:<14s} {s['limit_up_count']:>5d} {s['analyzed_count']:>6d} "
              f"{s['avg_satisfied_change']:>+7.2f}% {s['sector_change']:>+7.2f}%")

    # ── 4. 计算热门板块（交集） ──
    hot_codes = top25_codes & change_top30_codes
    print(f"\n=== 热门板块（交集）: {len(hot_codes)} 个 ===")

    if not hot_codes:
        print("没有同时命中两个条件的板块")
        conn.close()
        return

    # 汇总热门板块信息
    hot_sector_info = {}
    for s in top25_sectors:
        if s['code'] in hot_codes:
            hot_sector_info[s['code']] = s.copy()

    # 从涨跌统计补充信息
    for s in change_top30:
        if s['sector_code'] in hot_sector_info:
            hot_sector_info[s['sector_code']].update({
                'limit_up_count': s['limit_up_count'],
                'analyzed_count': s['analyzed_count'],
                'avg_satisfied_change': s['avg_satisfied_change'],
                'sector_change': s['sector_change'],
            })

    hot_sectors = sorted(hot_sector_info.values(), key=lambda x: x['satisfied_count'], reverse=True)

    print(f"{'板块名称':<15s} {'满足数':>6s} {'总比满足率':>10s} {'涨停':>5s} {'板块涨幅':>8s}")
    print("-" * 55)
    for s in hot_sectors:
        print(f"{s['name']:<14s} {s['satisfied_count']:>6d} {s['total_rate']:>9.2f}% "
              f"{s.get('limit_up_count', 0):>5d} {s.get('sector_change', 0):>+7.2f}%")

    # ── 5. 筛选候选股 ──
    print(f"\n=== 筛选命中热门板块的候选股 ===")

    # 获取最近5个交易日
    recent_dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT calc_date FROM t_stock_calc ORDER BY calc_date DESC LIMIT 5"
    ).fetchall()]
    print(f"分析日期范围: {recent_dates[-1]} ~ {recent_dates[0]}")

    # 查询热门板块中满足条件的股票（最近5天）
    ph = ','.join(['?'] * len(recent_dates))
    placeholders = ','.join(['?'] * len(hot_codes))

    # 龙头个股：最近5天内满足>=3天，且属于热门板块
    leader_rows = conn.execute(f"""
        SELECT sc.stock_code, st.stock_name,
               SUM(sc.is_satisfied) as sat_days,
               ROUND(AVG(sc.angle), 2) as avg_angle,
               ROUND(MAX(sc.close_price), 2) as latest_close,
               GROUP_CONCAT(DISTINCT ss.sector_code) as sector_codes,
               GROUP_CONCAT(DISTINCT s2.sector_name) as sector_names
        FROM t_stock_calc sc
        LEFT JOIN t_stock st ON sc.stock_code = st.stock_code
        JOIN t_sector_stock ss ON sc.stock_code = ss.stock_code
        LEFT JOIN t_sector s2 ON ss.sector_code = s2.sector_code
        WHERE sc.calc_date IN ({ph})
          AND ss.sector_code IN ({placeholders})
          AND st.stock_name NOT LIKE '%ST%'
        GROUP BY sc.stock_code
        HAVING SUM(sc.is_satisfied) >= 3
        ORDER BY sat_days DESC, avg_angle DESC
    """, recent_dates + list(hot_codes)).fetchall()

    print(f"\n--- 龙头个股 ({len(leader_rows)}只，满足>=3天) ---")
    print(f"{'代码':<8s} {'名称':<12s} {'天数':>4s} {'角度':>7s} {'收盘':>8s} {'所属热门板块'}")
    print("-" * 70)

    candidates = []
    for r in leader_rows:
        code, name, sat, angle, close, s_codes, s_names = r
        # 只保留属于热门板块的
        sector_list = []
        if s_codes and s_names:
            code_list = s_codes.split(',')
            name_list = s_names.split(',')
            for sc, sn in zip(code_list, name_list):
                if sc in hot_codes:
                    sector_list.append(sn)

        sector_str = ', '.join(sector_list) if sector_list else '-'
        print(f"{code:<8s} {name or '未知':<11s} {sat:>4d} {angle:>7.2f} {close:>8.2f}  {sector_str}")

        # 获取实时涨幅
        market = get_market(code)
        daily_chg = get_stock_daily_change(code, market)

        candidates.append({
            'code': code,
            'name': name or '',
            'satisfied_days': sat,
            'avg_angle': angle,
            'close_price': close,
            'daily_change': daily_chg,
            'hot_sectors': sector_list,
            'type': 'leader'
        })

    # 低位启动个股：前3天不满足，最近2天新启动，属于热门板块
    older_dates = recent_dates[2:]
    new_dates = recent_dates[:2]

    ph_new = ','.join(['?'] * len(new_dates))
    ph_old = ','.join(['?'] * len(older_dates))

    low_rows = conn.execute(f"""
        SELECT sc.stock_code, st.stock_name,
               SUM(sc.is_satisfied) as new_sat,
               ROUND(AVG(sc.angle), 2) as avg_angle,
               ROUND(MAX(sc.close_price), 2) as latest_close,
               GROUP_CONCAT(DISTINCT ss.sector_code) as sector_codes,
               GROUP_CONCAT(DISTINCT s2.sector_name) as sector_names
        FROM t_stock_calc sc
        LEFT JOIN t_stock st ON sc.stock_code = st.stock_code
        JOIN t_sector_stock ss ON sc.stock_code = ss.stock_code
        LEFT JOIN t_sector s2 ON ss.sector_code = s2.sector_code
        WHERE sc.calc_date IN ({ph_new})
          AND ss.sector_code IN ({placeholders})
          AND st.stock_name NOT LIKE '%ST%'
          AND sc.stock_code NOT IN (
              SELECT DISTINCT stock_code FROM t_stock_calc
              WHERE calc_date IN ({ph_old}) AND is_satisfied = 1
          )
        GROUP BY sc.stock_code
        HAVING SUM(sc.is_satisfied) >= 2
        ORDER BY new_sat DESC, avg_angle DESC
    """, new_dates + list(hot_codes) + older_dates).fetchall()

    print(f"\n--- 低位启动个股 ({len(low_rows)}只，新满足>=2天) ---")
    print(f"{'代码':<8s} {'名称':<12s} {'天数':>4s} {'角度':>7s} {'收盘':>8s} {'所属热门板块'}")
    print("-" * 70)

    for r in low_rows:
        code, name, sat, angle, close, s_codes, s_names = r
        sector_list = []
        if s_codes and s_names:
            code_list = s_codes.split(',')
            name_list = s_names.split(',')
            for sc, sn in zip(code_list, name_list):
                if sc in hot_codes:
                    sector_list.append(sn)

        sector_str = ', '.join(sector_list) if sector_list else '-'
        print(f"{code:<8s} {name or '未知':<11s} {sat:>4d} {angle:>7.2f} {close:>8.2f}  {sector_str}")

        market = get_market(code)
        daily_chg = get_stock_daily_change(code, market)

        candidates.append({
            'code': code,
            'name': name or '',
            'satisfied_days': sat,
            'avg_angle': angle,
            'close_price': close,
            'daily_change': daily_chg,
            'hot_sectors': sector_list,
            'type': 'low_start'
        })

    conn.close()

    # ── 6. 保存结果 ──
    result = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'latest_date': latest_date,
        'latest_run_id': latest_run_id,
        'hot_sector_count': len(hot_codes),
        'hot_sectors': hot_sectors,
        'leader_count': len([c for c in candidates if c['type'] == 'leader']),
        'low_start_count': len([c for c in candidates if c['type'] == 'low_start']),
        'candidates': candidates,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"  热门板块数: {len(hot_codes)}")
    print(f"  候选股总数: {len(candidates)} (龙头 {result['leader_count']} + 低位启动 {result['low_start_count']})")
    print(f"  结果已保存: {OUTPUT_FILE}")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
