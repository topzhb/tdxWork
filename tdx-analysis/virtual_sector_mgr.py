#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
virtual_sector_mgr.py — 虚拟板块管理工具

功能：
  管理自定义虚拟板块（如「医疗主题」888001），将多个概念板块的成分股聚合为一个虚拟主题板块。
  虚拟板块与真实板块一样参与满足率分析，但涨跌列在报告中标注为「主题」不计算。

用法：
  python virtual_sector_mgr.py list                    # 列出所有虚拟板块
  python virtual_sector_mgr.py info 888001             # 查看指定虚拟板块详情
  python virtual_sector_mgr.py sync                    # 将配置文件同步到数据库（创建/更新成分股）
  python virtual_sector_mgr.py sync --code 888001      # 只同步指定虚拟板块
  python virtual_sector_mgr.py add                     # 交互式新增虚拟板块
  python virtual_sector_mgr.py del 888001              # 删除虚拟板块（从数据库和配置文件）
  python virtual_sector_mgr.py add-sub 888001 880xxx   # 为虚拟板块添加子板块
  python virtual_sector_mgr.py del-sub 888001 880xxx   # 从虚拟板块移除子板块
  python virtual_sector_mgr.py search 关键词            # 在数据库中搜索真实板块（用于查找代码）

配置文件：virtual_sectors.json（与本脚本同目录）
"""

import os
import sys
import json
import sqlite3
import argparse
import datetime

# ============================================================
# 路径配置
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, 'virtual_sectors.json')

try:
    from tool_config import DB_FILE
except ImportError:
    DB_FILE = os.path.join(SCRIPT_DIR, '..', 'db', 'concept_weekly.db')


# ============================================================
# 工具函数
# ============================================================
def log(msg, level="INFO"):
    prefix = {"INFO": "[INFO]", "OK": "[ OK ]", "WARN": "[WARN]", "ERR": "[ERR ]"}.get(level, "[INFO]")
    print(f"{prefix} {msg}")


def load_config():
    """加载虚拟板块配置文件"""
    if not os.path.exists(CONFIG_FILE):
        log(f"配置文件不存在：{CONFIG_FILE}", "ERR")
        log("请先创建 virtual_sectors.json 配置文件", "INFO")
        sys.exit(1)
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_config(config):
    """保存配置文件"""
    config['_updated'] = datetime.date.today().isoformat()
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    log(f"配置已保存：{CONFIG_FILE}", "OK")


def get_db_conn():
    if not os.path.exists(DB_FILE):
        log(f"数据库不存在：{DB_FILE}", "ERR")
        sys.exit(1)
    return sqlite3.connect(DB_FILE)


def get_virtual_sector(config, code):
    """从配置中找到指定虚拟板块"""
    for vs in config.get('virtual_sectors', []):
        if vs['code'] == code:
            return vs
    return None


def next_virtual_code(config):
    """自动生成下一个虚拟板块代码（888001, 888002, ...）"""
    existing = [vs['code'] for vs in config.get('virtual_sectors', [])]
    n = 1
    while True:
        candidate = f"888{n:03d}"
        if candidate not in existing:
            return candidate
        n += 1


# ============================================================
# sync：将配置同步到数据库
# ============================================================
def sync_to_db(config, conn, target_code=None):
    """将虚拟板块配置同步到 t_sector 和 t_sector_stock 表"""
    virtual_sectors = config.get('virtual_sectors', [])
    if not virtual_sectors:
        log("配置文件中没有虚拟板块", "WARN")
        return

    for vs in virtual_sectors:
        code = vs['code']
        name = vs['name']
        sub_sectors = vs.get('sub_sectors', [])

        if target_code and code != target_code:
            continue

        log(f"同步虚拟板块：{name}（{code}）")

        # 1. 写入 t_sector
        conn.execute("""
            INSERT OR REPLACE INTO t_sector (sector_code, sector_name, created_at)
            VALUES (?, ?, ?)
        """, (code, name, vs.get('created_at', datetime.date.today().isoformat())))

        # 2. 收集子板块的成分股（并集去重）
        all_stocks = {}  # {stock_code: stock_name}
        for sub in sub_sectors:
            sub_code = sub.get('code')
            sub_name = sub.get('name', '')
            if not sub_code:
                continue

            # 查询子板块成分股
            rows = conn.execute("""
                SELECT ss.stock_code, s.stock_name
                FROM t_sector_stock ss
                JOIN t_stock s ON ss.stock_code = s.stock_code
                WHERE ss.sector_code = ?
            """, (sub_code,)).fetchall()

            if not rows:
                log(f"  子板块「{sub_name}」({sub_code}) 无成分股或不存在", "WARN")
                continue

            for stock_code, stock_name in rows:
                all_stocks[stock_code] = stock_name

            log(f"  子板块「{sub_name}」({sub_code}）贡献 {len(rows)} 只成分股")

        log(f"  合并后成分股总数（去重）：{len(all_stocks)} 只")

        # 3. 清空旧成分股，重新写入
        conn.execute("DELETE FROM t_sector_stock WHERE sector_code = ?", (code,))
        conn.executemany("""
            INSERT OR IGNORE INTO t_sector_stock (sector_code, stock_code, stock_name)
            VALUES (?, ?, ?)
        """, [(code, sc, sn) for sc, sn in all_stocks.items()])

        conn.commit()
        log(f"  {name}（{code}）同步完成，成分股：{len(all_stocks)} 只", "OK")


# ============================================================
# 子命令实现
# ============================================================
def cmd_list(args):
    config = load_config()
    conn = get_db_conn()
    virtual_sectors = config.get('virtual_sectors', [])

    print("=" * 60)
    print(f"虚拟板块列表（共 {len(virtual_sectors)} 个）")
    print("=" * 60)

    if not virtual_sectors:
        print("  （暂无虚拟板块）")
        return

    for vs in virtual_sectors:
        code = vs['code']
        name = vs['name']
        sub_count = len(vs.get('sub_sectors', []))
        created = vs.get('created_at', '-')

        # 查询数据库中实际成分股数量
        stock_count = conn.execute(
            "SELECT COUNT(*) FROM t_sector_stock WHERE sector_code = ?", (code,)
        ).fetchone()[0]

        # 查询最新一天的满足率
        latest = conn.execute("""
            SELECT satisfied_count, rank_no, satisfied_rate
            FROM t_daily_report
            WHERE sector_code = ?
            ORDER BY report_date DESC LIMIT 1
        """, (code,)).fetchone()

        print(f"\n  代码: {code}  名称: {name}")
        print(f"  关联子板块: {sub_count} 个  |  成分股: {stock_count} 只  |  创建: {created}")
        if latest:
            print(f"  最新满足数: {latest[0]}  排名: {latest[1]}  板块满足率: {latest[2]:.2f}%")
        else:
            print(f"  最新数据: 暂无（需运行 backfill 后才有数据）")

    conn.close()
    print()


def cmd_info(args):
    config = load_config()
    conn = get_db_conn()
    vs = get_virtual_sector(config, args.code)
    if not vs:
        log(f"未找到虚拟板块：{args.code}", "ERR")
        sys.exit(1)

    print("=" * 60)
    print(f"虚拟板块详情：{vs['name']}（{vs['code']}）")
    print("=" * 60)
    print(f"  描述: {vs.get('description', '-')}")
    print(f"  创建: {vs.get('created_at', '-')}")
    print()

    # 子板块信息
    sub_sectors = vs.get('sub_sectors', [])
    print(f"  关联子板块（{len(sub_sectors)} 个）：")
    total_stocks = 0
    for sub in sub_sectors:
        sub_code = sub.get('code', '-')
        sub_name = sub.get('name', '-')
        # 查询子板块实际成分股数
        cnt = conn.execute(
            "SELECT COUNT(*) FROM t_sector_stock WHERE sector_code = ?", (sub_code,)
        ).fetchone()[0]
        total_stocks_before = total_stocks
        # 查询在数据库中是否存在
        exists = conn.execute(
            "SELECT sector_name FROM t_sector WHERE sector_code = ?", (sub_code,)
        ).fetchone()
        status = f"{cnt} 只成分股" if exists else "【数据库中不存在！】"
        print(f"    {sub_code}  {sub_name}  {status}")

    # 虚拟板块成分股
    stock_count = conn.execute(
        "SELECT COUNT(*) FROM t_sector_stock WHERE sector_code = ?", (vs['code'],)
    ).fetchone()[0]
    print()
    print(f"  虚拟板块总成分股（去重后）：{stock_count} 只")
    if stock_count == 0:
        print("  ⚠ 成分股为空，请先运行：python virtual_sector_mgr.py sync")

    # 历史数据
    history = conn.execute("""
        SELECT report_date, satisfied_count, rank_no, satisfied_rate
        FROM t_daily_report
        WHERE sector_code = ?
        ORDER BY report_date DESC LIMIT 10
    """, (vs['code'],)).fetchall()

    if history:
        print(f"\n  最近10天数据：")
        print(f"  {'日期':12s} {'满足数':>6} {'排名':>6} {'板块满足率':>10}")
        print(f"  {'-'*40}")
        for row in history:
            print(f"  {row[0]:12s} {row[1]:>6} {row[2]:>6} {row[3]:>9.2f}%")
    else:
        print("\n  暂无历史数据（需运行 backfill 后才有数据）")

    conn.close()
    print()


def cmd_sync(args):
    config = load_config()
    conn = get_db_conn()
    print("=" * 60)
    print("同步虚拟板块到数据库")
    print("=" * 60)
    sync_to_db(config, conn, target_code=args.code)
    conn.close()
    print()
    print("同步完成！请运行以下命令重新分析历史数据：")
    print("  python concept_tool.py backfill --days 60 --no-skip")
    print("或只回溯今天：")
    print("  python concept_tool.py backfill --days 1 --no-skip")


def cmd_add(args):
    config = load_config()
    conn = get_db_conn()

    print("=" * 60)
    print("新增虚拟板块（交互模式）")
    print("=" * 60)
    print("提示：输入 Ctrl+C 可随时取消")
    print()

    # 自动分配代码
    default_code = next_virtual_code(config)
    code_input = input(f"板块代码 [默认 {default_code}]: ").strip()
    code = code_input if code_input else default_code

    # 检查代码是否已存在
    if get_virtual_sector(config, code):
        log(f"代码 {code} 已存在，请先删除或使用其他代码", "ERR")
        conn.close()
        sys.exit(1)

    name = input("板块名称（如：科技主题）: ").strip()
    if not name:
        log("板块名称不能为空", "ERR")
        sys.exit(1)

    desc = input("描述（可选）: ").strip()

    # 添加子板块
    sub_sectors = []
    print()
    print("添加关联子板块（输入板块代码，回车结束）：")
    print("提示：可用 'python virtual_sector_mgr.py search 关键词' 查找板块代码")
    while True:
        sub_input = input(f"  子板块代码 #{len(sub_sectors)+1}（回车结束）: ").strip()
        if not sub_input:
            break

        # 查询子板块名称
        row = conn.execute(
            "SELECT sector_name FROM t_sector WHERE sector_code = ?", (sub_input,)
        ).fetchone()
        if not row:
            log(f"  数据库中未找到板块 {sub_input}，请确认代码是否正确", "WARN")
            confirm = input("  是否仍然添加？(y/N): ").strip().lower()
            if confirm != 'y':
                continue
            sub_name = input("  手动输入板块名称: ").strip()
        else:
            sub_name = row[0]
            print(f"  ✓ 找到：{sub_name}")

        sub_sectors.append({"code": sub_input, "name": sub_name})

    if not sub_sectors:
        log("未添加任何子板块，操作取消", "WARN")
        conn.close()
        return

    # 确认
    print()
    print("即将创建虚拟板块：")
    print(f"  代码: {code}  名称: {name}")
    print(f"  描述: {desc or '无'}")
    print(f"  关联子板块:")
    for sub in sub_sectors:
        print(f"    {sub['code']}  {sub['name']}")
    confirm = input("\n确认创建？(y/N): ").strip().lower()
    if confirm != 'y':
        log("操作已取消", "WARN")
        conn.close()
        return

    # 写入配置
    new_vs = {
        "code": code,
        "name": name,
        "is_virtual": True,
        "description": desc,
        "created_at": datetime.date.today().isoformat(),
        "sub_sectors": sub_sectors
    }
    config.setdefault('virtual_sectors', []).append(new_vs)
    save_config(config)

    # 同步到数据库
    sync_to_db(config, conn, target_code=code)
    conn.close()

    log(f"虚拟板块「{name}」（{code}）创建成功！", "OK")
    print()
    print("后续步骤：")
    print("  1. 运行 backfill 生成历史数据：")
    print("     python concept_tool.py backfill --days 60 --no-skip")
    print("  2. 重新生成报告：")
    print("     python gen_concept_html_V1.3.py")


def cmd_del(args):
    config = load_config()
    vs = get_virtual_sector(config, args.code)
    if not vs:
        log(f"未找到虚拟板块：{args.code}", "ERR")
        sys.exit(1)

    print(f"即将删除虚拟板块：{vs['name']}（{vs['code']}）")
    print("⚠ 此操作将同时删除数据库中的相关记录！")
    confirm = input("确认删除？(y/N): ").strip().lower()
    if confirm != 'y':
        log("操作已取消", "WARN")
        return

    # 从配置中删除
    config['virtual_sectors'] = [v for v in config['virtual_sectors'] if v['code'] != args.code]
    save_config(config)

    # 从数据库删除
    conn = get_db_conn()
    conn.execute("DELETE FROM t_sector WHERE sector_code = ?", (args.code,))
    conn.execute("DELETE FROM t_sector_stock WHERE sector_code = ?", (args.code,))
    conn.execute("DELETE FROM t_daily_report WHERE sector_code = ?", (args.code,))
    conn.execute("DELETE FROM t_sector_stat WHERE sector_code = ?", (args.code,))
    conn.commit()
    conn.close()

    log(f"虚拟板块「{vs['name']}」（{args.code}）已删除", "OK")


def cmd_add_sub(args):
    """为虚拟板块添加子板块"""
    config = load_config()
    vs = get_virtual_sector(config, args.code)
    if not vs:
        log(f"未找到虚拟板块：{args.code}", "ERR")
        sys.exit(1)

    conn = get_db_conn()
    sub_code = args.sub_code

    # 检查是否已存在
    existing_codes = [s['code'] for s in vs.get('sub_sectors', [])]
    if sub_code in existing_codes:
        log(f"子板块 {sub_code} 已在虚拟板块中", "WARN")
        conn.close()
        return

    # 查询子板块名称
    row = conn.execute(
        "SELECT sector_name FROM t_sector WHERE sector_code = ?", (sub_code,)
    ).fetchone()
    if not row:
        log(f"数据库中未找到板块 {sub_code}", "ERR")
        conn.close()
        sys.exit(1)

    sub_name = row[0]
    vs.setdefault('sub_sectors', []).append({"code": sub_code, "name": sub_name})
    save_config(config)

    # 重新同步
    sync_to_db(config, conn, target_code=args.code)
    conn.close()

    log(f"已添加子板块「{sub_name}」({sub_code}) 到「{vs['name']}」", "OK")
    log("请重新运行 backfill 更新历史数据", "INFO")


def cmd_del_sub(args):
    """从虚拟板块移除子板块"""
    config = load_config()
    vs = get_virtual_sector(config, args.code)
    if not vs:
        log(f"未找到虚拟板块：{args.code}", "ERR")
        sys.exit(1)

    sub_code = args.sub_code
    original = vs.get('sub_sectors', [])
    target = next((s for s in original if s['code'] == sub_code), None)
    if not target:
        log(f"子板块 {sub_code} 不在虚拟板块中", "WARN")
        return

    vs['sub_sectors'] = [s for s in original if s['code'] != sub_code]
    save_config(config)

    # 重新同步
    conn = get_db_conn()
    sync_to_db(config, conn, target_code=args.code)
    conn.close()

    log(f"已从「{vs['name']}」移除子板块「{target['name']}」({sub_code})", "OK")
    log("请重新运行 backfill 更新历史数据", "INFO")


def cmd_search(args):
    """在数据库中搜索真实板块"""
    conn = get_db_conn()
    keyword = args.keyword

    rows = conn.execute(
        "SELECT sector_code, sector_name FROM t_sector WHERE sector_name LIKE ? ORDER BY sector_code",
        (f'%{keyword}%',)
    ).fetchall()

    conn.close()

    if not rows:
        print(f"未找到包含「{keyword}」的板块")
        return

    print(f"搜索「{keyword}」，找到 {len(rows)} 个板块：")
    print(f"{'代码':10s}  {'名称'}")
    print("-" * 40)
    for code, name in rows:
        # 跳过虚拟板块
        if code.startswith('888'):
            prefix = " [虚拟]"
        else:
            prefix = ""
        print(f"{code:10s}  {name}{prefix}")


# ============================================================
# 命令行解析
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        prog="virtual_sector_mgr",
        description="虚拟板块管理工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例：
  python virtual_sector_mgr.py list                    # 列出所有虚拟板块
  python virtual_sector_mgr.py info 888001             # 查看指定虚拟板块详情
  python virtual_sector_mgr.py sync                    # 同步所有虚拟板块到数据库
  python virtual_sector_mgr.py sync --code 888001      # 只同步指定虚拟板块
  python virtual_sector_mgr.py add                     # 交互式新增虚拟板块
  python virtual_sector_mgr.py del 888001              # 删除虚拟板块
  python virtual_sector_mgr.py add-sub 888001 880xxx   # 添加子板块
  python virtual_sector_mgr.py del-sub 888001 880xxx   # 移除子板块
  python virtual_sector_mgr.py search 创新药            # 搜索板块代码
"""
    )
    sub = parser.add_subparsers(dest='cmd', required=True)

    sub.add_parser('list', help='列出所有虚拟板块')

    p_info = sub.add_parser('info', help='查看虚拟板块详情')
    p_info.add_argument('code', help='虚拟板块代码（如 888001）')

    p_sync = sub.add_parser('sync', help='将配置同步到数据库')
    p_sync.add_argument('--code', help='只同步指定代码的虚拟板块')

    sub.add_parser('add', help='交互式新增虚拟板块')

    p_del = sub.add_parser('del', help='删除虚拟板块')
    p_del.add_argument('code', help='虚拟板块代码')

    p_add_sub = sub.add_parser('add-sub', help='为虚拟板块添加子板块')
    p_add_sub.add_argument('code', help='虚拟板块代码')
    p_add_sub.add_argument('sub_code', help='要添加的子板块代码')

    p_del_sub = sub.add_parser('del-sub', help='从虚拟板块移除子板块')
    p_del_sub.add_argument('code', help='虚拟板块代码')
    p_del_sub.add_argument('sub_code', help='要移除的子板块代码')

    p_search = sub.add_parser('search', help='搜索数据库中的真实板块')
    p_search.add_argument('keyword', help='搜索关键词')

    args = parser.parse_args()

    cmd_map = {
        'list': cmd_list,
        'info': cmd_info,
        'sync': cmd_sync,
        'add': cmd_add,
        'del': cmd_del,
        'add-sub': cmd_add_sub,
        'del-sub': cmd_del_sub,
        'search': cmd_search,
    }
    cmd_map[args.cmd](args)


if __name__ == '__main__':
    main()
