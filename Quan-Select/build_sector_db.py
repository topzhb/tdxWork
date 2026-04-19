"""
build_sector_db.py
------------------
板块数据已由 concept_weekly 项目维护在 concept_weekly.db 中。
本脚本用于检查 t_sector / t_sector_stock 数据是否就绪，
以及 t_daily_report 是否有可用于计算热门板块的数据。

用法：
  python build_sector_db.py            # 检查数据是否就绪
  python build_sector_db.py --update   # 强制打印详细统计

表（来自对方项目，只读）：
  t_sector       (sector_code PK, sector_name)
  t_sector_stock (sector_code, stock_code)   -- 6位代码，无市场前缀
  t_daily_report (report_date, run_id, sector_code, satisfied_count, ...)
  t_run_log      (run_id, satisfied_count, note, ...)
"""

import sqlite3
import os
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE     = os.path.join(_SCRIPT_DIR, "..", "db", "concept_weekly.db")


def need_update(conn: sqlite3.Connection) -> bool:
    """检查是否需要导入：t_sector 表为空则需要"""
    try:
        cur = conn.execute("SELECT COUNT(*) FROM t_sector")
        return cur.fetchone()[0] == 0
    except Exception:
        return True


def build(db_path: str, force: bool = False):
    conn = sqlite3.connect(db_path)

    # ── 检查 t_sector / t_sector_stock 数据 ──────────────────
    try:
        n_sec = conn.execute("SELECT COUNT(*) FROM t_sector").fetchone()[0]
        n_ss  = conn.execute("SELECT COUNT(*) FROM t_sector_stock").fetchone()[0]
    except Exception as e:
        print(f"  [ERROR] 无法读取 t_sector/t_sector_stock：{e}")
        conn.close()
        return

    # ── 检查 t_daily_report（热门板块数据源）────────────────
    try:
        n_dr   = conn.execute("SELECT COUNT(*) FROM t_daily_report").fetchone()[0]
        latest = conn.execute(
            "SELECT report_date, COUNT(*) FROM t_daily_report "
            "GROUP BY report_date ORDER BY report_date DESC LIMIT 1"
        ).fetchone()
        dr_info = f"t_daily_report: {n_dr} 条，最新日期: {latest[0] if latest else '无'} ({latest[1] if latest else 0} 个板块)"
    except Exception:
        dr_info = "t_daily_report: 不存在或无数据"

    if not force:
        print(f"  [OK] t_sector: {n_sec} 条板块，t_sector_stock: {n_ss} 条关联")
        print(f"  [OK] {dr_info}")
        print(f"  提示：板块数据由 concept_weekly 项目维护，无需本脚本导入")
        conn.close()
        return

    # force 模式下打印详细统计
    print(f"  [统计] t_sector: {n_sec} 条，t_sector_stock: {n_ss} 条")
    print(f"  [统计] {dr_info}")
    conn.close()


def verify(db_path: str):
    """简单校验：取5只样本，查各自所属板块"""
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # 优先从 qs_ebk_stocks 取，fallback 直接查 t_sector_stock 样本
    try:
        cur.execute("SELECT DISTINCT code FROM qs_ebk_stocks ORDER BY date DESC LIMIT 5")
        samples = [r[0] for r in cur.fetchall()]
    except Exception:
        samples = []

    if not samples:
        cur.execute("SELECT DISTINCT stock_code FROM t_sector_stock LIMIT 5")
        samples = [r[0] for r in cur.fetchall()]

    print("\n── 板块归属校验（样本）─────────────────────")
    for code in samples:
        stock6 = str(code)[-6:]
        cur.execute("""
            SELECT s.sector_name
            FROM t_sector_stock ss
            JOIN t_sector s ON s.sector_code = ss.sector_code
            WHERE ss.stock_code = ?
            ORDER BY s.sector_name
        """, (stock6,))
        secs = [r[0] for r in cur.fetchall()]
        print(f"  {stock6}  共 {len(secs)} 个板块")
        if secs:
            print(f"    → {' / '.join(secs[:6])}" + (" ..." if len(secs) > 6 else ""))

    conn.close()


if __name__ == "__main__":
    force = "--update" in sys.argv

    print(f"\n[1] 检查板块数据 {DB_FILE} ...")
    build(DB_FILE, force=force)

    print("\n[2] 校验...")
    verify(DB_FILE)

    print("\n[完成] 板块数据检查完毕")

