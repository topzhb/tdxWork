"""
main.py  —— 见龙在田精选系统主入口
------------------------------------------------------------
完整流程：
  [0] 板块基础数据（--update-sector 时强制刷新，否则自动跳过）
  [1] collect   每日采集：EBK→ebk_stocks + t_daily_report→trend_sectors
  [2] score     精选分析：板块筛选→K线→财务→综合评分→picks
  [3] report    报告生成：HTML + Excel + EBK

用法：
  python main.py                                    # 今日全流程
  python main.py --date 20260326                    # 指定日期
  python main.py --top 20                           # 精选个股 TOP20
  python main.py --sector-top 20                    # 热门板块数量
  python main.py --fund-strategy classic,surprise   # 稳健+超预期组合
  python main.py --fund-strategy growth,surprise    # 成长+超预期组合
  python main.py --skip-tech                        # 纯基本面模式（跳过K线）
  python main.py --skip-tech --fund-strategy classic,surprise
  python main.py --update-sector                    # 强制刷新板块基础数据
  python main.py --skip-collect                     # 跳过采集
  python main.py --skip-score                       # 跳过评分
  python main.py --only-report                      # 仅生成报告
  python main.py --with-excel                       # 同时生成Excel（默认不生成）
  python main.py --no-html / --no-ebk               # 禁用HTML或EBK输出

可选基本面策略（逗号组合）：
  classic   稳健价值型（PE+利润增速+营收+ROE+市值）
  growth    牛市成长型（PEG+高增速+利润弹性）
  surprise  超预期成长型（预期差+机构覆盖）

  例：--fund-strategy classic,surprise
      → 稳健价值(100分) + 超预期(100分) 取平均 = 组合基本面分
------------------------------------------------------------
"""

import os, sys, argparse, sqlite3
from datetime import date, datetime

# 强制行缓冲，管道下 print 立即可见
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
else:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, line_buffering=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE  = os.path.join(BASE_DIR, "..", "db", "concept_weekly.db")


# ══════════════════════════════════════════════════════════
# 辅助：格式化日期
# ══════════════════════════════════════════════════════════
def normalize_date(d: str) -> str:
    """YYYYMMDD or YYYY-MM-DD → YYYY-MM-DD"""
    if d is None:
        return date.today().strftime("%Y-%m-%d")
    d = d.replace("-", "")
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


# ══════════════════════════════════════════════════════════
# 辅助：检查当日数据是否已存在
# ══════════════════════════════════════════════════════════
def has_data(table: str, date_str: str) -> bool:
    try:
        conn = sqlite3.connect(DB_FILE)
        cur  = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE date=?", (date_str,))
        n    = cur.fetchone()[0]
        conn.close()
        return n > 0
    except Exception:
        return False


# ══════════════════════════════════════════════════════════
# 步骤封装
# ══════════════════════════════════════════════════════════
def step_sector(force: bool):
    print("\n" + "=" * 58)
    print("  [STEP 0] 板块基础数据")
    print("=" * 58)
    import build_sector_db
    import sqlite3

    conn = sqlite3.connect(DB_FILE)
    needs = build_sector_db.need_update(conn) or force
    conn.close()

    if not needs:
        # 快速统计已有数据
        conn2 = sqlite3.connect(DB_FILE)
        n_sec = conn2.execute("SELECT COUNT(*) FROM t_sector").fetchone()[0]
        n_ss  = conn2.execute("SELECT COUNT(*) FROM t_sector_stock").fetchone()[0]
        conn2.close()
        print(f"  [OK] t_sector 已有 {n_sec} 条，t_sector_stock 已有 {n_ss} 条")
        print(f"  提示：板块数据由 concept_weekly 项目维护，无需本项目导入")
        return

    print(f"  [ERROR] t_sector 表为空，请先运行 concept_weekly 项目导入板块数据")
    return


def step_collect(date_str: str, ebk_path: str, top_n: int,
                 use_3day: bool = True, allow_fallback: bool = True):
    print("\n" + "=" * 58)
    print(f"  [STEP 1] 每日采集  {date_str}")
    print("=" * 58)
    import collect
    collect.run(date_str=date_str, ebk_path=ebk_path, top_n=top_n,
                use_3day=use_3day, allow_fallback=allow_fallback)


def step_score(date_str: str, fund_strategy: str = "classic", skip_tech: bool = False,
               surprise_only: bool = False, no_tech_filter: bool = False,
               surprise_mode: str = "auto", qdiff_mode: str = "quarter"):
    print("\n" + "=" * 58)
    print(f"  [STEP 2] 精选分析  {date_str}")
    print("=" * 58)
    import score
    return score.run(date_str=date_str, fund_strategy=fund_strategy,
                     skip_tech=skip_tech, surprise_only=surprise_only,
                     no_tech_filter=no_tech_filter,
                     surprise_mode=surprise_mode,
                     qdiff_mode=qdiff_mode)


def step_report(date_str: str, top_n: int,
                no_html: bool, no_excel: bool, no_ebk: bool,
                strategy_label: str = "", strategy: str = "classic",
                skip_tech: bool = False,
                surprise_only: bool = False, no_top_limit: bool = False):
    print("\n" + "=" * 58)
    print(f"  [STEP 3] 报告生成  {date_str}  TOP{top_n}")
    print("=" * 58)
    import report
    return report.run(
        date_str=date_str, top_n=top_n,
        gen_html_flag=not no_html,
        gen_excel_flag=not no_excel,
        gen_ebk_flag=not no_ebk,
        strategy_label=strategy_label,
        strategy=strategy,
        skip_tech=skip_tech,
        surprise_only=surprise_only,
        no_top_limit=no_top_limit,
    )


# ══════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="见龙在田精选系统主入口",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python main.py                     今日全流程（热门板块TOP20，精选个股TOP30）
  python main.py --top 20            精选个股 TOP20
  python main.py --sector-top 20     热门板块取 TOP20
  python main.py --date 20260326     指定日期
  python main.py --update-sector     强制刷新板块数据后全流程
  python main.py --only-report       仅重新生成报告
  python main.py --skip-collect      跳过采集，只做评分+报告
        """
    )
    parser.add_argument("--date",          type=str,  default=None,
                        help="日期 YYYYMMDD 或 YYYY-MM-DD（默认今日）")
    parser.add_argument("--top",           type=int,  default=20,
                        help="精选个股 TOP N（默认20）")
    parser.add_argument("--sector-top",    type=int,  default=20,
                        help="热门板块取 TOP N（默认20）")
    parser.add_argument("--ebk",           type=str,  default=None,
                        help="指定 EBK 文件路径")
    parser.add_argument("--update-sector", action="store_true",
                        help="强制刷新板块基础数据")
    parser.add_argument("--skip-collect",  action="store_true",
                        help="跳过采集步骤")
    parser.add_argument("--skip-score",    action="store_true",
                        help="跳过评分步骤")
    parser.add_argument("--only-report",   action="store_true",
                        help="仅生成报告（等价于 --skip-collect --skip-score）")
    parser.add_argument("--no-html",       action="store_true", help="不生成 HTML")
    parser.add_argument("--with-excel",    action="store_true", help="生成 Excel 报告（默认不生成）")
    parser.add_argument("--no-ebk",        action="store_true", help="不生成 EBK")
    parser.add_argument("--fund-strategy", type=str, default="classic",
                        help=("基本面评分策略，支持逗号组合：classic / growth / surprise / single_line\n"
                              "例：classic,surprise（稳健+超预期，取平均）"))
    parser.add_argument("--skip-tech", action="store_true", default=False,
                        help="纯基本面模式：跳过K线读取和技术打分")
    parser.add_argument("--no-tech-filter", action="store_true", default=False,
                        help="禁用技术过滤（高位+不放量），恢复为旧技术权重评分模式")
    parser.add_argument("--surprise-only", action="store_true", default=False,
                        help=("超预期命中模式：仅输出 diff>0 的超预期股票，且不限TOP数量。"
                              "自动使用纯 surprise 策略（非组合）"))
    parser.add_argument("--no-top-limit", action="store_true", default=False,
                        help="无TOP截断模式：保留所有通过筛选的股票，不进行TOP N限制")
    parser.add_argument("--no-3day", action="store_true", default=False,
                        help="禁用三源交集，热门板块仅使用涨停+日线（双源模式）")
    parser.add_argument("--no-fallback", action="store_true", default=False,
                        help="三源交集为空时不退化为双源，直接返回空结果")
    parser.add_argument("--surprise-mode", type=str, default="auto",
                        choices=["auto", "forward", "actual"],
                        help=("超预期/预期差方向：auto(自动判定,默认) / "
                              "forward(前瞻，预期>实际) / "
                              "actual(验证，实际>预期)。surprise 和 single_line 策略生效。"))
    parser.add_argument("--qdiff-mode", type=str, default="quarter",
                        choices=["quarter", "ttm"],
                        help=("single_line 预期差对比模式：quarter(单季利润vs季度预测,默认) / "
                              "ttm(TTM实际vs年度预期)。仅 single_line 策略生效。"))
    args = parser.parse_args()

    # surprise-only 时强制使用纯 surprise 策略
    if args.surprise_only and args.fund_strategy != "surprise":
        args.fund_strategy = "surprise"
        print(f"[INFO] --surprise-only 已强制使用纯 surprise 策略")

    # --only-report 等价于跳过前两步
    if args.only_report:
        args.skip_collect = True
        args.skip_score   = True

    date_str = normalize_date(args.date)
    t_start  = datetime.now()

    from fund_strategies import get_strategy_label
    strat_label = get_strategy_label(args.fund_strategy)
    if getattr(args, "surprise_only", False):
        mode_label = "超预期命中"
    elif args.skip_tech:
        mode_label = "纯基本面"
    else:
        mode_label = "技术+基本面"

    top_label = f"TOP{args.top}" if not args.no_top_limit else "无截断"
    print("=" * 58)
    print("  见龙在田精选系统")
    print(f"  日期: {date_str}  热门板块TOP{args.sector_top}  精选个股{top_label}")
    print(f"  策略: {strat_label}  |  模式: {mode_label}")
    print(f"  开始: {t_start.strftime('%H:%M:%S')}")
    print("=" * 58)

    # STEP 0: 板块基础数据
    step_sector(force=args.update_sector)

    # STEP 1: 每日采集
    if args.skip_collect:
        if has_data("qs_ebk_stocks", date_str):
            print(f"\n[STEP 1] 跳过采集（qs_ebk_stocks 已有 {date_str} 数据）")
        else:
            print(f"\n[STEP 1] --skip-collect 但 qs_ebk_stocks 无当日数据，强制采集...")
            step_collect(date_str, args.ebk, args.sector_top,
                         use_3day=not args.no_3day, allow_fallback=not args.no_fallback)
    else:
        step_collect(date_str, args.ebk, args.sector_top,
                     use_3day=not args.no_3day, allow_fallback=not args.no_fallback)

    # 判断是否纯 surprise 策略（用于控制是否限制TOP数量）
    is_pure_surprise = (args.fund_strategy == "surprise")

    # STEP 2: 精选分析
    if args.skip_score:
        if has_data("qs_picks", date_str):
            print(f"\n[STEP 2] 跳过评分（qs_picks 已有 {date_str} 数据）")
        else:
            print(f"\n[STEP 2] --skip-score 但 qs_picks 无当日数据，强制评分...")
            step_score(date_str, fund_strategy=args.fund_strategy,
                       skip_tech=args.skip_tech, surprise_only=is_pure_surprise,
                       no_tech_filter=args.no_tech_filter,
                       surprise_mode=args.surprise_mode,
                       qdiff_mode=args.qdiff_mode)
    else:

        # 需要技术评分时，强制重新计算K线（行情数据可能多次刷新）
        if not args.skip_tech and has_data("qs_picks", date_str):
            print(f"\n[STEP 2] 需要技术评分，强制重新计算K线并更新数据...")
        
        step_score(date_str, fund_strategy=args.fund_strategy,
                   skip_tech=args.skip_tech, surprise_only=is_pure_surprise,
                   no_tech_filter=args.no_tech_filter,
                   surprise_mode=args.surprise_mode,
                   qdiff_mode=args.qdiff_mode)

    # STEP 3: 报告生成
    step_report(date_str, args.top,
                no_html=args.no_html,
                no_excel=not args.with_excel,
                no_ebk=args.no_ebk,
                strategy_label=strat_label,
                strategy=args.fund_strategy,
                skip_tech=args.skip_tech,
                surprise_only=is_pure_surprise,
                no_top_limit=args.no_top_limit)

    # ── 完成汇总 ──────────────────────────────────────────
    t_end    = datetime.now()
    elapsed  = (t_end - t_start).total_seconds()
    compact  = date_str.replace("-", "")

    print("\n" + "=" * 58)
    print("  全流程完成！")
    print(f"  耗时: {elapsed:.0f} 秒")
    print(f"  HTML:  picks/report_{date_str}.html")
    print(f"  Excel: picks/精选标的_{compact}_TOP{args.top}.xlsx")
    print(f"  EBK:   picks/精选标的_{compact}.EBK")
    print("=" * 58)


if __name__ == "__main__":
    main()
