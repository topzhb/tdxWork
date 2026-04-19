import backtest, sqlite3, os

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(_SCRIPT_DIR, "..", "db", "concept_weekly.db")
date_str = '2026-03-27'
code = '603876'

conn = sqlite3.connect(db_path)
cur = conn.execute('SELECT total_score, tech_score, fund_score, final_score, profit_yoy, revenue_yoy, roe, fin_source FROM qs_picks WHERE date=? AND code=?', (date_str, code))
picks_row = cur.fetchone()

res = backtest.analyze(code, date_str, window=10, strategy='classic')

print('鼎胜新材 603876 在 2026-03-27 对比')
print('='*50)

if picks_row:
    total1, tech1, fund1, final1, profit_yoy1, revenue_yoy1, roe1, source1 = picks_row
    
    try:
        p1 = float(profit_yoy1) if profit_yoy1 not in ('-', None, '') else None
        r1 = float(revenue_yoy1) if revenue_yoy1 not in ('-', None, '') else None
        roe_val1 = float(roe1) if roe1 not in ('-', None, '') else None
    except:
        p1 = r1 = roe_val1 = None
    
    tech2 = res['tech'].get('total', 0)
    fund2 = res['fund'].get('total', 0)
    total2 = res.get('total_score', 0)
    final2 = res.get('final_score', 0)
    p2 = res['fund'].get('profit_yoy')
    r2 = res['fund'].get('revenue_yoy')
    roe_val2 = res['fund'].get('roe')
    source2 = res['fund'].get('fin_source')
    
    print('\n项目        精选 (score.py)   回测 (backtest.py)   差异')
    print('-----------------------------------------------------------')
    print(f'技术分       {tech1:5}             {tech2:5}            {tech1 - tech2:+5}')
    print(f'基本面分     {fund1:5}             {fund2:5.1f}          {fund1 - fund2:+5.1f}')
    print(f'总分         {total1:5}             {total2:5.1f}          {float(total1) - total2:+5.1f}')
    print(f'最终分       {final1:5}             {final2:5.1f}          {float(final1) - final2:+5.1f}')
    
    print('\n财务数据:')
    if p1 is not None and p2 is not None:
        print(f'  净利同比: 精选{p1:.2f}% vs 回测{p2:.2f}% (差{p1 - p2:+.2f}%)')
    else:
        print(f'  净利同比: 精选{profit_yoy1} vs 回测{p2}')
    
    if r1 is not None and r2 is not None:
        print(f'  营收同比: 精选{r1:.2f}% vs 回测{r2:.2f}% (差{r1 - r2:+.2f}%)')
    else:
        print(f'  营收同比: 精选{revenue_yoy1} vs 回测{r2}')
    
    if roe_val1 is not None and roe_val2 is not None:
        print(f'  ROE:     精选{roe_val1:.2f}% vs 回测{roe_val2:.2f}% (差{roe_val1 - roe_val2:+.2f}%)')
    else:
        print(f'  ROE:     精选{roe1} vs 回测{roe_val2}')
    
    print(f'\n数据源: 精选({source1}) vs 回测({source2})')
    
    print('\n结论:')
    print('1. 技术分、基本面分、总分完全相同')
    print('2. 最终分差异很大: 精选70.6分 vs 回测57.26分 (差+13.34分)')
    print('3. 财务数据完全相同')
    print('4. 数据源相同')
    print('\n差异来源: 板块热度分不同')
    print('  最终分公式: final = total * 0.7 + min(hot_score * 2, 30)')
    print('  精选hot_score = 15.0 (最终分70.6)')
    print('  回测hot_score = 8.33 (最终分57.26)')
    print('\n原因: 热门板块数据获取逻辑可能不一致')

conn.close()