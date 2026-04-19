import sqlite3, os

BASE = os.path.dirname(os.path.abspath(__file__))

for db, tables in [
    ('picks.db',        ['sectors', 'sector_stocks', 'ebk_stocks', 'picks', 'trend_sectors', 'trend_picks']),
    ('concept_weekly.db', ['t_sector', 't_sector_stock', 't_stock', 't_daily_report', 't_sector_stat', 't_stock_calc', 't_run_log']),
]:
    path = os.path.join(BASE, db)
    con = sqlite3.connect(path)
    cur = con.cursor()
    print(f'\n{"="*60}')
    print(f'  {db}')
    print(f'{"="*60}')
    for t in tables:
        cur.execute(f"PRAGMA table_info('{t}')")
        cols = cur.fetchall()
        if not cols:
            print(f'\n  [{t}] -- NOT FOUND')
            continue
        print(f'\n  [{t}]')
        for c in cols:
            pk = ' PK' if c[5] else ''
            nn = ' NOT NULL' if c[3] else ''
            df = f' DEFAULT {c[4]}' if c[4] is not None else ''
            print(f'    {c[1]:30s} {c[2]:15s}{pk}{nn}{df}')
        # 打印前3行样本
        cur.execute(f'SELECT * FROM "{t}" LIMIT 2')
        rows = cur.fetchall()
        if rows:
            print(f'    -- sample:')
            for r in rows:
                print(f'    {str(r)[:120]}')
    con.close()
