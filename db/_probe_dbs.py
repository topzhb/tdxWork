import sqlite3, os

BASE = os.path.dirname(os.path.abspath(__file__))

for db in ['picks.db', 'concept_weekly.db']:
    path = os.path.join(BASE, db)
    print(f'\n=== {db} ===')
    if not os.path.exists(path):
        print('  [NOT FOUND]')
        continue
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY name")
    for row in cur.fetchall():
        cur2 = con.cursor()
        cur2.execute(f'SELECT COUNT(*) FROM "{row[0]}"')
        cnt = cur2.fetchone()[0]
        print(f'  {row[1]:5s}  {row[0]:40s}  {cnt:>8} rows')
    con.close()
