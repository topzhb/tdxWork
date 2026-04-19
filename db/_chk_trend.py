import sqlite3
c = sqlite3.connect('picks.db')
cols = [r[1] for r in c.execute("PRAGMA table_info('trend_sectors')")]
print("cols:", cols)
rows = c.execute("SELECT * FROM trend_sectors LIMIT 3").fetchall()
for r in rows:
    print(r)
