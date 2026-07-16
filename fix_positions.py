import sqlite3

conn = sqlite3.connect('trades.db')
c = conn.cursor()
c.execute("UPDATE positions SET status = 'CLOSED' WHERE status = 'OPEN'")
print(f'✅ Closed {c.rowcount} positions')
conn.commit()
conn.close()