import pyodbc
import os
import csv
import datetime
import hashlib
import json
import sqlite3
import subprocess
import sys

# ---------------------------------------------------------------------------
# CONFIGURATION
# Paths are derived from this script's own location so the file can live
# anywhere — just drop it next to db12024.bds.
# ---------------------------------------------------------------------------
SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
MDB_PATH      = os.path.join(SCRIPT_DIR, 'db12025.bds')
DB_PASSWORD   = 'ILoveMyINDIA'

# Git repo — same folder as this script so everything is self-contained.
REPO_DIR      = SCRIPT_DIR
REMOTE_URL    = 'https://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@github.com/ashishbansalmbm/Pehnava.git'

EXPORT_TABLE  = 'StockInfo'
EXPORT_DIR    = os.path.join(REPO_DIR, 'export')
EXPORT_CSV    = os.path.join(EXPORT_DIR, f'{EXPORT_TABLE}.csv')
EXPORT_DB     = os.path.join(EXPORT_DIR, f'{EXPORT_TABLE}.db')
VERSION_FILE  = os.path.join(EXPORT_DIR, f'{EXPORT_TABLE}_version.txt')
METADATA_FILE = os.path.join(EXPORT_DIR, f'{EXPORT_TABLE}_metadata.json')

EXPORT_QUERY = """
SELECT 
    s.[BCN], 
    First(i.[C1]) AS [Article], 
    First(i.[C2]) AS [Size], 
    First(i.[C3]) AS [Brand], 
    First(i.[C4]) AS [Colour], 
    First(i.[D3]) AS [MRP], 
    First(i.[D4]) AS [SalesPrice], 
    Sum(s.[Value1]) AS [TotalQty],
    First(i.[ItemCode]) AS [ItemCode],
    First(m.[name]) AS [ItemName]
FROM (([ItemParamDet] AS s 
INNER JOIN [ItemParamDet] AS i ON s.[BCN] = i.[BCN])
LEFT JOIN [master1] AS m ON i.[ItemCode] = m.[code])
WHERE s.[MCCode] = 201 
  AND s.[RecType] = 1 
  AND s.[Date] <= Date()
GROUP BY s.[BCN]
HAVING Sum(s.[Value1]) <> 0;
"""


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def step(msg: str):
    print(f'\n[+] {msg}')


def fail(msg: str):
    print(f'\n[!] {msg}', file=sys.stderr)
    sys.exit(1)


def compute_hash(path: str) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------------------------

def connect() -> pyodbc.Connection:
    if not os.path.exists(MDB_PATH):
        fail(f'Database file not found: {MDB_PATH}')

    conn_str = (
        'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={MDB_PATH};'
        f'PWD={DB_PASSWORD};'
    )
    pyodbc.pooling = False
    try:
        # timeout=10 prevents the ODBC driver from hanging indefinitely
        return pyodbc.connect(conn_str, timeout=10)
    except pyodbc.Error as e:
        fail(f'Connection failed: {e}')


# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------

def export_to_csv(conn: pyodbc.Connection) -> int:
    os.makedirs(EXPORT_DIR, exist_ok=True)
    cur = conn.cursor()
    cur.execute(EXPORT_QUERY)
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()
    with open(EXPORT_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(columns)
        w.writerows(rows)
    return len(rows)


def export_to_sqlite(row_count: int):
    with sqlite3.connect(EXPORT_DB) as db:
        cur = db.cursor()
        with open(EXPORT_CSV, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            cols = ', '.join(f'"{c}" TEXT' for c in headers)
            cur.execute(f'DROP TABLE IF EXISTS "{EXPORT_TABLE}"')
            cur.execute(f'CREATE TABLE "{EXPORT_TABLE}" ({cols})')
            ph = ', '.join('?' * len(headers))
            quoted = ', '.join(f'"{h}"' for h in headers)
            cur.executemany(
                f'INSERT INTO "{EXPORT_TABLE}" ({quoted}) VALUES ({ph})',
                reader
            )
        # Indices for fast lookup
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_itemcode ON "{EXPORT_TABLE}" ("ItemCode")')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_article ON "{EXPORT_TABLE}" ("Article")')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_bcn ON "{EXPORT_TABLE}" ("BCN")')
        db.commit()
    print(f'    SQLite : {EXPORT_DB} ({row_count} rows)')


def write_metadata(row_count: int):
    timestamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
    file_hash = compute_hash(EXPORT_CSV)

    with open(VERSION_FILE, 'w', encoding='utf-8') as f:
        f.write(f'{timestamp}\nhash:{file_hash}\nrows:{row_count}\n')

    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'table': EXPORT_TABLE,
            'rows': row_count,
            'timestamp': timestamp,
            'hash': file_hash,
        }, f, indent=2)

    print(f'    Hash  : {file_hash}')
    print(f'    Rows  : {row_count}')
    print(f'    Time  : {timestamp}')


# ---------------------------------------------------------------------------
# GIT SETUP & PUSH
# ---------------------------------------------------------------------------

def git(*args, check=True) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ['git', *args],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        fail(f'git {args[0]} failed:\n{result.stderr.strip()}')
    return result


def ensure_git_repo():
    """Create REPO_DIR if needed, initialise a git repo, and wire up the remote."""
    os.makedirs(REPO_DIR, exist_ok=True)

    if not os.path.isdir(os.path.join(REPO_DIR, '.git')):
        print('    Initialising new git repository...')
        git('init', '-b', 'main')
        git('config', 'user.email', 'backup@pehnava.local')
        git('config', 'user.name', 'Pehnava Backup')

    # Always make sure the remote points to the right URL
    remotes = git('remote', check=False).stdout.split()
    if 'origin' in remotes:
        git('remote', 'set-url', 'origin', REMOTE_URL)
    else:
        git('remote', 'add', 'origin', REMOTE_URL)
    print('    Git repo ready.')


def push_to_github():
    # Fetch remote so we know its current state, then reset local branch to
    # match it — avoids non-fast-forward rejections for a data-export repo
    # where we always want the latest snapshot to win.
    fetch = git('fetch', 'origin', 'main', check=False)
    if fetch.returncode == 0:
        git('reset', '--soft', 'origin/main', check=False)

    git('add',
        'export/StockInfo.db',
        'export/StockInfo.csv',
        'export/StockInfo_metadata.json',
        'export/StockInfo_version.txt')

    if git('diff', '--cached', '--quiet', check=False).returncode == 0:
        print('    No changes detected — nothing to push.')
        return

    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    git('commit', '-m', f'Backup: {timestamp}')
    git('push', '--force', REMOTE_URL, 'HEAD:main')
    print('    Pushed to GitHub successfully.')


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    step('Connecting to BusyWin database...')
    conn = connect()
    print('    Connected.')

    try:
        step('Exporting stock data to CSV...')
        row_count = export_to_csv(conn)
        print(f'    CSV   : {EXPORT_CSV} ({row_count} rows)')

        step('Converting to SQLite...')
        export_to_sqlite(row_count)

        step('Writing metadata...')
        write_metadata(row_count)

        step('Ensuring git repository is initialised...')
        ensure_git_repo()

        step('Committing and pushing to GitHub...')
        push_to_github()

    except Exception as e:
        fail(str(e))
    finally:
        conn.close()

    print('\n✅ Done.\n')


if __name__ == '__main__':
    main()
