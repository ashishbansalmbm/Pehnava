import pyodbc
import sys
import platform
import os
import csv
import datetime
import hashlib
import json
import sqlite3

# --- CONFIGURATION ---
MDB_PATH = r'C:\BusyWin 12.2\DATA\COMP0003\db12024.bds'
DB_PASSWORD = 'ILoveMyINDIA'
EXPORT_TABLE = 'StockInfo'
EXPORT_QUERY = """
SELECT 
    s.[BCN], 
    First(i.[C1]) AS [Article], 
    First(i.[C2]) AS [Size], 
    First(i.[C3]) AS [Brand], 
    First(i.[C4]) AS [Style], 
    First(i.[D3]) AS [MRP], 
    First(i.[D4]) AS [SalesPrice], 
    Sum(s.[Value1]) AS [TotalQty]
FROM [ItemParamDet] AS s 
INNER JOIN [ItemParamDet] AS i ON s.[BCN] = i.[BCN]
WHERE s.[MCCode] = 201 
  AND s.[RecType] = 1 
  AND s.[Date] <= Date()
GROUP BY s.[BCN]
HAVING Sum(s.[Value1]) <> 0;
"""
EXPORT_DIR = r'd:\busyConnector\export'
EXPORT_CSV = os.path.join(EXPORT_DIR, f'{EXPORT_TABLE}.csv')
VERSION_FILE = os.path.join(EXPORT_DIR, f'{EXPORT_TABLE}_version.txt')
METADATA_FILE = os.path.join(EXPORT_DIR, f'{EXPORT_TABLE}_metadata.json')

print('Available drivers:', pyodbc.drivers())


def connect_to_busy():
    if not os.path.exists(MDB_PATH):
        print(f'❌ ERROR: File not found at {MDB_PATH}')
        return None

    try:
        with open(MDB_PATH, 'rb') as f:
            header = f.read(20)
        print(f'File header: {header}')
        print('File can be opened for reading')
    except Exception as e:
        print(f'File open error: {e}')
        return None

    conn_str = (
        'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={MDB_PATH};'
        f'PWD={DB_PASSWORD};'
    )

    print('DEBUG: Connection string built.')
    print('DEBUG: Using Driver: Microsoft Access Driver (*.mdb, *.accdb)')

    try:
        print('DEBUG: Calling pyodbc.connect()...')
        pyodbc.pooling = False
        return pyodbc.connect(conn_str)
    except Exception as e:
        print(f'\n❌ CONNECTION FAILED: {e}')
        return None


def ensure_export_dir():
    os.makedirs(EXPORT_DIR, exist_ok=True)


def compute_file_hash(path):
    hash_md5 = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def export_query_to_csv(conn, query, output_path):
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

    ensure_export_dir()
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(row)

    return len(rows)


def write_version_metadata(query, row_count):
    timestamp = datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat()
    hash_value = compute_file_hash(EXPORT_CSV)
    version_info = {
        'table': EXPORT_TABLE,
        'query': query,
        'rows': row_count,
        'timestamp': timestamp,
        'hash': hash_value,
    }

    with open(VERSION_FILE, 'w', encoding='utf-8') as vfile:
        vfile.write(f"{timestamp}\n")
        vfile.write(f"hash:{hash_value}\n")
        vfile.write(f"rows:{row_count}\n")

    with open(METADATA_FILE, 'w', encoding='utf-8') as mfile:
        json.dump(version_info, mfile, indent=2)

    print(f'✅ Export version metadata written: {VERSION_FILE}')
    print(f'✅ Metadata JSON written: {METADATA_FILE}')


def export_csv_to_sqlite(csv_path, sqlite_path, table_name):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f'CSV file not found: {csv_path}')

    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()

        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader, None)
            if not headers:
                raise ValueError('CSV has no header row')

            cols = [f'"{c}" TEXT' for c in headers]
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            cur.execute(f'CREATE TABLE "{table_name}" ({", ".join(cols)})')

            placeholders = ', '.join('?' for _ in headers)
            insert_sql = f'INSERT INTO "{table_name}" ({", ".join([f"\"{h}\"" for h in headers])}) VALUES ({placeholders})'
            cur.executemany(insert_sql, reader)

        conn.commit()

    row_count = sum(1 for _ in open(csv_path, 'r', encoding='utf-8')) - 1
    print(f'✅ SQLite export complete: {sqlite_path} ({row_count} rows)')
    return row_count


def main():
    print(f'System: {platform.architecture()[0]} Python {platform.python_version()}')
    print('-' * 40)

    conn = connect_to_busy()

    if conn:
        print('✅ SUCCESS! Connected to BusyWin database.')

        try:
            row_count = export_query_to_csv(conn, EXPORT_QUERY, EXPORT_CSV)
            print(f'✅ Export complete: {EXPORT_CSV} ({row_count} rows)')
            write_version_metadata(EXPORT_QUERY, row_count)

            sqlite_path = os.path.join(EXPORT_DIR, f'{EXPORT_TABLE}.db')
            export_csv_to_sqlite(EXPORT_CSV, sqlite_path, EXPORT_TABLE)

            print('\nNext step: commit/export files to GitHub in', EXPORT_DIR)
            print('Example:')
            print('  cd d:\\busyConnector')
            print('  git add export/*')
            print('  git commit -m "Exported StockInfo data for sync"')
            print('  git push')
            print('\nMobile app should check version in', VERSION_FILE, 'or', METADATA_FILE)
            print('Or consume SQLite file at', sqlite_path)
        except Exception as e:
            print(f'Export Error: {e}')
        finally:
            conn.close()
    else:
        print('\n--- TROUBLESHOOTING ---')
        print('1. Close BusyWin: If the software is open, it may lock the database.')
        print('2. 32-bit vs 64-bit: BusyWin 12.2 is very old. It likely requires 32-bit Python.')
        print('3. Install 64-bit Engine: If you must use 64-bit Python, download AccessDatabaseEngine_X64.exe.')


if __name__ == '__main__':
    main()
