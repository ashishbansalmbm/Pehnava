import pyodbc
import os
import traceback

MDB_PATH = r'C:\BusyWin 12.2\DATA\COMP0003\db12024.bds'
DB_PASSWORD = 'ILoveMyINDIA'
conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + MDB_PATH + ';PWD=' + DB_PASSWORD + ';'
print('exists', os.path.exists(MDB_PATH))
try:
    conn = pyodbc.connect(conn_str)
    print('connected')
    cur = conn.cursor()
    tables = list(cur.tables(tableType='TABLE'))
    print('tables', [t.table_name for t in tables])
    try:
        cur.execute('SELECT COUNT(*) FROM ItemParamDet')
        print('ItemParamDet count', cur.fetchone()[0])
    except Exception as e:
        print('ItemParamDet count error', e)
    conn.close()
except Exception as e:
    print('connect error:', e)
    traceback.print_exc()

# import platform, os, sys, pyodbc
# print("Python arch", platform.architecture()[0])
# print("Python version", platform.python_version())
# print("OS", platform.system(), platform.machine())
# print("pyodbc drivers:", pyodbc.drivers())
# print("Py is 64-bit?", sys.maxsize > 2**32)