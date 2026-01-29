import cx_Oracle

conn = cx_Oracle.connect('system/SysPassword1@192.168.163.227:1527/pdb1')
cur = conn.cursor()

# AND-only slice SQLs (no OR/UNION), each already sargable for PK index
slice_sqls = [
    "SELECT /*+ INDEX(T_SPLIT_TEST PK_T_SPLIT_TEST) */ * FROM T_SPLIT_TEST WHERE DT = DATE '2026-01-26' AND ID >= 1 AND ID < 9",
    "SELECT /*+ INDEX(T_SPLIT_TEST PK_T_SPLIT_TEST) */ * FROM T_SPLIT_TEST WHERE DT = DATE '2026-01-26' AND ID >= 9",
    "SELECT /*+ INDEX(T_SPLIT_TEST PK_T_SPLIT_TEST) */ * FROM T_SPLIT_TEST WHERE DT > DATE '2026-01-26' AND DT < DATE '2026-01-27'",
    "SELECT /*+ INDEX(T_SPLIT_TEST PK_T_SPLIT_TEST) */ * FROM T_SPLIT_TEST WHERE DT = DATE '2026-01-27' AND ID < 7",
    "SELECT /*+ INDEX(T_SPLIT_TEST PK_T_SPLIT_TEST) */ * FROM T_SPLIT_TEST WHERE DT = DATE '2026-01-27' AND ID >= 7",
    "SELECT /*+ INDEX(T_SPLIT_TEST PK_T_SPLIT_TEST) */ * FROM T_SPLIT_TEST WHERE DT > DATE '2026-01-27' AND DT < DATE '2026-01-28'",
    "SELECT /*+ INDEX(T_SPLIT_TEST PK_T_SPLIT_TEST) */ * FROM T_SPLIT_TEST WHERE DT = DATE '2026-01-28' AND ID < 5",
    "SELECT /*+ INDEX(T_SPLIT_TEST PK_T_SPLIT_TEST) */ * FROM T_SPLIT_TEST WHERE DT = DATE '2026-01-28' AND ID >= 5 AND ID <= 10",
]

for idx, sql in enumerate(slice_sqls, 1):
    cur.execute("BEGIN EXECUTE IMMEDIATE 'EXPLAIN PLAN FOR " + sql.replace("'", "''") + "'; END;")
    cur.execute("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())")
    print(f"\n--- Plan for slice {idx} ---")
    for row in cur:
        print(row[0])

cur.close(); conn.close()
