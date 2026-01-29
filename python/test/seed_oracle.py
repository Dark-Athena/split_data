import cx_Oracle
import datetime

conn = cx_Oracle.connect('system/SysPassword1@192.168.163.227:1527/pdb1')
cur = conn.cursor()

stmts = [
    """
    CREATE TABLE T_SPLIT_TEST (
      DT  DATE        NOT NULL,
      ID  NUMBER(10)  NOT NULL,
      PAD VARCHAR2(50),
      CONSTRAINT PK_T_SPLIT_TEST PRIMARY KEY (DT, ID)
    )
    """,
    """
    CREATE TABLE T_SPLIT_SINGLE (
      ID NUMBER(10) PRIMARY KEY,
      PAD VARCHAR2(50)
    )
    """
]

for s in stmts:
    try:
        cur.execute(s)
    except cx_Oracle.DatabaseError as e:
        code = None
        try:
            code = e.args[0].code
        except Exception:
            pass
        if code == 955:  # ORA-00955 name already used
            pass
        else:
            raise

# populate T_SPLIT_TEST: 3 days, 10 rows per day
today = datetime.datetime.now().date()
cur.execute("DELETE FROM T_SPLIT_TEST")
for d in range(3):
    dt = today - datetime.timedelta(days=d)
    for i in range(1, 11):
        cur.execute(
            "INSERT INTO T_SPLIT_TEST (DT, ID, PAD) VALUES (:1, :2, :3)",
            (dt, i, f"row-{d*100 + i}")
        )

# populate T_SPLIT_SINGLE: 30 rows
cur.execute("DELETE FROM T_SPLIT_SINGLE")
for i in range(1, 31):
    cur.execute("INSERT INTO T_SPLIT_SINGLE (ID, PAD) VALUES (:1, :2)", (i, f"row-{i}"))

conn.commit()
cur.close()
conn.close()
print("done")
