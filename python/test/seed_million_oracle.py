import cx_Oracle

CONN_STR = "system/SysPassword1@192.168.163.227:1527/pdb1"

def main():
    conn = cx_Oracle.connect(CONN_STR)
    cur = conn.cursor()

    cur.execute(
        """
        BEGIN
          EXECUTE IMMEDIATE 'DROP TABLE T_SPLIT_MILLION PURGE';
        EXCEPTION
          WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
        END;
        """
    )

    cur.execute(
        """
        CREATE TABLE T_SPLIT_MILLION (
          A NUMBER(5) NOT NULL,
          B NUMBER(5) NOT NULL,
          C NUMBER(5) NOT NULL,
          PAD VARCHAR2(20),
          CONSTRAINT PK_SPLIT_MILLION PRIMARY KEY (A, B, C)
        )
        """
    )

    # Insert 1,000,000 rows via cartesian of 1..100 three times
    cur.execute(
        """
        INSERT /*+ APPEND */ INTO T_SPLIT_MILLION (A,B,C,PAD)
        SELECT a.lv, b.lv, c.lv, 'x'
        FROM (SELECT LEVEL lv FROM dual CONNECT BY LEVEL <= 100) a
        CROSS JOIN (SELECT LEVEL lv FROM dual CONNECT BY LEVEL <= 100) b
        CROSS JOIN (SELECT LEVEL lv FROM dual CONNECT BY LEVEL <= 100) c
        """
    )
    conn.commit()
    cur.close()
    conn.close()
    print("seeded 1,000,000 rows into T_SPLIT_MILLION")

if __name__ == "__main__":
    main()
