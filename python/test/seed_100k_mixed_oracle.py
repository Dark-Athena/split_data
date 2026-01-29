import cx_Oracle

CONN_STR = "system/SysPassword1@192.168.163.227:1527/pdb1"


def main():
    conn = cx_Oracle.connect(CONN_STR)
    cur = conn.cursor()

    cur.execute(
        """
        BEGIN
          EXECUTE IMMEDIATE 'DROP TABLE T_SPLIT_MIXED PURGE';
        EXCEPTION
          WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
        END;
        """
    )

    cur.execute(
        """
        CREATE TABLE T_SPLIT_MIXED (
          A NUMBER(5) NOT NULL,
          B VARCHAR2(8) NOT NULL,
          C NUMBER(5) NOT NULL,
          PAD VARCHAR2(20),
          CONSTRAINT PK_SPLIT_MIXED PRIMARY KEY (A, B, C)
        )
        """
    )

    # 100 * 10 * 100 = 100,000 rows; B is character key part
    cur.execute(
        """
        INSERT /*+ APPEND */ INTO T_SPLIT_MIXED (A, B, C, PAD)
        SELECT a.lv, 'L' || LPAD(b.lv, 2, '0'), c.lv, 'x'
        FROM (SELECT LEVEL lv FROM dual CONNECT BY LEVEL <= 100) a
        CROSS JOIN (SELECT LEVEL lv FROM dual CONNECT BY LEVEL <= 10) b
        CROSS JOIN (SELECT LEVEL lv FROM dual CONNECT BY LEVEL <= 100) c
        """
    )
    conn.commit()

    cur.close()
    conn.close()
    print("seeded 100,000 rows into T_SPLIT_MIXED (pk A:number, B:varchar, C:number)")


if __name__ == "__main__":
    main()
