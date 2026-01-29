import psycopg2

CONN_STR = "host=192.168.163.131 port=7456 user=ogadmin password=Mogdb@123 dbname=postgres"


def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS t_split_mixed")
    cur.execute(
        """
        CREATE TABLE t_split_mixed (
          a INT NOT NULL,
          b TEXT NOT NULL,
          c INT NOT NULL,
          pad TEXT,
          CONSTRAINT pk_split_mixed PRIMARY KEY (a, b, c)
        )
        """
    )

    cur.execute(
        """
        INSERT INTO t_split_mixed (a, b, c, pad)
        SELECT a, 'L' || LPAD(b::text, 2, '0'), c, 'x'
        FROM generate_series(1, 100) a
        CROSS JOIN generate_series(1, 10) b
        CROSS JOIN generate_series(1, 100) c
        """
    )

    cur.close()
    conn.close()
    print("seeded 100,000 rows into t_split_mixed (pk a:int, b:text, c:int)")


if __name__ == "__main__":
    main()
