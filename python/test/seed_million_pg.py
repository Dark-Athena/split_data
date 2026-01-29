import psycopg2

CONN_STR = "host=192.168.163.131 port=7456 user=ogadmin password=Mogdb@123 dbname=postgres"

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS t_split_million")
    cur.execute(
        """
        CREATE TABLE t_split_million (
          a int not null,
          b int not null,
          c int not null,
          pad text,
          CONSTRAINT pk_split_million PRIMARY KEY (a,b,c)
        )
        """
    )
    cur.execute("TRUNCATE t_split_million")
    cur.execute(
        """
        INSERT INTO t_split_million (a,b,c,pad)
        SELECT a, b, c, 'x'
        FROM generate_series(1,100) a
        CROSS JOIN generate_series(1,100) b
        CROSS JOIN generate_series(1,100) c
        """
    )
    cur.close()
    conn.close()
    print("seeded 1,000,000 rows into t_split_million")

if __name__ == "__main__":
    main()
