import argparse
import sys

import cx_Oracle
import psycopg2

from slice_sql import (
    generate_slice_sql,
    get_pk_columns_pg,
    get_pk_columns_ora,
)


def connect(dbtype: str, conn_str: str):
    if dbtype == "ora":
        return cx_Oracle.connect(conn_str)
    if dbtype == "pg":
        return psycopg2.connect(conn_str)
    raise ValueError("dbtype must be pg or ora")


def wrap_count(dbtype: str, sql: str) -> str:
    if dbtype == "pg":
        return f"SELECT COUNT(*) FROM ({sql}) AS t"
    return f"SELECT COUNT(*) FROM ({sql})"


def verify_table(dbtype: str, conn_str: str, table: str, slices: int):
    conn = connect(dbtype, conn_str)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = cur.fetchone()[0]

    sqls = [s.rstrip(";\n \t") for s in generate_slice_sql(conn, dbtype, table, slices)]

    counts = []
    for sql in sqls:
        cur.execute(wrap_count(dbtype, sql))
        counts.append(cur.fetchone()[0])

    # PK columns
    if dbtype == "pg":
        pk_cols = get_pk_columns_pg(conn, table)
    else:
        pk_cols = get_pk_columns_ora(conn, table)
    pk_list = ",".join(pk_cols)

    pk_selects = [sql.replace("SELECT *", f"SELECT {pk_list}") for sql in sqls]

    # overlap
    if dbtype == "pg":
        overlap_sql = "SELECT COUNT(*) FROM (" + " UNION ALL ".join(pk_selects) + f") AS t GROUP BY {pk_list} HAVING COUNT(*)>1"
        union_sql = "SELECT COUNT(*) FROM (" + " UNION ".join(pk_selects) + ") AS t"
    else:
        overlap_sql = "SELECT COUNT(*) FROM (" + " UNION ALL ".join(pk_selects) + f") GROUP BY {pk_list} HAVING COUNT(*)>1"
        union_sql = "SELECT COUNT(*) FROM (" + " UNION ".join(pk_selects) + ")"

    cur.execute(overlap_sql)
    overlap_rows = cur.fetchall()
    overlap_cnt = sum(r[0] for r in overlap_rows) if overlap_rows else 0

    cur.execute(union_sql)
    union_cnt = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        "table": table,
        "total": total,
        "slice_counts": counts,
        "sum_slices": sum(counts),
        "union_cnt": union_cnt,
        "overlap_cnt": overlap_cnt,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dbtype", choices=["pg", "ora"], required=True)
    ap.add_argument("--conn", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--slices", type=int, default=4)
    args = ap.parse_args()

    res = verify_table(args.dbtype, args.conn, args.table, args.slices)
    print(res)


if __name__ == "__main__":
    main()
