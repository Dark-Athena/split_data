#!/usr/bin/env python3
"""
Generate non-overlapping table-slicing SQL statements based on primary keys.
- Works for PostgreSQL and Oracle.
- Loads all primary key rows into memory, computes slice boundaries, and emits SQL.

Usage example:
    python slice_sql.py --dbtype pg --conn "dbname=db user=u password=p host=127.0.0.1" --table my_table --slices 8
"""

import argparse
import math
import datetime
import time
import sys
import logging
from dataclasses import dataclass
from typing import Any, Iterable, List, Sequence, Tuple, Optional

import numpy as np

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None

try:
    import cx_Oracle  # type: ignore
except ImportError:  # pragma: no cover
    cx_Oracle = None


# ---------- DB metadata helpers ----------

def get_pk_columns_pg(conn, table: "TableSpec", logger: Optional[logging.Logger] = None) -> List[str]:
    if logger:
        logger.debug("fetching PK (pg) for %s", table.qualified)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT conkey FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'p'",
            (table.qualified,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return []
        conkey = list(row[0])
        cur.execute(
            "SELECT attnum, attname FROM pg_attribute WHERE attrelid = %s::regclass",
            (table.qualified,),
        )
        m = {attnum: attname for attnum, attname in cur.fetchall()}
    cols = [m[k] for k in conkey if k in m]
    if logger:
        logger.debug("pk columns (pg) %s", cols)
    return cols


def get_pk_columns_ora(conn, table: "TableSpec", logger: Optional[logging.Logger] = None) -> List[str]:
    if logger:
        logger.debug("fetching PK (ora) for %s", table.qualified)
    if table.schema:
        sql = """
        SELECT acc.column_name
        FROM all_constraints ac
        JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
        WHERE ac.owner = :sch AND ac.table_name = :tbl AND ac.constraint_type = 'P'
        ORDER BY acc.position
        """
        params = dict(sch=table.schema.upper(), tbl=table.name.upper())
    else:
        sql = """
        SELECT acc.column_name
        FROM user_constraints ac
        JOIN user_cons_columns acc ON ac.constraint_name = acc.constraint_name
        WHERE ac.table_name = :tbl AND ac.constraint_type = 'P'
        ORDER BY acc.position
        """
        params = dict(tbl=table.name.upper())
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    cols = [r[0] for r in rows]
    if logger:
        logger.debug("pk columns (ora) %s", cols)
    return cols


def fetch_pk_boundaries(conn, table: "TableSpec", pk_cols: Sequence[str], k: int, dbtype: str, logger: Optional[logging.Logger] = None) -> List[Tuple[Any, ...]]:
    """Fetch only boundary rows using ROW_NUMBER to avoid full PK materialization.

    Returns an ordered list of boundary tuples of length m = number of unique boundaries (>=2).
    """
    col_list = ", ".join(pk_cols)
    order_by = ", ".join(pk_cols)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table.qualified}")
        total = cur.fetchone()[0]
        if total == 0:
            return []
        step = max(1, math.ceil(total / k))
        rns = list({1 + i * step for i in range(k)} | {total})
        rns.sort()
        if logger:
            logger.debug("count=%s step=%s rns=%s", total, step, rns)

        if dbtype == "ora":
            placeholders = ",".join(f":{i+1}" for i in range(len(rns)))
            sql = (
                f"SELECT {col_list} FROM ("
                f" SELECT {col_list}, ROW_NUMBER() OVER (ORDER BY {order_by}) rn FROM {table.qualified}"
                f" ) WHERE rn IN ({placeholders}) ORDER BY rn"
            )
            if logger:
                logger.debug("boundary SQL (ora) %s", sql)
            cur.arraysize = len(rns)
            cur.execute(sql, rns)
            rows = cur.fetchall()
        else:  # pg
            placeholders = ",".join(["%s"] * len(rns))
            sql = (
                f"SELECT {col_list} FROM ("
                f" SELECT {col_list}, ROW_NUMBER() OVER (ORDER BY {order_by}) rn FROM {table.qualified}"
                f" ) t WHERE rn IN ({placeholders}) ORDER BY rn"
            )
            if logger:
                logger.debug("boundary SQL (pg) %s", sql)
            cur.execute(sql, tuple(rns))
            rows = cur.fetchall()

    out = [tuple(r) for r in rows]
    if logger:
        logger.debug("fetched %s boundary rows", len(out))
    return out


# ---------- Formatting helpers ----------

def fmt_literal(v: Any) -> str:
    """Format literals for Oracle/PG with ANSI syntax.

    - datetime with non-zero time -> TIMESTAMP literal
    - date or midnight datetime   -> DATE literal
    """
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, datetime.datetime):
        if v.time() == datetime.time(0, 0, 0):
            return f"DATE '{v:%Y-%m-%d}'"
        return f"TIMESTAMP '{v:%Y-%m-%d %H:%M:%S}'"
    if isinstance(v, datetime.date):
        return f"DATE '{v:%Y-%m-%d}'"
    if v is None:
        return "NULL"
    return str(v)


# ---------- Single-column slicing ----------

def make_single_pk_slices_from_bounds(col: str, bounds: List[Any]) -> List[str]:
    """Build single-column ranges from ordered boundary values."""
    dedup: List[Any] = []
    for b in bounds:
        if not dedup or b != dedup[-1]:
            dedup.append(b)
    if len(dedup) == 1:
        dedup.append(dedup[0])

    sqls: List[str] = []
    for i in range(len(dedup) - 1):
        lo, hi = dedup[i], dedup[i + 1]
        if i == len(dedup) - 2:
            sqls.append(f"{col} >= {fmt_literal(lo)} AND {col} <= {fmt_literal(hi)}")
        else:
            sqls.append(f"{col} >= {fmt_literal(lo)} AND {col} < {fmt_literal(hi)}")
    return sqls


# ---------- Lexicographic slicing for composite PK ----------

def chunk_lex(pk_rows: List[Tuple[Any, ...]], k: int) -> List[Tuple[Tuple[Any, ...], Tuple[Any, ...], bool]]:
    """Return half-open slice boundaries using start tuples to avoid gaps.

    For non-last slices: [start_i, start_{i+1})
    For last slice:      [start_last, max_tuple] (right-closed)
    """
    data = sorted(pk_rows)
    n = len(data)
    step = max(1, math.ceil(n / k))
    starts = [data[i] for i in range(0, n, step)]
    slices: List[Tuple[Tuple[Any, ...], Tuple[Any, ...], bool]] = []
    for idx, lo in enumerate(starts):
        if idx + 1 < len(starts):
            hi = starts[idx + 1]
            slices.append((lo, hi, False))
        else:
            hi = data[-1]
            slices.append((lo, hi, True))
    return slices


def _ge_segments(cols: Sequence[str], bounds: Sequence[Any]) -> List[List[str]]:
    """Return list of pure-AND segments representing cols >= bounds (lex)."""
    if len(cols) == 1:
        return [[f"{cols[0]} >= {fmt_literal(bounds[0])}"]]
    head, tail = cols[0], cols[1:]
    head_val = fmt_literal(bounds[0])
    segments: List[List[str]] = []
    # Case 1: head = bound, tail >= tail_bounds
    for sub in _ge_segments(tail, bounds[1:]):
        segments.append([f"{head} = {head_val}"] + sub)
    # Case 2: head > bound (tail free)
    segments.append([f"{head} > {head_val}"])
    return segments


def _le_segments(cols: Sequence[str], bounds: Sequence[Any], inclusive: bool) -> List[List[str]]:
    """Return list of pure-AND segments representing cols <= bounds (lex)."""
    if len(cols) == 1:
        op = "<=" if inclusive else "<"
        return [[f"{cols[0]} {op} {fmt_literal(bounds[0])}"]]
    head, tail = cols[0], cols[1:]
    head_val = fmt_literal(bounds[0])
    segments: List[List[str]] = []
    # Case 1: head < bound (tail free)
    segments.append([f"{head} < {head_val}"])
    # Case 2: head = bound, tail <= tail_bounds
    for sub in _le_segments(tail, bounds[1:], inclusive):
        segments.append([f"{head} = {head_val}"] + sub)
    return segments


def build_composite_slice_wheres(cols: Sequence[str], left: Tuple[Any, ...], right: Tuple[Any, ...], is_last: bool) -> List[str]:
    """Produce multiple pure-AND WHERE strings (no OR/UNION) covering [left, right]."""
    if len(cols) == 1:
        op_hi = "<=" if is_last else "<"
        return [f"{cols[0]} >= {fmt_literal(left[0])} AND {cols[0]} {op_hi} {fmt_literal(right[0])}"]

    if left[0] == right[0]:
        tail_wheres = build_composite_slice_wheres(cols[1:], left[1:], right[1:], is_last)
        return [f"{cols[0]} = {fmt_literal(left[0])} AND {w}" for w in tail_wheres]

    wheres: List[str] = []

    # Lower band: first column fixed at left[0], tail >= left_tail
    for seg in _ge_segments(cols[1:], left[1:]):
        wheres.append(" AND ".join([f"{cols[0]} = {fmt_literal(left[0])}"] + seg))

    # Middle band: first column strictly between
    wheres.append(f"{cols[0]} > {fmt_literal(left[0])} AND {cols[0]} < {fmt_literal(right[0])}")

    # Upper band: first column fixed at right[0], tail <= right_tail (inclusive for last slice)
    for seg in _le_segments(cols[1:], right[1:], inclusive=is_last):
        wheres.append(" AND ".join([f"{cols[0]} = {fmt_literal(right[0])}"] + seg))

    return wheres


def generate_slice_sql(conn, dbtype: str, table: str, k: int, profile: bool = False, logger: Optional[logging.Logger] = None) -> List[str]:
    t0 = time.perf_counter()
    t_spec = parse_table(table)
    log = logger or logging.getLogger("slice_sql")

    if dbtype == "pg":
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is not installed")
        pk_cols = get_pk_columns_pg(conn, t_spec, logger=logger)
    elif dbtype == "ora":
        if cx_Oracle is None:
            raise RuntimeError("cx_Oracle is not installed")
        pk_cols = get_pk_columns_ora(conn, t_spec, logger=logger)
    else:
        raise ValueError("dbtype must be 'pg' or 'ora'")

    if not pk_cols:
        raise RuntimeError("Primary key not found")

    t1 = time.perf_counter()
    boundaries = fetch_pk_boundaries(conn, t_spec, pk_cols, k, dbtype, logger=logger)
    t2 = time.perf_counter()
    log.info("pk_cols=%s boundaries=%d", pk_cols, len(boundaries))
    if not boundaries:
        return []

    sql_list: List[str] = []

    if len(pk_cols) == 1:
        for cond in make_single_pk_slices_from_bounds(pk_cols[0], [b[0] for b in boundaries]):
            sql_list.append(f"SELECT * FROM {t_spec.qualified} WHERE {cond};")
    else:
        bounds = []
        for i in range(len(boundaries) - 1):
            lo = boundaries[i]
            hi = boundaries[i + 1]
            bounds.append((lo, hi, i == len(boundaries) - 2))
        for lo, hi, is_last in bounds:
            for where_expr in build_composite_slice_wheres(pk_cols, lo, hi, is_last=is_last):
                sql_list.append(f"SELECT * FROM {t_spec.qualified} WHERE {where_expr};")

    if logger:
        logger.info("sqls=%s", len(sql_list))

    if profile:
        t3 = time.perf_counter()
        print(
            f"[profile] pk_cols={pk_cols}, fetch={t2 - t1:.3f}s, total={t3 - t0:.3f}s, boundaries={len(boundaries)}, sqls={len(sql_list)}",
            file=sys.stderr,
        )

    return sql_list


# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate PK-based slice SQL statements")
    p.add_argument("--dbtype", choices=["pg", "ora"], required=True, help="Database type: pg or ora")
    p.add_argument("--conn", required=True, help="Connection string passed to driver connect()")
    p.add_argument("--table", required=True, help="Table name (optionally schema-qualified)")
    p.add_argument("--slices", type=int, default=8, help="Number of desired slices")
    p.add_argument("--profile", action="store_true", help="Print profiling info to stderr")
    p.add_argument("--log-level", default="INFO", help="Logging level: DEBUG, INFO, WARNING, ERROR")
    p.add_argument("--log-file", default="slice_sql.log", help="Log file path")
    return p.parse_args()


@dataclass
class TableSpec:
    schema: str
    name: str
    qualified: str


def parse_table(raw: str) -> TableSpec:
    if "." not in raw:
        return TableSpec(schema=None, name=raw, qualified=raw)
    schema, name = raw.split(".", 1)
    return TableSpec(schema=schema, name=name, qualified=f"{schema}.{name}")


def connect(dbtype: str, conn_str: str):
    if dbtype == "pg":
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is not installed")
        return psycopg2.connect(conn_str)
    if dbtype == "ora":
        if cx_Oracle is None:
            raise RuntimeError("cx_Oracle is not installed")
        return cx_Oracle.connect(conn_str)
    raise ValueError("Unsupported dbtype")


def main():
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        filename=args.log_file,
        filemode="a",
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("slice_sql")
    logger.info("start dbtype=%s table=%s slices=%s", args.dbtype, args.table, args.slices)
    with connect(args.dbtype, args.conn) as conn:
        sqls = generate_slice_sql(conn, args.dbtype, args.table, args.slices, profile=args.profile, logger=logger)
        for s in sqls:
            print(s)


if __name__ == "__main__":
    main()
