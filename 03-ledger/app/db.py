"""Small Postgres helper (same as in 02-job-queue): a connection pool, `tx()`
for one transaction, and `query()` for reads.

Each Store can live in its own Postgres schema (namespace), which is how the
tests get a clean, isolated ledger each without creating databases.
"""
from __future__ import annotations

import zlib

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class Store:
    def __init__(self, dsn: str, schema_sql: str, schema: str = "public", max_size: int = 20):
        self.schema = schema
        self.schema_sql = schema_sql
        with psycopg.connect(dsn, autocommit=True) as con:
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        self.pool = ConnectionPool(
            dsn, min_size=1, max_size=max_size, open=True,
            kwargs={"row_factory": dict_row, "autocommit": True, "options": f"-c search_path={schema}"},
        )
        self.create_tables()

    def create_tables(self) -> None:
        # Several processes may start at once and all run CREATE ... IF NOT
        # EXISTS; an advisory lock makes them take turns.
        def create(con):
            con.execute("SELECT pg_advisory_xact_lock(%s)", (zlib.crc32(self.schema.encode()),))
            con.execute(self.schema_sql)
        self.tx(create)

    def tx(self, fn):
        """Run fn(con) inside ONE transaction: everything commits, or nothing does."""
        with self.pool.connection() as con:
            with con.transaction():
                return fn(con)

    def query(self, sql: str, args: tuple = ()) -> list[dict]:
        with self.pool.connection() as con:
            return con.execute(sql, args or None).fetchall()

    def close(self) -> None:
        self.pool.close()
