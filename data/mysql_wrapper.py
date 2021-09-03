import mysql.connector
import pandas as pd
import os
from typing import List

from configuration import dbconfig

TABLES_INIT_SCRIPT = 'sql_scripts/init_tables.sql'
DEFAULT_PORT = 3306


def sql_select_query_builder(table: str,
                             key1=None, value1=None,
                             key2=None, value2=None,
                             key3=None, value3=None,
                             join=False,
                             table2=None,
                             common_key=None) -> str:
    # If common_key contains None, remove from list
    common_key = [k for k in common_key if k is not None] if common_key is not None else None

    def where(k, v, is_next=False):
        out = "and " if is_next else "where "
        if isinstance(v, range):
            out += f"{table}.{k} between {v[0]} and {v[-1]} "
        elif isinstance(v, list):
            out += f"{table}.{k} in ({','.join([str(i) for i in v])}) "
        elif isinstance(v, int):
            out += f"{table}.{k}={v} "
        elif isinstance(v, str):
            out += f"{table}.{k}='{v}' "
        else:
            assert False, "Unhandled value type in SQL query builder (where clause)"
        return out

    query = f"select * from {table} "

    if join and table2 and common_key:
        query += f"inner join {table2} on "
        if isinstance(common_key, str):
            common_key = [common_key]
        query += " and ".join([f"{table}.{k}={table2}.{k}" for k in common_key]) + " "

    first_where_clause = True

    if key1 is not None and value1 is not None:
        query += where(key1, value1, is_next=not first_where_clause)
        first_where_clause = False

    if key2 is not None and value2 is not None:
        query += where(key2, value2, is_next=not first_where_clause)
        first_where_clause = False

    if key3 is not None and value3 is not None:
        query += where(key3, value3, is_next=not first_where_clause)
        first_where_clause = False

    return query


class MySqlDB:

    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = mysql.connector.connect(
            host=dbconfig.MYSQL_HOST,
            user=dbconfig.MYSQL_USER,
            password=dbconfig.MYSQL_PW,
            database=dbconfig.MYSQL_SCHEMA,
            port=DEFAULT_PORT
        )
        self.cursor = self.conn.cursor(buffered=True)
        return self

    def is_connected(self) -> bool:
        return bool(self.conn)

    def insert(self, sql=None, values=None, many=False) -> int:
        inserted = 0
        assert self.is_connected()
        if sql is not None:
            if values is None:
                self.cursor.execute(sql)
            else:
                if many:
                    self.cursor.executemany(sql, values)
                else:
                    self.cursor.execute(sql, values)
        inserted += self.cursor.rowcount
        return inserted

    def read(self, sql):
        assert self.is_connected()
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        return res

    def execute_query(self, sql):
        assert self.is_connected()
        self.cursor.execute(sql)

    def execute_script(self, filepath):
        assert self.is_connected()
        with open(filepath, 'r') as f:
            query = f.read()
        for q in query.split(";")[:-1]:
            self.execute_query(q)

    def create_default_tables(self):
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TABLES_INIT_SCRIPT)
        self.execute_script(script_path)

    def close(self):

        # Commit and close

        if self.is_connected():
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
            self.cursor = None
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
