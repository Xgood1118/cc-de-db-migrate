# -*- coding: utf-8 -*-
"""数据库迁移。

骨架：一次性 select 全表，直接写进目标库；不映射类型，不分块，不校验。
"""
import sqlite3


def open_src(path):
    return sqlite3.connect(path)


def list_tables(conn):
    rows = conn.execute("select name from sqlite_master where type='table'").fetchall()
    return [r[0] for r in rows]


def copy_table(src, dst, table):
    """整表拷贝，返回拷贝行数。骨架不处理类型与冲突。"""
    cur = src.execute("select * from %s" % table)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    dst.executemany("insert into %s (%s) values (%s)" % (
        table, ",".join(cols), ",".join("?" * len(cols))), rows)
    dst.commit()
    return len(rows)


def migrate(src_path, dst_path):
    src = open_src(src_path)
    dst = open_src(dst_path)
    schema = src.execute(
        "select sql from sqlite_master where type='table' and sql is not null").fetchall()
    for (sql,) in schema:
        dst.execute(sql)
    total = {}
    for t in list_tables(src):
        total[t] = copy_table(src, dst, t)
    return total
