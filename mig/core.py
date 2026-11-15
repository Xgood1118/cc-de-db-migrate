# -*- coding: utf-8 -*-
"""SQLite 分块迁移核心。

能力：
- 按主键 keyset 分页读取（fetchmany 有界，不做整表 fetchall）
- 目标库 _mig_progress 表记录每表断点，重启从上次成功主键续跑，不重复插入
- 单批失败回滚并重试，超过次数后停止该表，其他表继续
- ThreadPoolExecutor 并发迁移多张表，每张表使用各自独立的连接
- BLOB 原样保留（空 BLOB 与 NULL 区分）；TIMESTAMP/布尔归一；REAL 的 NULL 不落成 0.0

只依赖 Python 标准库。源库始终以只读方式打开，不会被改动。
"""
import datetime
import json
import os
import re
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

PROGRESS_TABLE = "_mig_progress"
DEFAULT_BATCH_SIZE = 500
DEFAULT_RETRIES = 2

_PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  table_name TEXT PRIMARY KEY,
  last_pk    TEXT NOT NULL,
  updated_at TEXT NOT NULL
)
""" % PROGRESS_TABLE

_TS_TZ_RE = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})$")
_TS_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
_TS_OUTPUT = "%Y-%m-%d %H:%M:%S"
_TIMESTAMP_NAME_HINTS = frozenset((
    "created_at", "updated_at", "happened_at", "occurred_at", "ts", "time",
))


def open_src(path):
    """以只读方式打开源库，任何写操作都会被 SQLite 直接拒绝。"""
    uri = os.path.abspath(path).replace("\\", "/")
    return sqlite3.connect("file:///%s?mode=ro" % uri, uri=True)


def open_dst(path):
    """打开目标库；autocommit 模式，事务由调用方显式管理。"""
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def list_tables(conn):
    rows = conn.execute(
        "select name from sqlite_master where type='table'").fetchall()
    return [r[0] for r in rows
            if not r[0].startswith("sqlite_") and r[0] != PROGRESS_TABLE]


def table_meta(conn, table):
    """返回 (列名列表, 主键列名列表)。无显式主键时退化为 rowid。"""
    info = conn.execute('PRAGMA table_info("%s")' % table).fetchall()
    cols = [r[1] for r in info]
    pk_cols = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5] > 0]
    if not pk_cols:
        pk_cols = ["rowid"]
        cols = cols + ["rowid"]
    return cols, pk_cols


def column_kinds(conn, table):
    """每列的归一化类别：blob / timestamp / boolean / plain。"""
    info = conn.execute('PRAGMA table_info("%s")' % table).fetchall()
    kinds = {}
    for row in info:
        name, declared = row[1], (row[2] or "").upper()
        if "BLOB" in declared or "BINARY" in declared:
            kind = "blob"
        elif "BOOL" in declared:
            kind = "boolean"
        elif ("TIMESTAMP" in declared or "DATETIME" in declared
              or "DATE" in declared or "TIME" in declared
              or name.lower() in _TIMESTAMP_NAME_HINTS):
            kind = "timestamp"
        else:
            kind = "plain"
        kinds[name] = kind
    return kinds


def normalize_timestamp(value):
    """把 'YYYY-MM-DD HH:MM:SS' / ISO 'T' 分隔 / 带时区后缀的文本归一为
    'YYYY-MM-DD HH:MM:SS'；无法解析时原样返回，绝不丢数据。"""
    if not isinstance(value, str):
        return value
    text = _TS_TZ_RE.sub("", value.strip()).replace("T", " ")
    for fmt in _TS_FORMATS:
        try:
            return datetime.datetime.strptime(text, fmt).strftime(_TS_OUTPUT)
        except ValueError:
            continue
    return value


def normalize_bool(value):
    """布尔统一在目标端落成 INTEGER 0/1；NULL 保持 NULL。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return 1 if value else 0
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "t", "yes"):
            return 1
        if low in ("false", "0", "f", "no"):
            return 0
    return value


def normalize_value(value, kind):
    """按列类别归一单个值。"""
    if value is None:
        return None  # REAL/任何类型的 NULL 都保持 NULL，不会变成 0.0/""
    if kind == "blob":
        # bytes 原样透传：b"" 仍是空 BLOB，与 None 严格区分
        return value
    if kind == "boolean":
        return normalize_bool(value)
    if kind == "timestamp":
        return normalize_timestamp(value)
    return value


def _read_progress(dst, table):
    row = dst.execute(
        "select last_pk from %s where table_name=?" % PROGRESS_TABLE,
        (table,)).fetchone()
    if row is None:
        return None
    return json.loads(row[0])


def _write_progress(dst, table, last_pk):
    dst.execute(
        "INSERT INTO %s(table_name, last_pk, updated_at) VALUES(?,?,?)\n"
        "ON CONFLICT(table_name) DO UPDATE SET last_pk=excluded.last_pk,\n"
        "  updated_at=excluded.updated_at" % PROGRESS_TABLE,
        (table, json.dumps(last_pk),
         datetime.datetime.now().strftime(_TS_OUTPUT)))


def copy_table(src, dst, table, batch_size=DEFAULT_BATCH_SIZE,
               retries=DEFAULT_RETRIES, failure_hook=None):
    """分块拷贝单张表，返回该表统计 dict。

    签名与位置参数 (src, dst, table) 与骨架保持一致。
    - 按主键 keyset 分页，每批最多 batch_size 行，fetchmany 有界读取
    - 依据 _mig_progress 断点续跑，已落地的主键不会重复插入
    - 一批写入失败则整批回滚并重试 retries 次；仍失败则停止该表，
      已成功批次与断点保留
    failure_hook(table, batch_index, attempt, rows) 可在测试中注入失败。
    """
    cols, pk_cols = table_meta(src, table)
    kinds = column_kinds(src, table)
    pk_indexes = [cols.index(pk) for pk in pk_cols]
    single_pk = len(pk_cols) == 1

    expected_rows = src.execute(
        'SELECT count(*) FROM "%s"' % table).fetchone()[0]

    # 断点：优先取进度表；进度缺失但目标已有数据时，从目标最大主键兜底续跑
    last_pk = _read_progress(dst, table)
    if last_pk is None and single_pk:
        max_pk = dst.execute(
            'SELECT max("%s") FROM "%s"' % (pk_cols[0], table)).fetchone()[0]
        if max_pk is not None:
            last_pk = [max_pk]

    if last_pk is not None:
        if single_pk:
            skipped = dst.execute(
                'SELECT count(*) FROM "%s" WHERE "%s" <= ?'
                % (table, pk_cols[0]), (last_pk[0],)).fetchone()[0]
        else:
            placeholders = ",".join("?" * len(last_pk))
            skipped = dst.execute(
                'SELECT count(*) FROM "%s" WHERE (%s) <= (%s)'
                % (table, ",".join('"%s"' % c for c in pk_cols), placeholders),
                last_pk).fetchone()[0]
    else:
        skipped = 0

    quoted_pk = ",".join('"%s"' % c for c in pk_cols)
    quoted_all = ",".join('"%s"' % c for c in cols)
    insert_cols = [c for c in cols if c != "rowid"]
    insert_sql = 'INSERT INTO "%s" (%s) VALUES (%s)' % (
        table, ",".join('"%s"' % c for c in insert_cols),
        ",".join("?" * len(insert_cols)))

    failed_batches = 0
    rows_copied = 0
    batch_index = 0
    status = "completed"
    error = None

    def build_query(cursor_pk):
        """基于当前断点构造 keyset 分页查询与参数。"""
        sql = 'SELECT %s FROM "%s"' % (quoted_all, table)
        query_params = []
        if cursor_pk is not None:
            if single_pk:
                sql += ' WHERE "%s" > ?' % pk_cols[0]
            else:
                sql += " WHERE (%s) > (%s)" % (
                    quoted_pk, ",".join("?" * len(cursor_pk)))
            query_params.extend(cursor_pk)
        sql += " ORDER BY %s LIMIT ?" % quoted_pk
        query_params.append(batch_size)
        return sql, query_params

    while True:
        sql, query_params = build_query(last_pk)
        cursor = src.execute(sql, query_params)
        rows = cursor.fetchmany(batch_size)  # 有界读取，绝不整表 fetchall
        cursor.close()
        if not rows:
            break

        norm_rows = []
        for raw in rows:
            norm_rows.append(tuple(
                normalize_value(v, kinds.get(cols[i], "plain"))
                for i, v in enumerate(raw) if cols[i] != "rowid"))
        # 断点必须是该批最后一行的主键（rows 已按主键排序）
        batch_last_pk = [rows[-1][i] for i in pk_indexes]

        batch_committed = False
        for attempt in range(retries + 1):
            try:
                if failure_hook is not None:
                    failure_hook(table, batch_index, attempt, rows)
                dst.execute("BEGIN")
                dst.executemany(insert_sql, norm_rows)
                _write_progress(dst, table, batch_last_pk)
                dst.commit()
                batch_committed = True
                break
            except Exception as exc:  # 整批回滚；重试前断点不变，重查同一批
                dst.rollback()
                failed_batches += 1
                error = exc
        if not batch_committed:
            status = "stopped"
            break

        last_pk = batch_last_pk  # 仅提交成功后推进断点
        rows_copied += len(rows)
        batch_index += 1

    actual_rows = dst.execute(
        'SELECT count(*) FROM "%s"' % table).fetchone()[0]
    return {
        "table": table,
        "status": status,
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
        "rows_copied": rows_copied,
        "skipped_rows": skipped,
        "failed_batches": failed_batches,
        "batches": batch_index,
        "error": None if status == "completed" else str(error),
    }


def _ensure_target_schema(src_path, dst_path):
    """在目标库创建源库的表结构与断点表（已存在则跳过）。"""
    src = open_src(src_path)
    try:
        schema = src.execute(
            "select name, sql from sqlite_master "
            "where type='table' and sql is not null "
            "and name not like 'sqlite_%'").fetchall()
    finally:
        src.close()
    dst = open_dst(dst_path)
    try:
        dst.execute("PRAGMA journal_mode=WAL")
        existing = {r[0] for r in dst.execute(
            "select name from sqlite_master where type='table'").fetchall()}
        for name, sql in schema:
            if name not in existing:
                dst.execute(sql)
        dst.execute(_PROGRESS_DDL)
    finally:
        dst.close()


def _copy_one_table(src_path, dst_path, table, kwargs):
    # 每张表独立的源/目标连接，互不干扰
    src = open_src(src_path)
    dst = open_dst(dst_path)
    try:
        return copy_table(src, dst, table, **kwargs)
    finally:
        dst.close()
        src.close()


class MigrateStats(dict):
    """兼容骨架的返回值：stats[table] 仍给出该表行数；
    完整统计通过 .tables / .to_dict() 获取。"""

    def __init__(self, table_stats):
        super().__init__()
        self.tables = table_stats

    def __getitem__(self, key):
        return self.tables[key]["rows_copied"] + self.tables[key]["skipped_rows"]

    def __iter__(self):
        return iter(self.tables)

    def __len__(self):
        return len(self.tables)

    def __contains__(self, key):
        return key in self.tables

    def items(self):
        return [(t, s["rows_copied"] + s["skipped_rows"])
                for t, s in self.tables.items()]

    def to_dict(self):
        return {"tables": self.tables}


def migrate(src_path, dst_path, batch_size=DEFAULT_BATCH_SIZE,
            max_workers=None, retries=DEFAULT_RETRIES, failure_hook=None):
    """并发迁移源库全部表到目标库，返回 MigrateStats。

    每张表在独立线程、独立连接中迁移；单表失败停止不影响其他表。
    """
    _ensure_target_schema(src_path, dst_path)

    probe = open_src(src_path)
    try:
        tables = list_tables(probe)
    finally:
        probe.close()

    kwargs = {"batch_size": batch_size, "retries": retries,
              "failure_hook": failure_hook}
    workers = max_workers or min(8, max(1, len(tables)))
    table_stats = {}
    lock = threading.Lock()

    def worker(table):
        stats = _copy_one_table(src_path, dst_path, table, kwargs)
        with lock:
            table_stats[table] = stats

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, tables))

    return MigrateStats(table_stats)
