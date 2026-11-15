# -*- coding: utf-8 -*-
"""迁移后一致性校验。

逐表比对：
1. 行数
2. 主键集合（指出缺失/多余的具体主键）
3. 每列内容的 SHA-256 校验和；BLOB 与 NULL 都参与计算且通过类型标签区分
4. 内容不一致时定位到第一个不同的具体主键
"""
import hashlib

from .core import (column_kinds, list_tables, normalize_value, open_dst,
                   open_src, table_meta)

# 每个值带 1 字节类型标签，NULL / 空 BLOB / 空串 / 0 / 0.0 互不混淆
_TAG_NULL = b"N"
_TAG_BLOB = b"B"
_TAG_INT = b"I"
_TAG_FLOAT = b"F"
_TAG_TEXT = b"T"


def canonical(value):
    """把单个值编码成可比较、可哈希的字节串。"""
    if value is None:
        return _TAG_NULL
    if isinstance(value, bool):
        value = int(value)
    if isinstance(value, bytes):
        return _TAG_BLOB + len(value).to_bytes(8, "big") + value
    if isinstance(value, int):
        return _TAG_INT + str(value).encode("utf-8")
    if isinstance(value, float):
        return _TAG_FLOAT + repr(value).encode("utf-8")
    text = str(value).encode("utf-8")
    return _TAG_TEXT + len(text).to_bytes(8, "big") + text


def _ordered_rows(conn, table, cols, pk_cols):
    order = ",".join('"%s"' % c for c in pk_cols)
    sql = 'SELECT %s FROM "%s" ORDER BY %s' % (
        ",".join('"%s"' % c for c in cols), table, order)
    cur = conn.execute(sql)
    while True:
        batch = cur.fetchmany(500)
        if not batch:
            break
        for row in batch:
            yield row
    cur.close()


def verify_table(src_conn, dst_conn, table):
    """校验单张表，返回校验结果 dict（ok=True 表示完全一致）。"""
    cols, pk_cols = table_meta(src_conn, table)
    kinds = column_kinds(src_conn, table)
    pk_indexes = [cols.index(pk) for pk in pk_cols]
    value_cols = [c for c in cols if c != "rowid"]

    src_count = src_conn.execute(
        'SELECT count(*) FROM "%s"' % table).fetchone()[0]
    dst_count = dst_conn.execute(
        'SELECT count(*) FROM "%s"' % table).fetchone()[0]

    src_hashes, dst_hashes = {}, {}
    src_col_sum = {c: hashlib.sha256() for c in value_cols}
    dst_col_sum = {c: hashlib.sha256() for c in value_cols}

    def consume(conn, store, col_sum):
        kind_for = {c: kinds.get(c, "plain") for c in value_cols}
        for row in _ordered_rows(conn, table, cols, pk_cols):
            pk = tuple(row[i] for i in pk_indexes)
            per_row = {}
            for idx, name in enumerate(cols):
                if name == "rowid":
                    continue
                normed = normalize_value(row[idx], kind_for[name])
                enc = canonical(normed)
                col_sum[name].update(enc)
                per_row[name] = hashlib.sha256(enc).digest()
            store[pk] = per_row

    consume(src_conn, src_hashes, src_col_sum)
    consume(dst_conn, dst_hashes, dst_col_sum)

    src_pks = set(src_hashes)
    dst_pks = set(dst_hashes)
    missing = sorted(src_pks - dst_pks)
    extra = sorted(dst_pks - src_pks)

    checksum = {
        c: {"source": src_col_sum[c].hexdigest(),
            "destination": dst_col_sum[c].hexdigest(),
            "match": src_col_sum[c].digest() == dst_col_sum[c].digest()}
        for c in value_cols
    }

    first_mismatch_pk = None
    mismatched_columns = []
    for pk in sorted(src_pks & dst_pks):
        diff_cols = [c for c in value_cols
                     if src_hashes[pk][c] != dst_hashes[pk][c]]
        if diff_cols:
            first_mismatch_pk = list(pk)
            mismatched_columns = diff_cols
            break

    ok = (src_count == dst_count and not missing and not extra
          and first_mismatch_pk is None
          and all(v["match"] for v in checksum.values()))

    result = {
        "table": table,
        "ok": ok,
        "source_rows": src_count,
        "destination_rows": dst_count,
        "row_count_match": src_count == dst_count,
        "missing_pks": [list(pk) for pk in missing[:20]],
        "extra_pks": [list(pk) for pk in extra[:20]],
        "missing_pk_count": len(missing),
        "extra_pk_count": len(extra),
        "checksum": checksum,
    }
    if first_mismatch_pk is not None:
        result["first_mismatch_pk"] = first_mismatch_pk
        result["mismatched_columns"] = mismatched_columns
    return result


def verify_all(src_path, dst_path, tables=None):
    """校验所有表（或指定表）。返回 (全部是否一致, 逐表结果 dict)。"""
    src = open_src(src_path)
    dst = open_dst(dst_path)
    try:
        if tables is None:
            tables = list_tables(src)
        results = {}
        all_ok = True
        for table in tables:
            result = verify_table(src, dst, table)
            results[table] = result
            all_ok = all_ok and result["ok"]
        return all_ok, results
    finally:
        dst.close()
        src.close()
