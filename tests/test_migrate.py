# -*- coding: utf-8 -*-
"""迁移工具测试：分块读取、断点续跑、BLOB/NULL、类型归一、校验检出、并发。"""
import hashlib
import os
import sqlite3

from mig.cli import main
from mig.core import (PROGRESS_TABLE, copy_table, list_tables, migrate,
                      normalize_bool, normalize_timestamp, open_dst, open_src)
from mig.verify import canonical, verify_all, verify_table

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_SRC = os.path.join(ROOT, "source.db")


def make_source(path, big_rows=1200):
    """构造带 BLOB/NULL/时间戳/布尔/REAL 的三表源库。"""
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.executescript("""
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      avatar BLOB,
      created_at TIMESTAMP,
      active BOOLEAN,
      score REAL
    );
    CREATE TABLE events (
      id INTEGER PRIMARY KEY,
      user_id INTEGER,
      payload TEXT,
      happened_at TEXT
    );
    CREATE TABLE big_t (
      id INTEGER PRIMARY KEY,
      line TEXT
    );
    """)
    conn.execute("INSERT INTO users VALUES (?,?,?,?,?,?)",
                 (1, "a", b"\x89PNG\x00\x01", "2026-02-01 10:00:00", 1, 98.5))
    conn.execute("INSERT INTO users VALUES (?,?,?,?,?,?)",
                 (2, "b", None, None, 0, None))
    conn.execute("INSERT INTO users VALUES (?,?,?,?,?,?)",
                 (3, "c", b"", "2026-02-02T08:30:00", "true", 0.0))
    conn.execute("INSERT INTO users VALUES (?,?,?,?,?,?)",
                 (4, "d", b"x", "2026-03-03T09:00:00Z", "false", None))
    for i in range(1, 21):
        conn.execute("INSERT INTO events VALUES (?,?,?,?)",
                     (i, i % 3 + 1, "p%d" % i,
                      "2026-02-%02dT12:00:00" % (i % 28 + 1)))
    conn.executemany("INSERT INTO big_t VALUES (?,?)",
                     [(i, "line %d" % i) for i in range(1, big_rows + 1)])
    conn.commit()
    conn.close()


class GuardedCursor(sqlite3.Cursor):
    """对大表数据查询调用 fetchall 直接报错，证明读取是有界的。"""

    def fetchall(self):
        sql = getattr(self, "_guarded_sql", "") or ""
        if getattr(self, "_forbid_full", False) and "FROM big_t" in sql.upper():
            raise AssertionError("不允许对 big_t 整表 fetchall")
        return super().fetchall()


class GuardedConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        kwargs.pop("cursor_factory", None)
        super().__init__(*args, **kwargs)
        self._forbid_full = False

    def execute(self, sql, *args):
        cur = super(GuardedConnection, self).cursor(GuardedCursor)
        cur.execute(sql, *args)
        cur._guarded_sql = sql
        cur._forbid_full = self._forbid_full
        return cur


def test_chunked_read_is_bounded(tmp_path, monkeypatch):
    """分块：不得对大表 fetchall，批次数按批大小有界。"""
    import mig.core as core

    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    make_source(src, big_rows=1200)

    def guarded_open_src(path):
        uri = os.path.abspath(path).replace("\\", "/")
        return sqlite3.connect(
            "file:///%s?mode=ro" % uri, uri=True,
            factory=GuardedConnection, cursor_factory=GuardedCursor)

    monkeypatch.setattr(core, "open_src", guarded_open_src)
    stats = migrate(src, dst, batch_size=500)

    table_stats = stats.tables["big_t"]
    assert table_stats["status"] == "completed"
    assert table_stats["batches"] == 3  # 500 + 500 + 200
    assert table_stats["rows_copied"] == 1200

    conn = sqlite3.connect(dst)
    assert conn.execute("SELECT count(*) FROM big_t").fetchone()[0] == 1200
    # 边界批：最后一批恰好 200 行
    assert conn.execute("SELECT max(id) FROM big_t").fetchone()[0] == 1200
    conn.close()


def test_resume_skips_already_copied_rows(tmp_path):
    """断点续跑：重跑不重复插入，已成功的行计入 skipped。"""
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    make_source(src)

    stats1 = migrate(src, dst, batch_size=500)
    assert stats1.tables["big_t"]["rows_copied"] == 1200

    stats2 = migrate(src, dst, batch_size=500)
    again = stats2.tables["big_t"]
    assert again["rows_copied"] == 0
    assert again["skipped_rows"] == 1200
    assert again["failed_batches"] == 0

    conn = sqlite3.connect(dst)
    assert conn.execute("SELECT count(*) FROM big_t").fetchone()[0] == 1200
    # 断点表记录了最后成功主键
    last_pk = conn.execute(
        "SELECT last_pk FROM %s WHERE table_name='big_t'" % PROGRESS_TABLE
    ).fetchone()[0]
    assert last_pk == "[1200]"
    conn.close()


def test_batch_retry_then_success(tmp_path):
    """单批首次失败回滚重试，成功后无重复、无残留。"""
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    make_source(src)

    calls = {"n": 0}

    def hook(table, batch_index, attempt, rows):
        # 重试会重读同一批；只让第 0 批的第一次尝试失败一次
        if table == "big_t" and batch_index == 0 and attempt == 0 \
                and calls["n"] == 0:
            calls["n"] += 1
            raise RuntimeError("模拟瞬时写入失败")

    stats = migrate(src, dst, batch_size=500, failure_hook=hook)
    assert calls["n"] == 1
    big = stats.tables["big_t"]
    assert big["failed_batches"] == 1
    assert big["status"] == "completed"
    assert big["rows_copied"] == 1200

    ok, results = verify_all(src, dst)
    assert ok
    conn = sqlite3.connect(dst)
    assert conn.execute(
        "SELECT count(*) FROM big_t WHERE id=1").fetchone()[0] == 1
    conn.close()


def test_persistent_failure_stops_table_and_resumes(tmp_path):
    """重试 2 次（共 3 次尝试）仍失败：停该表、保留批次与断点，
    其他表继续；去掉故障后续跑补齐。"""
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    make_source(src)

    def always_fail(table, batch_index, attempt, rows):
        if table == "big_t" and batch_index == 1:
            raise RuntimeError("模拟持续故障")

    stats = migrate(src, dst, batch_size=500, retries=2,
                    failure_hook=always_fail)
    big = stats.tables["big_t"]
    assert big["status"] == "stopped"
    assert big["failed_batches"] == 3  # 初次 + 2 次重试
    assert big["rows_copied"] == 500   # 第 0 批已成功并保留
    # 其他表不受影响
    assert stats.tables["users"]["status"] == "completed"
    assert stats.tables["events"]["status"] == "completed"

    conn = sqlite3.connect(dst)
    assert conn.execute("SELECT count(*) FROM big_t").fetchone()[0] == 500
    conn.close()

    # 故障解除后重跑，从断点 500 继续，最终一致
    stats2 = migrate(src, dst, batch_size=500)
    big2 = stats2.tables["big_t"]
    assert big2["status"] == "completed"
    assert big2["rows_copied"] == 700
    assert big2["skipped_rows"] == 500

    ok, results = verify_all(src, dst)
    assert ok
    for result in results.values():
        assert result["ok"]


def test_blob_empty_blob_and_null_distinguished(tmp_path):
    """BLOB 原样迁移；空 BLOB 与 NULL 严格区分；REAL 的 NULL 不变 0.0。"""
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    make_source(src)
    migrate(src, dst)

    conn = sqlite3.connect(dst)
    rows = {r[0]: r[1:] for r in conn.execute(
        "SELECT id, typeof(avatar), avatar, typeof(score), score "
        "FROM users ORDER BY id")}
    # 有内容 BLOB
    assert rows[1][0] == "blob" and rows[1][1] == b"\x89PNG\x00\x01"
    # NULL
    assert rows[2][0] == "null" and rows[2][1] is None
    # 空 BLOB：类型仍是 blob，长度 0，不能与 NULL 混淆
    assert rows[3][0] == "blob" and rows[3][1] == b"" and len(rows[3][1]) == 0
    # REAL NULL 保持 NULL；0.0 仍是 REAL
    assert rows[2][2] == "null" and rows[2][3] is None
    assert rows[3][2] == "real" and rows[3][3] == 0.0
    conn.close()

    # 校验和层面 NULL 与空 BLOB 的编码必须不同
    assert canonical(None) != canonical(b"")
    assert canonical(b"") != canonical("")
    assert canonical(0) != canonical(0.0)


def test_type_normalization(tmp_path):
    """TIMESTAMP/ISO 时间归一；'true'/'false' 文本布尔落成 INTEGER 0/1。"""
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    make_source(src)
    migrate(src, dst)

    assert normalize_timestamp("2026-02-02T08:30:00") == "2026-02-02 08:30:00"
    assert normalize_timestamp("2026-03-03T09:00:00Z") == "2026-03-03 09:00:00"
    assert normalize_timestamp("2026-02-01 10:00:00") == "2026-02-01 10:00:00"
    assert normalize_bool("true") == 1 and normalize_bool("false") == 0
    assert normalize_bool(None) is None

    conn = sqlite3.connect(dst)
    # 两种文本时间统一成 'YYYY-MM-DD HH:MM:SS'
    assert conn.execute(
        "SELECT created_at FROM users WHERE id=1").fetchone()[0] == \
        "2026-02-01 10:00:00"
    assert conn.execute(
        "SELECT created_at FROM users WHERE id=3").fetchone()[0] == \
        "2026-02-02 08:30:00"
    assert conn.execute(
        "SELECT created_at FROM users WHERE id=4").fetchone()[0] == \
        "2026-03-03 09:00:00"
    # 文本布尔在目标端统一为 INTEGER
    for r in conn.execute("SELECT typeof(active), active FROM users ORDER BY id"):
        assert r[0] == "integer"
        assert r[1] in (0, 1)
    assert conn.execute(
        "SELECT active FROM users WHERE id=3").fetchone()[0] == 1
    assert conn.execute(
        "SELECT active FROM users WHERE id=4").fetchone()[0] == 0
    # events.happened_at 按列名归入时间列并归一
    assert conn.execute(
        "SELECT happened_at FROM events WHERE id=1").fetchone()[0] == \
        "2026-02-02 12:00:00"
    conn.close()

    ok, _ = verify_all(src, dst)
    assert ok


def test_verify_detects_content_mismatch_and_pk_diff(tmp_path):
    """校验能定位内容不一致的具体主键、缺失/多余主键，并驱动 CLI 退出码 1。"""
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    make_source(src)
    migrate(src, dst)

    # 1) 篡改内容：同一主键、某列值不同
    conn = sqlite3.connect(dst)
    conn.execute("UPDATE users SET name='被篡改' WHERE id=2")
    conn.commit()
    conn.close()

    s = open_src(src)
    d = open_dst(dst)
    result = verify_table(s, d, "users")
    assert not result["ok"]
    assert result["first_mismatch_pk"] == [2]
    assert "name" in result["mismatched_columns"]
    assert result["checksum"]["name"]["match"] is False
    assert result["checksum"]["id"]["match"] is True
    d.close()
    s.close()

    # CLI 对校验不一致必须返回退出码 1
    rc = main([src, dst])
    assert rc == 1

    # 2) 删除/新增主键：行数与主键集合均不一致（删 1 加 2）
    conn = sqlite3.connect(dst)
    conn.execute("DELETE FROM users WHERE id=2")
    conn.executemany("INSERT INTO users VALUES (?,?,?,?,?,?)",
                     [(98, "y", None, None, 1, 1.0),
                      (99, "x", None, None, 1, 1.0)])
    conn.commit()
    conn.close()

    ok, results = verify_all(src, dst)
    assert not ok
    r = results["users"]
    assert r["missing_pk_count"] == 1 and r["missing_pks"] == [[2]]
    assert r["extra_pk_count"] == 2 and r["extra_pks"] == [[98], [99]]
    assert r["row_count_match"] is False
    assert r["source_rows"] == 4 and r["destination_rows"] == 5


def test_concurrent_tables_independent(tmp_path):
    """多表并发迁移：各表独立连接，全部完成且校验一致。"""
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    make_source(src)
    stats = migrate(src, dst, batch_size=300, max_workers=3)
    assert sorted(stats.tables) == ["big_t", "events", "users"]
    assert all(s["status"] == "completed" for s in stats.tables.values())
    assert stats.tables["big_t"]["batches"] == 4  # 300*3 + 300
    ok, _ = verify_all(src, dst)
    assert ok


def test_source_db_never_modified(tmp_path):
    """源库只读打开，迁移与校验前后文件字节完全一致。"""
    dst = str(tmp_path / "dst.db")
    before = hashlib.sha256(open(SAMPLE_SRC, "rb").read()).hexdigest()
    stats = migrate(SAMPLE_SRC, dst)
    assert sorted(list_tables(open_src(SAMPLE_SRC))) == \
        ["big_log", "events", "users"]
    verify_all(SAMPLE_SRC, dst)
    after = hashlib.sha256(open(SAMPLE_SRC, "rb").read()).hexdigest()
    assert before == after
    # 兼容骨架返回值：stats[表名] 仍是行数
    assert stats["users"] == 3
    assert stats["events"] == 20
    assert stats["big_log"] == 8000


def test_copy_table_signature_and_sample_db(tmp_path):
    """copy_table(src, dst, table) 三参签名仍可直接调用，样例库迁移一致。"""
    dst = str(tmp_path / "dst.db")
    src_conn = open_src(SAMPLE_SRC)
    dst_conn = open_dst(dst)
    dst_conn.execute("""
    CREATE TABLE users (
      id INTEGER PRIMARY KEY, name TEXT NOT NULL, avatar BLOB,
      created_at TIMESTAMP, active BOOLEAN, score REAL)
    """)
    dst_conn.execute(
        "CREATE TABLE %s (table_name TEXT PRIMARY KEY, last_pk TEXT NOT NULL, "
        "updated_at TEXT NOT NULL)" % PROGRESS_TABLE)
    stats = copy_table(src_conn, dst_conn, "users", batch_size=2)
    dst_conn.close()
    src_conn.close()
    assert stats["rows_copied"] == 3
    assert stats["status"] == "completed"

    check = sqlite3.connect(dst)
    assert check.execute("SELECT count(*) FROM users").fetchone()[0] == 3
    check.close()
