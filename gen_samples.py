# -*- coding: utf-8 -*-
"""生成源库：三张表，含 BLOB、NULL、时间戳、布尔、以及 8000 行的大表。"""
import os
import sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "source.db")


def main():
    if os.path.exists(SRC):
        os.remove(SRC)
    conn = sqlite3.connect(SRC)
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
CREATE TABLE big_log (
  id INTEGER PRIMARY KEY,
  line TEXT
);
""")
    conn.execute("insert into users values (?,?,?,?,?,?)",
                 (1, "张三", b"\x89PNG\x00\x01", "2026-02-01 10:00:00", 1, 98.5))
    conn.execute("insert into users values (?,?,?,?,?,?)",
                 (2, "李四", None, None, 0, None))
    conn.execute("insert into users values (?,?,?,?,?,?)",
                 (3, "王五", b"", "2026-02-02T08:30:00", 1, 0.0))
    for i in range(1, 21):
        conn.execute("insert into events values (?,?,?,?)",
                     (i, i % 3 + 1, "payload%d" % i, "2026-02-%02d 12:00:00" % (i % 28 + 1)))
    conn.executemany("insert into big_log values (?,?)",
                     [(i, "log line %d" % i) for i in range(1, 8001)])
    conn.commit()
    conn.close()
    print("源库生成: %d 字节" % os.path.getsize(SRC))


if __name__ == "__main__":
    main()
