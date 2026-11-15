# -*- coding: utf-8 -*-
import os
import sqlite3
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mig.core import list_tables, migrate


class MigrateTest(unittest.TestCase):
    def test_tables(self):
        conn = sqlite3.connect(os.path.join(ROOT, "source.db"))
        self.assertEqual(sorted(list_tables(conn)), ["big_log", "events", "users"])
        conn.close()

    def test_copy_small(self):
        import tempfile
        import uuid
        dst = os.path.join(tempfile.gettempdir(), "mig_%s.db" % uuid.uuid4().hex)
        stats = migrate(os.path.join(ROOT, "source.db"), dst)
        self.assertEqual(stats["users"], 3)
        self.assertEqual(stats["events"], 20)
        conn = sqlite3.connect(dst)
        n = conn.execute("select count(*) from big_log").fetchone()[0]
        self.assertEqual(n, 8000)
        conn.close()


if __name__ == "__main__":
    unittest.main()
