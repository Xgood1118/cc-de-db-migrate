# -*- coding: utf-8 -*-
"""命令行入口：python -m mig.cli source.db target.db"""
import sys

from .core import migrate


def main(argv=None):
    argv = argv or sys.argv[1:]
    if len(argv) < 2:
        sys.stderr.write("需要源库与目标库\n")
        return 2
    try:
        stats = migrate(argv[0], argv[1])
    except Exception as exc:
        sys.stderr.write("迁移失败: %s\n" % exc)
        return 1
    for t, n in stats.items():
        print("%s: %d 行" % (t, n))
    return 0


if __name__ == "__main__":
    sys.exit(main())
