# -*- coding: utf-8 -*-
"""命令行入口：python -m mig.cli source.db target.db

流程：并发分块迁移 -> 逐表一致性校验 -> 输出 JSON 统计。
任何一张表校验不一致（或迁移中途停止）都以退出码 1 结束，
校验结果中包含具体表名与主键。
"""
import json
import sys

from .core import DEFAULT_BATCH_SIZE, migrate
from .verify import verify_all


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) < 2:
        sys.stderr.write("用法: python -m mig.cli <source.db> <target.db> "
                         "[batch_size]\n")
        return 2
    src_path, dst_path = argv[0], argv[1]
    batch_size = int(argv[2]) if len(argv) > 2 else DEFAULT_BATCH_SIZE

    try:
        stats = migrate(src_path, dst_path, batch_size=batch_size)
        all_ok, verify_results = verify_all(src_path, dst_path)
    except Exception as exc:
        sys.stderr.write("迁移失败: %s\n" % exc)
        return 1

    report = stats.to_dict()
    report["verification"] = {
        "ok": all_ok,
        "tables": verify_results,
    }
    # Windows 控制台默认 GBK，显式走 UTF-8 防止中文表内容/路径报错
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass
    print(json.dumps(report, ensure_ascii=False, indent=2))

    for table, result in verify_results.items():
        if result["ok"]:
            sys.stderr.write("[OK] %s: %d 行，列校验和全部一致\n"
                             % (table, result["destination_rows"]))
        else:
            if result.get("first_mismatch_pk") is not None:
                sys.stderr.write(
                    "[FAIL] %s: 主键 %s 列 %s 内容不一致\n"
                    % (table, result["first_mismatch_pk"],
                       ",".join(result["mismatched_columns"])))
            if result["missing_pk_count"]:
                sys.stderr.write("[FAIL] %s: 缺失主键 %d 个，示例 %s\n"
                                 % (table, result["missing_pk_count"],
                                    result["missing_pks"][:5]))
            if result["extra_pk_count"]:
                sys.stderr.write("[FAIL] %s: 多余主键 %d 个，示例 %s\n"
                                 % (table, result["extra_pk_count"],
                                    result["extra_pks"][:5]))
            if not result["row_count_match"]:
                sys.stderr.write(
                    "[FAIL] %s: 行数不一致 源=%d 目标=%d\n"
                    % (table, result["source_rows"],
                       result["destination_rows"]))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
