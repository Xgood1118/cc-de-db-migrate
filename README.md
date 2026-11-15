# db-migrate

SQLite 安全迁移工具：分块迁移大表、断点续跑、类型归一、失败重试与逐表一致性校验。
**只用 Python 标准库**，源库始终以只读（`mode=ro`）方式打开，不会被改动。

## 能力

- **分块迁移**：按主键 keyset 分页，每批默认 500 行，`fetchmany` 有界读取，不做整表
  `fetchall`，内存占用只随批大小增长。
- **断点续跑**：每批在目标库 `_mig_progress` 表内与数据同一事务记录最后成功主键；
  重启后从该主键继续，已落地的行计入 `skipped_rows`，绝不重复插入。
- **并发**：`ThreadPoolExecutor` 并发迁移多张表，每张表使用各自独立的源/目标连接，
  互不干扰；单表失败停止不影响其他表。
- **类型与语义**：
  - BLOB 原样迁移（`bytes` 透传），空 BLOB（`b""`）与 NULL 严格区分；
  - TIMESTAMP 文本与 ISO（`T` 分隔、带 `Z`/时区后缀）统一归一为
    `YYYY-MM-DD HH:MM:SS`，无法解析时原样保留；
  - BOOLEAN 的 `0/1` 与 `"true"/"false"` 在目标端统一落成 INTEGER `0/1`；
  - REAL 的 NULL 始终保持 NULL，不会变成 `0.0`。
- **失败恢复**：单批写入失败整批回滚并重试 2 次（共 3 次尝试）；仍失败则停止该表、
  保留已成功批次与断点，其他表继续。
- **一致性校验**：逐表比对行数、主键集合（缺失/多余，给出具体主键）、每列 SHA-256
  校验和；每个值带 1 字节类型标签参与哈希，NULL / 空 BLOB / 空串 / 0 / 0.0 互不混淆。
  任一表不一致时 CLI 以退出码 `1` 结束。
- **JSON 统计**：每张表输出预期行数、实际行数、跳过行数、失败批次数与校验结果。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `mig/core.py` | 只读连接、表/主键元数据、类型归一、`copy_table` 分块重试、`migrate` 并发调度与断点 |
| `mig/verify.py` | `verify_table` / `verify_all`：行数、主键集合、每列校验和与差异定位 |
| `mig/cli.py` | 命令行入口，迁移 + 校验 + JSON 报告 + 退出码 |
| `tests/` | 分块、断点、BLOB/NULL、类型归一、校验检出、重试恢复、并发等测试 |

`migrate(src_path, dst_path, ...)` 与 `copy_table(src, dst, table, ...)` 的签名与骨架
保持兼容；`migrate` 返回的 `MigrateStats` 仍支持 `stats[表名]` 取行数，完整统计见
`stats.tables` / `stats.to_dict()`。

## 运行

```powershell
python gen_samples.py
python -m mig.cli source.db target.db
python -m pytest tests -q
```

退出码：`0` 全部表校验一致；`1` 存在校验不一致或表迁移中途停止；`2` 参数错误。

## 实测输出

`python -m mig.cli source.db target.db`（stderr 摘要）：

```text
[OK] users: 3 行，列校验和全部一致
[OK] events: 20 行，列校验和全部一致
[OK] big_log: 8000 行，列校验和全部一致
```

stdout 的完整 JSON 报告：

```json
{
  "tables": {
    "users": {
      "table": "users",
      "status": "completed",
      "expected_rows": 3,
      "actual_rows": 3,
      "rows_copied": 3,
      "skipped_rows": 0,
      "failed_batches": 0,
      "batches": 1,
      "error": null
    },
    "events": {
      "table": "events",
      "status": "completed",
      "expected_rows": 20,
      "actual_rows": 20,
      "rows_copied": 20,
      "skipped_rows": 0,
      "failed_batches": 0,
      "batches": 1,
      "error": null
    },
    "big_log": {
      "table": "big_log",
      "status": "completed",
      "expected_rows": 8000,
      "actual_rows": 8000,
      "rows_copied": 8000,
      "skipped_rows": 0,
      "failed_batches": 0,
      "batches": 16,
      "error": null
    }
  },
  "verification": {
    "ok": true,
    "tables": {
      "users": {
        "table": "users",
        "ok": true,
        "source_rows": 3,
        "destination_rows": 3,
        "row_count_match": true,
        "missing_pks": [],
        "extra_pks": [],
        "missing_pk_count": 0,
        "extra_pk_count": 0,
        "checksum": {
          "id": {
            "source": "21f165d997a74c9d77d158f02883b351c7af3311e18d186238744ff0a4aefd19",
            "destination": "21f165d997a74c9d77d158f02883b351c7af3311e18d186238744ff0a4aefd19",
            "match": true
          },
          "name": {
            "source": "a8a336316b09ef22fbfd0b5c74ba97a39c97a3b1634d6388ce76ec3aa4d0b8c7",
            "destination": "a8a336316b09ef22fbfd0b5c74ba97a39c97a3b1634d6388ce76ec3aa4d0b8c7",
            "match": true
          },
          "avatar": {
            "source": "dc7b8920ddcfa586f358fa599dcbcb8d70c9e639ae04ed6d0747186f2549840a",
            "destination": "dc7b8920ddcfa586f358fa599dcbcb8d70c9e639ae04ed6d0747186f2549840a",
            "match": true
          },
          "created_at": {
            "source": "b4166cf8f369dee8e2895b2960da3c5221f4ec315b35a0975d3f019e8296a693",
            "destination": "b4166cf8f369dee8e2895b2960da3c5221f4ec315b35a0975d3f019e8296a693",
            "match": true
          },
          "active": {
            "source": "5b711f79012886e25c021bfb00b3d5dbaa79f3d52890b8d2ba7aab334ac95121",
            "destination": "5b711f79012886e25c021bfb00b3d5dbaa79f3d52890b8d2ba7aab334ac95121",
            "match": true
          },
          "score": {
            "source": "2f592324e053903ebdc7d22f95fd3f51f54f7111287dff8667af7bf5ec5d72c3",
            "destination": "2f592324e053903ebdc7d22f95fd3f51f54f7111287dff8667af7bf5ec5d72c3",
            "match": true
          }
        }
      },
      "events": {
        "table": "events",
        "ok": true,
        "source_rows": 20,
        "destination_rows": 20,
        "row_count_match": true,
        "missing_pks": [],
        "extra_pks": [],
        "missing_pk_count": 0,
        "extra_pk_count": 0,
        "checksum": {
          "id": {
            "source": "3a9a8bbc40624928877d6b0104344721d798d86cda69cb4b4973b3fd6a837c0d",
            "destination": "3a9a8bbc40624928877d6b0104344721d798d86cda69cb4b4973b3fd6a837c0d",
            "match": true
          },
          "user_id": {
            "source": "cec51fc1def1d47b8d178d69bf8fe68cc389e603b494010a58e59374b8e7558d",
            "destination": "cec51fc1def1d47b8d178d69bf8fe68cc389e603b494010a58e59374b8e7558d",
            "match": true
          },
          "payload": {
            "source": "a8bcc5762cc1bdb35c38ca8a844a98772e5456021a0623bcbf888a304f180a26",
            "destination": "a8bcc5762cc1bdb35c38ca8a844a98772e5456021a0623bcbf888a304f180a26",
            "match": true
          },
          "happened_at": {
            "source": "cce96dc019e08ecbcf7afd4130a10b68420d85e144e91782e809e0af06562c0d",
            "destination": "cce96dc019e08ecbcf7afd4130a10b68420d85e144e91782e809e0af06562c0d",
            "match": true
          }
        }
      },
      "big_log": {
        "table": "big_log",
        "ok": true,
        "source_rows": 8000,
        "destination_rows": 8000,
        "row_count_match": true,
        "missing_pks": [],
        "extra_pks": [],
        "missing_pk_count": 0,
        "extra_pk_count": 0,
        "checksum": {
          "id": {
            "source": "e3541d417207952f3d666c31c20fe91edf69f75912ab69e8f014dcbcae084a7c",
            "destination": "e3541d417207952f3d666c31c20fe91edf69f75912ab69e8f014dcbcae084a7c",
            "match": true
          },
          "line": {
            "source": "2baa82fc2eadb8619877a6bda6261969a9074c90a5eb43c10af083eadf6a832d",
            "destination": "2baa82fc2eadb8619877a6bda6261969a9074c90a5eb43c10af083eadf6a832d",
            "match": true
          }
        }
      }
    }
  }
}
```

再次运行同一条命令时，三张表全部从断点续跑：`rows_copied` 为 0，
`skipped_rows` 分别为 3 / 20 / 8000，目标表行数与校验和保持一致。

## 测试

```text
$ python -m pytest tests -q
............                                                             [100%]
12 passed
```

覆盖：分块有界读取（禁止大表 `fetchall`）、断点续跑不重复插入、瞬时失败重试、
持续失败停止该表后续跑补齐、BLOB/空 BLOB/NULL 区分、REAL NULL 保持、时间戳与布尔
归一、校验对内容篡改/缺失主键/多余主键/行数差异的检出及 CLI 退出码、多表并发、
源库字节级只读不变。
