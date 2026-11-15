# db-migrate

SQLite 迁移骨架。当前是整表读进内存再写目标库。

## 已知缺口

- 一次 fetchall，8000 行的 big_log 会全部进内存
- BLOB 与 NULL 不做区分处理
- 时间与布尔不归一
- 不校验一致性
- 无断点、无重试、无并发

## 运行

```powershell
python gen_samples.py
python -m mig.cli source.db target.db
python -m pytest tests -q
```
