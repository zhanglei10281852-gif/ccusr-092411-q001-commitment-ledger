# 节前采购承诺账本

采购承诺、供应批次与配货事件的领域资料。仓库保存的是项目启动所需的领域合同与示例数据，完整业务服务尚未建立。

- `domain/contract.json`：实体、状态、事件类型和时间约定。
- `examples/events.json`：一段按发生时间排列的示例事件。
- `tools/validate_contract.py`：使用 Python 标准库和 SQLite 内存表检查资料一致性。

## 构建

```bash
python3 -m compileall -q .
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 资料校验

```bash
python3 tools/validate_contract.py
```

所有命令均在项目根目录执行，不需要启动额外服务。
