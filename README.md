# 节前采购承诺账本

采购承诺、供应批次与配货的领域服务。系统以**事件溯源 + 版本化重算**实现，
让采购团队在任何时点都说得清：

- **可售量**（available）：批次总量中尚未被任何承诺占用、且未过保质期的部分；
- **锁定量**（reserved）：当前版本已经预占或确认、尚未出库的部分；
- **已履约量**（fulfilled）：已出库事实，任何重算版本都不能回收。

恒等式：`批次总量 = 可售量 + 锁定量 + 已履约量(+ 过期死库存)`。

## 目录

- `domain/contract.json`：实体、状态、事件类型、分配优先级与不变量约定。
- `ledger/`：核心服务
  - `events.py`：领域事件（全局唯一 `event_id`，带时区时间）。
  - `store.py`：SQLite 事件库、重算任务表、版本配货表；`BEGIN IMMEDIATE` 串行化并发写。
  - `engine.py`：事件回放校验、优先级配货、版本化重算（两阶段暂存/发布）、查询视图。
  - `cli.py`：命令行入口。
- `examples/`：最小样例与节前分版本剧本（`examples/holiday/`）。
- `tests/`：端到端测试（22 个用例）。
- `tools/validate_contract.py`：领域资料离线校验。

## 分配规则（共同决定谁先拿到货）

逐层比较，上一层用满后自动降级到下一层：

1. **合同锁量**（`contract.registered`，带合同的需求优先用满锁量额度）；
2. **区域保供底线**（`floor.set`，本区域底线未满时优先于渠道）；
3. **渠道优先级**：直营网点 > 加盟商 > 企业团购；
4. **提交时间**：同层级先到先得；
5. 批次选择 **FEFO**：同品类按到期时间升序；
6. 数量按批次**最小整件量向下取整**，已过保质期批次不参与分配。

上一版本已生效的预占是**既得权益**：新版本先保住既有锁定，只有供应缩量/批次
过期导致总可售量装不下全部权益时，才允许该品类按优先级重新排序并产生降级。

## 版本化重算

预占到期、客户撤单/部分确认、供应缩量、补货到场都会触发重算，生成**新版本号**：

- 历史版本的事件与配货方案原样保留，可沿版本号回溯；
- 已出库（`shipment.dispatched`）是外部事实，新版本绝不修改或回收；
  缩量使总量低于已出库量会被直接拒绝；
- 重算分两事务：**暂存**（派生事件 `published=0` 落盘 + checkpoint）→
  **发布**（推进当前版本指针）。死在发布前，恢复时凭 checkpoint 直接发布。

## 关键保证

- **幂等导入**：相同 `event_id` 重复导入跳过，绝不重复扣量；
- **并发抢库存**：两个操作员抢最后一件，`BEGIN IMMEDIATE` 保证恰好一个成功，
  败者事务整体回滚（40 组 × 4 并发压测零超卖）；
- **断点续算**：`recover()` 接续中断前尚未落定的重算；
- **完整理由链**：单笔承诺可查 提出 → 各版本预占/降级 → 到期释放/确认 → 出库兑现
  的每一步原因。

## 构建与测试

```bash
python3 -m compileall -q .
python3 -m unittest discover -s tests -v
python3 tools/validate_contract.py
```

## 命令行走查（节前剧本）

```bash
python3 -m ledger.cli --db /tmp/holiday.db init
for f in examples/holiday/*.json; do
  python3 -m ledger.cli --db /tmp/holiday.db import "$f"
done
python3 -m ledger.cli --db /tmp/holiday.db lots          # 三本账
python3 -m ledger.cli --db /tmp/holiday.db versions      # 全部配货版本
python3 -m ledger.cli --db /tmp/holiday.db commitment dmt-direct-080   # 理由链
python3 -m ledger.cli --db /tmp/holiday.db tick --as-of 2026-09-24T09:00:00+08:00
python3 -m ledger.cli --db /tmp/holiday.db recover       # 服务恢复后续算
```

## 作为库使用

```python
from ledger import CommitmentLedger

ledger = CommitmentLedger("ledger.db")
ledger.import_events([{...}, {...}])          # 幂等批量导入，自动重算
ledger.claim({"demand_id": "d-1", ...}, event_id="evt-...")  # 即时锁量（抢库存）
ledger.lots()                                  # 可售/锁定/已履约
ledger.commitment("d-1")                        # 状态 + 完整理由链
ledger.recover()                               # 重启后续算
```
