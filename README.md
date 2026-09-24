# 节前采购承诺账本

采购承诺、供应批次与配货的完整系统：只追加的事件账本 + 版本化分配引擎。
团队在任何时点都能说清一笔货的**可售量、锁定量、已确认待出量、已履约量**，
并能追溯任意承诺从提出、降级到兑现的完整理由。

## 核心规则

- **事件溯源**：供应登记/缩量、需求提交/撤单、预占/确认/降级/取消、出库全部落为只追加事件。
  `event_id` 主键去重，同一份事件文件重复导入不会重复扣量。
- **版本化分配**：每次供应缩量、补货到场、客户撤单、预占到期都追加一个 `allocation.versioned`
  新版本；**已出库是事实，任何版本不可倒回**（缩量低于已出库量直接拒绝），已确认承诺同样受保护。
- **谁先拿到货**（共同决定）：
  1. 合同锁量 `contract` > 区域保供底线 `regional_floor` > 直营/加盟/团购普通渠道；
  2. 普通渠道之间按需求日 `needed_at` 先后，再按提交时间；
  3. 批次按 FEFO 近保质期优先，且保质期必须覆盖需求日（含 `min_shelf_days`）；
  4. 数量按批次最小整件量 `case_size` 向下取整，不拆整件。
- **预占到期**：预占带 `expires_at`，到期不可确认；到期扫描或下一次重算释放货源，
  需求退出分配池，需重新提交才再排队。
- **并发安全**：所有写事务 `BEGIN IMMEDIATE` 串行化，确认支持 `expected_version` 乐观锁，
  两个人抢最后一份库存只有一个成功。
- **可恢复重算**：重算作业持久化为 `pending/running/done/failed`，服务重启后接管中断作业；
  作业产生的事件使用确定性 ID，重放不重复扣量。

## 目录

- `allocation/`：核心服务（标准库实现，SQLite 单文件存储）
  - `model.py`：事件类型、承诺状态、渠道、理由码
  - `service.py`：账本、分配引擎、并发控制、恢复作业
- `domain/contract.json`：实体、状态、事件类型与分配/并发/恢复策略
- `examples/events.json`：示例事件；`examples/festival_demo.py`：端到端可运行演示
- `tools/validate_contract.py`：领域资料离线校验
- `tests/`：23 个测试，覆盖全部业务约束与并发/恢复场景

## 快速上手

```python
from datetime import datetime, timedelta, timezone
from allocation import Ledger, DemandKind

tz = timezone(timedelta(hours=8))
ledger = Ledger("ledger.db")

ledger.register_supply("lot-pear-01", "pear", 100, case_size=5,
                       occurred_at=datetime(2026, 9, 21, tzinfo=tz),
                       expiry_date="2026-10-05")
ledger.submit_demand("d-080", "pear", DemandKind.CONTRACT.value, 60,
                     "store-080", datetime(2026, 9, 27, tzinfo=tz))

ledger.availability("pear")
# {available, reserved, confirmed_open, fulfilled, expired_holds, by_lot, ...}

cid = ledger.get_demand_status("d-080")["commitments"][-1]["commitment_id"]
ledger.confirm_commitment(cid)          # 可带 expected_version 做乐观锁
ledger.dispatch(cid, [("lot-pear-01", 60)])
ledger.get_commitment(cid)["history"]   # 提出→降级→确认→兑现的完整理由链
```

运行演示：

```bash
python3 examples/festival_demo.py
```

## 常用命令

| 场景 | API |
| --- | --- |
| 登记批次 / 缩量 / 补货 | `register_supply` / `change_supply_qty` |
| 提交 / 撤销需求 | `submit_demand` / `cancel_demand` |
| 确认承诺（乐观锁） | `confirm_commitment(cid, expected_version=v)` |
| 出库（事实冻结） | `dispatch(cid, [(lot_id, qty), ...])` |
| 预占到期扫描 | `expire_holds()` |
| 事件导入（幂等） | `append_events(events)` |
| 可售/锁定/履约 | `availability(sku)` |
| 承诺完整理由链 | `get_commitment(cid)` |
| 版本流水 | `list_versions(sku)` |
| 崩溃恢复 | `enqueue_recalc` / `recover_interrupted` |

## 构建与测试

```bash
python3 -m compileall -q .
python3 -m unittest discover -s tests -v
python3 tools/validate_contract.py
```

所有命令均在项目根目录执行，仅依赖 Python 3.11+ 标准库。
