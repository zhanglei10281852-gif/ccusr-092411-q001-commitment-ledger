"""领域引擎：事件回放、优先级配货、版本化重算与查询。

三本账（任意时点）::

    批次总量 = 可售量 + 锁定量 + 已履约量

- 已履约量来自 ``shipment.dispatched`` 事实，任何新版本都不能回收；
- 锁定量来自“当前版本”的配货方案；
- 其余为可售量。

每次外部事件导入都会触发一次重算，产生 **新版本号**：旧版本的事件与
配货方案原样保留，采购员可以沿版本号回溯一笔承诺从提出、降级到兑现的
完整理由。
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .events import IMPORTABLE_TYPES, Event, parse_time
from .store import Store

CHANNEL_RANK = {"direct": 2, "franchise": 3, "group": 4}
CHANNEL_NAME = {"direct": "直营网点", "franchise": "加盟商", "group": "企业团购"}


def effective_qty(demand: Demand) -> int:
    """承诺当前应保数量：客户只确认一部分后，未确认部分退回可售池。"""
    if demand.confirmed_qty is not None:
        return max(demand.confirmed_qty, demand.shipped_qty)
    return demand.quantity


class DomainError(ValueError):
    """违反领域规则（如超量出库、缩量低于已出库）。"""


class InventoryShortage(DomainError):
    """即时锁量时已无可售货源。"""


# ---------------------------------------------------------------- 回放模型


@dataclass
class Lot:
    lot_id: str
    sku: str
    total_qty: int
    case_size: int
    expiry: datetime
    arrived_at: datetime


@dataclass
class Demand:
    demand_id: str
    channel: str
    store_id: str
    region: str
    sku: str
    quantity: int
    submitted_at: datetime
    reserve_expire_at: datetime | None
    contract_id: str | None
    cancel_reason: str | None = None
    cancel_at: datetime | None = None
    confirmed_qty: int | None = None
    confirmed_at: datetime | None = None
    shipments: list[dict[str, Any]] = field(default_factory=list)

    @property
    def cancelled(self) -> bool:
        return self.cancel_reason is not None

    @property
    def shipped_qty(self) -> int:
        return sum(int(s["quantity"]) for s in self.shipments)


@dataclass
class Contract:
    contract_id: str
    customer_id: str
    sku: str
    locked_qty: int


@dataclass
class State:
    lots: dict[str, Lot] = field(default_factory=dict)
    demands: dict[str, Demand] = field(default_factory=dict)
    contracts: dict[str, Contract] = field(default_factory=dict)
    floors: dict[tuple[str, str], int] = field(default_factory=dict)
    shipped_by_lot: dict[str, int] = field(default_factory=dict)


def _require_int(payload: dict[str, Any], key: str, *, positive: bool = True) -> int:
    if key not in payload:
        raise ValueError(f"事件缺少 payload.{key}")
    value = payload[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"payload.{key} 必须是整数")
    if positive and value <= 0:
        raise ValueError(f"payload.{key} 必须为正整数")
    return value


def replay(events: list[Event]) -> State:
    """把外部事实事件回放成当前领域状态，并校验所有硬性规则。"""
    state = State()
    for event in events:
        etype, aid, p, at = event.event_type, event.aggregate_id, event.payload, event.occurred_at
        if etype == "supply.registered":
            if aid in state.lots:
                raise DomainError(f"供应批次重复登记：{aid}")
            quantity = _require_int(p, "quantity")
            case_size = _require_int(p, "case_size")
            state.lots[aid] = Lot(
                lot_id=aid,
                sku=str(p.get("sku", "")),
                total_qty=quantity,
                case_size=case_size,
                expiry=parse_time(p["expiry"]),
                arrived_at=at,
            )
            state.shipped_by_lot.setdefault(aid, 0)
        elif etype in ("supply.shrunk", "supply.replenished"):
            lot = state.lots.get(aid)
            if lot is None:
                raise DomainError(f"供应缩量/补货指向未知批次：{aid}")
            delta = _require_int(p, "delta")
            new_qty = lot.total_qty + (delta if etype == "supply.replenished" else -delta)
            if new_qty < state.shipped_by_lot.get(aid, 0):
                raise DomainError(
                    f"批次 {aid} 缩量后总量 {new_qty} 低于已出库量 "
                    f"{state.shipped_by_lot[aid]}，已出库事实不可倒回"
                )
            lot.total_qty = new_qty
        elif etype == "demand.submitted":
            if aid in state.demands:
                raise DomainError(f"门店需求重复提交：{aid}")
            channel = p.get("channel")
            if channel not in CHANNEL_RANK:
                raise DomainError(f"未知渠道：{channel}（应为 direct/franchise/group）")
            quantity = _require_int(p, "quantity")
            contract_id = p.get("contract_id")
            if contract_id is not None and contract_id not in state.contracts:
                raise DomainError(f"需求 {aid} 引用了未登记的合同：{contract_id}")
            expire_at = parse_time(p["reserve_expire_at"]) if p.get("reserve_expire_at") else None
            state.demands[aid] = Demand(
                demand_id=aid,
                channel=channel,
                store_id=str(p.get("store_id", "")),
                region=str(p.get("region", "")),
                sku=str(p.get("sku", "")),
                quantity=quantity,
                submitted_at=at,
                reserve_expire_at=expire_at,
                contract_id=contract_id,
            )
        elif etype == "demand.cancelled":
            demand = state.demands.get(aid)
            if demand is None:
                raise DomainError(f"撤单指向未知需求：{aid}")
            if demand.cancelled:
                raise DomainError(f"需求 {aid} 已撤单，不能再次撤单")
            demand.cancel_reason = str(p.get("reason", "客户撤单"))
            demand.cancel_at = at
        elif etype == "contract.registered":
            if aid in state.contracts:
                raise DomainError(f"合同重复登记：{aid}")
            state.contracts[aid] = Contract(
                contract_id=aid,
                customer_id=str(p.get("customer_id", "")),
                sku=str(p.get("sku", "")),
                locked_qty=_require_int(p, "locked_qty"),
            )
        elif etype == "floor.set":
            region = str(p.get("region", ""))
            sku = str(p.get("sku", ""))
            state.floors[(region, sku)] = _require_int(p, "min_qty")
        elif etype == "commitment.confirmed":
            demand = state.demands.get(aid)
            if demand is None:
                raise DomainError(f"确认指向未知承诺：{aid}")
            if demand.cancelled:
                raise DomainError(f"承诺 {aid} 已撤单，不能确认")
            if demand.confirmed_qty is not None:
                raise DomainError(f"承诺 {aid} 已确认，不能重复确认")
            confirmed_qty = p.get("confirmed_qty", demand.quantity)
            if not isinstance(confirmed_qty, int) or not 0 < confirmed_qty <= demand.quantity:
                raise DomainError(f"承诺 {aid} 确认数量必须在 1..{demand.quantity} 之间")
            demand.confirmed_qty = confirmed_qty
            demand.confirmed_at = at
        elif etype == "shipment.dispatched":
            lot_id = p.get("lot_id")
            lot = state.lots.get(lot_id)
            if lot is None:
                raise DomainError(f"出库指向未知批次：{lot_id}")
            demand = state.demands.get(aid)
            if demand is None:
                raise DomainError(f"出库指向未知承诺：{aid}")
            if demand.cancelled:
                raise DomainError(f"承诺 {aid} 已撤单，不能出库")
            if lot.sku != demand.sku:
                raise DomainError(f"出库批次 {lot_id} 的品类与承诺 {aid} 不一致")
            qty = _require_int(p, "quantity")
            if qty % lot.case_size != 0:
                raise DomainError(f"出库数量 {qty} 不是批次 {lot_id} 整件量 {lot.case_size} 的整数倍")
            if demand.shipped_qty + qty > demand.quantity:
                raise DomainError(
                    f"承诺 {aid} 出库 {demand.shipped_qty + qty} 超过需求量 {demand.quantity}"
                )
            if state.shipped_by_lot[lot_id] + qty > lot.total_qty:
                raise DomainError(
                    f"批次 {lot_id} 累计出库 {state.shipped_by_lot[lot_id] + qty} "
                    f"超过总量 {lot.total_qty}，没有足够库存兑现"
                )
            demand.shipments.append(
                {"shipment_id": event.event_id, "lot_id": lot_id, "quantity": qty, "at": at}
            )
            state.shipped_by_lot[lot_id] += qty
        else:  # pragma: no cover - 引擎只回放外部事实
            raise DomainError(f"重算时遇到不可回放的事件类型：{etype}")
    return state


# ---------------------------------------------------------------- 分配算法


def _floor_remaining(state: State, used: dict[tuple[str, str], int], region: str, sku: str) -> int:
    return max(0, state.floors.get((region, sku), 0) - used.get((region, sku), 0))


def allocate(
    state: State,
    as_of: datetime,
    previous: dict[str, dict[str, int]] | None = None,
) -> dict[str, Any]:
    """按领域优先级在单个版本内完成配货。

    优先级（逐层比较）：合同锁量 → 区域保供底线 → 渠道
    （直营 > 加盟 > 团购）→ 提交时间 → 需求编号；批次内按 FEFO
    （到期越早越先出），数量按批次最小整件量向下取整。

    客户只确认一部分时，承诺量收窄到确认量，多余预占退回可售池。
    """
    previous = previous or {}
    plan: dict[str, dict[str, int]] = {}
    notes: dict[str, list[str]] = {}

    # 1) 预占到期：未确认且超过预占时限的承诺本版本释放。
    expired: list[Demand] = []
    for demand in state.demands.values():
        if (
            not demand.cancelled
            and demand.confirmed_qty is None
            and demand.reserve_expire_at is not None
            and demand.reserve_expire_at <= as_of
        ):
            expired.append(demand)
    expired_ids = {d.demand_id for d in expired}

    # 2) 可售货源：未过保质期批次，FEFO 排序，先扣除已出库事实。
    sellable = sorted(
        (lot for lot in state.lots.values() if lot.expiry > as_of),
        key=lambda lot: (lot.expiry, lot.lot_id),
    )
    available = {
        lot.lot_id: lot.total_qty - state.shipped_by_lot.get(lot.lot_id, 0) for lot in sellable
    }

    contract_used: dict[str, int] = {}
    region_used: dict[tuple[str, str], int] = {}

    def plan_total(demand_id: str) -> int:
        return sum(plan.get(demand_id, {}).values())

    def remaining(d: Demand) -> int:
        return effective_qty(d) - d.shipped_qty - plan_total(d.demand_id)

    active = [
        d
        for d in state.demands.values()
        if not d.cancelled and d.demand_id not in expired_ids
    ]
    candidates = [d for d in active if remaining(d) > 0]

    def key(d: Demand) -> tuple[int, datetime, str]:
        if d.contract_id and contract_used.get(d.contract_id, 0) < state.contracts[d.contract_id].locked_qty:
            tier = 0
        elif _floor_remaining(state, region_used, d.region, d.sku) > 0:
            tier = 1
        else:
            tier = CHANNEL_RANK[d.channel]
        return tier, d.submitted_at, d.demand_id

    def draw(d: Demand, cap: int, layer: str, *, protect: bool = False) -> int:
        """按 FEFO 从批次中为 d 取至多 cap，返回实际取得数量。"""
        taken = 0
        for lot in sellable:
            if lot.sku != d.sku or cap - taken <= 0:
                continue
            raw = min(cap - taken, available[lot.lot_id])
            qty = raw - raw % lot.case_size  # 最小整件量向下取整
            if qty <= 0:
                continue
            plan.setdefault(d.demand_id, {})
            plan[d.demand_id][lot.lot_id] = plan[d.demand_id].get(lot.lot_id, 0) + qty
            available[lot.lot_id] -= qty
            taken += qty
            if d.contract_id is not None:
                contract_used[d.contract_id] = contract_used.get(d.contract_id, 0) + qty
            region_used[(d.region, d.sku)] = region_used.get((d.region, d.sku), 0) + qty
            entry = notes.setdefault(d.demand_id, [])
            entry.append(
                f"{layer}获得 {qty}（批次 {lot.lot_id}，到期 {lot.expiry.date()}，FEFO 优先）"
            )
            if qty < raw:
                entry.append(
                    f"批次 {lot.lot_id} 按最小整件量 {lot.case_size} 向下取整，舍去 {raw - qty}"
                )
        return taken

    # 3a) 权益保护：上一版本已生效的预占先保住。
    #     只有某品类总可售量不足以容纳全部既得权益（供应缩量/批次过期）时，
    #     才允许该品类整体按优先级重新排序并产生降级。
    lot_sku = {lot.lot_id: lot.sku for lot in sellable}
    capacity_by_sku: dict[str, int] = {}
    for lot_id, qty in available.items():
        capacity_by_sku[lot_sku[lot_id]] = capacity_by_sku.get(lot_sku[lot_id], 0) + qty
    entitlements = {
        d.demand_id: min(sum(previous.get(d.demand_id, {}).values()), remaining(d))
        for d in active
    }
    ent_by_sku: dict[str, int] = {}
    for d_id, qty in entitlements.items():
        if qty > 0:
            sku = state.demands[d_id].sku
            ent_by_sku[sku] = ent_by_sku.get(sku, 0) + qty
    protected_skus = {
        sku for sku, cap in capacity_by_sku.items() if ent_by_sku.get(sku, 0) <= cap
    }
    for d in sorted(candidates, key=key):
        held = entitlements.get(d.demand_id, 0)
        if held > 0 and d.sku in protected_skus:
            draw(d, held, "沿用上一版本已锁定量", protect=True)

    # 3b) 剩余需求按优先级取货（锁量/底线用满后自动降级层级）。
    candidates = [d for d in active if remaining(d) > 0]
    while candidates:
        chosen = min(candidates, key=key)
        tier, _, _ = key(chosen)
        cap = remaining(chosen)
        if tier == 0:
            cap = min(
                cap,
                state.contracts[chosen.contract_id].locked_qty  # type: ignore[index]
                - contract_used.get(chosen.contract_id, 0),
            )
            layer = f"合同锁量（{chosen.contract_id}）"
        elif tier == 1:
            cap = min(cap, _floor_remaining(state, region_used, chosen.region, chosen.sku))
            layer = f"区域保供底线（{chosen.region}）"
        else:
            layer = f"渠道优先级（{CHANNEL_NAME[chosen.channel]}）"

        drew = draw(chosen, cap, layer) > 0
        if remaining(chosen) <= 0:
            candidates.remove(chosen)
            continue
        if not drew:
            candidates.remove(chosen)  # 货源或整件余量已尽，本轮不可能再满足
            missing = remaining(chosen)
            if missing > 0:
                notes.setdefault(chosen.demand_id, []).append(f"货源不足，短欠 {missing}")

    # 4) 降级/收窄/释放的解释。
    for demand in state.demands.values():
        if demand.cancelled or demand.demand_id in expired_ids:
            continue
        got = plan_total(demand.demand_id)
        target = effective_qty(demand)
        if demand.confirmed_qty is not None and demand.confirmed_qty < demand.quantity:
            notes.setdefault(demand.demand_id, []).insert(
                0,
                f"客户仅确认 {demand.confirmed_qty}/{demand.quantity}，"
                f"未确认的 {demand.quantity - demand.confirmed_qty} 退回可售池",
            )
        gap = target - demand.shipped_qty - got
        if gap > 0:
            tag = "部分满足，降级为短欠" if got > 0 else "暂无可售货源"
            notes.setdefault(demand.demand_id, []).append(f"{tag} {gap}")

    for demand in expired:
        released = previous.get(demand.demand_id, {})
        reason = f"预占于 {demand.reserve_expire_at} 到期未确认，释放锁定量"
        if released:
            reason += "：" + "、".join(f"{lot} {qty}" for lot, qty in released.items())
        notes[demand.demand_id] = [reason]

    return {"plan": plan, "notes": notes, "expired": [d.demand_id for d in expired]}


# ----------------------------------------------------------------- 账本服务


class CommitmentLedger:
    """采购承诺与配货系统对外门面。"""

    def __init__(self, path: str = ":memory:") -> None:
        self.store = Store(path)

    def close(self) -> None:
        self.store.close()

    # ------------------------------------------------------------ 导入事实

    def import_events(self, events: list[Event | dict[str, Any]]) -> dict[str, Any]:
        """幂等导入一批外部事件，并自动触发重算。

        相同 ``event_id`` 再次导入直接跳过，绝不重复扣量；整批事件在一个
        立即写事务内校验入库，任一非法事件都会让整批回滚。
        """
        normalized: list[Event] = []
        for raw in events:
            normalized.append(raw if isinstance(raw, Event) else Event.from_dict(raw))
        for event in normalized:
            if event.event_type not in IMPORTABLE_TYPES:
                raise DomainError(f"事件类型 {event.event_type} 只能由重算引擎生成")

        conn = self.store.conn
        with self.store.transaction():
            known = self.store.existing_event_ids(conn, [e.event_id for e in normalized])
            fresh: list[Event] = []
            for event in normalized:
                if event.event_id in known:
                    continue
                # 同批内也不允许重复 id。
                if any(e.event_id == event.event_id for e in fresh):
                    raise DomainError(f"导入批次内 event_id 重复：{event.event_id}")
                fresh.append(event)
            # 先用“假设全部入库”的事件回放校验，规则不过则整批回滚。
            committed = self.store.external_events(conn)
            replay(committed + fresh)
            for event in fresh:
                self.store.insert_event(conn, event, published=True)
            if fresh:
                last_seq = conn.execute("SELECT MAX(seq) FROM events").fetchone()[0] or 0
                as_of = max(e.occurred_at for e in fresh)
                self._enqueue(conn, last_seq, as_of, reason="外部事件导入")
        if fresh:
            self.run_pending()
        return {"imported": len(fresh), "duplicated": len(normalized) - len(fresh)}

    def claim(self, demand: dict[str, Any], *, event_id: str, as_of: datetime | str | None = None) -> dict[str, Any]:
        """即时锁量：提交需求并在同一写事务内完成配货。

        两个操作员同时抢最后一份库存时，``BEGIN IMMEDIATE`` 把事务串行化：
        先到者基于旧事实提交，后到者必须在其提交后重新回放，发现可售量
        不足即抛 :class:`InventoryShortage`，成功数至多为一。
        """
        as_of = parse_time(as_of) if as_of else datetime.now().astimezone()
        event = Event(
            event_id=event_id,
            event_type="demand.submitted",
            aggregate_id=demand["demand_id"],
            occurred_at=as_of,
            payload={k: v for k, v in demand.items() if k != "demand_id"},
        )
        if event.event_type not in IMPORTABLE_TYPES:  # pragma: no cover
            raise DomainError("非法事件")
        conn = self.store.conn
        with self.store.transaction():
            if self.store.has_published_event(conn, "demand.submitted", event.aggregate_id):
                raise DomainError(f"门店需求重复提交：{event.aggregate_id}")
            replay(self.store.external_events(conn) + [event])
            self.store.insert_event(conn, event, published=True)
            run_id = self._enqueue(
                conn,
                conn.execute("SELECT MAX(seq) FROM events").fetchone()[0],
                as_of,
                reason="即时锁量",
            )
            row = self.store.get_run(conn, run_id)
            version = self._stage(conn, row)
            # 抢不到货源时在发布前抛错：同一事务整体回滚，需求与暂存都不留。
            allocated = sum(
                self.store.allocations_for_version(conn, version).get(event.aggregate_id, {}).values()
            )
            if allocated <= 0:
                raise InventoryShortage(
                    f"需求 {event.aggregate_id} 未分到任何货源（可售量为 0 或不足一个整件）"
                )
            self._publish(conn, row, version)
            view = self.commitment(event.aggregate_id)
        return view

    # ------------------------------------------------------------ 重算流水线

    def _discard_staging(self, conn, run_id: str) -> None:
        """新事实并入旧任务时，作废它尚未发布的暂存版本（可随时重建）。"""
        row = self.store.get_run(conn, run_id)
        if row is not None and row["version"] is not None:
            conn.execute("DELETE FROM allocations WHERE version=?", (row["version"],))
        conn.execute("DELETE FROM events WHERE run_id=? AND published=0", (run_id,))
        conn.execute(
            "UPDATE runs SET checkpoint='{}', version=NULL, status='pending', "
            "error='', updated_at=? WHERE run_id=?",
            (datetime.now().astimezone().isoformat(), run_id),
        )

    def _enqueue(self, conn, last_seq: int, as_of: datetime, *, reason: str) -> str:
        active = conn.execute(
            "SELECT run_id FROM runs WHERE status IN ('pending','running') ORDER BY rowid LIMIT 1"
        ).fetchone()
        if active is not None:
            run_id = active[0]
            current_seq = conn.execute(
                "SELECT input_seq FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()[0]
            if last_seq > current_seq:
                # 输入变了：旧暂存是过期投影，作废后用新输入重建。
                self._discard_staging(conn, run_id)
            conn.execute(
                "UPDATE runs SET input_seq=?, as_of=?, reason=?, updated_at=? WHERE run_id=?",
                (max(last_seq, current_seq), as_of.isoformat(), reason,
                 datetime.now().astimezone().isoformat(), run_id),
            )
            return run_id
        run_id = f"run-{uuid.uuid4().hex[:12]}"
        self.store.create_run(conn, run_id, as_of, reason)
        conn.execute("UPDATE runs SET input_seq=? WHERE run_id=?", (last_seq, run_id))
        return run_id

    def tick(self, as_of: datetime | str | None = None) -> int:
        """时间推进（无新事件也可触发）：让到期的预占在新版本中释放。

        没有任何预占在该时点到期时直接返回 0，不产生空版本。
        """
        moment = parse_time(as_of) if as_of else datetime.now().astimezone()
        conn = self.store.conn
        with self.store.transaction():
            state = replay(self.store.external_events(conn))
            due = [
                d.demand_id
                for d in state.demands.values()
                if not d.cancelled
                and d.confirmed_qty is None
                and d.reserve_expire_at is not None
                and d.reserve_expire_at <= moment
                and not self.store.has_published_event(conn, "reservation.expired", d.demand_id)
            ]
            if not due:
                return 0
            last_seq = conn.execute(
                "SELECT COALESCE(MAX(seq),0) FROM events WHERE run_id IS NULL"
            ).fetchone()[0]
            run_id = f"run-{uuid.uuid4().hex[:12]}"
            self.store.create_run(conn, run_id, moment, "时间推进：预占到期检查")
            conn.execute("UPDATE runs SET input_seq=? WHERE run_id=?", (last_seq, run_id))
        return self.run_pending()

    def _next_version(self, conn) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(v),0) FROM ("
            "SELECT MAX(version) AS v FROM events UNION ALL "
            "SELECT MAX(version) FROM runs UNION ALL "
            "SELECT MAX(version) FROM allocations)"
        ).fetchone()
        return int(row[0]) + 1

    def _claim_next(self, conn):
        row = conn.execute(
            "SELECT * FROM runs WHERE status IN ('pending','running') ORDER BY rowid LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        self.store.update_run(conn, row["run_id"], status="running", bump_attempt=True)
        return row

    def _stage(self, conn, row) -> int:
        """阶段一：回放 → 到期 → 配货，派生事件以 published=0 暂存并提交。

        暂存结果独立落盘：进程死在发布之前，重启后凭 checkpoint 直接发布，
        不必从头重算。
        """
        checkpoint = json.loads(row["checkpoint"] or "{}")
        if checkpoint.get("phase") == "staged":
            return int(row["version"])

        run_id = row["run_id"]
        input_seq = row["input_seq"]
        as_of = parse_time(row["as_of"])
        state = replay(self.store.external_events(conn, upto_seq=input_seq))
        current_version = int(self.store.get_meta(conn, "current_version", "0") or 0)
        previous = self.store.allocations_for_version(conn, current_version) if current_version else {}
        result = allocate(state, as_of, previous)
        version = self._next_version(conn)

        # 派生：预占到期事件（只在“首次到期”的版本留痕，之后版本不重复）。
        for demand_id in result["expired"]:
            demand = state.demands[demand_id]
            already_expired = conn.execute(
                "SELECT 1 FROM events WHERE event_type='reservation.expired' "
                "AND aggregate_id=? AND published=1 LIMIT 1",
                (demand_id,),
            ).fetchone()
            if already_expired:
                continue
            self.store.insert_event(
                conn,
                Event(
                    event_id=f"derived:v{version}:expiry:{demand_id}",
                    event_type="reservation.expired",
                    aggregate_id=demand_id,
                    occurred_at=as_of,
                    payload={"expire_at": demand.reserve_expire_at.isoformat()},  # type: ignore[union-attr]
                ),
                run_id=run_id,
                published=False,
            )

        # 派生：本版本每笔承诺的配货事件（含完整理由），未满足的也留痕。
        allocation_rows: list[tuple[str, str, int]] = []
        accounted: set[str] = set()
        for demand_id, lots in result["plan"].items():
            demand = state.demands[demand_id]
            allocated = sum(lots.values())
            shipped = demand.shipped_qty
            target = effective_qty(demand)
            shortfall = max(0, target - shipped - allocated)
            for lot_id, qty in lots.items():
                allocation_rows.append((demand_id, lot_id, qty))
            status_hint = "全部预占" if shortfall == 0 else f"部分预占，短欠 {shortfall}"
            self.store.insert_event(
                conn,
                Event(
                    event_id=f"derived:v{version}:reserved:{demand_id}",
                    event_type="commitment.reserved",
                    aggregate_id=demand_id,
                    occurred_at=as_of,
                    payload={
                        "requested": demand.quantity,
                        "target": target,
                        "shipped": shipped,
                        "allocated": allocated,
                        "shortfall": shortfall,
                        "lots": lots,
                        "result": status_hint,
                        "reasons": result["notes"].get(demand_id, []),
                    },
                ),
                run_id=run_id,
                published=False,
            )
            accounted.add(demand_id)

        for demand in state.demands.values():
            if demand.cancelled or demand.demand_id in result["expired"]:
                continue
            if demand.demand_id in accounted:
                continue
            target = effective_qty(demand)
            if demand.shipped_qty >= target:
                continue
            shortfall = target - demand.shipped_qty
            self.store.insert_event(
                conn,
                Event(
                    event_id=f"derived:v{version}:unmet:{demand.demand_id}",
                    event_type="commitment.reserved",
                    aggregate_id=demand.demand_id,
                    occurred_at=as_of,
                    payload={
                        "requested": demand.quantity,
                        "target": target,
                        "shipped": demand.shipped_qty,
                        "allocated": 0,
                        "shortfall": shortfall,
                        "lots": {},
                        "result": "暂无可售货源",
                        "reasons": result["notes"].get(demand.demand_id, ["暂无可售货源"]),
                    },
                ),
                run_id=run_id,
                published=False,
            )

        self.store.replace_allocations(conn, version, allocation_rows)
        self.store.update_run(
            conn,
            run_id,
            status="running",
            version=version,
            input_seq=input_seq,
            checkpoint={"phase": "staged", "version": version},
        )
        return version

    def _publish(self, conn, row, version: int) -> None:
        """阶段二：发布暂存事件并推进当前版本指针。

        已出库事实只存在于外部事件中，发布新版本永远不会修改或回收它们。
        """
        run_id = row["run_id"]
        self.store.publish_run(conn, run_id, version)
        current = int(self.store.get_meta(conn, "current_version", "0") or 0)
        if version > current:
            self.store.set_meta(conn, "current_version", str(version))
        self.store.update_run(conn, run_id, status="done", version=version)
        latest_row = conn.execute(
            "SELECT seq, occurred_at FROM events WHERE run_id IS NULL AND seq>? ORDER BY seq DESC LIMIT 1",
            (row["input_seq"],),
        ).fetchone()
        if latest_row is not None:
            # 发布期间又有新事实到达 → 立刻衔接下一次重算。
            self._enqueue(conn, int(latest_row["seq"]), parse_time(latest_row["occurred_at"]),
                          reason="发布后追补事件")

    #: 崩溃注入点：在“暂存已落盘、发布之前”抛错，用于演练断点续算。
    before_publish: Any = None

    def run_pending(self) -> int:
        """跑完所有未落定的重算。

        每个任务分两个事务：暂存事务提交后即使进程中断，恢复时也能凭
        checkpoint 直接发布；失败现场以独立事务保留为 ``failed``。
        """
        done = 0
        while True:
            with self.store.transaction() as conn:
                claimed = self._claim_next(conn)
            if claimed is None:
                break
            run_id = claimed["run_id"]
            try:
                # 事务 A：暂存（已 staged 则幂等跳过）。
                with self.store.transaction() as conn:
                    row = self.store.get_run(conn, run_id)
                    version = self._stage(conn, row)
                # —— 此处若进程崩溃：暂存与 checkpoint 已落盘 ——
                if self.before_publish is not None:
                    self.before_publish(run_id, version)
                # 事务 B：发布。
                with self.store.transaction() as conn:
                    row = self.store.get_run(conn, run_id)
                    self._publish(conn, row, version)
            except Exception as exc:
                with self.store.transaction() as conn:
                    self.store.update_run(conn, run_id, status="failed", error=repr(exc))
                raise
            done += 1
        return done

    def recover(self) -> int:
        """服务恢复入口：继续中断前尚未落定的重算。

        - 死在暂存之后：跳过回放，直接发布；
        - 死在暂存之前或被标记失败：从头重建该版本。
        """
        conn = self.store.conn
        with self.store.transaction():
            conn.execute(
                "UPDATE runs SET status='pending', error='' WHERE status='failed'"
            )
        return self.run_pending()

    # ------------------------------------------------------------ 查询视图

    def _current_state(self, conn) -> tuple[State, int]:
        # 派生事件是外部事实的投影，回放只吃外部事实。
        state = replay(self.store.external_events(conn))
        version = int(self.store.get_meta(conn, "current_version", "0") or 0)
        return state, version

    def lots(self, sku: str | None = None) -> list[dict[str, Any]]:
        """三本账：每个批次的总量 / 可售 / 锁定 / 已履约。"""
        conn = self.store.conn
        state, version = self._current_state(conn)
        # 以最新外部事实时间作为“当前业务时点”，避免展示与配货口径不一致。
        now_row = conn.execute(
            "SELECT occurred_at FROM events WHERE run_id IS NULL ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        business_now = parse_time(now_row[0]) if now_row else datetime.now().astimezone()
        alloc = self.store.allocations_for_version(conn, version) if version else {}
        views = []
        for lot_id, lot in state.lots.items():
            if sku is not None and lot.sku != sku:
                continue
            fulfilled = state.shipped_by_lot.get(lot_id, 0)
            # 配货方案只针对“尚未出库的剩余需求”，其数量即锁定量。
            reserved = sum(lots.get(lot_id, 0) for lots in alloc.values())
            free = lot.total_qty - fulfilled - reserved
            assert free >= 0, f"批次 {lot_id} 三本账不平"
            expired = lot.expiry <= business_now
            # 过保质期的自由库存不能再卖：计入 dead_stock，不计可售。
            sellable = 0 if expired else free
            assert lot.total_qty == sellable + reserved + fulfilled + (free if expired else 0)
            views.append(
                {
                    "lot_id": lot_id,
                    "sku": lot.sku,
                    "total": lot.total_qty,
                    "available": sellable,
                    "reserved": reserved,
                    "fulfilled": fulfilled,
                    "dead_stock": free if expired else 0,
                    "case_size": lot.case_size,
                    "expiry": lot.expiry.isoformat(),
                    "expired": expired,
                    "version": version,
                }
            )
        return sorted(views, key=lambda v: v["lot_id"])

    def commitment(self, demand_id: str) -> dict[str, Any]:
        """单笔承诺：数量状态 + 从提出到兑现的完整理由链。"""
        conn = self.store.conn
        state, version = self._current_state(conn)
        demand = state.demands.get(demand_id)
        if demand is None:
            raise DomainError(f"未知承诺：{demand_id}")
        alloc = self.store.allocations_for_version(conn, version) if version else {}
        lots = alloc.get(demand_id, {})
        allocated = sum(lots.values())
        target = effective_qty(demand)
        shortfall = max(0, target - demand.shipped_qty - allocated)

        expired_ever = bool(
            conn.execute(
                "SELECT 1 FROM events WHERE event_type='reservation.expired' "
                "AND aggregate_id=? AND published=1 LIMIT 1",
                (demand_id,),
            ).fetchone()
        )

        if demand.cancelled:
            status = "cancelled"
        elif expired_ever and demand.confirmed_qty is None:
            status = "cancelled"
        elif demand.shipped_qty >= target:
            status = "fulfilled"
        elif demand.confirmed_qty is not None:
            status = "confirmed" if allocated + demand.shipped_qty >= target else "degraded"
        elif version == 0:
            status = "proposed"  # 还没有任何版本尝试配货
        elif shortfall > 0:
            status = "degraded"  # 配货后仍有短欠（含一件未得）
        else:
            status = "reserved"

        return {
            "commitment_id": demand_id,
            "status": status,
            "channel": demand.channel,
            "store_id": demand.store_id,
            "region": demand.region,
            "sku": demand.sku,
            "contract_id": demand.contract_id,
            "requested": demand.quantity,
            "target": target,
            "allocated": allocated,
            "allocated_lots": lots,
            "confirmed_qty": demand.confirmed_qty,
            "fulfilled": demand.shipped_qty,
            "shortfall": shortfall,
            "version": version,
            "timeline": self._timeline(conn, demand_id),
        }

    def _timeline(self, conn, demand_id: str) -> list[dict[str, Any]]:
        # 只展示已发布事件：未发布的暂存不构成对采购员的承诺理由。
        events = self.store.load_events(conn, aggregate_id=demand_id, published_only=True)
        chain: list[dict[str, Any]] = []
        for event in events:
            p = event.payload
            if event.event_type == "demand.submitted":
                chain.append(
                    {
                        "at": event.occurred_at.isoformat(),
                        "version": event.version,
                        "kind": "proposed",
                        "title": "需求提出",
                        "detail": (
                            f"{CHANNEL_NAME.get(p.get('channel'), p.get('channel'))} "
                            f"{p.get('store_id')}（{p.get('region')}）需求 {p.get('quantity')}"
                            + (f"，合同 {p['contract_id']}" if p.get("contract_id") else "")
                            + (f"，预占截止 {p['reserve_expire_at']}" if p.get("reserve_expire_at") else "")
                        ),
                    }
                )
            elif event.event_type == "commitment.reserved":
                title = "全部预占" if p.get("shortfall", 0) == 0 and p.get("allocated", 0) > 0 else (
                    "降级预占" if p.get("allocated", 0) > 0 else "暂未配到货源"
                )
                chain.append(
                    {
                        "at": event.occurred_at.isoformat(),
                        "version": event.version,
                        "kind": "reserved",
                        "title": f"v{event.version} {title}",
                        "detail": "；".join(p.get("reasons", []))
                        or f"预占 {p.get('allocated', 0)}，短欠 {p.get('shortfall', 0)}",
                        "lots": p.get("lots", {}),
                    }
                )
            elif event.event_type == "reservation.expired":
                chain.append(
                    {
                        "at": event.occurred_at.isoformat(),
                        "version": event.version,
                        "kind": "expired",
                        "title": f"v{event.version} 预占到期释放",
                        "detail": f"截止 {p.get('expire_at')} 未获确认，锁定量退回可售池",
                    }
                )
            elif event.event_type == "commitment.confirmed":
                chain.append(
                    {
                        "at": event.occurred_at.isoformat(),
                        "version": event.version,
                        "kind": "confirmed",
                        "title": "客户确认",
                        "detail": f"确认数量 {p.get('confirmed_qty', '全部')}",
                    }
                )
            elif event.event_type == "shipment.dispatched":
                chain.append(
                    {
                        "at": event.occurred_at.isoformat(),
                        "version": event.version,
                        "kind": "fulfilled",
                        "title": "出库兑现",
                        "detail": f"批次 {p.get('lot_id')} 出库 {p.get('quantity')}（已出库事实不可倒回）",
                    }
                )
            elif event.event_type == "demand.cancelled":
                chain.append(
                    {
                        "at": event.occurred_at.isoformat(),
                        "version": event.version,
                        "kind": "cancelled",
                        "title": "客户撤单",
                        "detail": str(p.get("reason", "客户撤单")),
                    }
                )
        return chain

    def versions(self) -> list[dict[str, Any]]:
        conn = self.store.conn
        rows = conn.execute(
            "SELECT v.version, r.run_id, r.reason, r.as_of, r.status, r.attempts, "
            "(SELECT COUNT(*) FROM allocations a WHERE a.version=v.version) AS lines "
            "FROM (SELECT version FROM runs WHERE version IS NOT NULL UNION "
            "SELECT version FROM allocations) v "            "LEFT JOIN runs r ON r.version=v.version ORDER BY v.version"
        ).fetchall()
        current = int(self.store.get_meta(conn, "current_version", "0") or 0)
        return [
            {
                "version": row["version"],
                "run_id": row["run_id"],
                "reason": row["reason"],
                "as_of": row["as_of"],
                "status": row["status"],
                "attempts": row["attempts"],
                "allocation_lines": row["lines"],
                "current": row["version"] == current,
            }
            for row in rows
        ]
