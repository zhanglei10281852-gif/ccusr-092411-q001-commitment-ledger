"""采购承诺与配货核心服务。

存储为单个 SQLite 文件：
- ``events`` 是只追加账本，event_id 主键 + 幂等键双重去重；
- 其余表是在同一事务内随事件更新的物化状态，因此多进程/多线程并发访问安全；
- 所有写事务以 ``BEGIN IMMEDIATE`` 开始，配合承诺级状态条件更新，
  保证两个人抢最后一份库存时只有一个成功。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from .model import CommitmentState, DemandKind, EventType, ReasonCode

# 渠道优先级：数字越小越先拿到货
PRIORITY_RANK = {
    DemandKind.CONTRACT.value: 0,
    DemandKind.REGIONAL_FLOOR.value: 1,
    DemandKind.DIRECT.value: 2,
    DemandKind.FRANCHISEE.value: 2,
    DemandKind.GROUP_BUY.value: 2,
}

DEFAULT_HOLD_TTL_SECONDS = 24 * 3600
SCHEMA_VERSION = 1


class LedgerError(RuntimeError):
    """业务规则被违反。"""


class NotFoundError(LedgerError):
    pass


class ConflictError(LedgerError):
    """并发冲突：承诺状态已被别人改变，或库存已被抢走。"""


class HoldExpiredError(LedgerError):
    """预占已到期，必须重新参与分配。"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(dt: Optional[datetime] = None) -> str:
    return (dt or _now()).isoformat()


def _parse(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        raise ValueError("时间戳必须包含时区：" + ts)
    return dt


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Ledger:
    """采购承诺账本。一个实例对应一个 SQLite 文件，可被多线程共用。"""

    def __init__(self, path: str | Path = ":memory:", hold_ttl_seconds: int = DEFAULT_HOLD_TTL_SECONDS):
        self.path = str(path)
        self.hold_ttl = timedelta(seconds=hold_ttl_seconds)
        self._lock = threading.RLock()
        self._conn = self._connect()
        self._init_schema(self._conn)

    # ------------------------------------------------------------------ 基础设施

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None,
                               check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__ (self, *exc: object) -> None:
        self.close()

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);

            CREATE TABLE IF NOT EXISTS events(
                event_id        TEXT PRIMARY KEY,
                event_type      TEXT NOT NULL,
                aggregate_id    TEXT NOT NULL,
                occurred_at     TEXT NOT NULL,
                payload         TEXT NOT NULL DEFAULT '{}',
                idempotency_key TEXT UNIQUE,
                version         INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events(aggregate_id);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);

            CREATE TABLE IF NOT EXISTS lots(
                lot_id      TEXT PRIMARY KEY,
                sku         TEXT NOT NULL,
                qty         INTEGER NOT NULL,          -- 当前档口确认量（可缩量/补货）
                expiry_date TEXT,
                case_size   INTEGER NOT NULL,
                received_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS demands(
                demand_id       TEXT PRIMARY KEY,
                sku             TEXT NOT NULL,
                kind            TEXT NOT NULL,
                requested_qty   INTEGER NOT NULL,
                store_id        TEXT NOT NULL,
                needed_at       TEXT NOT NULL,
                min_shelf_days  INTEGER NOT NULL DEFAULT 0,
                submitted_at    TEXT NOT NULL,
                active          INTEGER NOT NULL DEFAULT 1,
                cancelled_at    TEXT,
                cancel_reason   TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_demands_sku ON demands(sku, active);

            CREATE TABLE IF NOT EXISTS commitments(
                commitment_id TEXT PRIMARY KEY,
                demand_id     TEXT NOT NULL,
                sku           TEXT NOT NULL,
                state         TEXT NOT NULL,
                requested_qty INTEGER NOT NULL,
                committed_qty INTEGER NOT NULL,
                expires_at    TEXT,
                version       INTEGER NOT NULL,
                created_at    TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_commit_demand ON commitments(demand_id);

            CREATE TABLE IF NOT EXISTS commitment_holds(
                commitment_id TEXT NOT NULL,
                lot_id        TEXT NOT NULL,
                qty           INTEGER NOT NULL,
                shipped_qty   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (commitment_id, lot_id)
            );

            CREATE TABLE IF NOT EXISTS shipments(
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                shipment_id   TEXT NOT NULL,
                event_id      TEXT NOT NULL,
                commitment_id TEXT NOT NULL,
                lot_id        TEXT NOT NULL,
                qty           INTEGER NOT NULL,
                occurred_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sku_meta(
                sku             TEXT PRIMARY KEY,
                current_version INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS recalc_jobs(
                job_id      TEXT PRIMARY KEY,
                sku         TEXT NOT NULL,
                reason      TEXT NOT NULL,
                as_of       TEXT NOT NULL,
                state       TEXT NOT NULL,            -- pending/running/done/failed
                attempts    INTEGER NOT NULL DEFAULT 0,
                claimed_at  TEXT,
                last_error  TEXT,
                created_at  TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )

    # ------------------------------------------------------------------ 事件落账

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        event_type: EventType,
        aggregate_id: str,
        occurred_at: str,
        payload: dict[str, Any],
        event_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        version: Optional[int] = None,
    ) -> str:
        """写入事件并更新物化状态。已存在的 event_id 直接跳过（幂等导入）。"""
        event_id = event_id or _new_id("evt")
        _parse(occurred_at)
        cursor = conn.execute(
            "INSERT OR IGNORE INTO events(event_id, event_type, aggregate_id, occurred_at,"
            " payload, idempotency_key, version) VALUES(?,?,?,?,?,?,?)",
            (
                event_id,
                event_type.value,
                aggregate_id,
                occurred_at,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                idempotency_key,
                version,
            ),
        )
        if cursor.rowcount == 0:
            return event_id  # 同事件再次导入：不重复扣量
        self._apply(conn, event_id, event_type, aggregate_id, occurred_at, payload, version)
        return event_id

    def _apply(
        self,
        conn: sqlite3.Connection,
        event_id: str,
        event_type: EventType,
        aggregate_id: str,
        occurred_at: str,
        p: dict[str, Any],
        version: Optional[int],
    ) -> None:
        """把单条事件折叠进物化状态（与事件插入同一事务）。"""
        if event_type is EventType.SUPPLY_REGISTERED:
            conn.execute(
                "INSERT INTO lots(lot_id, sku, qty, expiry_date, case_size, received_at)"
                " VALUES(?,?,?,?,?,?)",
                (aggregate_id, p["sku"], int(p["qty"]), p.get("expiry_date"),
                 int(p["case_size"]), occurred_at),
            )
        elif event_type is EventType.SUPPLY_QUANTITY_CHANGED:
            conn.execute("UPDATE lots SET qty=? WHERE lot_id=?", (int(p["new_qty"]), aggregate_id))
        elif event_type is EventType.DEMAND_SUBMITTED:
            conn.execute(
                "INSERT INTO demands(demand_id, sku, kind, requested_qty, store_id, needed_at,"
                " min_shelf_days, submitted_at) VALUES(?,?,?,?,?,?,?,?)",
                (aggregate_id, p["sku"], p["kind"], int(p["requested_qty"]), p["store_id"],
                 p["needed_at"], int(p.get("min_shelf_days", 0)), occurred_at),
            )
        elif event_type is EventType.DEMAND_CANCELLED:
            conn.execute(
                "UPDATE demands SET active=0, cancelled_at=?, cancel_reason=? WHERE demand_id=?",
                (occurred_at, p.get("reason"), aggregate_id),
            )
        elif event_type in (EventType.COMMITMENT_RESERVED, EventType.COMMITMENT_DEGRADED):
            state = CommitmentState.RESERVED.value if event_type is EventType.COMMITMENT_RESERVED \
                else CommitmentState.DEGRADED.value
            exists = conn.execute(
                "SELECT 1 FROM commitments WHERE commitment_id=?", (aggregate_id,)
            ).fetchone()
            if exists:
                conn.execute(
                    "UPDATE commitments SET state=?, requested_qty=?, committed_qty=?,"
                    " expires_at=?, version=? WHERE commitment_id=?",
                    (state, int(p["requested_qty"]), int(p["committed_qty"]),
                     p.get("expires_at"), int(p["version"]), aggregate_id),
                )
            else:
                conn.execute(
                    "INSERT INTO commitments(commitment_id, demand_id, sku, state, requested_qty,"
                    " committed_qty, expires_at, version, created_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (aggregate_id, p["demand_id"], p["sku"], state, int(p["requested_qty"]),
                     int(p["committed_qty"]), p.get("expires_at"), int(p["version"]),
                     occurred_at),
                )
            conn.execute("DELETE FROM commitment_holds WHERE commitment_id=?", (aggregate_id,))
            for lot_id, qty in p["holds"].items():
                conn.execute(
                    "INSERT INTO commitment_holds(commitment_id, lot_id, qty) VALUES(?,?,?)",
                    (aggregate_id, lot_id, int(qty)),
                )
        elif event_type is EventType.COMMITMENT_CONFIRMED:
            cursor = conn.execute(
                "UPDATE commitments SET state=? WHERE commitment_id=? AND state IN (?,?)",
                (CommitmentState.CONFIRMED.value, aggregate_id,
                 CommitmentState.RESERVED.value, CommitmentState.DEGRADED.value),
            )
            if cursor.rowcount == 0:
                row = conn.execute(
                    "SELECT state FROM commitments WHERE commitment_id=?", (aggregate_id,)
                ).fetchone()
                raise ConflictError(
                    f"承诺 {aggregate_id} 当前状态为 {row['state'] if row else '不存在'}，无法确认"
                )
        elif event_type is EventType.COMMITMENT_CANCELLED:
            conn.execute(
                "UPDATE commitments SET state=?, committed_qty=0 WHERE commitment_id=?",
                (CommitmentState.CANCELLED.value, aggregate_id),
            )
            conn.execute("DELETE FROM commitment_holds WHERE commitment_id=?", (aggregate_id,))
        elif event_type is EventType.SHIPMENT_DISPATCHED:
            # 事件挂在承诺聚合上，采购员看承诺理由链时能看到每一次兑现；
            # event_id 形如 "<shipment_id>:<lot_id>"，同一出库重复导入不会重复扣量。
            conn.execute(
                "INSERT INTO shipments(shipment_id, event_id, commitment_id, lot_id, qty, occurred_at)"
                " VALUES(?,?,?,?,?,?)",
                (p["shipment_id"], event_id, p["commitment_id"], p["lot_id"],
                 int(p["qty"]), occurred_at),
            )
            conn.execute(
                "UPDATE commitment_holds SET shipped_qty = shipped_qty + ?"
                " WHERE commitment_id=? AND lot_id=?",
                (int(p["qty"]), p["commitment_id"], p["lot_id"]),
            )
            shipped = conn.execute(
                "SELECT COALESCE(SUM(qty),0) AS q FROM shipments WHERE commitment_id=?",
                (p["commitment_id"],),
            ).fetchone()["q"]
            committed = conn.execute(
                "SELECT committed_qty FROM commitments WHERE commitment_id=?",
                (p["commitment_id"],),
            ).fetchone()["committed_qty"]
            if shipped >= committed:
                conn.execute(
                    "UPDATE commitments SET state=? WHERE commitment_id=?",
                    (CommitmentState.FULFILLED.value, p["commitment_id"]),
                )
        elif event_type is EventType.ALLOCATION_VERSIONED:
            conn.execute(
                "INSERT INTO sku_meta(sku, current_version) VALUES(?,?)"
                " ON CONFLICT(sku) DO UPDATE SET current_version=excluded.current_version",
                (aggregate_id, int(p["version"])),
            )

    # ------------------------------------------------------------------ 命令：供应

    def register_supply(
        self,
        lot_id: str,
        sku: str,
        qty: int,
        case_size: int,
        occurred_at: Optional[datetime] = None,
        expiry_date: Optional[str] = None,
        event_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """登记供应批次（档口确认量）。"""
        self._validate_qty(qty, case_size)
        if expiry_date is not None:
            datetime.strptime(expiry_date, "%Y-%m-%d")
        ts = _ts(occurred_at)
        with self._lock, self._conn:
            self._begin_immediate()
            if self._conn.execute("SELECT 1 FROM lots WHERE lot_id=?", (lot_id,)).fetchone():
                raise LedgerError(f"供应批次 {lot_id} 已存在")
            eid = self._insert_event(
                self._conn, EventType.SUPPLY_REGISTERED, lot_id, ts,
                {"sku": sku, "qty": qty, "case_size": case_size, "expiry_date": expiry_date},
                event_id=event_id, idempotency_key=idempotency_key,
            )
            self._plan_and_commit(self._conn, sku, ReasonCode.SUPPLY_REPLENISHED.value, ts)
        return eid

    def change_supply_qty(
        self,
        lot_id: str,
        new_qty: int,
        occurred_at: Optional[datetime] = None,
        reason: str = "档口复核",
        event_id: Optional[str] = None,
    ) -> Optional[int]:
        """供应缩量或补货到场：改批次确认量并立即形成新的分配版本。

        已出库的事实不可倒回——new_qty 不得低于该批已出库量。
        返回新分配版本号（无变化时返回 None）。
        """
        if new_qty < 0:
            raise LedgerError("数量不能为负")
        ts = _ts(occurred_at)
        with self._lock, self._conn:
            self._begin_immediate()
            lot = self._require_lot(lot_id)
            if new_qty == lot["qty"]:
                return None
            shipped = self._fulfilled_by_lot(self._conn, lot_id)
            if new_qty < shipped:
                raise LedgerError(
                    f"批次 {lot_id} 已出库 {shipped}，不能缩到 {new_qty}：已出库事实不可倒回"
                )
            self._insert_event(
                self._conn, EventType.SUPPLY_QUANTITY_CHANGED, lot_id, ts,
                {"new_qty": new_qty, "old_qty": lot["qty"], "reason": reason},
                event_id=event_id,
            )
            code = (ReasonCode.SUPPLY_SHRUNK if new_qty < lot["qty"]
                    else ReasonCode.SUPPLY_REPLENISHED).value
            summary = self._plan_and_commit(self._conn, lot["sku"], code, ts)
        return summary["version"] if summary else None

    # ------------------------------------------------------------------ 命令：需求

    def submit_demand(
        self,
        demand_id: str,
        sku: str,
        kind: str,
        requested_qty: int,
        store_id: str,
        needed_at: datetime,
        occurred_at: Optional[datetime] = None,
        min_shelf_days: int = 0,
        event_id: Optional[str] = None,
    ) -> Optional[int]:
        """提交门店/渠道需求并立即参与分配，返回分配版本号。"""
        if kind not in PRIORITY_RANK:
            raise LedgerError(f"未知需求渠道：{kind}")
        if requested_qty <= 0:
            raise LedgerError("需求量必须为正")
        ts = _ts(occurred_at)
        needed = needed_at if isinstance(needed_at, str) else _ts(needed_at)
        with self._lock, self._conn:
            self._begin_immediate()
            if self._conn.execute("SELECT 1 FROM demands WHERE demand_id=?", (demand_id,)).fetchone():
                raise LedgerError(f"需求 {demand_id} 已存在")
            self._insert_event(
                self._conn, EventType.DEMAND_SUBMITTED, demand_id, ts,
                {"sku": sku, "kind": kind, "requested_qty": requested_qty,
                 "store_id": store_id, "needed_at": needed, "min_shelf_days": min_shelf_days},
                event_id=event_id,
            )
            summary = self._plan_and_commit(
                self._conn, sku, ReasonCode.DEMAND_SUBMITTED.value, ts
            )
        return summary["version"] if summary else None

    def cancel_demand(
        self,
        demand_id: str,
        occurred_at: Optional[datetime] = None,
        reason: str = "customer_cancelled",
    ) -> Optional[int]:
        """客户撤单：释放预占，并让排队需求在新版本中补位。已确认/已履约的承诺不可撤。"""
        ts = _ts(occurred_at)
        with self._lock, self._conn:
            self._begin_immediate()
            demand = self._require_demand(demand_id)
            if not demand["active"]:
                raise LedgerError(f"需求 {demand_id} 已撤销")
            alive = self._conn.execute(
                "SELECT * FROM commitments WHERE demand_id=? AND state IN (?,?,?,?)",
                (demand_id, CommitmentState.RESERVED.value, CommitmentState.DEGRADED.value,
                 CommitmentState.CONFIRMED.value, CommitmentState.FULFILLED.value),
            ).fetchone()
            if alive and alive["state"] in (CommitmentState.CONFIRMED.value,
                                            CommitmentState.FULFILLED.value):
                raise ConflictError(
                    f"需求 {demand_id} 的承诺已{alive['state']}，不能撤单"
                )
            self._insert_event(
                self._conn, EventType.DEMAND_CANCELLED, demand_id, ts, {"reason": reason},
            )
            if alive:
                self._cancel_commitment(
                    self._conn, alive["commitment_id"], ts,
                    ReasonCode.DEMAND_CANCELLED_BY_CUSTOMER.value, demand["sku"],
                    detail="客户撤单，预占释放",
                )
            summary = self._plan_and_commit(
                self._conn, demand["sku"], ReasonCode.REALLOCATION.value, ts
            )
        return summary["version"] if summary else None

    # ------------------------------------------------------------------ 命令：承诺

    def confirm_commitment(
        self,
        commitment_id: str,
        occurred_at: Optional[datetime] = None,
        expected_version: Optional[int] = None,
        buyer: str = "buyer",
    ) -> str:
        """采购员确认承诺。预占到期则拒绝；并发下只有一个确认成功。"""
        ts = _ts(occurred_at)
        with self._lock, self._conn:
            self._begin_immediate()
            row = self._conn.execute(
                "SELECT * FROM commitments WHERE commitment_id=?", (commitment_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"承诺 {commitment_id} 不存在")
            if expected_version is not None and row["version"] != expected_version:
                raise ConflictError(
                    f"承诺版本已变化：你看到的 v{expected_version}，当前 v{row['version']}"
                )
            if row["state"] in (CommitmentState.CONFIRMED.value, CommitmentState.FULFILLED.value):
                raise ConflictError(f"承诺 {commitment_id} 已被确认/履约")
            if row["state"] == CommitmentState.CANCELLED.value:
                raise ConflictError(f"承诺 {commitment_id} 已取消")
            if row["expires_at"] and _parse(ts) >= _parse(row["expires_at"]):
                raise HoldExpiredError(
                    f"承诺 {commitment_id} 的预占已于 {row['expires_at']} 到期"
                )
            eid = self._insert_event(
                self._conn, EventType.COMMITMENT_CONFIRMED, commitment_id, ts,
                {"reason": ReasonCode.CONFIRMED_BY_BUYER.value, "buyer": buyer},
            )
        return eid

    def dispatch(
        self,
        commitment_id: str,
        lines: Iterable[tuple[str, int] | dict[str, int]],
        occurred_at: Optional[datetime] = None,
        shipment_id: Optional[str] = None,
    ) -> str:
        """出库。出库即事实：此后任何重算都不能动走已出库的量。"""
        ts = _ts(occurred_at)
        shipment_id = shipment_id or _new_id("ship")
        normalized = [
            (item if isinstance(item, tuple) else (next(iter(item)), next(iter(item.values()))))
            for item in lines
        ]
        if not normalized:
            raise LedgerError("出库明细不能为空")
        with self._lock, self._conn:
            self._begin_immediate()
            row = self._conn.execute(
                "SELECT * FROM commitments WHERE commitment_id=?", (commitment_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"承诺 {commitment_id} 不存在")
            if row["state"] not in (CommitmentState.CONFIRMED.value, CommitmentState.FULFILLED.value):
                raise ConflictError(f"承诺 {commitment_id} 状态为 {row['state']}，不能出库")
            already_shipped = self._shipped_for(self._conn, commitment_id)
            if already_shipped >= row["committed_qty"]:
                raise ConflictError(f"承诺 {commitment_id} 已全部出库")
            total_out = 0
            for lot_id, qty in normalized:
                if qty <= 0:
                    raise LedgerError("出库数量必须为正")
                hold = self._conn.execute(
                    "SELECT * FROM commitment_holds WHERE commitment_id=? AND lot_id=?",
                    (commitment_id, lot_id),
                ).fetchone()
                if hold is None:
                    raise LedgerError(f"承诺 {commitment_id} 未占用批次 {lot_id}")
                available_on_hold = hold["qty"] - hold["shipped_qty"]
                if qty > available_on_hold:
                    raise ConflictError(
                        f"批次 {lot_id} 该承诺可出库 {available_on_hold}，请求 {qty}"
                    )
                lot = self._require_lot(lot_id)
                already = self._fulfilled_by_lot(self._conn, lot_id)
                free_lot = lot["qty"] - already
                if qty > free_lot:
                    raise ConflictError(
                        f"批次 {lot_id} 实物余量 {free_lot}，不足以出库 {qty}"
                    )
                total_out += qty
                self._insert_event(
                    self._conn, EventType.SHIPMENT_DISPATCHED, commitment_id, ts,
                    {"shipment_id": shipment_id, "commitment_id": commitment_id,
                     "lot_id": lot_id, "qty": qty, "reason": ReasonCode.SHIPPED.value},
                    event_id=f"{shipment_id}:{lot_id}",
                )
            remaining = row["committed_qty"] - already_shipped
            if total_out > remaining:
                raise ConflictError("出库总量超过承诺未履约量")
        return shipment_id

    def expire_holds(self, as_of: Optional[datetime] = None) -> dict[str, int]:
        """主动扫描到期预占，每个相关 SKU 形成一个新版本。"""
        ts = _ts(as_of)
        touched: dict[str, int] = {}
        with self._lock, self._conn:
            self._begin_immediate()
            rows = self._conn.execute(
                "SELECT DISTINCT sku FROM commitments WHERE state IN (?,?) AND expires_at <= ?",
                (CommitmentState.RESERVED.value, CommitmentState.DEGRADED.value, ts),
            ).fetchall()
            for row in rows:
                summary = self._plan_and_commit(
                    self._conn, row["sku"], ReasonCode.RESERVE_EXPIRED.value, ts
                )
                if summary:
                    touched[row["sku"]] = summary["version"]
        return touched

    # ------------------------------------------------------------------ 分配引擎

    def _begin_immediate(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    @staticmethod
    def _fulfilled_by_lot(conn: sqlite3.Connection, lot_id: str) -> int:
        return conn.execute(
            "SELECT COALESCE(SUM(qty),0) AS q FROM shipments WHERE lot_id=?", (lot_id,)
        ).fetchone()["q"]

    @staticmethod
    def _shipped_for(conn: sqlite3.Connection, commitment_id: str) -> int:
        return conn.execute(
            "SELECT COALESCE(SUM(qty),0) AS q FROM shipments WHERE commitment_id=?",
            (commitment_id,),
        ).fetchone()["q"]

    def _next_version(self, conn: sqlite3.Connection, sku: str) -> int:
        row = conn.execute(
            "SELECT current_version FROM sku_meta WHERE sku=?", (sku,)
        ).fetchone()
        return (row["current_version"] if row else 0) + 1

    def _plan_and_commit(
        self,
        conn: sqlite3.Connection,
        sku: str,
        reason: str,
        as_of: str,
        job_id: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """在当前事务内重算某 SKU 的全部承诺，追加一个新版本。

        无任何变化时不产生版本，返回 None。
        """
        plan = self._build_plan(conn, sku, as_of)
        new_version = self._next_version(conn, sku)
        events: list[tuple[EventType, str, dict[str, Any]]] = []

        # 1) 到期预占取消，量回池子
        for cid in plan["expired"]:
            events.append((
                EventType.COMMITMENT_CANCELLED, cid,
                {"reason": ReasonCode.RESERVE_EXPIRED.value, "sku": sku,
                 "version": new_version, "detail": "预占到期未确认，释放给更高优先需求"},
            ))

        # 2) 现存 reserved/degraded 承诺与重算结果对比
        for item in plan["results"]:
            prior = item["prior"]
            qty = item["qty"]
            holds = item["holds"]
            demand = item["demand"]
            if prior is None:
                if qty <= 0:
                    continue
                cid = f"commit-{demand['demand_id']}-v{new_version}"
                expires = _ts(_parse(as_of) + self.hold_ttl)
                events.append((
                    EventType.COMMITMENT_RESERVED, cid,
                    {"demand_id": demand["demand_id"], "sku": sku,
                     "requested_qty": demand["requested_qty"], "committed_qty": qty,
                     "holds": holds, "expires_at": expires, "version": new_version,
                     "reason": ReasonCode.ALLOCATED_INITIAL.value,
                     "priority_rank": PRIORITY_RANK[demand["kind"]],
                     "detail": item["allocation_note"]},
                ))
                continue
            cid = prior["commitment_id"]
            if qty == prior["committed_qty"] and _holds_match(conn, cid, holds):
                continue
            if qty <= 0:
                events.append((
                    EventType.COMMITMENT_CANCELLED, cid,
                    {"reason": reason, "short_reason": item["short_reason"],
                     "sku": sku, "version": new_version, "detail": item["short_detail"]},
                ))
            elif qty < prior["committed_qty"]:
                events.append((
                    EventType.COMMITMENT_DEGRADED, cid,
                    {"demand_id": demand["demand_id"], "sku": sku,
                     "requested_qty": demand["requested_qty"], "committed_qty": qty,
                     "old_qty": prior["committed_qty"], "holds": holds,
                     "expires_at": prior["expires_at"], "version": new_version,
                     "reason": reason, "short_reason": item["short_reason"],
                     "detail": item["short_detail"]},
                ))
            else:
                events.append((
                    EventType.COMMITMENT_RESERVED, cid,
                    {"demand_id": demand["demand_id"], "sku": sku,
                     "requested_qty": demand["requested_qty"], "committed_qty": qty,
                     "old_qty": prior["committed_qty"], "holds": holds,
                     "expires_at": prior["expires_at"], "version": new_version,
                     "reason": (ReasonCode.SUPPLY_REPLENISHED.value
                                if reason == ReasonCode.SUPPLY_REPLENISHED.value
                                else ReasonCode.REALLOCATION.value),
                     "detail": "新版本中补齐预占量"},
                ))

        shortages = [
            {"demand_id": item["demand"]["demand_id"], "kind": item["demand"]["kind"],
             "requested_qty": item["demand"]["requested_qty"],
             "short_qty": item["demand"]["requested_qty"] - item["qty"],
             "reason": item["short_reason"], "detail": item["short_detail"],
             "notes": item["notes"]}
            for item in plan["results"]
            if item["qty"] < item["demand"]["requested_qty"]
        ]

        if not events and not shortages and not plan["confirmed_deficit"]:
            return None

        version_payload = {
            "version": new_version,
            "reason": reason,
            "as_of": as_of,
            "job_id": job_id,
            "allocations": [
                {"commitment_id": self._commitment_id_for(item, new_version),
                 "demand_id": item["demand"]["demand_id"],
                 "kind": item["demand"]["kind"],
                 "requested_qty": item["demand"]["requested_qty"],
                 "committed_qty": item["qty"],
                 "holds": item["holds"]}
                for item in plan["results"] if item["qty"] > 0
            ],
            "shortages": shortages,
            "confirmed_deficit": plan["confirmed_deficit"],
        }
        events.append((EventType.ALLOCATION_VERSIONED, sku, version_payload))

        for idx, (etype, agg_id, payload) in enumerate(events):
            eid: Optional[str] = None
            idem: Optional[str] = None
            if job_id is not None:
                # 确定性事件 ID：作业崩溃重跑不会重复落账
                eid = f"evt-{job_id}-v{new_version}-{idx:03d}"
            self._insert_event(conn, etype, agg_id, as_of, payload, event_id=eid,
                               idempotency_key=idem, version=new_version)
        return version_payload

    @staticmethod
    def _commitment_id_for(item: dict[str, Any], new_version: int) -> str:
        if item["prior"] is not None:
            return item["prior"]["commitment_id"]
        return f"commit-{item['demand']['demand_id']}-v{new_version}"

    def _build_plan(self, conn: sqlite3.Connection, sku: str, as_of: str) -> dict[str, Any]:
        """纯读计算：给定当前状态，算出新版本下每个需求应得的量与批次占用。"""
        expired_ids = {
            r["commitment_id"]
            for r in conn.execute(
                "SELECT commitment_id FROM commitments WHERE sku=? AND state IN (?,?)"
                " AND expires_at <= ?",
                (sku, CommitmentState.RESERVED.value, CommitmentState.DEGRADED.value, as_of),
            )
        }

        # 池子 = 档口确认量 - 已出库 - 受保护的已确认未出库量。
        # 命令一律针对"当前已落账的物理状态"重算，业务时间只参与排序和留痕，
        # 因此并发命令无论获锁先后，最终收敛到同一份按优先级的分配。
        pool: dict[str, int] = {}
        lots: dict[str, sqlite3.Row] = {}
        for lot in conn.execute("SELECT * FROM lots WHERE sku=? ORDER BY lot_id", (sku,)):
            lots[lot["lot_id"]] = lot
            pool[lot["lot_id"]] = lot["qty"] - self._fulfilled_by_lot(conn, lot["lot_id"])

        confirmed_deficit: list[dict[str, Any]] = []
        protected_rows = conn.execute(
            "SELECT h.commitment_id, h.lot_id, h.qty - h.shipped_qty AS open_qty,"
            " c.state FROM commitment_holds h JOIN commitments c ON c.commitment_id=h.commitment_id"
            " WHERE c.sku=? AND c.state=?",
            (sku, CommitmentState.CONFIRMED.value),
        ).fetchall()
        for r in protected_rows:
            open_qty = r["open_qty"]
            if r["lot_id"] not in pool:
                pool.setdefault(r["lot_id"], 0)
            pool[r["lot_id"]] -= open_qty
        for lot_id, free in pool.items():
            if free < 0:
                confirmed_deficit.append(
                    {"lot_id": lot_id, "deficit": -free,
                     "reason": ReasonCode.SUPPLY_SHRUNK.value,
                     "detail": "供应缩量已伤及已确认承诺，需人工追补货源"}
                )
                pool[lot_id] = 0

        # 候选需求：有效需求 + 没有活着的承诺（到期/此前取消的可重新参与）
        alive_states = (CommitmentState.RESERVED.value, CommitmentState.DEGRADED.value,
                        CommitmentState.CONFIRMED.value, CommitmentState.FULFILLED.value)
        candidates: list[dict[str, Any]] = []
        for demand in conn.execute(
            "SELECT * FROM demands WHERE sku=? AND active=1", (sku,)
        ):
            prior = conn.execute(
                "SELECT * FROM commitments WHERE demand_id=? AND state IN (?,?,?,?)"
                " ORDER BY version DESC LIMIT 1",
                (demand["demand_id"], *alive_states),
            ).fetchone()
            if prior is not None and prior["commitment_id"] in expired_ids:
                # 预占到期取消后，需求退出分配池；客户/门店需重新提交需求才再排队
                continue
            if prior is not None and prior["state"] in (CommitmentState.CONFIRMED.value,
                                                        CommitmentState.FULFILLED.value):
                continue  # 受保护，不参与重排
            candidates.append({"demand": demand, "prior": prior})

        candidates.sort(key=lambda c: (
            PRIORITY_RANK[c["demand"]["kind"]],
            c["demand"]["needed_at"],
            c["demand"]["submitted_at"],
            c["demand"]["demand_id"],
        ))

        preempted = False
        results = []
        for cand in candidates:
            demand = cand["demand"]
            qty, holds, notes = self._greedy_lots(conn, demand, lots, pool, as_of)
            for lot_id, lot_qty in holds.items():
                pool[lot_id] -= lot_qty
            short = demand["requested_qty"] - qty
            reason, detail = self._shortage_reason(conn, demand, lots, qty, short,
                                                   preempted, notes, as_of)
            results.append({
                "demand": demand, "prior": cand["prior"], "qty": qty, "holds": holds,
                "notes": notes, "short_reason": reason, "short_detail": detail,
                "allocation_note": "按 合同锁量>区域保供>普通渠道、需求日先后 排序；"
                                   "批次按近保质期优先并取整件",
            })
            if qty > 0:
                # 排在后面的需求若缺货，原因才可能是被前面高优先级/更早需求抢占
                preempted = True
        return {"expired": sorted(expired_ids), "results": results,
                "confirmed_deficit": confirmed_deficit}

    def _greedy_lots(
        self,
        conn: sqlite3.Connection,
        demand: sqlite3.Row,
        lots: dict[str, sqlite3.Row],
        pool: dict[str, int],
        as_of: str,
    ) -> tuple[int, dict[str, int], list[str]]:
        """FEFO + 保质期适配 + 最小整件量，给单笔需求贪心取货。"""
        need_date = _parse(demand["needed_at"]).date() + timedelta(days=demand["min_shelf_days"])
        ordered = sorted(
            lots.values(),
            key=lambda l: (
                l["expiry_date"] is None,           # 有保质期的先出（近效期先出）
                l["expiry_date"] or "9999-12-31",
                l["received_at"],
                l["lot_id"],
            ),
        )
        remaining = demand["requested_qty"]
        holds: dict[str, int] = {}
        notes: list[str] = []
        for lot in ordered:
            if remaining <= 0:
                break
            free = pool.get(lot["lot_id"], 0)
            if free <= 0:
                continue
            if lot["expiry_date"] is not None:
                expiry = datetime.strptime(lot["expiry_date"], "%Y-%m-%d").date()
                if expiry < need_date:
                    notes.append(f"批次 {lot['lot_id']} 保质期 {lot['expiry_date']}"
                                 f" 早于需求日 {need_date.isoformat()}，跳过")
                    continue
            take = min(remaining, free)
            case = max(lot["case_size"], 1)
            rounded = (take // case) * case
            if rounded == 0:
                notes.append(f"批次 {lot['lot_id']} 余量 {take} 不足一个整件（{case}），跳过")
                continue
            if rounded < take:
                notes.append(f"批次 {lot['lot_id']} 按整件量 {case} 向下取整，"
                             f"少给 {take - rounded}")
            holds[lot["lot_id"]] = rounded
            remaining -= rounded
        return demand["requested_qty"] - remaining, holds, notes

    @staticmethod
    def _shortage_reason(
        conn: sqlite3.Connection,
        demand: sqlite3.Row,
        lots: dict[str, sqlite3.Row],
        got: int,
        short: int,
        higher_priority_served: bool,
        notes: list[str],
        as_of: str,
    ) -> tuple[str, str]:
        if short <= 0:
            return ReasonCode.ALLOCATED_IN_VERSION.value, "足量满足"
        need_date = _parse(demand["needed_at"]).date() + timedelta(days=demand["min_shelf_days"])
        fit_lots = [
            l for l in lots.values()
            if l["expiry_date"] is None
            or datetime.strptime(l["expiry_date"], "%Y-%m-%d").date() >= need_date
        ]
        if lots and not fit_lots:
            return (ReasonCode.LOT_EXPIRY_UNFIT.value,
                    f"全部 {len(lots)} 个批次都撑不到需求日 {need_date.isoformat()}")
        if got > 0 and any("整件" in n for n in notes):
            return (ReasonCode.CASE_SIZE_ROUNDED.value,
                    "受最小整件量限制，尾量无法凑成整件")
        if higher_priority_served:
            return (ReasonCode.HIGHER_PRIORITY_PREEMPTED.value,
                    "合同锁量/区域保供/更早需求已优先取走货源")
        return ReasonCode.SHORT_SUPPLY.value, "可售货源不足"

    def _cancel_commitment(
        self, conn: sqlite3.Connection, commitment_id: str, ts: str,
        reason: str, sku: str, detail: str = "",
    ) -> None:
        self._insert_event(
            conn, EventType.COMMITMENT_CANCELLED, commitment_id, ts,
            {"reason": reason, "sku": sku, "detail": detail},
        )

    # ------------------------------------------------------------------ 可恢复重算作业

    def enqueue_recalc(self, sku: str, reason: str, as_of: Optional[datetime] = None) -> str:
        """登记一个待执行重算。服务崩溃重启后仍可捞起重跑。"""
        job_id = _new_id("job")
        ts = _ts(as_of)
        with self._lock, self._conn:
            self._begin_immediate()
            self._conn.execute(
                "INSERT INTO recalc_jobs(job_id, sku, reason, as_of, state, created_at)"
                " VALUES(?,?,?,?,?,?)",
                (job_id, sku, reason, ts, "pending", ts),
            )
        return job_id

    def run_pending_jobs(self, limit: int = 10, stale_seconds: int = 30) -> list[dict[str, Any]]:
        """执行所有未完成的重算；中断在 running 状态的作业（服务重启后）会被接管。"""
        done: list[dict[str, Any]] = []
        for _ in range(limit):
            with self._lock, self._conn:
                self._begin_immediate()
                cutoff = _ts(_now() - timedelta(seconds=stale_seconds))
                job = self._conn.execute(
                    "SELECT * FROM recalc_jobs WHERE state='pending'"
                    " OR (state='running' AND claimed_at < ?)"
                    " ORDER BY created_at LIMIT 1",
                    (cutoff,),
                ).fetchone()
                if job is None:
                    break
                claimed = _ts(_now())
                self._conn.execute(
                    "UPDATE recalc_jobs SET state='running', claimed_at=?, attempts=attempts+1,"
                    " last_error=NULL WHERE job_id=?",
                    (claimed, job["job_id"]),
                )
                try:
                    summary = self._plan_and_commit(
                        self._conn, job["sku"], job["reason"], job["as_of"],
                        job_id=job["job_id"],
                    )
                except Exception as exc:  # 作业失败落库，不影响其他作业
                    self._conn.execute(
                        "UPDATE recalc_jobs SET state='failed', last_error=? WHERE job_id=?",
                        (repr(exc), job["job_id"]),
                    )
                    raise
                self._conn.execute(
                    "UPDATE recalc_jobs SET state='done' WHERE job_id=?", (job["job_id"],)
                )
            done.append({"job_id": job["job_id"], "sku": job["sku"], "summary": summary})
        return done

    def recover_interrupted(self) -> list[dict[str, Any]]:
        """服务恢复后调用：接着完成中断前尚未落定的重算。"""
        return self.run_pending_jobs()

    def job_status(self, job_id: str) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM recalc_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"作业 {job_id} 不存在")
        return dict(row)

    # ------------------------------------------------------------------ 事件导入

    def append_events(self, events: Iterable[dict[str, Any]]) -> dict[str, int]:
        """批量导入外部事件流。

        event_id 重复的事件直接跳过，状态折叠也是幂等的——
        同一份事件文件再次导入不会重复扣量。
        """
        inserted = 0
        skipped = 0
        touched_skus: set[str] = set()
        known = {e.value for e in EventType}
        with self._lock, self._conn:
            self._begin_immediate()
            for raw in events:
                event_id = raw["event_id"]
                event_type = raw["event_type"]
                if event_type not in known:
                    raise LedgerError(f"未知事件类型：{event_type}")
                if self._conn.execute(
                    "SELECT 1 FROM events WHERE event_id=?", (event_id,)
                ).fetchone():
                    skipped += 1
                    continue
                etype = EventType(event_type)
                self._insert_event(
                    self._conn, etype, raw["aggregate_id"], raw["occurred_at"],
                    raw.get("payload", {}), event_id=event_id,
                    idempotency_key=raw.get("idempotency_key"), version=raw.get("version"),
                )
                inserted += 1
                payload = raw.get("payload", {})
                if etype in (EventType.SUPPLY_REGISTERED, EventType.DEMAND_SUBMITTED):
                    touched_skus.add(payload["sku"])
                elif etype is EventType.SUPPLY_QUANTITY_CHANGED:
                    lot = self._conn.execute(
                        "SELECT sku FROM lots WHERE lot_id=?", (raw["aggregate_id"],)
                    ).fetchone()
                    if lot is not None:
                        touched_skus.add(lot["sku"])
            # 只有本次确实写入了新事件才重算；整文件重复导入时 inserted=0，结果不变
            for sku in touched_skus:
                self._plan_and_commit(
                    self._conn, sku, ReasonCode.ALLOCATED_IN_VERSION.value, _ts()
                )
        return {"inserted": inserted, "skipped": skipped}

    # ------------------------------------------------------------------ 查询

    def availability(self, sku: str, as_of: Optional[datetime] = None) -> dict[str, Any]:
        """某时点的可售 / 锁定 / 已确认待出库 / 已履约。

        已过 expires_at 但尚未跑过期扫描的预占，视为已失效（计入 expired_holds），
        不再算作锁定量——保证任何时点报出的可售量都真实可承诺。
        """
        now_ts = _ts(as_of)
        with self._lock:
            rows = self._conn.execute("SELECT * FROM lots WHERE sku=?", (sku,)).fetchall()
            total = sum(r["qty"] for r in rows)
            fulfilled = self._conn.execute(
                "SELECT COALESCE(SUM(s.qty),0) AS q FROM shipments s"
                " JOIN commitments c ON c.commitment_id=s.commitment_id WHERE c.sku=?",
                (sku,),
            ).fetchone()["q"]

            def sum_holds(states: tuple[str, ...], open_only: bool,
                          not_expired: bool = False) -> int:
                placeholders = ",".join("?" for _ in states)
                qty_expr = "h.qty - h.shipped_qty" if open_only else "h.qty"
                extra = " AND (c.expires_at IS NULL OR c.expires_at > ?)" if not_expired else ""
                params = [sku, *states]
                if not_expired:
                    params.append(now_ts)
                return self._conn.execute(
                    f"SELECT COALESCE(SUM({qty_expr}),0) AS q FROM commitment_holds h"
                    " JOIN commitments c ON c.commitment_id=h.commitment_id"
                    f" WHERE c.sku=? AND c.state IN ({placeholders}){extra}",
                    params,
                ).fetchone()["q"]

            confirmed = sum_holds((CommitmentState.CONFIRMED.value,), open_only=True)
            reserved = sum_holds(
                (CommitmentState.RESERVED.value, CommitmentState.DEGRADED.value),
                open_only=False, not_expired=True,
            )
            expired_holds = sum_holds(
                (CommitmentState.RESERVED.value, CommitmentState.DEGRADED.value),
                open_only=False,
            ) - reserved
            available = total - fulfilled - confirmed - reserved
            by_lot = []
            for lot in rows:
                lot_fulfilled = self._fulfilled_by_lot(self._conn, lot["lot_id"])
                lot_reserved = self._conn.execute(
                    "SELECT COALESCE(SUM(h.qty),0) AS q FROM commitment_holds h"
                    " JOIN commitments c ON c.commitment_id=h.commitment_id"
                    " WHERE h.lot_id=? AND c.state IN (?,?)"
                    " AND (c.expires_at IS NULL OR c.expires_at > ?)",
                    (lot["lot_id"], CommitmentState.RESERVED.value, CommitmentState.DEGRADED.value,
                     now_ts),
                ).fetchone()["q"]
                lot_confirmed = self._conn.execute(
                    "SELECT COALESCE(SUM(h.qty - h.shipped_qty),0) AS q FROM commitment_holds h"
                    " JOIN commitments c ON c.commitment_id=h.commitment_id"
                    " WHERE h.lot_id=? AND c.state=?",
                    (lot["lot_id"], CommitmentState.CONFIRMED.value),
                ).fetchone()["q"]
                by_lot.append({
                    "lot_id": lot["lot_id"], "qty": lot["qty"],
                    "expiry_date": lot["expiry_date"], "case_size": lot["case_size"],
                    "available": lot["qty"] - lot_fulfilled - lot_confirmed - lot_reserved,
                    "reserved": lot_reserved, "confirmed_open": lot_confirmed,
                    "fulfilled": lot_fulfilled,
                })
            return {
                "sku": sku,
                "as_of": now_ts,
                "available": available,
                "reserved": reserved,
                "confirmed_open": confirmed,
                "fulfilled": fulfilled,
                "expired_holds": expired_holds,
                "total_supply": total,
                "by_lot": by_lot,
            }

    def get_commitment(self, commitment_id: str) -> dict[str, Any]:
        """承诺详情：从提出、降级到兑现的完整理由链。"""
        row = self._conn.execute(
            "SELECT * FROM commitments WHERE commitment_id=?", (commitment_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"承诺 {commitment_id} 不存在")
        holds = [
            dict(h) for h in self._conn.execute(
                "SELECT lot_id, qty, shipped_qty FROM commitment_holds"
                " WHERE commitment_id=? ORDER BY lot_id", (commitment_id,))
        ]
        history = []
        for e in self._conn.execute(
            "SELECT event_id, event_type, occurred_at, payload, version FROM events"
            " WHERE aggregate_id=? ORDER BY rowid", (commitment_id,)
        ):
            payload = json.loads(e["payload"])
            history.append({
                "event_id": e["event_id"], "event_type": e["event_type"],
                "occurred_at": e["occurred_at"], "version": e["version"],
                "reason": payload.get("reason"), "detail": payload.get("detail"),
                "from_qty": payload.get("old_qty"), "to_qty": payload.get("committed_qty"),
            })
        # 需求提出本身也要进理由链
        demand_events = self._conn.execute(
            "SELECT event_id, event_type, occurred_at, payload, version FROM events"
            " WHERE aggregate_id=? AND event_type=? ORDER BY rowid",
            (row["demand_id"], EventType.DEMAND_SUBMITTED.value),
        ).fetchall()
        for e in reversed(demand_events):
            payload = json.loads(e["payload"])
            history.insert(0, {
                "event_id": e["event_id"], "event_type": e["event_type"],
                "occurred_at": e["occurred_at"], "version": e["version"],
                "reason": ReasonCode.DEMAND_SUBMITTED.value,
                "detail": f"{payload['kind']} 渠道需求 {payload['requested_qty']}，"
                          f"门店 {payload['store_id']}，需求日 {payload['needed_at']}",
            })
        shipped = self._shipped_for(self._conn, commitment_id)
        return {
            "commitment_id": commitment_id,
            "demand_id": row["demand_id"],
            "sku": row["sku"],
            "state": row["state"],
            "requested_qty": row["requested_qty"],
            "committed_qty": row["committed_qty"],
            "shipped_qty": shipped,
            "version": row["version"],
            "expires_at": row["expires_at"],
            "created_at": row["created_at"],
            "holds": holds,
            "history": history,
        }

    def get_demand_status(self, demand_id: str) -> dict[str, Any]:
        """需求视角：当前承诺、历次承诺、以及每个版本中未满足的理由。"""
        demand = self._require_demand(demand_id)
        commitments = [
            dict(r) for r in self._conn.execute(
                "SELECT commitment_id, state, committed_qty, version FROM commitments"
                " WHERE demand_id=? ORDER BY version", (demand_id,))
        ]
        shortages = []
        for e in self._conn.execute(
            "SELECT payload, occurred_at, version FROM events WHERE event_type=?",
            (EventType.ALLOCATION_VERSIONED.value,),
        ):
            payload = json.loads(e["payload"])
            for s in payload.get("shortages", []):
                if s["demand_id"] == demand_id:
                    shortages.append({"version": payload["version"],
                                      "occurred_at": e["occurred_at"], **s})
        return {"demand": dict(demand), "commitments": commitments, "shortages": shortages}

    def list_versions(self, sku: Optional[str] = None) -> list[dict[str, Any]]:
        sql = "SELECT event_id, aggregate_id, occurred_at, payload FROM events"
        sql += " WHERE event_type=?" if sku is None else " WHERE event_type=? AND aggregate_id=?"
        sql += " ORDER BY rowid"
        params = (EventType.ALLOCATION_VERSIONED.value,) if sku is None else \
            (EventType.ALLOCATION_VERSIONED.value, sku)
        out = []
        for r in self._conn.execute(sql, params):
            payload = json.loads(r["payload"])
            out.append({"event_id": r["event_id"], "sku": r["aggregate_id"],
                        "occurred_at": r["occurred_at"], **payload})
        return out

    def list_events(self, aggregate_id: Optional[str] = None) -> list[dict[str, Any]]:
        if aggregate_id is None:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY rowid").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE aggregate_id=? ORDER BY rowid",
                (aggregate_id,)).fetchall()
        return [{"event_id": r["event_id"], "event_type": r["event_type"],
                 "aggregate_id": r["aggregate_id"], "occurred_at": r["occurred_at"],
                 "payload": json.loads(r["payload"]), "version": r["version"]}
                for r in rows]

    # ------------------------------------------------------------------ 小工具

    def _require_lot(self, lot_id: str) -> sqlite3.Row:
        row = self._conn.execute("SELECT * FROM lots WHERE lot_id=?", (lot_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"供应批次 {lot_id} 不存在")
        return row

    def _require_demand(self, demand_id: str) -> sqlite3.Row:
        row = self._conn.execute("SELECT * FROM demands WHERE demand_id=?", (demand_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"需求 {demand_id} 不存在")
        return row

    @staticmethod
    def _validate_qty(qty: int, case_size: int) -> None:
        if qty <= 0:
            raise LedgerError("供应量必须为正")
        if case_size <= 0:
            raise LedgerError("最小整件量必须为正")


def _holds_match(conn: sqlite3.Connection, commitment_id: str, holds: dict[str, int]) -> bool:
    rows = conn.execute(
        "SELECT lot_id, qty FROM commitment_holds WHERE commitment_id=?", (commitment_id,)
    ).fetchall()
    return {r["lot_id"]: r["qty"] for r in rows} == holds
