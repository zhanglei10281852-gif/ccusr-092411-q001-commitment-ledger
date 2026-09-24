"""领域类型：事件、状态、理由码与只读快照。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class EventType(str, Enum):
    SUPPLY_REGISTERED = "supply.registered"
    SUPPLY_QUANTITY_CHANGED = "supply.quantity_changed"
    DEMAND_SUBMITTED = "demand.submitted"
    DEMAND_CANCELLED = "demand.cancelled"
    COMMITMENT_RESERVED = "commitment.reserved"
    COMMITMENT_DEGRADED = "commitment.degraded"
    COMMITMENT_CONFIRMED = "commitment.confirmed"
    COMMITMENT_CANCELLED = "commitment.cancelled"
    SHIPMENT_DISPATCHED = "shipment.dispatched"
    ALLOCATION_VERSIONED = "allocation.versioned"


class CommitmentState(str, Enum):
    PROPOSED = "proposed"
    RESERVED = "reserved"
    CONFIRMED = "confirmed"
    FULFILLED = "fulfilled"
    DEGRADED = "degraded"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in (CommitmentState.FULFILLED, CommitmentState.CANCELLED)


class DemandKind(str, Enum):
    """需求渠道。优先级为 合同锁量 > 区域保供底线 > 普通渠道。"""

    DIRECT = "direct"            # 直营网点
    FRANCHISEE = "franchisee"    # 加盟商
    GROUP_BUY = "group_buy"      # 企业团购
    REGIONAL_FLOOR = "regional_floor"  # 区域保供底线
    CONTRACT = "contract"        # 合同锁量


class ReasonCode(str, Enum):
    """承诺生命周期上每一次变化的理由，供采购员追溯完整链路。"""

    DEMAND_SUBMITTED = "demand_submitted"
    ALLOCATED_INITIAL = "allocated_initial"
    ALLOCATED_IN_VERSION = "allocated_in_version"
    SHORT_SUPPLY = "short_supply"
    RESERVE_EXPIRED = "reserve_expired"
    SUPPLY_SHRUNK = "supply_shrunk"
    SUPPLY_REPLENISHED = "supply_replenished"
    DEMAND_CANCELLED_BY_CUSTOMER = "demand_cancelled_by_customer"
    REALLOCATION = "reallocation"
    HIGHER_PRIORITY_PREEMPTED = "higher_priority_preempted"
    FEFO_LOT_CHOSEN = "fefo_lot_chosen"
    CASE_SIZE_ROUNDED = "case_size_rounded"
    LOT_EXPIRY_UNFIT = "lot_expiry_unfit"
    CONFIRMED_BY_BUYER = "confirmed_by_buyer"
    SHIPPED = "shipped"
    MANUALLY_CANCELLED = "manually_cancelled"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    aggregate_id: str
    occurred_at: str
    payload: dict[str, Any] = field(default_factory=dict)
    idempotency_key: Optional[str] = None
    version: Optional[int] = None  # 所属分配版本，业务事件为空


@dataclass(frozen=True)
class LotView:
    lot_id: str
    sku: str
    available_qty: int          # 当前可用（原始量 - 已出库，未经预占扣减）
    original_qty: int
    expiry_date: Optional[str]
    case_size: int
    received_at: str
    version: int


@dataclass(frozen=True)
class DemandView:
    demand_id: str
    sku: str
    kind: str
    requested_qty: int
    store_id: str
    needed_at: str
    submitted_at: str
    priority_rank: int
    active: bool
    cancelled_at: Optional[str]
    cancel_reason: Optional[str]


@dataclass(frozen=True)
class CommitmentView:
    commitment_id: str
    demand_id: str
    sku: str
    state: str
    requested_qty: int
    committed_qty: int
    version: int
    holds: tuple[dict[str, Any], ...]
    created_at: str
    expires_at: Optional[str]
    history: tuple[dict[str, Any], ...]
    projected: bool = False  # 投影：尚未落为正式事件（重算预览）


@dataclass(frozen=True)
class AvailabilityView:
    """某 SKU 在任意时点的可售/锁定/已履约三件套。"""

    sku: str
    as_of: str
    available: int            # 可售：未被任何承诺占用
    reserved: int             # 锁定：reserved + degraded 承诺持有量
    confirmed: int            # 已确认待出库
    fulfilled: int            # 已履约（已出库）
    by_lot: tuple[dict[str, Any], ...]

    @property
    def total_supply(self) -> int:
        return self.available + self.reserved + self.confirmed + self.fulfilled
