"""采购承诺与配货系统。

只追加的事件账本 + 版本化重算：
- 供应批次、需求、承诺、出库都以事件落账，event_id 天然幂等；
- 每次供应缩量/补货、撤单、预占到期产生新的分配版本，已出库事实不可回滚。
"""
from __future__ import annotations

from .model import (
    CommitmentState,
    DemandKind,
    EventType,
    ReasonCode,
)
from .service import Ledger, LedgerError, ConflictError, HoldExpiredError

__all__ = [
    "CommitmentState",
    "DemandKind",
    "EventType",
    "ReasonCode",
    "Ledger",
    "LedgerError",
    "ConflictError",
    "HoldExpiredError",
]
