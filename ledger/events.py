"""领域事件。

事件是系统中唯一的事实来源。业务标识在同类型内唯一，``event_id``
全局唯一：相同 ``event_id`` 重复导入只会被忽略，绝不重复扣量。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

# 领域合同 domain/contract.json 中登记的全部事件类型。
EVENT_TYPES = frozenset(
    {
        "supply.registered",
        "supply.shrunk",
        "supply.replenished",
        "demand.submitted",
        "demand.cancelled",
        "contract.registered",
        "floor.set",
        "reservation.expired",
        "commitment.reserved",
        "commitment.confirmed",
        "shipment.dispatched",
    }
)

# 由外部系统/人工导入的事件；其余事件（reservation.expired、
# commitment.reserved）只允许由重算引擎生成。
IMPORTABLE_TYPES = frozenset(
    {
        "supply.registered",
        "supply.shrunk",
        "supply.replenished",
        "demand.submitted",
        "demand.cancelled",
        "contract.registered",
        "floor.set",
        "commitment.confirmed",
        "shipment.dispatched",
    }
)


def parse_time(value: str | datetime) -> datetime:
    """解析 ISO 8601 时间，强制带时区。"""
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        raise ValueError(f"时间必须包含时区：{value}")
    return moment


@dataclass(frozen=True)
class Event:
    """一条不可变事件。"""

    event_id: str
    event_type: str
    aggregate_id: str
    occurred_at: datetime
    payload: dict[str, Any] = field(default_factory=dict)
    seq: int = 0
    run_id: str | None = None
    version: int | None = None

    def __post_init__(self) -> None:
        if self.event_type not in EVENT_TYPES:
            raise ValueError(f"未知事件类型：{self.event_type}")
        if not self.event_id:
            raise ValueError("event_id 不能为空")
        if not self.aggregate_id:
            raise ValueError("aggregate_id 不能为空")
        object.__setattr__(self, "occurred_at", parse_time(self.occurred_at))

    def to_row(self) -> tuple[str, str, str, str, str]:
        return (
            self.event_id,
            self.event_type,
            self.aggregate_id,
            self.occurred_at.isoformat(),
            json.dumps(self.payload, ensure_ascii=False, sort_keys=True),
        )

    @classmethod
    def from_row(cls, row: tuple) -> "Event":
        seq, event_id, event_type, aggregate_id, occurred_at, payload_raw = row[:6]
        run_id = row[6] if len(row) > 6 else None
        version = row[7] if len(row) > 7 else None
        return cls(
            event_id=event_id,
            event_type=event_type,
            aggregate_id=aggregate_id,
            occurred_at=parse_time(occurred_at),
            payload=json.loads(payload_raw),
            seq=seq,
            run_id=run_id,
            version=version,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        try:
            return cls(
                event_id=data["event_id"],
                event_type=data["event_type"],
                aggregate_id=data["aggregate_id"],
                occurred_at=data["occurred_at"],
                payload=data.get("payload", {}) or {},
            )
        except KeyError as exc:
            raise ValueError(f"事件缺少字段：{exc.args[0]}") from exc

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at.isoformat(),
            "payload": self.payload,
        }
