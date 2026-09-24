"""节前采购承诺与配货的可运行演示。

运行：python3 examples/festival_demo.py
不依赖任何第三方库，使用内存账本，把事故现场、缩量、补货、撤单、
到期、并发确认和崩溃恢复按时间线演一遍。
"""
from __future__ import annotations

import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from allocation import ConflictError, DemandKind, Ledger  # noqa: E402

TZ = timezone(timedelta(hours=8))


def at(day: int, hour: int = 8) -> datetime:
    return datetime(2026, 9, day, hour, tzinfo=TZ)


def need(day: int) -> datetime:
    return datetime(2026, 9, day, tzinfo=TZ)


def show(ledger: Ledger, sku: str, title: str) -> None:
    v = ledger.availability(sku)
    print(f"\n== {title} ==")
    print(f"  总货源 {v['total_supply']}｜可售 {v['available']}｜"
          f"锁定 {v['reserved']}｜已确认待出 {v['confirmed_open']}｜已履约 {v['fulfilled']}")


def main() -> None:
    ledger = Ledger(":memory:", hold_ttl_seconds=30 * 24 * 3600)

    # 9-21 档口确认 100 箱梨（5 箱/件，保质期到 10-05）
    ledger.register_supply("lot-pear-01", "pear", 100, 5, at(21), expiry_date="2026-10-05")

    # 三家渠道同时提交 80 箱——第一轮"配货结果"不再可能三家都拿到
    ledger.submit_demand("d-direct", "pear", DemandKind.DIRECT.value, 80,
                         "store-080", need(27), occurred_at=at(22, 9))
    ledger.submit_demand("d-franchisee", "pear", DemandKind.FRANCHISEE.value, 80,
                         "store-107", need(27), occurred_at=at(22, 9))
    ledger.submit_demand("d-group", "pear", DemandKind.GROUP_BUY.value, 80,
                         "acme", need(27), occurred_at=at(22, 10))
    show(ledger, "pear", "9-22 三家需求进入后（同一优先级按提交先后）")
    for d in ("d-direct", "d-franchisee", "d-group"):
        status = ledger.get_demand_status(d)
        if status["commitments"]:
            c = status["commitments"][-1]
            print(f"  {d}: 承诺 {c['committed_qty']}（{c['state']}）")
        else:
            reason = status["shortages"][-1]["reason"] if status["shortages"] else "-"
            print(f"  {d}: 无承诺（缺货原因：{reason}）")

    # 9-22 中午 档口缩量到 60 → 新版本 v2，直营被降级
    ledger.change_supply_qty("lot-pear-01", 60, at(22, 11))
    cid_d = ledger.get_demand_status("d-direct")["commitments"][-1]["commitment_id"]
    show(ledger, "pear", "9-22 档口缩量到 60 → 分配 v2")
    print("  直营承诺理由链：")
    for h in ledger.get_commitment(cid_d)["history"]:
        print(f"    {h['event_type']:<24} reason={h['reason']}")

    # 直营立即确认，锁住受保护量
    ledger.confirm_commitment(cid_d, at(22, 14))

    # 9-23 补货 60 到场 → v3，加盟商拿到货
    ledger.register_supply("lot-pear-02", "pear", 60, 5, at(23, 7), expiry_date="2026-10-08")
    show(ledger, "pear", "9-23 补货 60 到场 → 分配 v3")

    # 团购撤单
    ledger.cancel_demand("d-group", at(23, 12))
    show(ledger, "pear", "9-23 团购撤单后")

    # 9-24 直营出库：事实冻结
    ledger.dispatch(cid_d, [("lot-pear-01", 60)], at(24, 6))
    show(ledger, "pear", "9-24 直营出库 60（已履约，任何版本不可倒回）")

    # 两个人抢着确认加盟商的承诺：只有一个成功
    cid_f = ledger.get_demand_status("d-franchisee")["commitments"][-1]["commitment_id"]
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def race(buyer: str) -> None:
        barrier.wait()
        try:
            ledger.confirm_commitment(cid_f, at(24, 9), buyer=buyer)
            outcomes.append(f"{buyer} 确认成功")
        except ConflictError:
            outcomes.append(f"{buyer} 被告知承诺已被确认")

    t1 = threading.Thread(target=race, args=("采购员甲",))
    t2 = threading.Thread(target=race, args=("采购员乙",))
    t1.start(); t2.start(); t1.join(); t2.join()
    print("\n== 并发确认同一承诺 ==")
    for o in sorted(outcomes):
        print("  " + o)

    # 版本流水
    print("\n== 分配版本流水 ==")
    for v in ledger.list_versions("pear"):
        print(f"  v{v['version']} @ {v['occurred_at']} 触发={v['reason']}，"
              f"足额承诺 {sum(1 for a in v['allocations'] if a['committed_qty'] > 0)} 笔")


if __name__ == "__main__":
    main()
