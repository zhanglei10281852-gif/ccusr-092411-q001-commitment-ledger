"""采购承诺与配货系统的端到端测试。

覆盖：三本账、合同锁量/区域底线/渠道/时间优先级、FEFO、最小整件量、
版本化重算、已出库不可倒回、理由链、幂等导入、并发抢库存、断点续算。
"""
from __future__ import annotations

import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ledger.engine import CommitmentLedger, DomainError, InventoryShortage
from ledger.events import Event

T = "2026-09-{day:02d}T{h:02d}:00:00+08:00"


def ev(event_id, event_type, aggregate_id, at, **payload):
    return Event(event_id, event_type, aggregate_id, at, payload)


def supply(eid, lot_id, day=20, h=8, *, sku="pear", quantity=100, case_size=10, expiry_day=30):
    return ev(
        f"evt-{eid}", "supply.registered", lot_id,
        T.format(day=day, h=h),
        sku=sku, quantity=quantity, case_size=case_size,
        expiry=f"2026-09-{expiry_day}T20:00:00+08:00",
    )


def demand(eid, did, day, h, *, channel="direct", store=None, region="north",
           sku="pear", quantity=10, contract_id=None, expire=None):
    payload = {"channel": channel, "store_id": store or did, "region": region,
               "sku": sku, "quantity": quantity}
    if contract_id:
        payload["contract_id"] = contract_id
    if expire:
        payload["reserve_expire_at"] = expire
    return ev(f"evt-{eid}", "demand.submitted", did, T.format(day=day, h=h), **payload)


def contract(eid, cid, day=19, h=9, *, customer="cust-1", sku="pear", locked_qty=50):
    return ev(f"evt-{eid}", "contract.registered", cid, T.format(day=day, h=h),
              customer_id=customer, sku=sku, locked_qty=locked_qty)


def floor(eid, day=19, h=10, *, region="north", sku="pear", min_qty=30):
    return ev(f"evt-{eid}", "floor.set", f"floor-{region}-{sku}", T.format(day=day, h=h),
              region=region, sku=sku, min_qty=min_qty)


class LedgerTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = CommitmentLedger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()


class ThreeBooksTest(LedgerTestBase):
    def test_total_equals_available_plus_reserved_plus_fulfilled(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=100),
            demand(2, "d-1", 22, 9, quantity=40),
        ])
        lot = self.ledger.lots()[0]
        self.assertEqual((lot["total"], lot["available"], lot["reserved"], lot["fulfilled"]),
                         (100, 60, 40, 0))

    def test_fulfilled_moves_out_of_reserved(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=100),
            demand(2, "d-1", 22, 9, quantity=40),
            ev("evt-3", "shipment.dispatched", "d-1", "2026-09-23T09:00:00+08:00",
               lot_id="lot-1", quantity=20),
        ])
        lot = self.ledger.lots()[0]
        self.assertEqual((lot["available"], lot["reserved"], lot["fulfilled"]), (60, 20, 20))


class PriorityTest(LedgerTestBase):
    def test_contract_lock_beats_earlier_direct_demand(self):
        # 直营 d-earlier 先提，团购 d-contract 后提但带合同锁量。
        self.ledger.import_events([
            supply(1, "lot-1", quantity=50),
            contract(2, "c-1", locked_qty=50),
            demand(3, "d-earlier", 22, 8, channel="direct", quantity=50),
            demand(4, "d-contract", 22, 9, channel="group",
                   store="g1", region="south", quantity=50, contract_id="c-1"),
        ])
        self.assertEqual(self.ledger.commitment("d-contract")["allocated"], 50)
        self.assertEqual(self.ledger.commitment("d-earlier")["allocated"], 0)

    def test_region_floor_beats_channel_rank(self):
        # 外地直营先提，本地团购后提但本地保供底线未满足。
        self.ledger.import_events([
            supply(1, "lot-1", quantity=30),
            floor(2, region="north", min_qty=30),
            demand(3, "d-direct", 22, 8, channel="direct", region="south", quantity=30),
            demand(4, "d-floor", 22, 9, channel="group",
                   store="g1", region="north", quantity=30),
        ])
        self.assertEqual(self.ledger.commitment("d-floor")["allocated"], 30)
        self.assertEqual(self.ledger.commitment("d-direct")["allocated"], 0)

    def test_channel_rank_direct_franchise_group(self):
        events = [supply(1, "lot-1", quantity=30)]
        events.append(demand(2, "d-group", 22, 8, channel="group", store="g", region="south", quantity=30))
        events.append(demand(3, "d-franchise", 22, 9, channel="franchise", store="f", region="south", quantity=30))
        events.append(demand(4, "d-direct", 22, 10, channel="direct", store="s", region="south", quantity=30))
        self.ledger.import_events(events)
        self.assertEqual(self.ledger.commitment("d-direct")["allocated"], 30)
        self.assertEqual(self.ledger.commitment("d-franchise")["allocated"], 0)
        self.assertEqual(self.ledger.commitment("d-group")["allocated"], 0)

    def test_same_tier_earlier_submission_wins(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=10),
            demand(2, "d-late", 22, 10, quantity=10),
            demand(3, "d-early", 22, 9, quantity=10),
        ])
        self.assertEqual(self.ledger.commitment("d-early")["allocated"], 10)
        self.assertEqual(self.ledger.commitment("d-late")["allocated"], 0)


class LotRulesTest(LedgerTestBase):
    def test_fefo_uses_earliest_expiry_first(self):
        self.ledger.import_events([
            supply(1, "lot-old", quantity=50, expiry_day=25),
            supply(2, "lot-new", quantity=50, expiry_day=29),
            demand(3, "d-1", 22, 9, quantity=30),
        ])
        self.assertEqual(self.ledger.commitment("d-1")["allocated_lots"], {"lot-old": 30})

    def test_case_size_round_down(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=12, case_size=6),
            demand(2, "d-1", 22, 9, quantity=10),
        ])
        view = self.ledger.commitment("d-1")
        self.assertEqual(view["allocated"], 6)
        self.assertEqual(view["shortfall"], 4)
        self.assertEqual(view["status"], "degraded")
        self.assertTrue(any("向下取整" in r for r in view["timeline"][-1]["detail"].split("；")))

    def test_expired_lot_is_dead_stock(self):
        self.ledger.import_events([
            supply(1, "lot-dead", quantity=20, expiry_day=19),  # 已过保质期
            demand(2, "d-1", 22, 9, quantity=20),
        ])
        lot = next(v for v in self.ledger.lots() if v["lot_id"] == "lot-dead")
        self.assertEqual((lot["available"], lot["dead_stock"]), (0, 20))
        self.assertEqual(self.ledger.commitment("d-1")["allocated"], 0)


class VersioningTest(LedgerTestBase):
    def test_shrink_creates_new_version_and_keeps_history(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=20, case_size=2),
            demand(2, "d-1", 22, 9, quantity=6),
            demand(3, "d-2", 22, 10, quantity=6),
        ])
        v1 = self.ledger.commitment("d-2")
        self.assertEqual((v1["allocated"], v1["shortfall"]), (6, 0))
        self.assertEqual(v1["version"], 1)

        self.ledger.import_events([
            ev("evt-4", "supply.shrunk", "lot-1", "2026-09-23T08:00:00+08:00", delta=10),
        ])
        v2 = self.ledger.commitment("d-2")
        self.assertEqual(v2["version"], 2)
        self.assertEqual((v2["allocated"], v2["shortfall"]), (4, 2))
        # 理由链保留两个版本的配货事实。
        versions_in_chain = {t["version"] for t in v2["timeline"] if t["kind"] == "reserved"}
        self.assertEqual(versions_in_chain, {1, 2})
        self.assertTrue([v for v in self.ledger.versions() if v["version"] == 1])

    def test_replenish_allows_previously_unmet_commitment(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=5),
            demand(2, "d-1", 22, 9, quantity=10),
        ])
        self.assertEqual(self.ledger.commitment("d-1")["status"], "degraded")
        self.ledger.import_events([
            ev("evt-3", "supply.replenished", "lot-1", "2026-09-23T08:00:00+08:00", delta=10),
        ])
        view = self.ledger.commitment("d-1")
        self.assertEqual(view["allocated"], 10)
        self.assertEqual(view["status"], "reserved")

    def test_cancel_releases_stock_to_next_demand(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=10),
            demand(2, "d-1", 22, 9, quantity=10),
            demand(3, "d-2", 22, 10, quantity=10),
        ])
        self.assertEqual(self.ledger.commitment("d-2")["allocated"], 0)
        self.ledger.import_events([
            ev("evt-4", "demand.cancelled", "d-1", "2026-09-23T12:00:00+08:00",
               reason="客户临时撤单"),
        ])
        self.assertEqual(self.ledger.commitment("d-1")["status"], "cancelled")
        self.assertEqual(self.ledger.commitment("d-2")["allocated"], 10)

    def test_partial_confirm_releases_unconfirmed(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=20, case_size=2),
            demand(2, "d-1", 22, 9, quantity=10),
            demand(3, "d-2", 22, 10, quantity=10),
        ])
        self.assertEqual(self.ledger.commitment("d-2")["allocated"], 10)
        self.ledger.import_events([
            ev("evt-4", "commitment.confirmed", "d-1", "2026-09-23T11:00:00+08:00",
               confirmed_qty=6),
        ])
        # d-1 收窄到 6，释放 4，但 d-2 已持有 10，没有第三个需求承接；
        # 批次可售回升 4。
        self.assertEqual(self.ledger.commitment("d-1")["target"], 6)
        self.assertEqual(self.ledger.lots()[0]["available"], 4)

    def test_reservation_expiry_releases_and_does_not_rebind(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=10),
            demand(2, "d-hold", 22, 9, quantity=10,
                   expire="2026-09-23T12:00:00+08:00"),
        ])
        self.assertEqual(self.ledger.lots()[0]["reserved"], 10)
        produced = self.ledger.tick("2026-09-23T13:00:00+08:00")
        self.assertEqual(produced, 1)
        view = self.ledger.commitment("d-hold")
        self.assertEqual(view["status"], "cancelled")
        self.assertTrue(any(t["kind"] == "expired" for t in view["timeline"]))
        self.assertEqual(self.ledger.lots()[0]["available"], 10)
        # 时间再推进不会产生重复版本或重复扣量。
        self.assertEqual(self.ledger.tick("2026-09-24T09:00:00+08:00"), 0)
        # 补货到量，已到期承诺不会复活，新需求可以拿。
        self.ledger.import_events([
            demand(5, "d-new", 24, 10, quantity=10),
        ])
        self.assertEqual(self.ledger.commitment("d-new")["allocated"], 10)
        self.assertEqual(self.ledger.commitment("d-hold")["status"], "cancelled")


class FulfillmentImmutabilityTest(LedgerTestBase):
    def test_shrink_below_shipped_is_rejected(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=100),
            demand(2, "d-1", 22, 9, quantity=40),
            ev("evt-3", "shipment.dispatched", "d-1", "2026-09-23T09:00:00+08:00",
               lot_id="lot-1", quantity=40),
        ])
        with self.assertRaises(DomainError):
            self.ledger.import_events([
                ev("evt-4", "supply.shrunk", "lot-1", "2026-09-24T08:00:00+08:00", delta=70),
            ])
        # 被拒事件整批回滚：版本不增加、已履约不变。
        self.assertEqual(self.ledger.lots()[0]["fulfilled"], 40)
        self.assertEqual(self.ledger.versions()[-1]["version"], 1)

    def test_over_shipment_is_rejected(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=100),
            demand(2, "d-1", 22, 9, quantity=10),
        ])
        with self.assertRaises(DomainError):
            self.ledger.import_events([
                ev("evt-3", "shipment.dispatched", "d-1", "2026-09-23T09:00:00+08:00",
                   lot_id="lot-1", quantity=20),
            ])

    def test_shipped_qty_survives_later_shrink_and_reallocation(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=100),
            demand(2, "d-a", 22, 8, quantity=60),
            demand(3, "d-b", 22, 9, quantity=60),
            ev("evt-4", "shipment.dispatched", "d-b", "2026-09-23T08:00:00+08:00",
               lot_id="lot-1", quantity=40),
        ])
        self.ledger.import_events([
            ev("evt-5", "supply.shrunk", "lot-1", "2026-09-23T10:00:00+08:00", delta=20),
        ])
        lot = self.ledger.lots()[0]
        self.assertEqual(lot["total"], 80)
        self.assertEqual(lot["fulfilled"], 40)          # 已出库事实不动
        # d-b 已出 40，重算只能在剩余 40 内继续排。
        self.assertGreaterEqual(self.ledger.commitment("d-b")["fulfilled"], 40)


class IdempotencyTest(LedgerTestBase):
    def test_same_events_imported_twice_do_not_double_deduct(self):
        events = [
            supply(1, "lot-1", quantity=10),
            demand(2, "d-1", 22, 9, quantity=10),
        ]
        first = self.ledger.import_events(events)
        versions_after_first = len(self.ledger.versions())
        second = self.ledger.import_events(events)
        self.assertEqual((first["imported"], first["duplicated"]), (2, 0))
        self.assertEqual((second["imported"], second["duplicated"]), (0, 2))
        self.assertEqual(len(self.ledger.versions()), versions_after_first)
        self.assertEqual(self.ledger.lots()[0]["reserved"], 10)


class ConcurrencyTest(unittest.TestCase):
    def test_two_clerks_race_for_last_case_only_one_wins(self):
        with TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "race.db")
            seed = CommitmentLedger(db)
            seed.import_events([supply(1, "lot-x", quantity=6, case_size=6)])
            seed.close()

            outcomes: list[str] = []
            lock = threading.Lock()

            def clerk(did: str) -> None:
                ledger = CommitmentLedger(db)
                try:
                    ledger.claim(
                        {"demand_id": did, "channel": "direct", "store_id": did,
                         "region": "north", "sku": "pear", "quantity": 6},
                        event_id=f"evt-claim-{did}",
                        as_of="2026-09-22T09:00:00+08:00",
                    )
                    with lock:
                        outcomes.append("won")
                except InventoryShortage:
                    with lock:
                        outcomes.append("lost")
                finally:
                    ledger.close()

            t1 = threading.Thread(target=clerk, args=("s-1",))
            t2 = threading.Thread(target=clerk, args=("s-2",))
            t1.start(); t2.start()
            t1.join(); t2.join()

            # 恰好一个成功、一个失败，谁赢由先拿到写锁的一方决定。
            self.assertEqual(sorted(outcomes), ["lost", "won"])
            check = CommitmentLedger(db)
            lot = check.lots()[0]
            # 只锁定一件，绝不超卖；败者事务整体回滚。
            self.assertEqual((lot["available"], lot["reserved"]), (0, 6))
            committed = check.store.conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type='demand.submitted'"
            ).fetchone()[0]
            self.assertEqual(committed, 1)
            check.close()


class RecoveryTest(unittest.TestCase):
    def test_interrupted_recompute_resumes_from_checkpoint(self):
        with TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "race.db")
            ledger = CommitmentLedger(db)
            # 在“暂存落盘之后、发布之前”注入崩溃。
            ledger.before_publish = lambda run_id, version: (_ for _ in ()).throw(
                RuntimeError("服务在发布前中断")
            )
            with self.assertRaises(RuntimeError):
                ledger.import_events([
                    supply(1, "lot-1", quantity=10),
                    demand(2, "d-1", 22, 9, quantity=10),
                ])
            # 中断现场：事实已入库，版本尚未发布，任务保留失败原因。
            self.assertEqual(
                ledger.store.conn.execute("SELECT status FROM runs").fetchone()[0],
                "failed",
            )
            self.assertEqual(
                ledger.store.get_meta(ledger.store.conn, "current_version", "0"), "0"
            )
            ledger.close()

            # 新进程启动并恢复：不重做回放，直接把暂存版本发布出去。
            restarted = CommitmentLedger(db)
            completed = restarted.recover()
            self.assertEqual(completed, 1)
            self.assertEqual(
                restarted.store.get_meta(restarted.store.conn, "current_version"), "1"
            )
            self.assertEqual(restarted.commitment("d-1")["allocated"], 10)
            self.assertEqual(restarted.lots()[0]["reserved"], 10)
            self.assertEqual(
                restarted.store.conn.execute("SELECT attempts FROM runs").fetchone()[0], 2
            )
            restarted.close()


class TimelineTest(LedgerTestBase):
    def test_full_chain_propose_degrade_fulfill(self):
        self.ledger.import_events([
            supply(1, "lot-1", quantity=10),
            demand(2, "d-1", 22, 9, quantity=20),
        ])
        self.ledger.import_events([
            supply(3, "lot-2", quantity=20),
        ])
        self.ledger.import_events([
            ev("evt-4", "commitment.confirmed", "d-1", "2026-09-23T10:00:00+08:00"),
            ev("evt-5", "shipment.dispatched", "d-1", "2026-09-23T15:00:00+08:00",
               lot_id="lot-1", quantity=10),
            ev("evt-6", "shipment.dispatched", "d-1", "2026-09-23T16:00:00+08:00",
               lot_id="lot-2", quantity=10),
        ])
        view = self.ledger.commitment("d-1")
        self.assertEqual(view["status"], "fulfilled")
        kinds = [t["kind"] for t in view["timeline"]]
        self.assertEqual(kinds[0], "proposed")
        self.assertIn("reserved", kinds)
        self.assertEqual(kinds.count("fulfilled"), 2)
        self.assertEqual(kinds[-1], "fulfilled")
        # 降级理由出现在第一版配货事件中。
        first_reserve = next(t for t in view["timeline"] if t["kind"] == "reserved")
        self.assertIn("短欠 10", first_reserve["detail"])


if __name__ == "__main__":
    unittest.main()
