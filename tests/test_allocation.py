"""采购承诺与配货系统的端到端测试。"""
from __future__ import annotations

import threading
import unittest
from datetime import datetime, timedelta, timezone

from allocation import (
    CommitmentState,
    ConflictError,
    DemandKind,
    HoldExpiredError,
    Ledger,
    LedgerError,
)

TZ = timezone(timedelta(hours=8))


def at(y: int, m: int, d: int, h: int = 8, minute: int = 0) -> datetime:
    return datetime(y, m, d, h, minute, tzinfo=TZ)


def need(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, 0, 0, tzinfo=TZ)


class LedgerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # 测试场景跨多天，TTL 设 30 天；到期类用例相对 expires_at 显式推算
        self.ledger = Ledger(":memory:", hold_ttl_seconds=30 * 24 * 3600)

    def tearDown(self) -> None:
        self.ledger.close()

    # ------------------------------------------------------------ 基础口径

    def test_available_reserved_fulfilled_always_sums_to_supply(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 10, at(2026, 9, 21),
                                    expiry_date="2026-10-10")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 30,
                                  "store-080", need(2026, 9, 28))
        view = self.ledger.availability("pear")
        self.assertEqual((view["available"], view["reserved"], view["fulfilled"]), (70, 30, 0))
        self.assertEqual(view["total_supply"], 100)

        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.ledger.confirm_commitment(cid, at(2026, 9, 23, 10))
        view = self.ledger.availability("pear")
        self.assertEqual((view["available"], view["confirmed_open"], view["reserved"]),
                         (70, 30, 0))

        self.ledger.dispatch(cid, [("lot-1", 30)], at(2026, 9, 24, 9))
        view = self.ledger.availability("pear")
        self.assertEqual((view["available"], view["confirmed_open"], view["fulfilled"]),
                         (70, 0, 30))
        self.assertEqual(self.ledger.get_commitment(cid)["state"],
                         CommitmentState.FULFILLED.value)

    # ------------------------------------------------------------ 优先级

    def test_priority_contract_floor_then_normal_channels(self) -> None:
        """同一紧俏水果：合同锁量 > 区域保供底线 > 普通渠道（按需求日先后来）。"""
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        # 三家普通渠道先来
        self.ledger.submit_demand("d-direct", "pear", DemandKind.DIRECT.value, 40,
                                  "store-001", need(2026, 9, 28))
        self.ledger.submit_demand("d-franchisee", "pear", DemandKind.FRANCHISEE.value, 40,
                                  "store-002", need(2026, 9, 28))
        self.ledger.submit_demand("d-group", "pear", DemandKind.GROUP_BUY.value, 40,
                                  "store-003", need(2026, 9, 28))
        # 合同锁量与保供底线后到，仍要插队
        self.ledger.submit_demand("d-floor", "pear", DemandKind.REGIONAL_FLOOR.value, 30,
                                  "region-a", need(2026, 9, 29))
        self.ledger.submit_demand("d-contract", "pear", DemandKind.CONTRACT.value, 50,
                                  "contractor-x", need(2026, 9, 30))

        def qty(demand_id: str) -> int:
            return self.ledger.get_demand_status(demand_id)["commitments"][-1]["committed_qty"]

        self.assertEqual(qty("d-contract"), 50)
        self.assertEqual(qty("d-floor"), 30)
        # 只剩 20，普通渠道按需求日/提交顺序：直营先到先得，其余为 0
        self.assertEqual(qty("d-direct"), 20)
        self.assertEqual(qty("d-franchisee"), 0)
        self.assertEqual(qty("d-group"), 0)

        status = self.ledger.get_demand_status("d-franchisee")
        self.assertEqual(status["shortages"][-1]["reason"],
                         "higher_priority_preempted")

    def test_normal_channels_tie_broken_by_needed_date(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 50, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("later", "pear", DemandKind.GROUP_BUY.value, 40,
                                  "g", need(2026, 10, 5), occurred_at=at(2026, 9, 22, 9))
        self.ledger.submit_demand("earlier", "pear", DemandKind.DIRECT.value, 40,
                                  "s", need(2026, 9, 27), occurred_at=at(2026, 9, 22, 10))
        self.assertEqual(
            self.ledger.get_demand_status("earlier")["commitments"][-1]["committed_qty"], 40)
        self.assertEqual(
            self.ledger.get_demand_status("later")["commitments"][-1]["committed_qty"], 10)

    # ------------------------------------------------------------ FEFO / 保质期 / 整件

    def test_fefo_picks_earliest_expiry_that_covers_need_date(self) -> None:
        self.ledger.register_supply("lot-near", "pear", 50, 1, at(2026, 9, 20),
                                    expiry_date="2026-09-30")
        self.ledger.register_supply(
            "lot-far", "pear", 50, 1, at(2026, 9, 21), expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 29))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        holds = {h["lot_id"]: h["qty"] for h in
                 self.ledger.get_commitment(cid)["holds"]}
        self.assertEqual(holds, {"lot-near": 50, "lot-far": 10})

    def test_lot_expiring_before_need_date_is_skipped(self) -> None:
        self.ledger.register_supply("lot-near", "pear", 50, 1, at(2026, 9, 20),
                                    expiry_date="2026-09-25")
        self.ledger.register_supply("lot-far", "pear", 50, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28))
        status = self.ledger.get_demand_status("d1")
        self.assertEqual(status["commitments"][-1]["committed_qty"], 50)
        self.assertEqual(status["shortages"][-1]["reason"], "short_supply")

    def test_case_size_rounds_down(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 25, 12, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 25, "s1",
                                  need(2026, 9, 28))
        status = self.ledger.get_demand_status("d1")
        # 25 按 12 整件向下取整 = 24
        self.assertEqual(status["commitments"][-1]["committed_qty"], 24)
        self.assertEqual(status["shortages"][-1]["reason"], "case_size_rounded")

    # ------------------------------------------------------------ 版本化重算

    def test_supply_shrink_degrades_in_new_version(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.assertEqual(self.ledger.get_commitment(cid)["version"], 1)

        v2 = self.ledger.change_supply_qty("lot-1", 40, at(2026, 9, 22, 12))
        self.assertEqual(v2, 2)
        c = self.ledger.get_commitment(cid)
        self.assertEqual(c["state"], CommitmentState.DEGRADED.value)
        self.assertEqual(c["committed_qty"], 40)
        self.assertEqual(c["version"], 2)
        reasons = [h["reason"] for h in c["history"]]
        self.assertIn("allocated_initial", reasons)
        self.assertIn("supply_shrunk", reasons)

        versions = self.ledger.list_versions("pear")
        self.assertEqual([v["version"] for v in versions], [1, 2])
        self.assertEqual(versions[1]["reason"], "supply_shrunk")

    def test_confirmed_commitment_protected_on_shrink(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.ledger.confirm_commitment(cid, at(2026, 9, 22, 9))
        self.ledger.submit_demand("d2", "pear", DemandKind.FRANCHISEE.value, 40, "s2",
                                  need(2026, 9, 29))
        # 缩到 70：已确认的 60 不动，d2 从 40 降到 10
        self.ledger.change_supply_qty("lot-1", 70, at(2026, 9, 22, 15))
        self.assertEqual(self.ledger.get_commitment(cid)["committed_qty"], 60)
        self.assertEqual(self.ledger.get_commitment(cid)["state"],
                         CommitmentState.CONFIRMED.value)
        cid2 = self.ledger.get_demand_status("d2")["commitments"][-1]["commitment_id"]
        self.assertEqual(self.ledger.get_commitment(cid2)["committed_qty"], 10)

    def test_shipped_fact_cannot_roll_back(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.ledger.confirm_commitment(cid, at(2026, 9, 22, 9))
        self.ledger.dispatch(cid, [("lot-1", 60)], at(2026, 9, 23, 8))
        with self.assertRaisesRegex(LedgerError, "已出库事实不可倒回"):
            self.ledger.change_supply_qty("lot-1", 50, at(2026, 9, 23, 10))
        # 缩到 60（恰好等于已出库）允许，但池子为 0
        self.ledger.change_supply_qty("lot-1", 60, at(2026, 9, 23, 11))
        self.assertEqual(self.ledger.availability("pear")["available"], 0)

    def test_replenishment_fills_waiting_demand_in_new_version(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 50, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 80, "s1",
                                  need(2026, 9, 28))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.assertEqual(self.ledger.get_commitment(cid)["committed_qty"], 50)
        self.ledger.register_supply("lot-2", "pear", 50, 1, at(2026, 9, 22, 8),
                                    expiry_date="2026-10-25")
        c = self.ledger.get_commitment(cid)
        self.assertEqual(c["committed_qty"], 80)
        self.assertEqual(c["state"], CommitmentState.RESERVED.value)
        self.assertEqual(len(self.ledger.list_versions("pear")), 2)

    def test_customer_cancel_frees_and_backfills_queue(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 80, "s1",
                                  need(2026, 9, 28))
        self.ledger.submit_demand("d2", "pear", DemandKind.FRANCHISEE.value, 40, "s2",
                                  need(2026, 9, 29))
        cid1 = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.assertEqual(
            self.ledger.get_demand_status("d2")["commitments"][-1]["committed_qty"], 20)
        self.ledger.cancel_demand("d1", at(2026, 9, 22, 14))
        self.assertEqual(self.ledger.get_commitment(cid1)["state"],
                         CommitmentState.CANCELLED.value)
        cid2 = self.ledger.get_demand_status("d2")["commitments"][-1]["commitment_id"]
        self.assertEqual(self.ledger.get_commitment(cid2)["committed_qty"], 40)
        self.assertEqual(self.ledger.availability("pear")["available"], 60)

    def test_confirmed_demand_cannot_be_cancelled(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.ledger.confirm_commitment(cid, at(2026, 9, 22, 9))
        with self.assertRaises(ConflictError):
            self.ledger.cancel_demand("d1", at(2026, 9, 22, 10))

    # ------------------------------------------------------------ 预占到期

    def test_hold_expiry_blocks_confirm_and_reallocates(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28), occurred_at=at(2026, 9, 21, 9))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        expires = self.ledger.get_commitment(cid)["expires_at"]

        with self.assertRaises(HoldExpiredError):
            self.ledger.confirm_commitment(cid, _plus(expires, minutes=1))
        expiry_now = _plus(expires, minutes=1)
        # 尚未跑过期扫描时，查询口径也不再把它算作锁定
        self.assertEqual(self.ledger.availability("pear", as_of=expiry_now)["expired_holds"], 60)
        self.assertEqual(self.ledger.availability("pear", as_of=expiry_now)["reserved"], 0)

        # 排队需求触发新版本，到期量被释放
        self.ledger.submit_demand("d2", "pear", DemandKind.FRANCHISEE.value, 100, "s2",
                                  need(2026, 9, 29),
                                  occurred_at=_plus(expires, minutes=5))
        self.assertEqual(self.ledger.get_commitment(cid)["state"],
                         CommitmentState.CANCELLED.value)
        self.assertEqual(
            self.ledger.get_demand_status("d2")["commitments"][-1]["committed_qty"], 100)

    def test_expire_holds_scan_releases_in_batch(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28), occurred_at=at(2026, 9, 21, 9))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        expires = self.ledger.get_commitment(cid)["expires_at"]
        touched = self.ledger.expire_holds(_plus(expires, minutes=1))
        self.assertIn("pear", touched)
        self.assertEqual(self.ledger.get_commitment(cid)["state"],
                         CommitmentState.CANCELLED.value)
        self.assertEqual(self.ledger.availability("pear")["available"], 100)

    # ------------------------------------------------------------ 完整理由链

    def test_commitment_history_covers_proposal_degrade_fulfilment(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.ledger.change_supply_qty("lot-1", 40, at(2026, 9, 22, 8))
        self.ledger.confirm_commitment(cid, at(2026, 9, 22, 10))
        self.ledger.dispatch(cid, [("lot-1", 40)], at(2026, 9, 23, 8))

        history = self.ledger.get_commitment(cid)["history"]
        chain = [(h["event_type"], h["reason"]) for h in history]
        self.assertEqual(chain[0], ("demand.submitted", "demand_submitted"))
        self.assertIn(("commitment.reserved", "allocated_initial"), chain)
        degrade = next(h for h in history if h["event_type"] == "commitment.degraded")
        self.assertEqual(degrade["from_qty"], 60)
        self.assertEqual(degrade["to_qty"], 40)
        self.assertIn(("commitment.confirmed", "confirmed_by_buyer"), chain)
        self.assertEqual(chain[-1], ("shipment.dispatched", "shipped"))

    # ------------------------------------------------------------ 幂等

    def test_duplicate_event_import_does_not_double_deduct(self) -> None:
        events = [
            {"event_id": "imp-1", "event_type": "supply.registered",
             "aggregate_id": "lot-1", "occurred_at": at(2026, 9, 21).isoformat(),
             "payload": {"sku": "pear", "qty": 100, "case_size": 1,
                         "expiry_date": "2026-10-20"}},
            {"event_id": "imp-2", "event_type": "demand.submitted",
             "aggregate_id": "d1", "occurred_at": at(2026, 9, 22, 9).isoformat(),
             "payload": {"sku": "pear", "kind": "direct", "requested_qty": 60,
                         "store_id": "s1", "needed_at": need(2026, 9, 28).isoformat()}},
        ]
        first = self.ledger.append_events(events)
        second = self.ledger.append_events(events)
        self.assertEqual(first, {"inserted": 2, "skipped": 0})
        self.assertEqual(second, {"inserted": 0, "skipped": 2})
        self.assertEqual(self.ledger.availability("pear")["reserved"], 60)
        # supply + demand + 预占 + 分配版本
        self.assertEqual(len(self.ledger.list_events()), 4)

    def test_idempotent_command_calls(self) -> None:
        e1 = self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                         expiry_date="2026-10-20", event_id="evt-x-1")
        # 同样的命令带同样 event_id 再来一次：批次已存在，报业务错而非重复扣量
        with self.assertRaises(LedgerError):
            self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                        expiry_date="2026-10-20", event_id="evt-x-1")
        self.assertEqual(e1, "evt-x-1")

    # ------------------------------------------------------------ 并发

    def test_two_buyers_race_for_last_unit_only_one_wins(self) -> None:
        """两个人同时确认同一份最后库存（同一承诺），只能有一个成功。"""
        self.ledger.register_supply("lot-1", "pear", 10, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 10, "s1",
                                  need(2026, 9, 28))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]

        outcomes: list[str] = []
        barrier = threading.Barrier(2)

        def confirm(buyer: str) -> None:
            barrier.wait()
            try:
                self.ledger.confirm_commitment(cid, at(2026, 9, 22, 9), buyer=buyer)
                outcomes.append(f"{buyer}:ok")
            except ConflictError:
                outcomes.append(f"{buyer}:conflict")

        t1 = threading.Thread(target=confirm, args=("buyer-a",))
        t2 = threading.Thread(target=confirm, args=("buyer-b",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(sorted(outcomes), ["buyer-a:conflict", "buyer-b:ok"])
        self.assertEqual(
            self.ledger.get_commitment(cid)["state"], CommitmentState.CONFIRMED.value)

    def test_concurrent_demand_submissions_never_overcommit(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        results: list[int] = []
        barrier = threading.Barrier(8)

        def submit(idx: int) -> None:
            barrier.wait()
            self.ledger.submit_demand(
                f"d{idx}", "pear", DemandKind.DIRECT.value, 20, f"s{idx}",
                need(2026, 9, 28), occurred_at=at(2026, 9, 22, 9, idx))

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for i in range(8):
            commits = self.ledger.get_demand_status(f"d{i}")["commitments"]
            results.append(commits[-1]["committed_qty"] if commits else 0)
        self.assertEqual(sum(results), 100)
        view = self.ledger.availability("pear")
        self.assertEqual(view["reserved"] + view["available"], 100)

    def test_optimistic_version_guard(self) -> None:
        self.ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                    expiry_date="2026-10-20")
        self.ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                  need(2026, 9, 28))
        cid = self.ledger.get_demand_status("d1")["commitments"][-1]["commitment_id"]
        self.ledger.change_supply_qty("lot-1", 50, at(2026, 9, 22, 8))  # 版本变 2
        with self.assertRaisesRegex(ConflictError, "版本已变化"):
            self.ledger.confirm_commitment(cid, at(2026, 9, 22, 9), expected_version=1)
        self.ledger.confirm_commitment(cid, at(2026, 9, 22, 10), expected_version=2)

    # ------------------------------------------------------------ 崩溃恢复

    def test_interrupted_recalc_job_is_resumed_after_restart(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ledger.db"
            ledger = Ledger(db)
            ledger.register_supply("lot-1", "pear", 100, 1, at(2026, 9, 21),
                                   expiry_date="2026-10-20")
            ledger.submit_demand("d1", "pear", DemandKind.DIRECT.value, 60, "s1",
                                 need(2026, 9, 28))
            # 模拟崩溃现场：缩量事件已落账，但重算还没跑
            from allocation import EventType
            with ledger._conn:
                ledger._conn.execute("BEGIN IMMEDIATE")
                ledger._insert_event(
                    ledger._conn, EventType.SUPPLY_QUANTITY_CHANGED,
                    "lot-1", at(2026, 9, 22, 8).isoformat(),
                    {"new_qty": 40, "old_qty": 100, "reason": "档口复核"},
                    event_id="evt-crash-shrink",
                )
            job_id = ledger.enqueue_recalc("pear", "supply_shrunk")
            with ledger._conn:
                ledger._conn.execute(
                    "UPDATE recalc_jobs SET state='running', claimed_at=? WHERE job_id=?",
                    (at(2020, 1, 1).isoformat(), job_id))
            ledger.close()

            # 服务恢复：新实例接管中断的作业，降级在新版本中落定
            recovered = Ledger(db)
            done = recovered.recover_interrupted()
            self.assertEqual(len(done), 1)
            self.assertEqual(done[0]["job_id"], job_id)
            self.assertEqual(recovered.job_status(job_id)["state"], "done")
            cid = recovered.get_demand_status("d1")["commitments"][-1]["commitment_id"]
            self.assertEqual(recovered.get_commitment(cid)["committed_qty"], 40)
            self.assertEqual(recovered.get_commitment(cid)["version"], 2)

            # 再次恢复不会重复落账（确定性事件 ID）
            again = recovered.recover_interrupted()
            self.assertEqual(again, [])
            self.assertEqual(len(recovered.list_versions("pear")), 2)
            recovered.close()

    # ------------------------------------------------------------ 端到端

    def test_festival_scenario_end_to_end(self) -> None:
        """复现节前事故：同一批货同时进三家承诺单，系统自动给出唯一可行分配。"""
        self.ledger.register_supply("lot-pear-01", "pear", 100, 5, at(2026, 9, 21),
                                    expiry_date="2026-10-05")
        # 三家在第一轮配货结果公布后各自拿着"承诺单"
        self.ledger.submit_demand("d-direct", "pear", DemandKind.DIRECT.value, 80,
                                  "store-080", need(2026, 9, 27))
        self.ledger.submit_demand("d-franchisee", "pear", DemandKind.FRANCHISEE.value, 80,
                                  "store-107", need(2026, 9, 27))
        self.ledger.submit_demand("d-group", "pear", DemandKind.GROUP_BUY.value, 80,
                                  "acme", need(2026, 9, 27))
        view = self.ledger.availability("pear")
        self.assertLessEqual(view["reserved"], 100)
        self.assertEqual(view["available"] + view["reserved"], 100)

        # 档口缩量到 60：产生 v2，只有优先级最高的直营（同优先级需求日相同→先提交）拿到
        self.ledger.change_supply_qty("lot-pear-01", 60, at(2026, 9, 22, 11))
        cid_d = self.ledger.get_demand_status("d-direct")["commitments"][-1]["commitment_id"]
        self.assertEqual(self.ledger.get_commitment(cid_d)["committed_qty"], 60)

        # 直营确认：此后它的 60 是受保护事实，重算不能再动
        self.ledger.confirm_commitment(cid_d, at(2026, 9, 22, 14))

        # 补货 60 到场：v3 按提交顺序给加盟商；团购仍无货可拿
        self.ledger.register_supply("lot-pear-02", "pear", 60, 5, at(2026, 9, 23, 7),
                                    expiry_date="2026-10-08")
        cid_f = self.ledger.get_demand_status("d-franchisee")["commitments"][-1]["commitment_id"]
        self.assertEqual(self.ledger.get_commitment(cid_f)["committed_qty"], 60)
        group_commits = self.ledger.get_demand_status("d-group")["commitments"]
        self.assertTrue(all(c["committed_qty"] == 0 for c in group_commits))

        # 团购客户撤单：账实一致，已确认的 60 + 加盟商锁定 60
        self.ledger.cancel_demand("d-group", at(2026, 9, 23, 12))
        view = self.ledger.availability("pear")
        self.assertEqual(view["total_supply"], 120)
        self.assertEqual(view["confirmed_open"], 60)
        self.assertEqual(view["reserved"], 60)
        self.assertEqual(view["available"], 0)

        # 直营出库 60 —— 此后任何版本都改不了这 60 的事实
        self.ledger.dispatch(cid_d, [("lot-pear-01", 60)], at(2026, 9, 24, 6))
        self.assertEqual(self.ledger.availability("pear")["fulfilled"], 60)


def _plus(ts: str, **delta) -> datetime:
    return datetime.fromisoformat(ts) + timedelta(**delta)


if __name__ == "__main__":
    unittest.main()
