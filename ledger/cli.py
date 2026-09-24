"""命令行入口：事件导入、重算恢复与查询。

用法示例::

    python3 -m ledger.cli init --db ledger.db
    python3 -m ledger.cli import --db ledger.db examples/holiday_events.json
    python3 -m ledger.cli lots --db ledger.db
    python3 -m ledger.cli commitment --db ledger.db demand-001
    python3 -m ledger.cli versions --db ledger.db
    python3 -m ledger.cli recover --db ledger.db
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .engine import CommitmentLedger, DomainError


def _print(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ledger", description="采购承诺与配货系统")
    parser.add_argument("--db", default="ledger.db", help="SQLite 数据库文件（默认 ledger.db）")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="初始化数据库（建表）")

    p_import = sub.add_parser("import", help="幂等导入事件 JSON 数组")
    p_import.add_argument("file", help="事件文件路径")

    p_tick = sub.add_parser("tick", help="时间推进：释放已到期预占")
    p_tick.add_argument("--as-of", default=None, help="时点（ISO 8601，默认当前时间）")

    sub.add_parser("recover", help="服务恢复：续算中断的重算")
    sub.add_parser("lots", help="查看各批次可售/锁定/已履约三本账")

    p_commit = sub.add_parser("commitment", help="查看单笔承诺状态与完整理由链")
    p_commit.add_argument("demand_id")

    sub.add_parser("versions", help="查看全部配货版本")

    args = parser.parse_args(argv)
    db_path = ":memory:" if args.command == "init" and args.db == ":memory:" else args.db
    ledger = CommitmentLedger(db_path)
    try:
        if args.command == "init":
            print(f"数据库已就绪：{db_path}")
        elif args.command == "import":
            data = json.loads(Path(args.file).read_text(encoding="utf-8"))
            _print(ledger.import_events(data))
        elif args.command == "tick":
            _print({"versions_produced": ledger.tick(args.as_of)})
        elif args.command == "recover":
            _print({"runs_completed": ledger.recover()})
        elif args.command == "lots":
            _print(ledger.lots())
        elif args.command == "commitment":
            _print(ledger.commitment(args.demand_id))
        elif args.command == "versions":
            _print(ledger.versions())
    except DomainError as exc:
        print(f"领域规则冲突：{exc}", file=sys.stderr)
        return 2
    finally:
        ledger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
