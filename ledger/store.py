"""SQLite 存储层。

只负责持久化，不含业务规则：

- ``events``   不可变事件日志（外部事实 ``published=1``；重算产生的事件
  先以 ``published=0`` 落库，发布时才挂上版本号，因此重算中断不会污染
  已发布视图）。
- ``runs``     每次重算一个任务，带阶段检查点，服务重启后可继续。
- ``allocations`` 每个已发布版本的最终配货方案（承诺 × 批次 × 数量）。
- ``meta``     当前版本指针等。

所有写事务都以 ``BEGIN IMMEDIATE`` 开始，配合 busy timeout，把跨进程/
跨线程的并发写串行化——“抢最后一份库存”时第二个写者必须在第一个
提交后基于新事实重新判断。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from .events import Event

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL UNIQUE,
    event_type   TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    occurred_at  TEXT NOT NULL,
    payload      TEXT NOT NULL DEFAULT '{}',
    run_id       TEXT,
    version      INTEGER,
    published    INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    reason        TEXT NOT NULL DEFAULT '',
    as_of         TEXT NOT NULL,
    status        TEXT NOT NULL,              -- pending | running | done | failed
    attempts      INTEGER NOT NULL DEFAULT 0,
    version       INTEGER,
    input_seq     INTEGER NOT NULL DEFAULT 0,
    checkpoint    TEXT NOT NULL DEFAULT '{}',
    error         TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS allocations (
    version       INTEGER NOT NULL,
    commitment_id TEXT NOT NULL,
    lot_id        TEXT NOT NULL,
    quantity      INTEGER NOT NULL,
    PRIMARY KEY (version, commitment_id, lot_id)
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Store:
    """文件或内存 SQLite 存储。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """立即取写锁的事务，提交或回滚成对出现。"""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    # ---------------------------------------------------------------- events

    def existing_event_ids(self, conn: sqlite3.Connection, ids: list[str]) -> set[str]:
        if not ids:
            return set()
        marks = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"SELECT event_id FROM events WHERE event_id IN ({marks})", ids
        ).fetchall()
        return {row[0] for row in rows}

    def insert_event(
        self,
        conn: sqlite3.Connection,
        event: Event,
        *,
        run_id: str | None = None,
        published: bool = True,
    ) -> int:
        """插入一条事件，返回自增 seq。event_id 冲突由调用方预先排除。"""
        cursor = conn.execute(
            "INSERT INTO events(event_id, event_type, aggregate_id, occurred_at, "
            "payload, run_id, version, published) VALUES (?,?,?,?,?,?,?,?)",
            event.to_row() + (run_id, None, 1 if published else 0),
        )
        return int(cursor.lastrowid)

    def load_events(
        self,
        conn: sqlite3.Connection,
        *,
        published_only: bool = False,
        run_id: str | None = None,
        aggregate_id: str | None = None,
        upto_seq: int | None = None,
    ) -> list[Event]:
        sql = (
            "SELECT seq, event_id, event_type, aggregate_id, occurred_at, payload, "
            "run_id, version FROM events WHERE 1=1"
        )
        args: list[Any] = []
        if published_only:
            sql += " AND published=1"
        if run_id is not None:
            sql += " AND run_id=?"
            args.append(run_id)
        if aggregate_id is not None:
            sql += " AND aggregate_id=?"
            args.append(aggregate_id)
        if upto_seq is not None:
            sql += " AND seq<=?"
            args.append(upto_seq)
        sql += " ORDER BY seq"
        return [Event.from_row(tuple(row)) for row in conn.execute(sql, args)]

    def external_events(self, conn: sqlite3.Connection, upto_seq: int | None = None) -> list[Event]:
        """外部事实事件（run_id 为空），是重算回放的唯一输入。"""
        sql = (
            "SELECT seq, event_id, event_type, aggregate_id, occurred_at, payload, "
            "run_id, version FROM events WHERE run_id IS NULL"
        )
        args: list[Any] = []
        if upto_seq is not None:
            sql += " AND seq<=?"
            args.append(upto_seq)
        sql += " ORDER BY seq"
        return [Event.from_row(tuple(row)) for row in conn.execute(sql, args)]

    def has_published_event(
        self, conn: sqlite3.Connection, event_type: str, aggregate_id: str
    ) -> bool:
        row = conn.execute(
            "SELECT 1 FROM events WHERE event_type=? AND aggregate_id=? AND published=1 LIMIT 1",
            (event_type, aggregate_id),
        ).fetchone()
        return row is not None

    def publish_run(
        self, conn: sqlite3.Connection, run_id: str, version: int
    ) -> None:
        conn.execute(
            "UPDATE events SET published=1, version=? WHERE run_id=?",
            (version, run_id),
        )

    # ------------------------------------------------------------------ runs

    def create_run(
        self, conn: sqlite3.Connection, run_id: str, as_of: datetime, reason: str
    ) -> None:
        now = datetime.now().astimezone().isoformat()
        conn.execute(
            "INSERT INTO runs(run_id, reason, as_of, status, attempts, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (run_id, reason, as_of.isoformat(), "pending", 0, now, now),
        )

    def get_run(self, conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
        return conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()

    def list_runs(self, conn: sqlite3.Connection, status: str | None = None) -> list[sqlite3.Row]:
        if status is None:
            sql = "SELECT * FROM runs ORDER BY rowid"
            rows = conn.execute(sql).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM runs WHERE status=? ORDER BY rowid", (status,)
            ).fetchall()
        return list(rows)

    def update_run(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        *,
        status: str | None = None,
        checkpoint: dict[str, Any] | None = None,
        error: str | None = None,
        version: int | None = None,
        input_seq: int | None = None,
        bump_attempt: bool = False,
    ) -> None:
        fields = ["updated_at=?"]
        args: list[Any] = [datetime.now().astimezone().isoformat()]
        if status is not None:
            fields.append("status=?")
            args.append(status)
        if checkpoint is not None:
            fields.append("checkpoint=?")
            args.append(json.dumps(checkpoint, ensure_ascii=False))
        if error is not None:
            fields.append("error=?")
            args.append(error)
        if version is not None:
            fields.append("version=?")
            args.append(version)
        if input_seq is not None:
            fields.append("input_seq=?")
            args.append(input_seq)
        if bump_attempt:
            fields.append("attempts=attempts+1")
        args.append(run_id)
        conn.execute(f"UPDATE runs SET {', '.join(fields)} WHERE run_id=?", args)

    # ----------------------------------------------------------- allocations

    def replace_allocations(
        self, conn: sqlite3.Connection, version: int, rows: list[tuple[str, str, int]]
    ) -> None:
        conn.execute("DELETE FROM allocations WHERE version=?", (version,))
        conn.executemany(
            "INSERT INTO allocations(version, commitment_id, lot_id, quantity) VALUES (?,?,?,?)",
            [(version, c, lot, q) for c, lot, q in rows if q > 0],
        )

    def allocations_for_version(
        self, conn: sqlite3.Connection, version: int
    ) -> dict[str, dict[str, int]]:
        result: dict[str, dict[str, int]] = {}
        rows = conn.execute(
            "SELECT commitment_id, lot_id, quantity FROM allocations WHERE version=?",
            (version,),
        ).fetchall()
        for commitment_id, lot_id, quantity in rows:
            result.setdefault(commitment_id, {})[lot_id] = quantity
        return result

    # ------------------------------------------------------------------ meta

    def get_meta(self, conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return default if row is None else row[0]

    def set_meta(self, conn: sqlite3.Connection, key: str, value: str) -> None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def max_version(self, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM events").fetchone()
        return int(row[0])
