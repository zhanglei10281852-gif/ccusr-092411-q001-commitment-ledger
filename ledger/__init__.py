"""采购承诺与配货系统。

事件溯源 + 版本化重算的核心实现：

- :mod:`ledger.events`  事件定义与（反）序列化
- :mod:`ledger.store`   SQLite 事件库、重算任务表
- :mod:`ledger.engine`  领域规则、分配算法、版本重算与查询视图
- :mod:`ledger.cli`     命令行入口
"""

from .engine import CommitmentLedger
from .events import Event

__all__ = ["CommitmentLedger", "Event"]
