from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List


class TransType(Enum):
    READ_ONLY = 0,
    READ_WRITE = 1


class TransState(Enum):
    RUNNING = 1,
    ABORTED = 2,
    BLOCKED = 3,
    COMITTED = 4


@dataclass
class Transaction:
    id_trans: str
    startTime: Any
    transactionType: TransType
    transactionState: TransState
    sitesAccessed: List = field(default_factory=list)
