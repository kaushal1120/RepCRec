from dataclasses import dataclass, field
from typing import Any, List

from lock_type import LockType


@dataclass
class Lock:
    lock_type: LockType
    lock_queue: List = field(default_factory=list)
    lock_owners: List = field(default_factory=list)
