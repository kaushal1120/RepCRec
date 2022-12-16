from dataclasses import dataclass

READ = "READ"
WRITE = "WRITE"


@dataclass
class Operation:
    op: str
    id_trans: str
    id_val: str
    value: str = ''
