from dataclasses import dataclass
from typing import NewType, TypeAlias

_Point = NewType("_Point", str)
_SQLPoint: TypeAlias = _Point


@dataclass
class SQLPoint:
    point: _SQLPoint
    equal: bool
    greater: bool
    less: bool

    @property
    def greater_less(self) -> str:
        res: str = ""
        if self.greater is True:
            res += ">"
        elif self.less is True:
            res += "<"

        if self.equal is True:
            res += "="

        return res
