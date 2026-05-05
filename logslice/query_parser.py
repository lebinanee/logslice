"""Query parser for logslice unified query syntax.

Supports expressions like:
  level=error
  level=error AND service=api
  message~="timeout" OR level=warn
  timestamp>=2024-01-01T00:00:00Z
"""

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Union


class Operator(Enum):
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    CONTAINS = "~="


class LogicalOp(Enum):
    AND = auto()
    OR = auto()


@dataclass
class Condition:
    field: str
    operator: Operator
    value: str

    def match(self, record: dict) -> bool:
        field_value = record.get(self.field)
        if field_value is None:
            return False
        fv = str(field_value)
        v = self.value
        if self.operator == Operator.EQ:
            return fv == v
        elif self.operator == Operator.NEQ:
            return fv != v
        elif self.operator == Operator.CONTAINS:
            return v.lower() in fv.lower()
        elif self.operator == Operator.GTE:
            return fv >= v
        elif self.operator == Operator.GT:
            return fv > v
        elif self.operator == Operator.LTE:
            return fv <= v
        elif self.operator == Operator.LT:
            return fv < v
        return False


@dataclass
class Query:
    conditions: List[Union[Condition, "Query"]]
    operators: List[LogicalOp]

    def match(self, record: dict) -> bool:
        if not self.conditions:
            return True
        result = self.conditions[0].match(record)
        for op, cond in zip(self.operators, self.conditions[1:]):
            if op == LogicalOp.AND:
                result = result and cond.match(record)
            elif op == LogicalOp.OR:
                result = result or cond.match(record)
        return result


_CONDITION_RE = re.compile(
    r'([\w\.]+)\s*(>=|<=|!=|~=|>|<|=)\s*"?([^"\s]+)"?'
)
_LOGICAL_RE = re.compile(r'\b(AND|OR)\b')


def parse_query(query_str: str) -> Optional[Query]:
    """Parse a query string into a Query object."""
    if not query_str or not query_str.strip():
        return Query(conditions=[], operators=[])

    tokens = _LOGICAL_RE.split(query_str.strip())
    conditions: List[Condition] = []
    operators: List[LogicalOp] = []

    for token in tokens:
        token = token.strip()
        if token == "AND":
            operators.append(LogicalOp.AND)
        elif token == "OR":
            operators.append(LogicalOp.OR)
        else:
            m = _CONDITION_RE.match(token)
            if m:
                field, op_str, value = m.group(1), m.group(2), m.group(3)
                conditions.append(Condition(field=field, operator=Operator(op_str), value=value))
            elif token:
                raise ValueError(f"Invalid query token: '{token}'")

    if len(operators) != max(0, len(conditions) - 1):
        raise ValueError("Mismatched conditions and logical operators")

    return Query(conditions=conditions, operators=operators)
