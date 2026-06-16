import re
from typing import List, Set, Optional
from datetime import datetime, timedelta


class CronParser:
    _FIELD_RANGES = [
        (0, 59),
        (0, 23),
        (1, 31),
        (1, 12),
        (0, 6),
    ]
    _FIELD_NAMES = ["minute", "hour", "day", "month", "weekday"]

    def __init__(self, expression: str):
        parts = expression.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {expression}, expected 5 fields")
        self.expression = expression
        self.fields: List[Set[int]] = []
        for i, part in enumerate(parts):
            self.fields.append(self._parse_field(part, *self._FIELD_RANGES[i]))

    @classmethod
    def _parse_field(cls, expr: str, min_val: int, max_val: int) -> Set[int]:
        values: Set[int] = set()
        for part in expr.split(","):
            values.update(cls._parse_part(part, min_val, max_val))
        return values

    @classmethod
    def _parse_part(cls, part: str, min_val: int, max_val: int) -> Set[int]:
        step_match = re.match(r"^(.+)/(\d+)$", part)
        if step_match:
            range_expr = step_match.group(1)
            step = int(step_match.group(2))
            rng = cls._parse_range(range_expr, min_val, max_val)
            return set(range(min(rng), max(rng) + 1, step))

        if part == "*":
            return set(range(min_val, max_val + 1))

        return cls._parse_range(part, min_val, max_val)

    @classmethod
    def _parse_range(cls, expr: str, min_val: int, max_val: int) -> Set[int]:
        if "-" in expr:
            start_str, end_str = expr.split("-", 1)
            start = int(start_str)
            end = int(end_str)
        else:
            val = int(expr)
            start = end = val
        if start < min_val or end > max_val or start > end:
            raise ValueError(f"Value out of range [{min_val}, {max_val}]: {expr}")
        return set(range(start, end + 1))

    def matches(self, dt: datetime) -> bool:
        return (
            dt.minute in self.fields[0]
            and dt.hour in self.fields[1]
            and dt.day in self.fields[2]
            and dt.month in self.fields[3]
            and dt.weekday() in self.fields[4]
        )

    def next_run_after(self, after: Optional[datetime] = None) -> datetime:
        if after is None:
            after = datetime.now()
        dt = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(525600):
            if self.matches(dt):
                return dt
            dt += timedelta(minutes=1)
        raise RuntimeError("Cannot find next run time within a year")
