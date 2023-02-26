import datetime
import re

from asyncio_task_queues.types import List, Optional, Set, Tuple

_re_cron_step = re.compile(r"^(.*)/(\d+)$")
_re_cron_range = re.compile(r"^(\d+)-(\d+)$")
_re_cron_wildcard = re.compile(r"^\*$")
_re_cron_digit = re.compile(r"^(\d+)$")


class Schedule:
    seconds: Set[int]
    minutes: Set[int]
    hours: Set[int]
    days: Set[int]
    months: Set[int]
    weekdays: Set[int]

    type: str
    value: str

    def __init__(self, *, type: str, value: str):
        self.type = type
        self.value = value

        self.seconds = set()
        self.minutes = set()
        self.hours = set()
        self.days = set()
        self.months = set()
        self.weekdays = set()

        self.build()

    def _build_cron(self):
        def error(detail: Optional[str] = None) -> ValueError:
            message = f"invalid cron expression: {self.value}"
            if detail is not None:
                message += f" ({detail})"
            return ValueError(message)

        parts = self.value.split(" ")
        num_parts = len(parts)
        valid_num_parts = {5, 6}
        if not num_parts in valid_num_parts:
            raise error(f"invalid num parts: {num_parts} != {valid_num_parts}")

        meta: List[Tuple[Set[int], int, int]] = [
            (self.seconds, 0, 59),
            (self.minutes, 0, 59),
            (self.hours, 0, 59),
            (self.days, 1, 31),
            (self.months, 1, 12),
            (self.weekdays, 0, 6),
        ][-num_parts:]

        for part, (values, min, max) in zip(parts, meta):
            for subpart in part.split(","):
                step = 1

                step_match = _re_cron_step.match(subpart)
                if step_match:
                    subpart = step_match.group(1)
                    step = int(step_match.group(2))

                range_match = _re_cron_range.match(subpart)
                wildcard_match = _re_cron_wildcard.match(subpart)
                digit_match = _re_cron_digit.match(subpart)
                if range_match:
                    start = int(range_match.group(1))
                    stop = int(range_match.group(2))
                elif wildcard_match:
                    start = min
                    stop = max
                elif digit_match:
                    start = stop = int(digit_match.group(1))
                else:
                    raise error(f"{subpart} cannot be parsed")

                if step <= 0:
                    raise error(f"step {step} < 0")
                if start < min:
                    raise error(f"start {start} < {min}")
                if stop > max:
                    raise error(f"stop {stop} > {max}")

                for value in range(start, stop + 1, step):
                    values.add(value)
        if not self.seconds:
            self.seconds.add(0)

    def build(self):
        if self.type == "cron":
            self._build_cron()
        else:
            raise NotImplementedError(self.type)

    def next_datetime(self, prev: datetime.datetime) -> datetime.datetime:
        curr = prev + datetime.timedelta(seconds=1)

        def weekday() -> int:
            value = curr.weekday() + 1
            while value >= 7:
                value -= 7
            return value

        while curr.second not in self.seconds:
            curr = curr + datetime.timedelta(seconds=1)
        while curr.minute not in self.minutes:
            curr = curr + datetime.timedelta(minutes=1)
        while curr.hour not in self.hours:
            curr = curr + datetime.timedelta(hours=1)
        while not all(
            [
                curr.day in self.days,
                curr.month in self.months,
                weekday() in self.weekdays,
            ]
        ):
            curr = curr + datetime.timedelta(days=1)

        return curr

    @classmethod
    def cron(cls, expression: str) -> "Schedule":
        return cls(type="cron", value=expression)
