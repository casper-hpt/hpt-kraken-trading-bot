from __future__ import annotations

import time
import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class Scheduler:
    interval_minutes: int = 15
    align_to_boundary: bool = True
    boundary_offset_seconds: int = 5  # add a small delay after the boundary

    def sleep_until_next_tick(self) -> None:
        if not self.align_to_boundary:
            time.sleep(self.interval_minutes * 60)
            return

        now = dt.datetime.now()
        # Next boundary at minute % interval == 0
        minute = now.minute
        next_minute_block = (minute // self.interval_minutes + 1) * self.interval_minutes
        add_hours = 0
        if next_minute_block >= 60:
            next_minute_block -= 60
            add_hours = 1

        nxt = now.replace(second=0, microsecond=0, minute=next_minute_block) + dt.timedelta(hours=add_hours)
        nxt = nxt + dt.timedelta(seconds=self.boundary_offset_seconds)
        sleep_s = (nxt - now).total_seconds()
        if sleep_s < 0:
            sleep_s = self.interval_minutes * 60
        time.sleep(sleep_s)
